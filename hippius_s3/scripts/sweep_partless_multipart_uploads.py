#!/usr/bin/env python3
"""One-time sweep: delete incomplete multipart uploads that never received a part.

WHY THIS EXISTS

The mpu-reaper (`list_abandoned_versions.sql`) joins `multipart_uploads` to `parts` with an
INNER join, so an upload with zero parts can never be selected and therefore never reaped.
Nothing else deletes them either — the only other paths are `abort_multipart_upload` and the
same-key DELETE in `delete_object_endpoint`. So they accumulate forever.

Measured on prod 2026-07-23: **874,823 partless incomplete uploads older than 48h, out of
904,246 incomplete total — 97% of the backlog.**

That is not just untidy. The reaper's candidate scan walks `idx_multipart_uploads_initiated_at`
oldest-first, and these are the oldest rows, so every cycle pays to skip past all of them. It is
why the rewritten query costs ~8s instead of ~0.5s, and the cost grows linearly with a
population that has no other GC. Clearing it is what makes that query cheap and keeps it cheap.

WHY DELETING IS SAFE

A partless upload is an MPU header with no data behind it: no `parts` row, therefore no
`part_chunks`, no chunk on the SSD or in any backend. Deleting the header is exactly what
`AbortMultipartUpload` does, and the only FK pointing at `multipart_uploads` is
`parts.upload_id ON DELETE CASCADE` — which has nothing to cascade here. The age gate is the
same `stale_seconds` policy the reaper already applies to abandoned uploads, so a client that
initiated an MPU and has not uploaded a part in 48h is treated identically to today.

OPERATIONAL SHAPE

Deliberately NOT one big DELETE. This script exists because of an incident where a single
96-minute statement pinned the xmin horizon and stopped VACUUM database-wide; repeating that
sin while cleaning up after it would be poor form. So: bounded batches, each its own
autocommit statement, a hard `statement_timeout`, and a pause between batches so autovacuum and
normal traffic get the primary back.

    # look first — prints counts and a sample, touches nothing
    python -m hippius_s3.scripts.sweep_partless_multipart_uploads

    # then, for real
    python -m hippius_s3.scripts.sweep_partless_multipart_uploads --yes

Resumable: it re-queries each batch, so an interrupted run is continued simply by running it
again.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import asyncpg  # noqa: E402
import redis.asyncio as async_redis  # noqa: E402

from hippius_s3.config import get_config  # noqa: E402


logger = logging.getLogger("sweep-partless-mpu")

# Counting the whole population is a seq-scan-ish query on a 139M-row table; it is worth it
# once for the dry run, but give it room without being unbounded.
COUNT_TIMEOUT_MS = 300_000

SELECT_BATCH_SQL = """
SELECT mu.upload_id, mu.object_id
FROM multipart_uploads mu
WHERE COALESCE(mu.is_completed, false) = false
  AND mu.initiated_at < now() - make_interval(secs => $1)
  AND NOT EXISTS (SELECT 1 FROM parts p WHERE p.upload_id = mu.upload_id)
ORDER BY mu.initiated_at
LIMIT $2
"""

COUNT_SQL = """
SELECT count(*) FROM multipart_uploads mu
WHERE COALESCE(mu.is_completed, false) = false
  AND mu.initiated_at < now() - make_interval(secs => $1)
  AND NOT EXISTS (SELECT 1 FROM parts p WHERE p.upload_id = mu.upload_id)
"""

DELETE_SQL = "DELETE FROM multipart_uploads WHERE upload_id = ANY($1::uuid[])"


async def _dlq_protected_ids(config) -> set[str]:  # noqa: ANN001
    """Object ids with an in-flight DLQ entry, which the janitor and reaper both spare.

    A partless upload has no data to protect, so this cannot matter in principle — but both
    existing reapers gate on it, and silently being the one caller that does not is exactly the
    sort of inconsistency that is uncomfortable to explain after the fact.
    """
    from workers.run_janitor_in_loop import get_all_dlq_object_ids

    redis_client = async_redis.from_url(config.redis_queues_url)
    try:
        return await get_all_dlq_object_ids(redis_client)
    finally:
        await redis_client.aclose()


async def main_async(args: argparse.Namespace) -> int:
    config = get_config()
    stale_seconds = args.stale_seconds if args.stale_seconds > 0 else config.mpu_stale_seconds

    protected = set() if args.skip_dlq_check else await _dlq_protected_ids(config)
    logger.info("DLQ-protected object ids: %d", len(protected))

    pool = await asyncpg.create_pool(
        config.database_url,
        min_size=1,
        max_size=2,
        command_timeout=args.statement_timeout_ms / 1000,
        server_settings={"statement_timeout": str(args.statement_timeout_ms)},
    )
    try:
        if args.count:
            async with pool.acquire() as conn:
                await conn.execute(f"SET statement_timeout = {COUNT_TIMEOUT_MS}")
                total = await conn.fetchval(COUNT_SQL, stale_seconds)
            logger.info("partless incomplete uploads older than %ss: %d", stale_seconds, total)

        deleted = 0
        skipped = 0
        batches = 0
        started = time.monotonic()

        while True:
            if args.max_batches and batches >= args.max_batches:
                logger.info("reached --max-batches=%d; stopping", args.max_batches)
                break

            async with pool.acquire() as conn:
                rows = await conn.fetch(SELECT_BATCH_SQL, stale_seconds, args.batch_size)

            if not rows:
                logger.info("no more partless uploads; done")
                break

            targets = []
            for row in rows:
                object_id = row["object_id"]
                if object_id is not None and str(object_id) in protected:
                    skipped += 1
                    continue
                targets.append(row["upload_id"])

            batches += 1

            if not args.yes:
                logger.info(
                    "[DRY RUN] batch %d: would delete %d upload(s), skip %d DLQ-protected; sample=%s",
                    batches,
                    len(targets),
                    len(rows) - len(targets),
                    [str(u) for u in targets[:3]],
                )
                # A dry run must not loop forever re-reading the same rows it never deletes.
                logger.info("[DRY RUN] stopping after one batch. Re-run with --yes to delete.")
                break

            if targets:
                async with pool.acquire() as conn:
                    await conn.execute(DELETE_SQL, targets)
                deleted += len(targets)

            elapsed = time.monotonic() - started
            rate = deleted / elapsed if elapsed > 0 else 0
            logger.info(
                "batch %d: deleted %d (total %d, %.0f/s, skipped %d)", batches, len(targets), deleted, rate, skipped
            )

            # Hand the primary back between batches: this runs against the same instance the
            # drain and the api are using, and the whole point is to not be the heavy query.
            await asyncio.sleep(args.sleep_between)

        logger.info("finished: deleted=%d skipped_dlq=%d batches=%d", deleted, skipped, batches)
    finally:
        await pool.close()
    return 0


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="One-time sweep of incomplete multipart uploads that never got a part.")
    ap.add_argument(
        "--stale-seconds",
        type=int,
        default=0,
        help="Only uploads older than this. 0 (default) uses HIPPIUS_MPU_STALE_SECONDS, matching the reaper.",
    )
    ap.add_argument("--batch-size", type=int, default=5000, help="Uploads deleted per statement (default 5000)")
    ap.add_argument("--max-batches", type=int, default=0, help="Stop after N batches (0 = until exhausted)")
    ap.add_argument("--sleep-between", type=float, default=0.5, help="Seconds to pause between batches (default 0.5)")
    ap.add_argument(
        "--statement-timeout-ms",
        type=int,
        default=60_000,
        help="Hard ceiling per statement. Do not raise casually: an unbounded statement pins the xmin horizon.",
    )
    ap.add_argument("--count", action="store_true", help="Also report the total population up front (slow on prod)")
    ap.add_argument(
        "--skip-dlq-check",
        action="store_true",
        help="Skip the DLQ protection lookup (only if redis-queues is unreachable)",
    )
    ap.add_argument("--yes", action="store_true", help="Actually delete. Without this it is a dry run.")
    args = ap.parse_args()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
