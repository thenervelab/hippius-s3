"""Seed `bucket_storage_usage` for every bucket, then declare the rollup usable.

The triggers in 20260910120000_storage_usage_rollup.sql start recording deltas the moment the
migration lands, but a delta stream is not a total: until every bucket's counter has been set from
the objects tables once, the rollup holds only "bytes moved since the migration". This script is
that once.

Until it finishes, `storage_usage_rollup_state.backfilled_at` stays NULL, usage_service REFUSES to
serve a number, and the plans-cacher keeps publishing its previous roll. So a backfill that dies
part way through degrades to the pre-existing behaviour rather than to a wrong bill, and re-running
is always safe.

    python -m hippius_s3.scripts.backfill_bucket_storage_usage --dry-run
    python -m hippius_s3.scripts.backfill_bucket_storage_usage --apply

SAFE TO RUN CONCURRENTLY WITH LIVE TRAFFIC, and safe to run twice. Each bucket is recomputed in its
own transaction by recompute_bucket_storage_usage(), which SETS the counter (never adds) inside a
single snapshot that also discards that bucket's pending ledger rows -- so a recompute converges on
the truth instead of double-counting whatever the compactor has already folded, and a write landing
mid-run is either included in the recompute or left in the ledger for the compactor, never both.

ONE BUCKET PER TRANSACTION, deliberately. Each recompute takes the rollup's global advisory lock,
which blocks the compactor; holding it across all ~46k buckets would stall compaction for the whole
run. Per-bucket, compaction interleaves.

--dry-run reports what each bucket's counter WOULD move by without writing anything, which is the
number to sanity-check before the real run: on a freshly migrated estate every bucket should move
from 0 to its true size. It runs the SAME recompute as --apply and rolls it back, rather than asking
the question a second way -- a dry run whose arithmetic can disagree with the apply it predicts is
worse than no dry run.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import uuid

import asyncpg

from hippius_s3.config import get_config
from hippius_s3.services import storage_rollup_service
from hippius_s3.utils import get_query


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("backfill_bucket_storage_usage")

_ZERO_UUID = uuid.UUID("00000000-0000-0000-0000-000000000000")

# Bucket ids fetched per keyset page. Only bounds the id list, not any aggregate.
_PAGE_SIZE = 1000


async def _bucket_ids(conn: asyncpg.Connection) -> list[uuid.UUID]:
    """Every bucket id, including soft-deleted ones -- see list_bucket_ids_for_backfill.sql."""
    cursor = _ZERO_UUID
    ids: list[uuid.UUID] = []

    while True:
        rows = await conn.fetch(get_query("list_bucket_ids_for_backfill"), cursor, _PAGE_SIZE)
        if not rows:
            return ids
        ids.extend(row["bucket_id"] for row in rows)
        cursor = rows[-1]["bucket_id"]


async def main_async(args: argparse.Namespace) -> int:
    config = get_config()
    # RAISE statement_timeout, or the big buckets can never be seeded. Production sets a 1-minute
    # server-side statement_timeout for the application role, and the largest bucket's aggregate
    # takes longer than that. asyncpg's own `timeout=` cannot help: whichever limit is SHORTER
    # fires, and the server's was. The result was a backfill that aborts on that bucket every time
    # -- safely, since backfilled_at is only set after a complete pass, but permanently.
    conn = await asyncpg.connect(
        config.database_url,
        server_settings={"statement_timeout": f"{int(args.timeout * 1000)}"},
    )

    try:
        bucket_ids = await _bucket_ids(conn)
        logger.info(f"{len(bucket_ids)} bucket(s) to seed (dry_run={not args.apply})")

        total_bytes = 0
        moved = 0

        for index, bucket_id in enumerate(bucket_ids, start=1):
            # BOTH modes run the SAME recompute; --dry-run just rolls it back. A dry run that
            # asked the question a different way could answer differently from the apply it is
            # meant to predict, which is the one thing a dry run must not do -- and it would be a
            # third copy of an aggregate that already exists twice.
            if args.apply:
                result = await storage_rollup_service.recompute_bucket(conn, bucket_id, args.timeout)
            else:
                tx = conn.transaction()
                await tx.start()
                try:
                    result = await storage_rollup_service.recompute_bucket(conn, bucket_id, args.timeout)
                finally:
                    await tx.rollback()
            before, after = result.bytes_before, result.bytes_after

            total_bytes += after
            if before != after:
                moved += 1
                logger.info(f"bucket={bucket_id} {before} -> {after} bytes")

            if index % 500 == 0:
                logger.info(f"progress: {index}/{len(bucket_ids)} bucket(s), {total_bytes} bytes so far")

            if args.sleep_ms:
                await asyncio.sleep(args.sleep_ms / 1000)

        logger.info(f"seeded {len(bucket_ids)} bucket(s), {moved} changed, {total_bytes} bytes total")

        if not args.apply:
            logger.info("dry run: nothing written and backfilled_at left NULL. Re-run with --apply.")
            return 0

        # LAST, and only on a complete pass: this is the flag usage_service gates on.
        backfilled_at = await conn.fetchval(get_query("mark_storage_usage_backfilled"))
        logger.info(f"rollup marked backfilled at {backfilled_at}; usage_service will now serve it")
        return 0
    finally:
        await conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--dry-run", dest="apply", action="store_false", help="report only (default)")
    group.add_argument("--apply", dest="apply", action="store_true", help="write the counters and set backfilled_at")
    parser.set_defaults(apply=False)
    parser.add_argument(
        "--timeout",
        type=float,
        default=600.0,
        help="bound on ONE bucket's aggregate, applied as the connection's statement_timeout AND as "
        "asyncpg's own (default 600s; the largest prod bucket takes well over the 60s the server "
        "otherwise imposes)",
    )
    parser.add_argument(
        "--sleep-ms",
        type=int,
        default=0,
        help="pause between buckets, to keep a long run off the primary's back",
    )
    args = parser.parse_args()

    return asyncio.run(main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
