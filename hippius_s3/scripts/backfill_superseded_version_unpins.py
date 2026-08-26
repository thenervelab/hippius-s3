"""Drain the backlog of superseded versions that were never unpinned.

Until the versioned-delete fix, `handle_delete_object` enqueued an unpin for
`current_object_version` only. Every superseded version of a deleted object therefore kept live
`chunk_backend` rows forever — and because `hard_delete_object`'s readiness gate waits on ALL
versions of the object being unpinned, those objects also became permanently un-hard-deletable.

Deploying the fix stops new ones accruing; it does not retroactively enqueue anything for the
existing backlog. This script does that, on demand.

Read-only by default: `--dry-run` (the default) reports the object count, version count and
reclaimable bytes without enqueuing anything. Pass `--apply` to actually enqueue.

    python -m hippius_s3.scripts.backfill_superseded_version_unpins --dry-run
    python -m hippius_s3.scripts.backfill_superseded_version_unpins --apply --limit 5000
"""

from __future__ import annotations

import argparse
import asyncio
import datetime
import logging

import asyncpg
import redis.asyncio as async_redis

from hippius_s3.config import get_config
from hippius_s3.queue import UnpinChainRequest
from hippius_s3.queue import enqueue_unpin_request
from hippius_s3.queue import initialize_queue_client


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("backfill_superseded_unpins")

_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
_ZERO_UUID = "00000000-0000-0000-0000-000000000000"

# Soft-deleted objects past the grace window, each flagged with whether it still holds a live
# backend copy on a version OTHER than the one the original delete unpinned.
#
# The slice is returned UNFILTERED with a per-row `needs_unpin` boolean, mirroring
# find_objects_ready_for_hard_delete. Filtering inside the query would make a page where no
# candidate qualifies come back empty, which the caller cannot distinguish from "scan exhausted" —
# and since most soft-deleted objects have only one version, the very first page would end the run.
#
# Byte accounting comes from object_versions, NOT from the chunk join: summing size_bytes across
# parts x part_chunks x chunk_backend counts each version once per (chunk x backend) row, which
# inflates the figure by orders of magnitude — and that figure is what gates the prod run.
#
# Keyset-paged on (deleted_at, object_id) via idx_objects_deleted, MATERIALIZED so the per-object
# probes run against a small slice instead of folding into a hash join over ~343M chunk_backend rows.
_SELECT = """
WITH c AS MATERIALIZED (
    SELECT object_id, current_object_version, deleted_at, bucket_id
    FROM objects
    WHERE deleted_at IS NOT NULL
      AND deleted_at < now() - ($4::text || ' hours')::interval
      AND (deleted_at, object_id) > ($2::timestamptz, $3::uuid)
    ORDER BY deleted_at, object_id
    LIMIT $1
)
SELECT c.object_id,
       c.deleted_at,
       b.main_account_id,
       EXISTS (
           SELECT 1
           FROM parts p
           JOIN part_chunks pc ON pc.part_id = p.part_id
           JOIN chunk_backend cb ON cb.chunk_id = pc.id
           WHERE p.object_id = c.object_id
             AND NOT cb.deleted
             AND cb.backend_identifier IS NOT NULL
             AND p.object_version <> c.current_object_version
       ) AS needs_unpin,
       COALESCE((
           SELECT sum(ov.size_bytes)
           FROM object_versions ov
           WHERE ov.object_id = c.object_id
             AND ov.object_version <> c.current_object_version
       ), 0) AS superseded_bytes
FROM c
JOIN buckets b ON b.bucket_id = c.bucket_id
ORDER BY c.deleted_at, c.object_id
"""


def _fmt_bytes(n: int) -> str:
    value = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(value) < 1024.0:
            return f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{value:.1f} PiB"


async def main_async(args: argparse.Namespace) -> int:
    config = get_config()
    db = await asyncpg.connect(config.database_url)
    redis_queues_client = async_redis.from_url(config.redis_queues_url)
    initialize_queue_client(redis_queues_client)

    cursor_at = _EPOCH
    cursor_id = _ZERO_UUID
    scanned = 0
    matched = 0
    reclaimable = 0

    try:
        while args.limit <= 0 or matched < args.limit:
            rows = await db.fetch(_SELECT, args.batch, cursor_at, cursor_id, str(args.older_than_hours))
            if not rows:
                break  # the ring is exhausted: no candidates at all past the cursor

            for row in rows:
                scanned += 1
                if not row["needs_unpin"]:
                    continue

                matched += 1
                reclaimable += int(row["superseded_bytes"] or 0)

                if args.apply:
                    # One request per object, NULL version = "every version". The unpinner resolves
                    # the list under its own batching; expanding it here would enqueue hundreds of
                    # thousands of entries for a heavily-appended object.
                    await enqueue_unpin_request(
                        payload=UnpinChainRequest(
                            address=row["main_account_id"],
                            object_id=str(row["object_id"]),
                            object_version=None,
                            ray_id=None,
                        )
                    )
                    # Pace the enqueue so a large backlog cannot flood redis-queues the way the
                    # 1.29M-entry unpin overrun did.
                    if args.sleep_ms:
                        await asyncio.sleep(args.sleep_ms / 1000.0)

                if args.limit > 0 and matched >= args.limit:
                    break

            last = rows[-1]
            cursor_at = last["deleted_at"]
            cursor_id = str(last["object_id"])

            logger.info(
                "progress: scanned=%d matched=%d reclaimable=%s cursor=%s",
                scanned,
                matched,
                _fmt_bytes(reclaimable),
                cursor_at.isoformat(),
            )
    finally:
        await db.close()
        await redis_queues_client.aclose()

    mode = "ENQUEUED" if args.apply else "DRY RUN (nothing enqueued)"
    logger.info(
        "%s — scanned=%d objects_needing_unpin=%d reclaimable=%s",
        mode,
        scanned,
        matched,
        _fmt_bytes(reclaimable),
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually enqueue unpins. Without this the script only reports.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Explicit no-op form of the default.")
    parser.add_argument("--batch", type=int, default=500, help="Objects fetched per DB round trip.")
    parser.add_argument("--limit", type=int, default=0, help="Stop after N objects (0 = no limit).")
    parser.add_argument(
        "--older-than-hours",
        type=int,
        default=24,
        help="Only objects soft-deleted at least this long ago, so an in-flight unpin can still land.",
    )
    parser.add_argument("--sleep-ms", type=int, default=50, help="Pause between objects when applying.")
    args = parser.parse_args()

    if args.dry_run:
        args.apply = False

    return asyncio.run(main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
