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

# Soft-deleted objects past the grace window that still hold live backend copies on a version
# OTHER than the one the original delete unpinned. Keyset-paged on (deleted_at, object_id) via
# idx_objects_deleted, and MATERIALIZED so the per-object EXISTS probes run against a small slice
# instead of folding into a hash join over the ~336M-row chunk_backend table.
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
       c.current_object_version,
       c.deleted_at,
       b.main_account_id,
       v.versions,
       v.reclaimable_bytes
FROM c
JOIN buckets b ON b.bucket_id = c.bucket_id
CROSS JOIN LATERAL (
    SELECT array_agg(DISTINCT p.object_version) AS versions,
           COALESCE(sum(ov.size_bytes), 0) AS reclaimable_bytes
    FROM parts p
    JOIN part_chunks pc ON pc.part_id = p.part_id
    JOIN chunk_backend cb ON cb.chunk_id = pc.id
    LEFT JOIN object_versions ov
           ON ov.object_id = p.object_id AND ov.object_version = p.object_version
    WHERE p.object_id = c.object_id
      AND NOT cb.deleted
      AND cb.backend_identifier IS NOT NULL
      AND p.object_version <> c.current_object_version
) v
WHERE v.versions IS NOT NULL
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
    objects = 0
    versions = 0
    reclaimable = 0

    try:
        while args.limit <= 0 or objects < args.limit:
            batch = min(args.batch, args.limit - objects) if args.limit > 0 else args.batch
            rows = await db.fetch(_SELECT, batch, cursor_at, cursor_id, str(args.older_than_hours))
            if not rows:
                break

            for row in rows:
                object_id = str(row["object_id"])
                stale_versions = sorted(int(v) for v in row["versions"])
                objects += 1
                versions += len(stale_versions)
                reclaimable += int(row["reclaimable_bytes"] or 0)

                if args.apply:
                    for object_version in stale_versions:
                        await enqueue_unpin_request(
                            payload=UnpinChainRequest(
                                address=row["main_account_id"],
                                object_id=object_id,
                                object_version=object_version,
                                ray_id=None,
                            )
                        )
                    # Pace the enqueue so a large backlog cannot flood redis-queues the way the
                    # 1.29M-entry unpin overrun did.
                    if args.sleep_ms:
                        await asyncio.sleep(args.sleep_ms / 1000.0)

            last = rows[-1]
            cursor_at = last["deleted_at"]
            cursor_id = str(last["object_id"])

            logger.info(
                "progress: objects=%d versions=%d reclaimable=%s cursor=%s",
                objects,
                versions,
                _fmt_bytes(reclaimable),
                cursor_at.isoformat(),
            )
    finally:
        await db.close()
        await redis_queues_client.aclose()

    mode = "ENQUEUED" if args.apply else "DRY RUN (nothing enqueued)"
    logger.info(
        "%s — objects=%d superseded_versions=%d reclaimable=%s",
        mode,
        objects,
        versions,
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
