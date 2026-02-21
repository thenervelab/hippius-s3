#!/usr/bin/env python3
"""
One-shot recovery script: re-enqueue objects missing a specific storage backend.

Finds objects from the past N days that are missing chunks for a given backend
and pushes UploadChainRequest payloads directly into that backend's Redis upload
queue (``{backend}_upload_requests``).

Optionally, ``--require-backend`` restricts results to objects that already have
chunks on another backend (e.g. only recover Arion for objects that have IPFS).
Without it, all objects missing the target backend are included.

Usage (on an API pod):
    # Arion recovery (objects that have IPFS but not Arion)
    python -m hippius_s3.scripts.recover_missing_backend --backend arion --require-backend ipfs --dry-run
    python -m hippius_s3.scripts.recover_missing_backend --backend arion --require-backend ipfs

    # OVH recovery (all objects missing OVH, regardless of other backends)
    python -m hippius_s3.scripts.recover_missing_backend --backend ovh --dry-run
    python -m hippius_s3.scripts.recover_missing_backend --backend ovh

    # Tuning
    python -m hippius_s3.scripts.recover_missing_backend --backend arion --days 7 --batch-size 500
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
import uuid
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import asyncpg
import redis.asyncio as async_redis

from hippius_s3.config import get_config
from hippius_s3.queue import Chunk
from hippius_s3.queue import UploadChainRequest
from hippius_s3.queue import initialize_queue_client


logger = logging.getLogger(__name__)


def _build_count_query(require_backend: str | None) -> str:
    require_join = ""
    if require_backend:
        require_join = f"""
JOIN chunk_backend cb_req ON (
    cb_req.chunk_id = pc.id
    AND cb_req.backend = '{require_backend}'
    AND NOT cb_req.deleted
    AND cb_req.backend_identifier IS NOT NULL
)"""

    return f"""
SELECT
    COUNT(DISTINCT o.object_id) AS object_count,
    COUNT(DISTINCT pc.id) AS chunk_count
FROM objects o
JOIN object_versions ov ON (o.object_id = ov.object_id AND o.current_object_version = ov.object_version)
JOIN parts p ON (p.object_id = o.object_id AND p.object_version = ov.object_version)
JOIN part_chunks pc ON pc.part_id = p.part_id
{require_join}
WHERE o.deleted_at IS NULL
  AND ov.created_at >= NOW() - make_interval(days => $1)
  AND NOT EXISTS (
    SELECT 1 FROM chunk_backend cb_target
    WHERE cb_target.chunk_id = pc.id
      AND cb_target.backend = $2
      AND NOT cb_target.deleted
  )
"""


def _build_fetch_query(require_backend: str | None) -> str:
    require_join = ""
    if require_backend:
        require_join = f"""
JOIN chunk_backend cb_req ON (
    cb_req.chunk_id = pc.id
    AND cb_req.backend = '{require_backend}'
    AND NOT cb_req.deleted
    AND cb_req.backend_identifier IS NOT NULL
)"""

    return f"""
SELECT
    o.object_id,
    o.object_key,
    ov.object_version,
    b.bucket_name,
    b.main_account_id AS address,
    p.upload_id,
    array_agg(DISTINCT p.part_number ORDER BY p.part_number) AS part_numbers
FROM objects o
JOIN object_versions ov ON (o.object_id = ov.object_id AND o.current_object_version = ov.object_version)
JOIN buckets b ON o.bucket_id = b.bucket_id
JOIN parts p ON (p.object_id = o.object_id AND p.object_version = ov.object_version)
JOIN part_chunks pc ON pc.part_id = p.part_id
{require_join}
WHERE o.deleted_at IS NULL
  AND ov.created_at >= NOW() - make_interval(days => $1)
  AND NOT EXISTS (
    SELECT 1 FROM chunk_backend cb_target
    WHERE cb_target.chunk_id = pc.id
      AND cb_target.backend = $2
      AND NOT cb_target.deleted
  )
GROUP BY o.object_id, o.object_key, ov.object_version, ov.created_at, b.bucket_name, b.main_account_id, p.upload_id
ORDER BY ov.created_at ASC
"""


async def main() -> None:
    parser = argparse.ArgumentParser(description="Re-enqueue objects missing a storage backend")
    parser.add_argument("--backend", required=True, help="Target backend to recover (arion, ovh, ipfs)")
    parser.add_argument(
        "--require-backend",
        default=None,
        help="Only include objects that already have this backend (e.g. --require-backend ipfs)",
    )
    parser.add_argument("--days", type=int, default=14, help="Look-back window in days (default: 14)")
    parser.add_argument("--dry-run", action="store_true", help="Print counts and sample payloads without enqueuing")
    parser.add_argument(
        "--batch-size", type=int, default=200, help="LPUSH batch size for Redis pipelining (default: 200)"
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    config = get_config()
    db = await asyncpg.connect(config.database_url)
    redis_queues = async_redis.from_url(config.redis_queues_url)
    initialize_queue_client(redis_queues)

    backend = args.backend
    queue_name = f"{backend}_upload_requests"

    count_query = _build_count_query(args.require_backend)
    fetch_query = _build_fetch_query(args.require_backend)

    # Pre-flight count
    row = await db.fetchrow(count_query, args.days, backend)
    object_count = row["object_count"]
    chunk_count = row["chunk_count"]

    require_msg = f" (requiring {args.require_backend})" if args.require_backend else ""
    logger.info(
        f"Found {object_count} objects with {chunk_count} chunks missing {backend}{require_msg} (past {args.days} days)"
    )

    if object_count == 0:
        logger.info("Nothing to do.")
        await db.close()
        await redis_queues.close()
        return

    # Check current queue length before
    queue_len_before = await redis_queues.llen(queue_name)
    logger.info(f"Current {queue_name} queue length: {queue_len_before}")

    if args.dry_run:
        logger.info("DRY RUN — fetching first 10 rows for inspection")

    # Fetch all rows
    rows = await db.fetch(fetch_query, args.days, backend)
    logger.info(f"Query returned {len(rows)} object rows")

    enqueued = 0
    batch: list[str] = []

    for row in rows:
        object_id = str(row["object_id"])
        part_numbers = list(row["part_numbers"])
        upload_id_raw = row["upload_id"]

        payload = UploadChainRequest(
            address=str(row["address"]),
            bucket_name=str(row["bucket_name"]),
            object_key=str(row["object_key"]),
            object_id=object_id,
            object_version=int(row["object_version"]),
            chunks=[Chunk(id=int(pn)) for pn in part_numbers],
            upload_id=str(upload_id_raw) if upload_id_raw is not None else None,
            upload_backends=[backend],
            request_id=uuid.uuid4().hex,
            first_enqueued_at=time.time(),
            attempts=0,
        )

        if args.dry_run:
            if enqueued < 10:
                logger.info(
                    f"  Would enqueue: object_id={object_id} "
                    f"bucket={row['bucket_name']} key={row['object_key']} "
                    f"version={row['object_version']} parts={part_numbers} "
                    f"upload_id={upload_id_raw}"
                )
            enqueued += 1
            continue

        batch.append(payload.model_dump_json())

        if len(batch) >= args.batch_size:
            await redis_queues.lpush(queue_name, *batch)
            enqueued += len(batch)
            logger.info(f"  Enqueued batch: {enqueued}/{len(rows)}")
            batch = []

    # Flush remaining
    if batch and not args.dry_run:
        await redis_queues.lpush(queue_name, *batch)
        enqueued += len(batch)

    if args.dry_run:
        logger.info(f"DRY RUN complete. Would enqueue {enqueued} payloads to {queue_name}")
    else:
        queue_len_after = await redis_queues.llen(queue_name)
        logger.info(
            f"Done. Enqueued {enqueued} payloads to {queue_name}. Queue length: {queue_len_before} -> {queue_len_after}"
        )

    await db.close()
    await redis_queues.close()


if __name__ == "__main__":
    asyncio.run(main())
