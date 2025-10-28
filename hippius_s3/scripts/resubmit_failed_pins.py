from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
from typing import Any

import asyncpg
import redis.asyncio as async_redis

from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.config import get_config
from hippius_s3.queue import Chunk
from hippius_s3.queue import UploadChainRequest
from hippius_s3.queue import enqueue_upload_request


logger = logging.getLogger(__name__)


async def fetch_failed_simple_objects(db: Any, hours: int, include_pinning: bool = False) -> list[dict]:
    rows = await db.fetch(
        """
        SELECT o.object_id,
               o.object_key,
               b.bucket_name,
               b.is_public,
               b.main_account_id
        FROM objects o
        JOIN buckets b ON b.bucket_id = o.bucket_id
        WHERE (o.status = 'failed' OR (o.status = 'pinning' AND $2))
          AND o.multipart = FALSE
          AND o.created_at > (NOW() AT TIME ZONE 'UTC') - make_interval(hours => $1)
        ORDER BY o.created_at DESC
        """,
        hours,
        include_pinning,
    )
    return [dict(r) for r in rows]


async def main() -> None:
    parser = argparse.ArgumentParser(description="Resubmit failed simple uploads by re-enqueuing pin requests")
    parser.add_argument("--hours", type=int, default=72, help="Look back window in hours (default: 72)")
    parser.add_argument(
        "--seed",
        type=str,
        default=os.getenv("RESUBMISSION_SEED_PHRASE") or os.getenv("SEED_PHRASE") or "",
        help="Seed phrase to use for encryption/pin (required for private buckets)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print planned actions without enqueueing")
    parser.add_argument("--include-pinning", action="store_true", help="Include items currently in 'pinning' status")
    args = parser.parse_args()

    config = get_config()
    db = await asyncpg.connect(config.database_url)
    redis_client = async_redis.from_url(config.redis_url)
    obj_cache = RedisObjectPartsCache(redis_client)

    try:
        candidates = await fetch_failed_simple_objects(db, args.hours, include_pinning=args.include_pinning)
        if not candidates:
            logger.info("No failed simple uploads found in the specified window.")
            return

        logger.info(f"Found {len(candidates)} failed simple uploads in last {args.hours}h")

        enqueued = 0
        skipped_no_cache = 0
        skipped_private_no_seed = 0

        for row in candidates:
            object_id = str(row["object_id"]) if row.get("object_id") is not None else ""
            object_key = str(row.get("object_key") or "")
            bucket_name = str(row.get("bucket_name") or "")
            is_public = bool(row.get("is_public"))
            address = str(row.get("main_account_id") or "")

            if not object_id or not object_key or not bucket_name or not address:
                continue

            # Ensure part 0 is still in cache (assume current version = 1 for legacy script)
            if not await obj_cache.exists(object_id, 1, 1):
                skipped_no_cache += 1
                continue

            should_encrypt = not is_public
            seed_phrase = args.seed if should_encrypt else ""
            if should_encrypt and not seed_phrase:
                skipped_private_no_seed += 1
                continue

            payload = UploadChainRequest(
                substrate_url=config.substrate_url,
                ipfs_node=config.ipfs_store_url,
                address=address,
                subaccount=address,
                subaccount_seed_phrase=seed_phrase,
                bucket_name=bucket_name,
                object_key=object_key,
                should_encrypt=should_encrypt,
                object_id=object_id,
                object_version=1,
                chunks=[Chunk(id=1)],
                upload_id=None,
            )

            if args.dry_run:
                logger.info(f"DRY_RUN would enqueue: {address} {bucket_name}/{object_key} (encrypt={should_encrypt})")
            else:
                # Mark as pinning and enqueue
                await db.execute("UPDATE objects SET status = 'pinning' WHERE object_id = $1", object_id)
                await enqueue_upload_request(payload=payload, redis_client=redis_client)
                enqueued += 1

        logger.info(
            f"Done. enqueued={enqueued} skipped_no_cache={skipped_no_cache} skipped_private_no_seed={skipped_private_no_seed}"
        )

    finally:
        with contextlib.suppress(Exception):
            await redis_client.close()
        with contextlib.suppress(Exception):
            await db.close()


if __name__ == "__main__":
    asyncio.run(main())
