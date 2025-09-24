#!/usr/bin/env python3
import asyncio  # noqa: I001
import contextlib
import logging
import sys
from pathlib import Path
from typing import Union
import time

import asyncpg
import redis.asyncio as async_redis

# Add parent directory to path to import hippius_s3 modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.ipfs_service import IPFSService
from hippius_s3.queue import MultipartUploadChainRequest
from hippius_s3.queue import SimpleUploadChainRequest
from hippius_s3.queue import UploadChainRequest
from hippius_s3.queue import dequeue_upload_request
from hippius_s3.queue import move_due_retries_to_primary
from hippius_s3.workers.pinner import Pinner
from workers.substrate import submit_storage_request


def _get_batch_context(upload_requests):
    """Extract aggregate context from batch of upload requests for logging."""
    if not upload_requests:
        return "no_requests"

    request_ids = [req.request_id for req in upload_requests if req.request_id]
    attempts = [req.attempts or 0 for req in upload_requests]
    object_ids = [req.object_id for req in upload_requests]

    return f"requests={len(upload_requests)} request_ids={request_ids[:3]}{'...' if len(request_ids) > 3 else ''} attempts={attempts[:3]}{'...' if len(attempts) > 3 else ''} objects={object_ids[:3]}{'...' if len(object_ids) > 3 else ''}"


config = get_config()

# Set logging level based on config
log_level = getattr(logging, config.log_level.upper(), logging.INFO)
logging.basicConfig(level=log_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Config is loaded globally, no module-level constants needed


"""Processing helpers are implemented in hippius_s3.workers.pinner.Pinner"""


"""Deprecated multipart chunk handler removed in favor of Pinner._process_multipart_chunk"""


async def process_upload_request(
    upload_requests: list[Union[SimpleUploadChainRequest, MultipartUploadChainRequest, UploadChainRequest]],
    db: asyncpg.Connection,
    redis_client: async_redis.Redis,
) -> bool:
    """Process upload requests by processing chunks, encrypting, and updating database."""
    ipfs_service = IPFSService(config, redis_client)
    pinner = Pinner(db, ipfs_service, redis_client, config)
    seed_phrase = upload_requests[0].subaccount_seed_phrase  # All requests for same user have same seed phrase

    # Update all objects status to 'pinning' when processing starts
    for payload in upload_requests:
        await db.execute(
            "UPDATE objects SET status = 'pinning' WHERE object_id = $1",
            payload.object_id,
        )
        logger.info(f"Updated object {payload.object_id} status to 'pinning'")

    # Process batch with per-item isolation
    files, succeeded_payloads = await pinner.process_batch(upload_requests)

    # Optionally skip substrate publish if disabled
    if not config.publish_to_chain:
        logger.info("Skipping substrate publish because publish_to_chain=false; marking objects as uploaded")
        for payload in succeeded_payloads:
            await db.execute(
                "UPDATE objects SET status = 'uploaded' WHERE object_id = $1",
                payload.object_id,
            )
            logger.info(f"Updated object {payload.object_id} status to 'uploaded'")
        return True

    # Submit storage request to substrate
    cids = [file.file_hash for file in files]
    if not cids:
        logger.info(
            f"No successful items to publish; skipping substrate submission for this batch ({_get_batch_context(upload_requests)})"
        )
        return True
    tx_hash = await submit_storage_request(cids=cids, seed_phrase=seed_phrase, substrate_url=config.substrate_url)

    logger.info(
        f"Processed {len(succeeded_payloads)} upload requests with transaction: {tx_hash} ({_get_batch_context(succeeded_payloads)})"
    )

    # Update all processed objects status to 'uploaded'
    for payload in succeeded_payloads:
        await db.execute(
            "UPDATE objects SET status = 'uploaded' WHERE object_id = $1",
            payload.object_id,
        )
        logger.info(f"Updated object {payload.object_id} status to 'uploaded'")

    return True


async def run_pinner_loop():
    # Connect to database and Redis
    db = await asyncpg.connect(config.database_url)
    redis_client = async_redis.from_url(config.redis_url)

    logger.info("Starting workers service...")
    logger.info(f"Redis URL: {config.redis_url}")
    logger.info(f"Database connected: {config.database_url}")
    # user -> { 'items': [ChainRequest...], 'first_at': epoch_seconds }
    user_upload_requests: dict[str, dict[str, object]] = {}

    try:
        while True:
            # Pull due retries
            try:
                moved = await move_due_retries_to_primary(redis_client, now_ts=time.time(), max_items=64)
                if moved:
                    logger.info(f"Moved {moved} due retry requests back to primary queue")
            except Exception:
                logger.debug("Failed moving due retries", exc_info=True)

            upload_request = await dequeue_upload_request(redis_client)

            now = time.time()

            if upload_request:
                entry = user_upload_requests.get(upload_request.address)
                if entry is None:
                    user_upload_requests[upload_request.address] = {"items": [upload_request], "first_at": now}
                else:
                    items = entry.get("items")
                    if isinstance(items, list):
                        items.append(upload_request)
                    else:
                        entry["items"] = [upload_request]
                    # Preserve first_at

                # Flush immediately if batch exceeds thresholds
                entry = user_upload_requests.get(upload_request.address)
                if entry is not None:
                    items = entry.get("items")
                    first_at = entry.get("first_at", now)
                    size_ok = isinstance(items, list) and len(items) >= config.pinner_batch_max_items
                    age_ok = isinstance(first_at, (int, float)) and (
                        now - float(first_at) >= config.pinner_batch_max_age_sec
                    )
                    if size_ok or age_ok:
                        batch = list(items) if isinstance(items, list) else []
                        success = await process_upload_request(batch, db, redis_client)
                        if success:
                            logger.info(
                                f"Processed user's {upload_request.address} with {len(batch)} files (flush reason: {'size' if size_ok else 'age'})"
                            )
                        else:
                            logger.info(
                                f"Failed to process {len(batch)} pin requests for user {upload_request.address} (flush reason: {'size' if size_ok else 'age'})"
                            )
                        # Reset entry for user
                        user_upload_requests.pop(upload_request.address, None)

            else:
                # No items in queue: flush all pending batches
                if user_upload_requests:
                    for user, entry in list(user_upload_requests.items()):
                        items = entry.get("items")
                        batch = list(items) if isinstance(items, list) else []
                        if not batch:
                            user_upload_requests.pop(user, None)
                            continue
                        success = await process_upload_request(batch, db, redis_client)
                        if success:
                            logger.info(f"SUCCESSFULLY processed user's {user} with {len(batch)} files")
                        else:
                            logger.info(f"Failed to batch and serve {len(batch)} pin requests for user {user}")
                        user_upload_requests.pop(user, None)
                await asyncio.sleep(0.1)

    except KeyboardInterrupt:
        logger.info("Pinner service stopping...")
    except Exception as e:
        logger.error(f"Error in pinner loop: {e}")
    finally:
        with contextlib.suppress(Exception):
            await redis_client.close()
            logger.info("Redis client closed")
        with contextlib.suppress(Exception):
            await db.close()


if __name__ == "__main__":
    asyncio.run(run_pinner_loop())
