#!/usr/bin/env python3
import asyncio
import logging
import sys
import time
from pathlib import Path

import asyncpg
import redis.asyncio as async_redis
from pydantic import ValidationError
from redis.exceptions import BusyLoadingError


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.ipfs_service import IPFSService
from hippius_s3.queue import dequeue_upload_request
from hippius_s3.queue import enqueue_retry_request
from hippius_s3.queue import move_due_retries_to_primary
from hippius_s3.workers.uploader import Uploader
from hippius_s3.workers.uploader import classify_error
from hippius_s3.workers.uploader import compute_backoff_ms


config = get_config()

log_level = getattr(logging, config.log_level.upper(), logging.INFO)
logging.basicConfig(level=log_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def run_uploader_loop():
    db = await asyncpg.connect(config.database_url)
    redis_client = async_redis.from_url(config.redis_url)

    logger.info("Starting uploader service...")
    logger.info(f"Redis URL: {config.redis_url}")
    logger.info(f"Database connected: {config.database_url}")

    ipfs_service = IPFSService(config, redis_client)
    uploader = Uploader(db, ipfs_service, redis_client, config)

    while True:
        try:
            moved = await move_due_retries_to_primary(redis_client, now_ts=time.time(), max_items=64)
            if moved:
                logger.info(f"Moved {moved} due retry requests back to primary queue")
        except BusyLoadingError:
            logger.warning("Redis is still loading dataset, waiting 2 seconds...")
            await asyncio.sleep(2)
            continue
        except Exception as e:
            logger.error(f"Error moving retry requests: {e}")
            await asyncio.sleep(1)
            continue

        try:
            upload_request = await dequeue_upload_request(redis_client)
        except ValidationError as e:
            logger.error(f"Invalid queue data, skipping: {e}")
            await asyncio.sleep(0.1)
            continue
        except BusyLoadingError:
            logger.warning("Redis is still loading dataset, waiting 2 seconds...")
            await asyncio.sleep(2)
            continue

        if upload_request:
            logger.info(
                f"Processing upload request object_id={upload_request.object_id} chunks={len(upload_request.chunks)} attempts={upload_request.attempts or 0}"
            )

            try:
                await uploader.process_upload(upload_request)
            except Exception as e:
                logger.exception(f"Upload failed object_id={upload_request.object_id}")
                err_str = str(e)
                error_type = classify_error(e)
                attempts_next = (upload_request.attempts or 0) + 1

                if error_type == "transient" and attempts_next <= config.uploader_max_attempts:
                    delay_ms = compute_backoff_ms(
                        attempts_next, config.uploader_backoff_base_ms, config.uploader_backoff_max_ms
                    )
                    await enqueue_retry_request(
                        upload_request, redis_client, delay_seconds=delay_ms / 1000.0, last_error=err_str
                    )
                else:
                    await uploader._push_to_dlq(upload_request, err_str, error_type)
                    await db.execute(
                        "UPDATE objects SET status = 'failed' WHERE object_id = $1", upload_request.object_id
                    )
        else:
            await asyncio.sleep(0.1)


if __name__ == "__main__":
    asyncio.run(run_uploader_loop())
