#!/usr/bin/env python3
import asyncio
import logging
import sys
import time
from pathlib import Path

import asyncpg
import redis.asyncio as async_redis
from pydantic import ValidationError


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.ipfs_service import IPFSService
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.monitoring import initialize_metrics_collector
from hippius_s3.queue import dequeue_upload_request
from hippius_s3.queue import enqueue_retry_request
from hippius_s3.queue import move_due_retries_to_primary
from hippius_s3.redis_utils import with_redis_retry
from hippius_s3.workers.uploader import Uploader
from hippius_s3.workers.uploader import classify_error
from hippius_s3.workers.uploader import compute_backoff_ms


config = get_config()

setup_loki_logging(config, "uploader")
logger = logging.getLogger(__name__)


async def run_uploader_loop():
    db_pool = await asyncpg.create_pool(config.database_url, min_size=2, max_size=10)
    redis_client = async_redis.from_url(config.redis_url)
    redis_queues_client = async_redis.from_url(config.redis_queues_url)

    from hippius_s3.queue import initialize_queue_client

    initialize_queue_client(redis_queues_client)
    initialize_metrics_collector(redis_client)

    logger.info("Starting uploader service...")
    logger.info(f"Redis URL: {config.redis_url}")
    logger.info(f"Redis Queues URL: {config.redis_queues_url}")
    logger.info(f"Database pool created: {config.database_url}")

    ipfs_service = IPFSService(config, redis_client)
    uploader = Uploader(db_pool, ipfs_service, redis_client, config)

    while True:
        try:
            moved, redis_queues_client = await with_redis_retry(
                lambda rc: move_due_retries_to_primary(now_ts=time.time(), max_items=64),
                redis_queues_client,
                config.redis_queues_url,
                "move due retries",
            )
            if moved:
                logger.info(f"Moved {moved} due retry requests back to primary queue")
        except Exception as e:
            logger.error(f"Error moving retry requests: {e}")
            await asyncio.sleep(1)
            continue

        try:
            upload_request, redis_queues_client = await with_redis_retry(
                lambda rc: dequeue_upload_request(),
                redis_queues_client,
                config.redis_queues_url,
                "dequeue upload request",
            )
        except ValidationError as e:
            logger.error(f"Invalid queue data, skipping: {e}")
            await asyncio.sleep(0.1)
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
                        upload_request, delay_seconds=delay_ms / 1000.0, last_error=err_str
                    )
                    get_metrics_collector().record_uploader_operation(
                        main_account=upload_request.address,
                        success=False,
                        attempt=attempts_next,
                    )
                else:
                    await uploader._push_to_dlq(upload_request, err_str, error_type)
                    async with db_pool.acquire() as db:
                        await db.execute(
                            "UPDATE object_versions SET status = 'failed' WHERE object_id = $1 AND object_version = $2",
                            upload_request.object_id,
                            int(getattr(upload_request, "object_version", 1) or 1),
                        )
        else:
            await asyncio.sleep(0.1)


if __name__ == "__main__":
    asyncio.run(run_uploader_loop())
