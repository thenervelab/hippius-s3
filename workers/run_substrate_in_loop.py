#!/usr/bin/env python3
import asyncio
import logging
import sys
import time
from pathlib import Path

import asyncpg
import redis.asyncio as async_redis


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.queue import dequeue_substrate_request
from hippius_s3.queue import enqueue_substrate_retry_request
from hippius_s3.queue import move_substrate_due_retries_to_primary
from hippius_s3.workers.substrate import SubstrateWorker
from hippius_s3.workers.substrate import compute_substrate_backoff_ms


config = get_config()

log_level = getattr(logging, config.log_level.upper(), logging.INFO)
logging.basicConfig(level=log_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def run_substrate_loop():
    db = await asyncpg.connect(config.database_url)
    redis_client = async_redis.from_url(config.redis_url)

    logger.info("Starting substrate service...")
    logger.info(f"Redis URL: {config.redis_url}")
    logger.info(f"Database connected: {config.database_url}")

    substrate_worker = SubstrateWorker(db, config)

    pending_requests = []
    first_request_time = None

    while True:
        try:
            moved = await move_substrate_due_retries_to_primary(redis_client, now_ts=time.time(), max_items=64)
            if moved:
                logger.info(f"Moved {moved} due substrate retry requests back to primary queue")
        except async_redis.exceptions.BusyLoadingError:
            logger.warning("Redis is still loading dataset, waiting 2 seconds...")
            await asyncio.sleep(2)
            continue
        except Exception as e:
            logger.error(f"Error moving substrate retry requests: {e}")
            await asyncio.sleep(1)
            continue

        try:
            substrate_request = await dequeue_substrate_request(redis_client)
        except async_redis.exceptions.BusyLoadingError:
            logger.warning("Redis is still loading dataset, waiting 2 seconds...")
            await asyncio.sleep(2)
            continue

        now = time.time()

        if substrate_request:
            if not config.publish_to_chain:
                logger.info(f"Skipping substrate publish (disabled) object_id={substrate_request.object_id}")
                await db.execute(
                    "UPDATE objects SET status = 'published' WHERE object_id = $1",
                    substrate_request.object_id,
                )
                continue

            if not pending_requests:
                first_request_time = now

            pending_requests.append(substrate_request)

            batch_full = len(pending_requests) >= config.substrate_batch_size
            batch_aged = first_request_time and (now - first_request_time) >= config.substrate_batch_max_age_sec

            if batch_full or batch_aged:
                logger.info(
                    f"Flushing substrate batch size={len(pending_requests)} reason={'size' if batch_full else 'age'}"
                )
                try:
                    await substrate_worker.process_batch(pending_requests)
                except Exception as e:
                    logger.exception(f"Substrate batch submission failed count={len(pending_requests)}")
                    err_str = str(e)

                    should_retry = False
                    if pending_requests:
                        first_attempts = (pending_requests[0].attempts or 0) + 1
                        should_retry = first_attempts <= config.substrate_max_retries

                    if should_retry:
                        for req in pending_requests:
                            attempts_next = (req.attempts or 0) + 1
                            delay_ms = compute_substrate_backoff_ms(
                                attempts_next, config.substrate_retry_base_ms, config.substrate_retry_max_ms
                            )
                            await enqueue_substrate_retry_request(
                                req, redis_client, delay_seconds=delay_ms / 1000.0, last_error=err_str
                            )
                    else:
                        for req in pending_requests:
                            await db.execute("UPDATE objects SET status = 'failed' WHERE object_id = $1", req.object_id)

                pending_requests = []
                first_request_time = None
        else:
            if pending_requests:
                logger.info(f"Flushing pending substrate batch size={len(pending_requests)} reason=queue_empty")
                try:
                    await substrate_worker.process_batch(pending_requests)
                except Exception as e:
                    logger.exception(f"Substrate batch submission failed count={len(pending_requests)}")
                    err_str = str(e)

                    should_retry = False
                    if pending_requests:
                        first_attempts = (pending_requests[0].attempts or 0) + 1
                        should_retry = first_attempts <= config.substrate_max_retries

                    if should_retry:
                        for req in pending_requests:
                            attempts_next = (req.attempts or 0) + 1
                            delay_ms = compute_substrate_backoff_ms(
                                attempts_next, config.substrate_retry_base_ms, config.substrate_retry_max_ms
                            )
                            await enqueue_substrate_retry_request(
                                req, redis_client, delay_seconds=delay_ms / 1000.0, last_error=err_str
                            )
                    else:
                        for req in pending_requests:
                            await db.execute("UPDATE objects SET status = 'failed' WHERE object_id = $1", req.object_id)

                pending_requests = []
                first_request_time = None

            await asyncio.sleep(0.1)


if __name__ == "__main__":
    asyncio.run(run_substrate_loop())
