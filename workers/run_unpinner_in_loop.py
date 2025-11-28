#!/usr/bin/env python3
import asyncio
import logging
import sys
import time
from pathlib import Path

import httpx
import redis.asyncio as async_redis


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.monitoring import initialize_metrics_collector
from hippius_s3.queue import UnpinChainRequest
from hippius_s3.queue import dequeue_unpin_request
from hippius_s3.queue import enqueue_unpin_retry_request
from hippius_s3.queue import move_due_unpin_retries_to_primary
from hippius_s3.redis_utils import with_redis_retry
from hippius_s3.services.hippius_api_service import HippiusApiClient
from hippius_s3.workers.uploader import classify_error
from hippius_s3.workers.uploader import compute_backoff_ms


config = get_config()

setup_loki_logging(config, "unpinner")
logger = logging.getLogger(__name__)


async def process_unpin_request(request: UnpinChainRequest) -> None:
    """Process a single unpin request."""
    try:
        logger.info(f"Processing unpin request: {request.name}")
        async with HippiusApiClient() as api_client:
            unpin_result = await api_client.unpin_file(
                request.cid,
                account_ss58=request.address,
            )
        logger.info(f"Successfully unpinned CID via API: {unpin_result=}")

    except Exception as e:
        logger.error(f"Failed to process unpin request {request.name}: {e}")
        error_class = classify_error(e)

        attempts_next = (request.attempts or 0) + 1
        max_attempts = 5

        if error_class == "transient" and attempts_next <= max_attempts:
            delay_ms = compute_backoff_ms(attempts_next, base_ms=1000, max_ms=60000)
            delay_sec = delay_ms / 1000.0

            logger.info(
                f"Scheduling retry for {request.name} "
                f"(attempt {attempts_next}/{max_attempts}, delay {delay_sec:.1f}s, error_class={error_class})"
            )

            await enqueue_unpin_retry_request(
                request,
                delay_seconds=delay_sec,
                last_error=str(e),
            )
        else:
            logger.warning(
                f"Unpin request {request.name} failed permanently or exhausted retries "
                f"(attempts={attempts_next}, error_class={error_class}, error={e})"
            )


async def run_unpinner_loop() -> None:
    redis_client = async_redis.from_url(config.redis_url)
    redis_queues_client = async_redis.from_url(config.redis_queues_url)

    from hippius_s3.queue import initialize_queue_client
    from hippius_s3.redis_cache import initialize_cache_client

    initialize_queue_client(redis_queues_client)
    initialize_cache_client(redis_client)
    initialize_metrics_collector(redis_client)

    logger.info("Starting unpinner service...")
    logger.info(f"Redis URL: {config.redis_url}")
    logger.info(f"Redis Queues URL: {config.redis_queues_url}")

    while True:
        await move_due_unpin_retries_to_primary()

        unpin_request, redis_queues_client = await with_redis_retry(
            lambda rc: dequeue_unpin_request(),
            redis_queues_client,
            config.redis_queues_url,
            "dequeue unpin request",
        )

        if not unpin_request:
            await asyncio.sleep(1)
            continue

        await process_unpin_request(unpin_request)


if __name__ == "__main__":
    while True:
        try:
            asyncio.run(run_unpinner_loop())
        except KeyboardInterrupt:
            logger.info("Unpinner service stopped by user")
            break
        except Exception as e:
            logger.error(f"Unpinner crashed, restarting in 5 seconds: {e}", exc_info=True)
            time.sleep(5)
