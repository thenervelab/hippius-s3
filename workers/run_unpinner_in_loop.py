#!/usr/bin/env python3
import asyncio
import logging
import sys
import time
from pathlib import Path

import redis.asyncio as async_redis


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.dlq.unpin_dlq import UnpinDLQManager
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.monitoring import initialize_metrics_collector
from hippius_s3.queue import UnpinChainRequest
from hippius_s3.queue import dequeue_unpin_request
from hippius_s3.queue import enqueue_unpin_retry_request
from hippius_s3.queue import move_due_unpin_retries_to_primary
from hippius_s3.redis_utils import with_redis_retry
from hippius_s3.services.hippius_api_service import HippiusApiClient
from hippius_s3.services.ray_id_service import get_logger_with_ray_id
from hippius_s3.services.ray_id_service import ray_id_context
from hippius_s3.workers.error_classifier import classify_unpin_error
from hippius_s3.workers.uploader import compute_backoff_ms


config = get_config()

setup_loki_logging(config, "unpinner")
logger = logging.getLogger(__name__)


async def process_unpin_request(
    request: UnpinChainRequest, worker_logger: logging.LoggerAdapter, dlq_manager: UnpinDLQManager
) -> None:
    """Process a single unpin request."""
    try:
        worker_logger.info(f"Processing unpin request: {request.name}")
        async with HippiusApiClient() as api_client:
            unpin_result = await api_client.unpin_file(
                request.cid,
                account_ss58=request.address,
            )
        worker_logger.info(f"Successfully unpinned CID via API: {unpin_result=}")

        get_metrics_collector().record_unpinner_operation(
            main_account=request.address,
            success=True,
        )

    except Exception as e:
        worker_logger.error(f"Failed to process unpin request {request.name}: {e}")
        error_class = classify_unpin_error(e)

        attempts_next = (request.attempts or 0) + 1
        max_attempts = config.unpinner_max_attempts

        if error_class == "transient" and attempts_next <= max_attempts:
            delay_ms = compute_backoff_ms(
                attempts_next, base_ms=config.unpinner_backoff_base_ms, max_ms=config.unpinner_backoff_max_ms
            )
            delay_sec = delay_ms / 1000.0

            worker_logger.info(
                f"Scheduling retry for {request.name} "
                f"(attempt {attempts_next}/{max_attempts}, delay {delay_sec:.1f}s, error_class={error_class})"
            )

            await enqueue_unpin_retry_request(
                request,
                delay_seconds=delay_sec,
                last_error=str(e),
            )

            get_metrics_collector().record_unpinner_operation(
                main_account=request.address,
                success=False,
                attempt=attempts_next,
            )
        else:
            worker_logger.warning(
                f"Unpin request {request.name} failed permanently or exhausted retries "
                f"(attempts={attempts_next}, error_class={error_class}, error={e}), pushing to DLQ"
            )
            await dlq_manager.push(request, str(e), error_class)

            get_metrics_collector().record_unpinner_operation(
                main_account=request.address,
                success=False,
                error_type=error_class,
            )


async def run_unpinner_loop() -> None:
    redis_client = async_redis.from_url(config.redis_url)
    redis_queues_client = async_redis.from_url(config.redis_queues_url)

    from hippius_s3.queue import initialize_queue_client
    from hippius_s3.redis_cache import initialize_cache_client

    initialize_queue_client(redis_queues_client)
    initialize_cache_client(redis_client)
    initialize_metrics_collector(redis_client)

    dlq_manager = UnpinDLQManager(redis_queues_client)

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

        ray_id = unpin_request.ray_id or "no-ray-id"
        ray_id_context.set(ray_id)
        worker_logger = get_logger_with_ray_id(__name__, ray_id)
        await process_unpin_request(unpin_request, worker_logger, dlq_manager)


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
