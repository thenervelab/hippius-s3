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
from hippius_s3.services.hippius_api_service import UnpinResponse
from hippius_s3.workers.uploader import classify_error
from hippius_s3.workers.uploader import compute_backoff_ms


config = get_config()

setup_loki_logging(config, "unpinner")
logger = logging.getLogger(__name__)


async def unpin_on_api(cids: list[str]) -> list[UnpinResponse]:
    if not cids:
        return []

    semaphore = asyncio.Semaphore(config.unpinner_parallelism)

    async def unpin_with_limit(api_client: HippiusApiClient, cid: str) -> UnpinResponse:
        async with semaphore:
            return await api_client.unpin_file(cid)

    async with HippiusApiClient() as api_client:
        return await asyncio.gather(*[unpin_with_limit(api_client, cid) for cid in cids])


async def unpin_from_local_ipfs(cid: str) -> None:
    """Unpin a CID from the local IPFS node."""
    base_url = config.ipfs_store_url.rstrip("/")
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            unpin_url = f"{base_url}/api/v0/pin/rm?arg={cid}"
            logger.debug(f"Unpinning CID from local IPFS: {cid}")
            unpin_response = await client.post(unpin_url)
            unpin_response.raise_for_status()
            logger.debug(f"Successfully unpinned CID from local IPFS: {cid}")
    except Exception as e:
        logger.warning(f"Failed to unpin CID {cid} from local IPFS: {e}")


async def process_unpin_request(request: UnpinChainRequest) -> None:
    """Process a single unpin request."""
    try:
        logger.info(f"Processing unpin request: {request.name}")

        await unpin_on_api([request.cid])
        logger.info(f"Successfully unpinned CID via API: {request.cid}")

        await unpin_from_local_ipfs(request.cid)

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
