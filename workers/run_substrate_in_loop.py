#!/usr/bin/env python3
import asyncio
import logging
import sys
from pathlib import Path

import asyncpg
import redis.asyncio as async_redis


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.monitoring import initialize_metrics_collector
from hippius_s3.queue import dequeue_substrate_request
from hippius_s3.queue import enqueue_substrate_request
from hippius_s3.redis_utils import with_redis_retry
from hippius_s3.workers.substrate import SubstrateWorker


config = get_config()

setup_loki_logging(config, "substrate")
logger = logging.getLogger(__name__)

EPOCH_SLEEP_SECONDS = 600


async def run_substrate_loop():
    db = await asyncpg.connect(config.database_url)
    redis_client = async_redis.from_url(config.redis_url)
    redis_queues_client = async_redis.from_url(config.redis_queues_url)

    from hippius_s3.queue import initialize_queue_client

    initialize_queue_client(redis_queues_client)
    initialize_metrics_collector(redis_client)

    logger.info("Starting substrate service...")
    logger.info(f"Redis URL: {config.redis_url}")
    logger.info(f"Redis Queues URL: {config.redis_queues_url}")
    logger.info(f"Database connected: {config.database_url}")

    substrate_worker = SubstrateWorker(db, config)

    while True:
        requests = []

        while True:
            substrate_request, redis_queues_client = await with_redis_retry(
                lambda rc: dequeue_substrate_request(),
                redis_queues_client,
                config.redis_queues_url,
                "dequeue substrate request",
            )

            if substrate_request:
                if not config.publish_to_chain:
                    logger.info(f"Skipping substrate publish (disabled) object_id={substrate_request.object_id}")
                    await db.execute(
                        "UPDATE object_versions SET status = 'uploaded' WHERE object_id = $1 AND object_version = $2",
                        substrate_request.object_id,
                        int(getattr(substrate_request, "object_version", 1) or 1),
                    )
                    continue
                requests.append(substrate_request)
            else:
                break

        if not requests:
            logger.info("No substrate requests in queue, sleeping 10s...")
            await asyncio.sleep(10)
            continue

        logger.info(f"Collected {len(requests)} substrate requests from queue")

        try:
            await substrate_worker.process_batch(requests)
            logger.info(f"Successfully submitted batch of {len(requests)} requests")
        except Exception:
            logger.exception(f"Substrate batch submission failed for {len(requests)} requests")
            for req in requests:
                await enqueue_substrate_request(req)
            logger.info(f"Requeued {len(requests)} failed requests")
        finally:
            logger.info(f"Sleeping {EPOCH_SLEEP_SECONDS}s until next epoch...")
            await asyncio.sleep(EPOCH_SLEEP_SECONDS)


if __name__ == "__main__":
    asyncio.run(run_substrate_loop())
