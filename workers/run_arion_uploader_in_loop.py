#!/usr/bin/env python3
import asyncio
import logging
import sys
import time
from pathlib import Path

import asyncpg
from pydantic import ValidationError

sys.path.insert(0, str(Path(__file__).parent.parent))

from opentelemetry import trace

from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.monitoring import initialize_metrics_collector
from hippius_s3.queue import dequeue_upload_request
from hippius_s3.queue import enqueue_retry_request
from hippius_s3.queue import move_due_upload_retries
from hippius_s3.redis_utils import with_redis_retry
from hippius_s3.services.arion_service import ArionClient
from hippius_s3.services.ray_id_service import get_logger_with_ray_id
from hippius_s3.services.ray_id_service import ray_id_context
from hippius_s3.workers.uploader import Uploader
from hippius_s3.workers.uploader import classify_error
from hippius_s3.workers.uploader import compute_backoff_ms


config = get_config()
tracer = trace.get_tracer(__name__)

setup_loki_logging(config, "arion-uploader")
logger = logging.getLogger(__name__)


async def run_arion_uploader_loop():
    from redis.asyncio import Redis

    from hippius_s3.queue import initialize_queue_client

    db_pool = await asyncpg.create_pool(config.database_url, min_size=2, max_size=10)
    redis_queues_client = Redis.from_url(config.redis_queues_url)

    initialize_queue_client(redis_queues_client)
    initialize_metrics_collector()

    arion_client = ArionClient()

    logger.info("Starting Arion uploader service...")
    logger.info("Backend: arion")

    uploader = Uploader(
        db_pool,
        redis_queues_client,
        config,
        backend_name="arion",
        backend_client=arion_client,
    )

    queue_name = "arion_upload_requests"

    while True:
        try:
            moved, redis_queues_client = await with_redis_retry(
                lambda rc: move_due_upload_retries(backend_name="arion", now_ts=time.time(), max_items=64),
                redis_queues_client,
                config.redis_queues_url,
                "move due retries",
            )
            if moved:
                logger.info(f"Moved {moved} due retry requests back to arion queue")
        except Exception as e:
            logger.error(f"Error moving retry requests: {e}")
            await asyncio.sleep(1)
            continue

        try:
            upload_request, redis_queues_client = await with_redis_retry(
                lambda rc: dequeue_upload_request(queue_name),
                redis_queues_client,
                config.redis_queues_url,
                "dequeue upload request",
            )
        except ValidationError as e:
            logger.error(f"Invalid queue data, skipping: {e}")
            await asyncio.sleep(0.1)
            continue

        if upload_request:
            ray_id = upload_request.ray_id or "no-ray-id"
            ray_id_context.set(ray_id)
            worker_logger = get_logger_with_ray_id(__name__, ray_id)

            worker_logger.info(
                f"Processing Arion upload request object_id={upload_request.object_id} chunks={len(upload_request.chunks)} attempts={upload_request.attempts or 0}"
            )

            with tracer.start_as_current_span(
                "uploader.job",
                attributes={
                    "object_id": upload_request.object_id,
                    "hippius.ray_id": ray_id,
                    "backend": "arion",
                    "attempts": upload_request.attempts or 0,
                },
            ) as span:
                try:
                    await uploader.process_upload(upload_request)
                except Exception as e:
                    span.record_exception(e)
                    span.set_status(trace.StatusCode.ERROR, str(e))
                    worker_logger.exception(f"Arion upload failed object_id={upload_request.object_id}")
                    err_str = str(e)
                    error_type = classify_error(e)
                    attempts_next = (upload_request.attempts or 0) + 1

                    if error_type == "transient" and attempts_next <= config.uploader_max_attempts:
                        delay_ms = compute_backoff_ms(
                            attempts_next, config.uploader_backoff_base_ms, config.uploader_backoff_max_ms
                        )
                        await enqueue_retry_request(
                            upload_request, backend_name="arion", delay_seconds=delay_ms / 1000.0, last_error=err_str
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
    asyncio.run(run_arion_uploader_loop())
