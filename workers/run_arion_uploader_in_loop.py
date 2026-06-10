#!/usr/bin/env python3
import asyncio
import contextlib
import logging
import sys
import time
from pathlib import Path

import asyncpg
from pydantic import ValidationError

from hippius_s3.redis_cache import initialize_cache_client


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
from hippius_s3.sentry import init_sentry
from hippius_s3.services.arion_service import ArionClient
from hippius_s3.services.ray_id_service import get_logger_with_ray_id
from hippius_s3.services.ray_id_service import ray_id_context
from hippius_s3.workers.errors import classify_error
from hippius_s3.workers.errors import compute_backoff_ms
from hippius_s3.workers.errors import extract_http_status_code
from hippius_s3.workers.uploader import Uploader


config = get_config()
tracer = trace.get_tracer(__name__)

setup_loki_logging(config, "arion-uploader")
logger = logging.getLogger(__name__)
init_sentry("arion-uploader", is_worker=True)


async def _handle_upload(uploader, db_pool, upload_request) -> None:
    """Process one upload request and route failures to retry/DLQ.

    This is the body the serial loop used to run inline; it now runs as a
    bounded-concurrent task so a slow object never blocks other requests.
    """
    ray_id = upload_request.ray_id or "no-ray-id"
    ray_id_context.set(ray_id)
    worker_logger = get_logger_with_ray_id(__name__, ray_id)
    worker_logger.info(
        f"Processing Arion upload request object_id={upload_request.object_id} "
        f"chunks={len(upload_request.chunks)} attempts={upload_request.attempts or 0}"
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
            status_code = extract_http_status_code(e)
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
                    backend="arion",
                    attempt=attempts_next,
                    status_code=status_code,
                )
            else:
                await uploader._push_to_dlq(upload_request, err_str, error_type, status_code=status_code)
                async with db_pool.acquire() as db:
                    await db.execute(
                        "UPDATE object_versions SET status = 'failed' WHERE object_id = $1 AND object_version = $2",
                        upload_request.object_id,
                        int(getattr(upload_request, "object_version", 1) or 1),
                    )


async def run_arion_uploader_loop():
    from redis.asyncio import Redis

    from hippius_s3.queue import initialize_queue_client
    from hippius_s3.redis_utils import create_redis_client

    pool_max = max(2, int(config.uploader_db_pool_max))
    db_pool = await asyncpg.create_pool(config.database_url, min_size=2, max_size=pool_max)
    redis_client = create_redis_client(config.redis_url)
    redis_queues_client = Redis.from_url(config.redis_queues_url)

    initialize_queue_client(redis_queues_client)
    initialize_cache_client(redis_client)
    initialize_metrics_collector(redis_client)

    arion_client = ArionClient()
    uploader = Uploader(
        db_pool, redis_client, redis_queues_client, config, backend_name="arion", backend_client=arion_client
    )

    queue_name = "arion_upload_requests"
    max_inflight = max(1, int(config.uploader_max_inflight))
    logger.info(
        f"Starting Arion uploader service (max_inflight={max_inflight} "
        f"arion_concurrency={config.arion_upload_concurrency} db_pool_max={pool_max})"
    )

    # Periodic retry-mover — one per pod, off the per-request hot path (running it
    # per dequeue across N concurrent workers would multiply Redis load).
    async def _retry_mover() -> None:
        while True:
            try:
                # `dequeue_upload_request` / `move_due_upload_retries` use the module-global
                # queue client (set by initialize_queue_client), not the passed `rc`, so the
                # client returned here is unused — don't reassign the shared var (the main
                # loop owns it; concurrent reassignment would be a race).
                moved, _ = await with_redis_retry(
                    lambda rc: move_due_upload_retries(backend_name="arion", now_ts=time.time(), max_items=256),
                    redis_queues_client,
                    config.redis_queues_url,
                    "move due retries",
                )
                if moved:
                    logger.info(f"Moved {moved} due retry requests back to arion queue")
            except Exception as e:
                logger.error(f"Error moving retry requests: {e}")
            await asyncio.sleep(2.0)

    inflight: set[asyncio.Task[None]] = set()

    def _reap(tasks: set[asyncio.Task[None]]) -> None:
        # Surface task errors only. Per-request failures are already routed to retry/DLQ
        # inside _handle_upload (which never re-raises), so a task raises here only on a
        # routing-path failure. The asyncpg pool self-heals dropped connections on the
        # next acquire and the queue client recovers via its own pool, so there is no
        # separate pod-level reconnect to perform.
        for t in tasks:
            inflight.discard(t)
            err = t.exception()
            if err is not None and not isinstance(err, asyncio.CancelledError):
                logger.error(f"inflight uploader task error: {err}")

    mover_task = asyncio.create_task(_retry_mover())
    try:
        while True:
            _reap({t for t in inflight if t.done()})

            # Capacity gate — keep at most max_inflight requests processing at once.
            if len(inflight) >= max_inflight:
                done_wait, _ = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
                _reap(done_wait)
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

            if upload_request is None:
                if inflight:
                    done_wait, _ = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED, timeout=0.5)
                    _reap(done_wait)
                else:
                    await asyncio.sleep(0.1)
                continue

            inflight.add(asyncio.create_task(_handle_upload(uploader, db_pool, upload_request)))
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Arion uploader stopping…")
    finally:
        mover_task.cancel()
        for t in inflight:
            t.cancel()
        # gather (not suppress) so cancelled tasks' CancelledError — a BaseException
        # that contextlib.suppress(Exception) would NOT catch — is absorbed here.
        await asyncio.gather(mover_task, *inflight, return_exceptions=True)
        with contextlib.suppress(Exception):
            await redis_client.aclose()
        with contextlib.suppress(Exception):
            await redis_queues_client.aclose()
        with contextlib.suppress(Exception):
            await db_pool.close()


if __name__ == "__main__":
    asyncio.run(run_arion_uploader_loop())
