"""Shared unpinner logic parameterized by backend_name and backend_client.

Each per-backend entry point (run_ipfs_unpinner_in_loop.py, run_arion_unpinner_in_loop.py)
instantiates the correct client and calls run_unpinner_loop from here.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from typing import Protocol

import asyncpg
import redis.asyncio as async_redis

from hippius_s3.config import get_config
from hippius_s3.dlq.unpin_dlq import UnpinDLQManager
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.monitoring import initialize_metrics_collector
from hippius_s3.queue import UnpinChainRequest
from hippius_s3.queue import dequeue_unpin_request
from hippius_s3.queue import enqueue_unpin_retry_request
from hippius_s3.queue import move_due_unpin_retries
from hippius_s3.redis_utils import with_redis_retry
from hippius_s3.services.ray_id_service import get_logger_with_ray_id
from hippius_s3.services.ray_id_service import ray_id_context
from hippius_s3.utils import get_query
from hippius_s3.workers.error_classifier import classify_unpin_error
from hippius_s3.workers.uploader import compute_backoff_ms


logger = logging.getLogger(__name__)


class UnpinBackendClient(Protocol):
    """Protocol for backend clients that can unpin files."""

    async def unpin_file(self, identifier: str, **kwargs: Any) -> Any: ...

    async def __aenter__(self) -> Any: ...

    async def __aexit__(self, *args: Any) -> Any: ...


async def process_unpin_request(
    request: UnpinChainRequest,
    *,
    backend_name: str,
    backend_client_factory: Any,
    worker_logger: logging.LoggerAdapter,
    dlq_manager: UnpinDLQManager,
    db_pool: asyncpg.Pool,
) -> None:
    """Process a single unpin request for a specific backend."""
    config = get_config()

    try:
        # Query chunk_backend for identifiers belonging to this backend
        async with db_pool.acquire() as conn:
            obj_version = request.object_version
            rows = await conn.fetch(
                get_query("get_chunk_backend_identifiers"),
                backend_name,
                request.object_id,
                obj_version,
            )

        if not rows:
            max_empty_retries = 6
            attempts = request.attempts or 0
            if attempts < max_empty_retries:
                next_attempt = attempts + 1
                delay_ms = compute_backoff_ms(
                    next_attempt,
                    base_ms=config.unpinner_backoff_base_ms,
                    max_ms=config.unpinner_backoff_max_ms,
                )
                worker_logger.info(
                    f"No {backend_name} identifiers found for object_id={request.object_id} "
                    f"version={request.object_version}, scheduling retry "
                    f"(attempt {next_attempt}/{max_empty_retries}, "
                    f"delay={delay_ms / 1000.0:.1f}s)"
                )
                await enqueue_unpin_retry_request(
                    request,
                    backend_name=backend_name,
                    delay_seconds=delay_ms / 1000.0,
                    last_error="no_chunk_backend_rows",
                )
                return
            worker_logger.info(
                f"No {backend_name} identifiers found for object_id={request.object_id} "
                f"version={request.object_version} after {max_empty_retries} retry attempts, "
                f"nothing to unpin"
            )
            return

        worker_logger.info(
            f"Processing unpin for {backend_name}: object_id={request.object_id} identifiers={len(rows)}"
        )

        # Unpin each identifier via the backend client
        async with backend_client_factory() as client:
            for row in rows:
                identifier = row["backend_identifier"]
                chunk_id = row["chunk_id"]
                try:
                    await client.unpin_file(identifier, account_ss58=request.address)
                    worker_logger.info(f"Unpinned {backend_name} identifier={identifier}")
                except Exception as unpin_err:
                    worker_logger.warning(f"Failed to unpin {backend_name} identifier={identifier}: {unpin_err}")
                    # Continue to soft-delete the row anyway — the backend may have
                    # already removed it, or it will be retried via the DLQ.

                # Soft-delete the chunk_backend row
                try:
                    async with db_pool.acquire() as conn:
                        await conn.fetchval(
                            get_query("soft_delete_chunk_backend_by_chunk_id"),
                            backend_name,
                            chunk_id,
                        )
                except Exception as db_err:
                    worker_logger.warning(f"Failed to soft-delete chunk_backend row chunk_id={chunk_id}: {db_err}")

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
                backend_name=backend_name,
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


async def run_unpinner_loop(
    *,
    backend_name: str,
    backend_client_factory: Any,
    queue_name: str,
) -> None:
    """Main loop for a per-backend unpinner worker."""
    config = get_config()

    redis_client = async_redis.from_url(config.redis_url)
    redis_queues_client = async_redis.from_url(config.redis_queues_url)
    db_pool = await asyncpg.create_pool(
        dsn=config.database_url,
        min_size=1,
        max_size=3,
    )

    from hippius_s3.queue import initialize_queue_client
    from hippius_s3.redis_cache import initialize_cache_client

    initialize_queue_client(redis_queues_client)
    initialize_cache_client(redis_client)
    initialize_metrics_collector(redis_client)

    dlq_manager = UnpinDLQManager(redis_queues_client)

    logger.info(f"Starting {backend_name} unpinner service...")
    logger.info(f"Queue: {queue_name}")
    logger.info(f"Redis URL: {config.redis_url}")
    logger.info(f"Redis Queues URL: {config.redis_queues_url}")
    logger.info(f"Database: {config.database_url}")

    try:
        while True:
            await move_due_unpin_retries(backend_name=backend_name)

            unpin_request, redis_queues_client = await with_redis_retry(
                lambda rc: dequeue_unpin_request(queue_name),
                redis_queues_client,
                config.redis_queues_url,
                f"dequeue {backend_name} unpin request",
            )

            if not unpin_request:
                await asyncio.sleep(1)
                continue

            ray_id = unpin_request.ray_id or "no-ray-id"
            ray_id_context.set(ray_id)
            worker_logger = get_logger_with_ray_id(__name__, ray_id)
            await process_unpin_request(
                unpin_request,
                backend_name=backend_name,
                backend_client_factory=backend_client_factory,
                worker_logger=worker_logger,
                dlq_manager=dlq_manager,
                db_pool=db_pool,
            )
    finally:
        if db_pool:
            await db_pool.close()
