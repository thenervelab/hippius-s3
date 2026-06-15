"""Shared unpinner logic parameterized by backend_name and backend_client.

Each per-backend entry point (run_arion_unpinner_in_loop.py)
instantiates the correct client and calls run_unpinner_loop from here.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from typing import Protocol

import asyncpg
import redis.asyncio as async_redis
from opentelemetry import trace

from hippius_s3.config import get_config
from hippius_s3.dlq.unpin_dlq import UnpinDLQManager
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.monitoring import initialize_metrics_collector
from hippius_s3.queue import UnpinChainRequest
from hippius_s3.queue import dequeue_unpin_request
from hippius_s3.queue import enqueue_unpin_retry_request
from hippius_s3.queue import move_due_unpin_retries
from hippius_s3.redis_utils import create_redis_client
from hippius_s3.redis_utils import with_redis_retry
from hippius_s3.services.ray_id_service import get_logger_with_ray_id
from hippius_s3.services.ray_id_service import ray_id_context
from hippius_s3.utils import get_query
from hippius_s3.workers.errors import classify_unpin_error
from hippius_s3.workers.errors import compute_backoff_ms


logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


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
    sem: asyncio.Semaphore | None = None,
) -> None:
    """Process a single unpin request for a specific backend.

    A request expands to N chunk identifiers; their backend DELETEs run concurrently bounded by the
    shared per-pod `sem` (passed by the loop so all in-flight requests share one Arion-DELETE budget).
    Falls back to a per-request semaphore when called standalone (e.g. tests).
    """
    config = get_config()
    if sem is None:
        sem = asyncio.Semaphore(max(1, int(config.unpinner_parallelism)))

    with tracer.start_as_current_span(
        "unpinner.process_unpin",
        attributes={
            "object_id": request.object_id,
            "object_version": str(request.object_version or "all"),
            "backend": backend_name,
            "hippius.account.main": request.address,
        },
    ) as span:
        try:
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

            span.set_attribute("num_identifiers", len(rows))

            async with backend_client_factory() as client:
                # Unpin each identifier concurrently, bounded by the shared per-pod `sem`. Each
                # identifier is best-effort (a failed DELETE/soft-delete logs a warning but must not
                # fail the whole request), mirroring the original serial behavior.
                async def _unpin_one(row: Any) -> None:
                    identifier = row["backend_identifier"]
                    chunk_id = row["chunk_id"]
                    async with sem:
                        try:
                            await client.unpin_file(identifier, account_ss58=request.address)
                            worker_logger.info(f"Unpinned {backend_name} identifier={identifier}")
                        except Exception as unpin_err:
                            worker_logger.warning(
                                f"Failed to unpin {backend_name} identifier={identifier}: {unpin_err}"
                            )

                        try:
                            async with db_pool.acquire() as conn:
                                await conn.fetchval(
                                    get_query("soft_delete_chunk_backend_by_chunk_id"),
                                    backend_name,
                                    chunk_id,
                                )
                        except Exception as db_err:
                            worker_logger.warning(
                                f"Failed to soft-delete chunk_backend row chunk_id={chunk_id}: {db_err}"
                            )

                await asyncio.gather(*[_unpin_one(row) for row in rows])

            get_metrics_collector().record_unpinner_operation(
                main_account=request.address,
                success=True,
                backend=backend_name,
            )

        except Exception as e:
            span.record_exception(e)
            span.set_status(trace.StatusCode.ERROR, str(e))
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
                    backend=backend_name,
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
                    backend=backend_name,
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

    redis_client = create_redis_client(config.redis_url)
    redis_queues_client = async_redis.from_url(config.redis_queues_url)

    delete_concurrency = max(1, int(config.unpinner_parallelism))
    max_inflight = max(1, int(config.unpinner_max_inflight))
    # Peak concurrent pool acquires = up to `delete_concurrency` soft-deletes (each held inside
    # delete_sem) + up to `max_inflight` per-request initial fetches. Floor the pool to that sum so
    # scaling the concurrency knobs can never starve acquire() and silently wedge the pod (asyncpg
    # acquire blocks indefinitely with no timeout).
    pool_max = max(2, int(config.unpinner_db_pool_max), delete_concurrency + max_inflight)
    db_pool = await asyncpg.create_pool(
        dsn=config.database_url,
        min_size=2,
        max_size=pool_max,
    )

    from hippius_s3.queue import initialize_queue_client
    from hippius_s3.redis_cache import initialize_cache_client

    initialize_queue_client(redis_queues_client)
    initialize_cache_client(redis_client)
    initialize_metrics_collector(redis_client)

    dlq_manager = UnpinDLQManager(redis_queues_client)

    delete_sem = asyncio.Semaphore(delete_concurrency)
    logger.info(
        f"Starting {backend_name} unpinner service (queue={queue_name} max_inflight={max_inflight} "
        f"delete_concurrency={delete_concurrency} db_pool_max={pool_max})"
    )

    async def _handle_unpin(request: UnpinChainRequest) -> None:
        ray_id = request.ray_id or "no-ray-id"
        ray_id_context.set(ray_id)
        worker_logger = get_logger_with_ray_id(__name__, ray_id)
        with tracer.start_as_current_span(
            "unpinner.job",
            attributes={
                "object_id": request.object_id,
                "hippius.ray_id": ray_id,
                "backend": backend_name,
                "hippius.account.main": request.address,
                "attempts": request.attempts or 0,
            },
        ):
            await process_unpin_request(
                request,
                backend_name=backend_name,
                backend_client_factory=backend_client_factory,
                worker_logger=worker_logger,
                dlq_manager=dlq_manager,
                db_pool=db_pool,
                sem=delete_sem,
            )

    # Periodic retry-mover — one per pod, off the per-request hot path (running it per dequeue across
    # N concurrent workers would multiply Redis load).
    async def _retry_mover() -> None:
        while True:
            try:
                await move_due_unpin_retries(backend_name=backend_name)
            except Exception as e:
                logger.error(f"Error moving {backend_name} unpin retries: {e}")
            await asyncio.sleep(2.0)

    inflight: set[asyncio.Task[None]] = set()

    def _reap(tasks: set[asyncio.Task[None]]) -> None:
        # Per-request failures are already routed to retry/DLQ inside process_unpin_request (which
        # never re-raises), so a task raises here only on a routing-path failure (rare).
        for t in tasks:
            inflight.discard(t)
            err = t.exception()
            if err is not None and not isinstance(err, asyncio.CancelledError):
                logger.error(f"inflight {backend_name} unpinner task error: {err}")

    mover_task = asyncio.create_task(_retry_mover())
    try:
        while True:
            _reap({t for t in inflight if t.done()})

            # Capacity gate — keep at most max_inflight requests processing at once.
            if len(inflight) >= max_inflight:
                done_wait, _ = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
                _reap(done_wait)
                continue

            unpin_request, redis_queues_client = await with_redis_retry(
                lambda rc: dequeue_unpin_request(queue_name),
                redis_queues_client,
                config.redis_queues_url,
                f"dequeue {backend_name} unpin request",
            )

            if not unpin_request:
                if inflight:
                    done_wait, _ = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED, timeout=0.5)
                    _reap(done_wait)
                else:
                    await asyncio.sleep(0.1)
                continue

            inflight.add(asyncio.create_task(_handle_unpin(unpin_request)))
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info(f"{backend_name} unpinner stopping…")
    finally:
        mover_task.cancel()
        for t in inflight:
            t.cancel()
        # gather (not suppress) so cancelled tasks' CancelledError is absorbed here.
        await asyncio.gather(mover_task, *inflight, return_exceptions=True)
        if db_pool:
            await db_pool.close()
