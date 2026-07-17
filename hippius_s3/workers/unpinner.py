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
from hippius_s3.services.arion_service import BatchEndpointUnavailable
from hippius_s3.services.ray_id_service import get_logger_with_ray_id
from hippius_s3.services.ray_id_service import ray_id_context
from hippius_s3.utils import get_query
from hippius_s3.workers.errors import classify_unpin_error
from hippius_s3.workers.errors import compute_backoff_ms


logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

# HCFS caps POST /delete_files at 1000 file_ids per call. The loop clamps to this regardless of the
# configured batch size.
_HCFS_BATCH_HARD_CAP = 1000
# Short brpop timeout used to non-blockingly drain extra requests into one batch after the first
# blocking dequeue, and the wall-clock ceiling on that assembly window.
_ASSEMBLY_DRAIN_TIMEOUT = 0.05
_ASSEMBLY_WINDOW_SECONDS = 0.25


class UnpinBackendClient(Protocol):
    """Protocol for backend clients that can unpin files."""

    async def unpin_file(self, identifier: str, **kwargs: Any) -> Any: ...

    async def unpin_files_batch(self, file_ids: list[str], account_ss58: str, folder_hash: str = "") -> Any: ...

    async def __aenter__(self) -> Any: ...

    async def __aexit__(self, *args: Any) -> Any: ...


class _ReqEntry:
    """One request inside a batch group, plus the set of file_ids it contributed."""

    def __init__(self, request: UnpinChainRequest) -> None:
        self.request = request
        self.file_ids: set[str] = set()


class _BatchGroup:
    """Accumulates requests that share one (address, folder_hash) so their chunk deletes coalesce
    into a single POST /delete_files payload. A file_id may map to MULTIPLE chunk_ids (backend
    identifiers are not unique) and may be contributed by MULTIPLE requests — both are unioned so the
    payload is deduped while every owning chunk_id / request is still tracked for fan-out."""

    def __init__(self, address: str, folder_hash: str) -> None:
        self.address = address
        self.folder_hash = folder_hash
        self.entries: list[_ReqEntry] = []
        self.file_id_chunk_ids: dict[str, set[int]] = {}

    def add(self, request: UnpinChainRequest, rows: list[Any]) -> int:
        """Add a request's chunk_backend rows; returns how many NEW unique file_ids it introduced."""
        before = len(self.file_id_chunk_ids)
        entry = _ReqEntry(request)
        for row in rows:
            file_id = row["backend_identifier"]
            chunk_id = row["chunk_id"]
            entry.file_ids.add(file_id)
            self.file_id_chunk_ids.setdefault(file_id, set()).add(chunk_id)
        self.entries.append(entry)
        return len(self.file_id_chunk_ids) - before


async def _route_failed_request(
    request: UnpinChainRequest,
    *,
    backend_name: str,
    error_class: str,
    last_error: str,
    worker_logger: logging.LoggerAdapter,
    dlq_manager: UnpinDLQManager,
    config: Any,
) -> None:
    """Route a request that did NOT fully succeed to retry (transient, budget remaining) or DLQ —
    mirrors process_unpin_request's except-block routing so the batch path behaves identically."""
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
            last_error=last_error,
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
            f"(attempts={attempts_next}, error_class={error_class}, error={last_error}), pushing to DLQ"
        )
        await dlq_manager.push(request, last_error, error_class)
        get_metrics_collector().record_unpinner_operation(
            main_account=request.address,
            success=False,
            backend=backend_name,
            error_type=error_class,
        )


async def _fallback_per_file(
    group: _BatchGroup,
    *,
    backend_name: str,
    client: Any,
    worker_logger: logging.LoggerAdapter,
    dlq_manager: UnpinDLQManager,
    db_pool: asyncpg.Pool,
    sem: asyncio.Semaphore,
) -> None:
    """Batch endpoint 404'd → run the existing per-file path for every request in the group. Each
    process_unpin_request re-fetches its rows and owns its own DELETE + soft-delete + retry/DLQ
    routing, so the group's requests get exactly the legacy semantics."""
    worker_logger.warning(
        f"Batch endpoint unavailable (404) for {backend_name}; falling back to per-file unpin for "
        f"{len(group.entries)} request(s)"
    )
    await asyncio.gather(
        *[
            process_unpin_request(
                entry.request,
                backend_name=backend_name,
                client=client,
                worker_logger=worker_logger,
                dlq_manager=dlq_manager,
                db_pool=db_pool,
                sem=sem,
            )
            for entry in group.entries
        ]
    )


async def process_unpin_batch(
    group: _BatchGroup,
    *,
    backend_name: str,
    client: Any,
    worker_logger: logging.LoggerAdapter,
    dlq_manager: UnpinDLQManager,
    db_pool: asyncpg.Pool,
    sem: asyncio.Semaphore,
    config: Any,
    max_files: int,
) -> None:
    """Delete a group's deduped file_ids via POST /delete_files (split into <=max_files sub-batches),
    then fan out per HCFS-item status and route each owning request.

    Invariant A9: a chunk is soft-deleted ONLY when its file_id's backend delete succeeded
    (status deleted/already_deleted) AND the soft-delete DB write itself succeeded. A request is
    acked ONLY when ALL of its file_ids cleared both gates; otherwise it is routed to retry/DLQ.
    """
    all_file_ids = list(group.file_id_chunk_ids.keys())
    cap = max(1, min(int(max_files), _HCFS_BATCH_HARD_CAP))

    resolved_ok: set[str] = set()
    errored: dict[str, str] = {}  # file_id -> error_class
    error_msgs: dict[str, str] = {}

    for start in range(0, len(all_file_ids), cap):
        sub = all_file_ids[start : start + cap]
        async with sem:
            try:
                result = await client.unpin_files_batch(sub, group.address, group.folder_hash)
            except BatchEndpointUnavailable:
                # Old server without the endpoint — fall back the whole group to per-file. No
                # soft-deletes have happened yet for THIS group in the batch path.
                await _fallback_per_file(
                    group,
                    backend_name=backend_name,
                    client=client,
                    worker_logger=worker_logger,
                    dlq_manager=dlq_manager,
                    db_pool=db_pool,
                    sem=sem,
                )
                return
            except Exception as batch_err:
                # Whole sub-batch failed (500/timeout/auth/network) after retry_on_error exhausted.
                # NO soft-deletes; classify and route every owning request accordingly.
                ec = classify_unpin_error(batch_err)
                worker_logger.warning(
                    f"Batch delete call failed for {backend_name} ({len(sub)} file_ids, error_class={ec}): {batch_err}"
                )
                for file_id in sub:
                    errored[file_id] = ec
                    error_msgs[file_id] = str(batch_err)
                continue

        seen: set[str] = set()
        for item in result.deleted:
            resolved_ok.add(item.file_id)
            seen.add(item.file_id)
        for err in result.errors:
            ec = classify_unpin_error(Exception(f"{err.code}: {err.message or ''}"))
            errored[err.file_id] = ec
            error_msgs[err.file_id] = f"{err.code}: {err.message or ''}"
            seen.add(err.file_id)
        # Defensive: a sent file_id absent from BOTH deleted and errors is a contract violation —
        # never soft-delete it silently; treat as a transient failure so its request is retried.
        for file_id in sub:
            if file_id not in seen:
                errored.setdefault(file_id, "transient")
                error_msgs.setdefault(file_id, "file_id missing from batch response")

    # A9 gate: soft-delete only file_ids whose backend delete succeeded. A file_id counts as fully
    # cleared only if EVERY one of its chunk_ids soft-deleted without a DB error.
    soft_deleted_ok: set[str] = set()

    async def _soft_delete_file(file_id: str) -> None:
        ok = True
        for chunk_id in group.file_id_chunk_ids[file_id]:
            try:
                async with db_pool.acquire() as conn:
                    await conn.fetchval(
                        get_query("soft_delete_chunk_backend_by_chunk_id"),
                        backend_name,
                        chunk_id,
                    )
            except Exception as db_err:
                ok = False
                worker_logger.warning(
                    f"Failed to soft-delete chunk_backend row chunk_id={chunk_id} (file_id={file_id}): {db_err}"
                )
        if ok:
            soft_deleted_ok.add(file_id)

    await asyncio.gather(*[_soft_delete_file(file_id) for file_id in resolved_ok])

    for entry in group.entries:
        request = entry.request
        if all(file_id in soft_deleted_ok for file_id in entry.file_ids):
            get_metrics_collector().record_unpinner_operation(
                main_account=request.address,
                success=True,
                backend=backend_name,
            )
            continue

        classes: list[str] = []
        msgs: list[str] = []
        for file_id in entry.file_ids:
            if file_id in soft_deleted_ok:
                continue
            if file_id in errored:
                classes.append(errored[file_id])
                msgs.append(error_msgs.get(file_id, ""))
            else:
                # Backend-deleted but the soft-delete DB write failed — retry (A9: never ack).
                classes.append("transient")
                msgs.append(f"soft-delete failed for file_id={file_id}")

        if "permanent" in classes:
            error_class = "permanent"
        elif "transient" in classes:
            error_class = "transient"
        else:
            error_class = "unknown"
        last_error = "; ".join(m for m in msgs if m)[:500] or "batch unpin partial failure"

        await _route_failed_request(
            request,
            backend_name=backend_name,
            error_class=error_class,
            last_error=last_error,
            worker_logger=worker_logger,
            dlq_manager=dlq_manager,
            config=config,
        )


async def process_unpin_request(
    request: UnpinChainRequest,
    *,
    backend_name: str,
    client: UnpinBackendClient | None = None,
    backend_client_factory: Any = None,
    worker_logger: logging.LoggerAdapter,
    dlq_manager: UnpinDLQManager,
    db_pool: asyncpg.Pool,
    sem: asyncio.Semaphore | None = None,
) -> None:
    """Process a single unpin request for a specific backend.

    A request expands to N chunk identifiers; their backend DELETEs run concurrently bounded by the
    shared per-pod `sem` (passed by the loop so all in-flight requests share one Arion-DELETE budget).
    Falls back to a per-request semaphore when called standalone (e.g. tests).

    The loop passes its long-lived `client` so every request reuses one Arion connection pool (no TLS
    handshake per request, which at high concurrency would storm ephemeral ports). When `client` is
    None (standalone callers/tests) one is constructed from `backend_client_factory` for this request.
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

            # Unpin each identifier concurrently, bounded by the shared per-pod `sem`. Each
            # identifier is best-effort (a failed DELETE/soft-delete logs a warning but must not
            # fail the whole request), mirroring the original serial behavior.
            async def _unpin_all(active_client: Any) -> None:
                async def _unpin_one(row: Any) -> None:
                    identifier = row["backend_identifier"]
                    chunk_id = row["chunk_id"]
                    async with sem:
                        try:
                            await active_client.unpin_file(identifier, account_ss58=request.address)
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

            # Reuse the loop's live client when supplied; only build (and close) a per-request client
            # for standalone callers/tests that don't hand one in.
            if client is not None:
                await _unpin_all(client)
            else:
                async with backend_client_factory() as owned_client:
                    await _unpin_all(owned_client)

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
    # DB connection budget. A dispatched request briefly holds one pool conn for its initial
    # chunk-identifier fetch (released before any DELETE), then in its gather phase holds up to
    # `delete_concurrency` conns for concurrent soft-deletes (bounded by the shared per-pod
    # `delete_sem`). So the throughput-optimal per-pod pool is ~ `max_inflight` (fetch conns) +
    # `delete_concurrency` (soft-delete conns). We CAP that at HIPPIUS_UNPINNER_DB_POOL_MAX so raising
    # max_inflight (an ops secret) can't balloon Postgres connections — per pod × replicas already
    # runs close to the server's max_connections. The cap may trade throughput for connection count
    # but is clamped up to the deadlock-safe floor: one request can hold up to `delete_concurrency`
    # soft-delete conns at once, +1 so the next request's fetch always makes progress. (Nothing
    # acquires a second conn while holding one and asyncpg acquire() has no timeout, so an undersized
    # pool throttles rather than deadlocks — the floor just keeps liveness comfortable.)
    ideal_pool = delete_concurrency + max_inflight
    deadlock_floor = delete_concurrency + 1
    capped_pool = min(ideal_pool, int(config.unpinner_db_pool_max))
    if capped_pool < deadlock_floor:
        logger.warning(
            f"HIPPIUS_UNPINNER_DB_POOL_MAX={config.unpinner_db_pool_max} is below the deadlock-safe "
            f"floor {deadlock_floor} (parallelism+1); honoring the floor instead"
        )
        capped_pool = deadlock_floor
    pool_max = max(2, capped_pool)
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

    batch_enabled = bool(config.unpinner_batch_delete_enabled)
    batch_max_files = max(1, min(int(config.unpinner_batch_max_files), _HCFS_BATCH_HARD_CAP))
    folder_hash = config.unpinner_folder_hash or ""

    logger.info(
        f"Starting {backend_name} unpinner service (queue={queue_name} max_inflight={max_inflight} "
        f"delete_concurrency={delete_concurrency} db_pool_max={pool_max} batch_delete={batch_enabled} "
        f"batch_max_files={batch_max_files} folder_hash={folder_hash!r})"
    )

    async def _handle_unpin(request: UnpinChainRequest, client: Any) -> None:
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
                client=client,
                worker_logger=worker_logger,
                dlq_manager=dlq_manager,
                db_pool=db_pool,
                sem=delete_sem,
            )

    async def _handle_no_identifiers(request: UnpinChainRequest, worker_logger: logging.LoggerAdapter) -> None:
        # Same retry-then-drop behavior as the per-file path: a request with no chunk_backend rows
        # yet is retried up to 6 times (pin commit may not have landed) then dropped.
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
                f"(attempt {next_attempt}/{max_empty_retries}, delay={delay_ms / 1000.0:.1f}s)"
            )
            await enqueue_unpin_retry_request(
                request,
                backend_name=backend_name,
                delay_seconds=delay_ms / 1000.0,
                last_error="no_chunk_backend_rows",
            )
        else:
            worker_logger.info(
                f"No {backend_name} identifiers found for object_id={request.object_id} "
                f"version={request.object_version} after {max_empty_retries} retry attempts, nothing to unpin"
            )

    async def _dequeue_one(block_timeout: float) -> UnpinChainRequest | None:
        nonlocal redis_queues_client
        req, redis_queues_client = await with_redis_retry(
            lambda rc: dequeue_unpin_request(queue_name, block_timeout=block_timeout),
            redis_queues_client,
            config.redis_queues_url,
            f"dequeue {backend_name} unpin request",
        )
        return req

    async def _assemble_batch(first: UnpinChainRequest) -> dict[tuple[str, str], _BatchGroup]:
        # Accumulate requests, grouped by (address, folder_hash), fetching each request's
        # chunk_backend rows so we can bound the batch by file_id count. Stops when a group fills to
        # the HCFS cap, the queue drains, or the assembly window elapses. folder_hash is effectively
        # constant (config default) so in practice every request lands in one group per address.
        groups: dict[tuple[str, str], _BatchGroup] = {}
        deadline = asyncio.get_event_loop().time() + _ASSEMBLY_WINDOW_SECONDS
        req: UnpinChainRequest | None = first
        while req is not None:
            ray_id = req.ray_id or "no-ray-id"
            req_logger = get_logger_with_ray_id(__name__, ray_id)
            async with db_pool.acquire() as conn:
                rows = await conn.fetch(
                    get_query("get_chunk_backend_identifiers"),
                    backend_name,
                    req.object_id,
                    req.object_version,
                )
            if not rows:
                await _handle_no_identifiers(req, req_logger)
            else:
                key = (req.address, folder_hash)
                group = groups.get(key)
                if group is None:
                    group = _BatchGroup(req.address, folder_hash)
                    groups[key] = group
                group.add(req, rows)

            if any(len(g.file_id_chunk_ids) >= batch_max_files for g in groups.values()):
                break
            if asyncio.get_event_loop().time() >= deadline:
                break
            req = await _dequeue_one(_ASSEMBLY_DRAIN_TIMEOUT)
        return groups

    async def _handle_batch(group: _BatchGroup, client: Any) -> None:
        ray_id = group.entries[0].request.ray_id or "no-ray-id" if group.entries else "no-ray-id"
        ray_id_context.set(ray_id)
        worker_logger = get_logger_with_ray_id(__name__, ray_id)
        with tracer.start_as_current_span(
            "unpinner.batch",
            attributes={
                "backend": backend_name,
                "hippius.ray_id": ray_id,
                "hippius.account.main": group.address,
                "num_requests": len(group.entries),
                "num_file_ids": len(group.file_id_chunk_ids),
            },
        ):
            await process_unpin_batch(
                group,
                backend_name=backend_name,
                client=client,
                worker_logger=worker_logger,
                dlq_manager=dlq_manager,
                db_pool=db_pool,
                sem=delete_sem,
                config=config,
                max_files=batch_max_files,
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
        # One backend client for the whole loop — every dispatched request reuses this connection
        # pool instead of re-handshaking TLS per request (a handshake/ephemeral-port storm at high
        # inflight). Mirrors run_arion_uploader_in_loop's single ArionClient().
        async with backend_client_factory() as client:
            try:
                while True:
                    _reap({t for t in inflight if t.done()})

                    # Capacity gate — keep at most max_inflight requests/batches processing at once.
                    if len(inflight) >= max_inflight:
                        done_wait, _ = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
                        _reap(done_wait)
                        continue

                    if batch_enabled:
                        first = await _dequeue_one(block_timeout=3)
                        if first is None:
                            if inflight:
                                done_wait, _ = await asyncio.wait(
                                    inflight, return_when=asyncio.FIRST_COMPLETED, timeout=0.5
                                )
                                _reap(done_wait)
                            else:
                                await asyncio.sleep(0.1)
                            continue

                        groups = await _assemble_batch(first)
                        for group in groups.values():
                            inflight.add(asyncio.create_task(_handle_batch(group, client)))
                        continue

                    unpin_request, redis_queues_client = await with_redis_retry(
                        lambda rc: dequeue_unpin_request(queue_name),
                        redis_queues_client,
                        config.redis_queues_url,
                        f"dequeue {backend_name} unpin request",
                    )

                    if not unpin_request:
                        if inflight:
                            done_wait, _ = await asyncio.wait(
                                inflight, return_when=asyncio.FIRST_COMPLETED, timeout=0.5
                            )
                            _reap(done_wait)
                        else:
                            await asyncio.sleep(0.1)
                        continue

                    inflight.add(asyncio.create_task(_handle_unpin(unpin_request, client)))
            finally:
                # Drain in-flight requests while the shared client is still open: they hold it for
                # their Arion DELETEs, so the client must outlive them — closing it first (the
                # `async with` __aexit__) would fail every in-flight unpin mid-request on shutdown.
                for t in inflight:
                    t.cancel()
                await asyncio.gather(*inflight, return_exceptions=True)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info(f"{backend_name} unpinner stopping…")
    finally:
        mover_task.cancel()
        # gather (not suppress) so the cancelled mover's CancelledError is absorbed here.
        await asyncio.gather(mover_task, return_exceptions=True)
        if db_pool:
            await db_pool.close()
