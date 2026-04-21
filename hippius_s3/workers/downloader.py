# -*- coding: utf-8 -*-
"""Shared per-backend download worker.

Each backend entry point (``run_arion_downloader_in_loop.py``) provides:

* ``backend_name`` — e.g. ``"arion"``
* ``queue_name``   — e.g. ``"arion_download_requests"``
* ``fetch_fn``     — ``async (identifier, account_address) -> bytes``

This module dequeues download requests, looks up the backend-specific
identifier for each chunk, fetches the ciphertext, writes it to the shared
filesystem cache (``FileSystemPartsStore``), and publishes a pub/sub
notification so streamers waiting on the chunk can proceed.

Meta is written EAGERLY at part start (using ``num_chunks`` / ``chunk_size``
from DB) so partial-range fills become readable per-chunk as they land.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import time
from collections.abc import Awaitable
from collections.abc import Callable

import asyncpg
import redis.asyncio as async_redis
from opentelemetry import trace

from hippius_s3.cache import FileSystemPartsStore
from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.cache import create_fs_store
from hippius_s3.config import get_config
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.queue import DownloadChainRequest
from hippius_s3.queue import PartToDownload
from hippius_s3.redis_utils import create_redis_client
from hippius_s3.utils import get_query
from hippius_s3.utils.timing import log_timing


logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


async def _write_part_meta_from_db(
    *,
    db_pool: asyncpg.Pool,
    fs_store: FileSystemPartsStore,
    object_id: str,
    object_version: int,
    part_number: int,
) -> None:
    """Write meta.json eagerly from the DB parts row.

    Reader uses meta.json existence as a "part is known" signal; per-chunk
    file presence is the "this chunk is ready" signal. Writing meta up-front
    lets partial fills (range requests) become readable chunk-by-chunk.

    Idempotent: meta content is deterministic from DB, so re-writes are safe.
    """
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT size_bytes, chunk_size_bytes FROM parts WHERE object_id = $1 "
            "AND object_version = $2 AND part_number = $3",
            object_id,
            int(object_version),
            int(part_number),
        )
    if not row:
        logger.warning(
            "No parts row for meta: object_id=%s v=%s part=%s",
            object_id,
            object_version,
            part_number,
        )
        return
    size_bytes = int(row["size_bytes"] or 0)
    chunk_size = int(row["chunk_size_bytes"] or 0) or (4 * 1024 * 1024)
    num_chunks = 0 if size_bytes <= 0 else (size_bytes + chunk_size - 1) // chunk_size
    await fs_store.set_meta(
        object_id,
        int(object_version),
        int(part_number),
        chunk_size=chunk_size,
        num_chunks=num_chunks,
        size_bytes=size_bytes,
    )


async def process_download_request(
    download_request: DownloadChainRequest,
    *,
    backend_name: str,
    fetch_fn: Callable[[str, str], Awaitable[bytes]],
    db_pool: asyncpg.Pool,
    obj_cache: RedisObjectPartsCache,
    fs_store: FileSystemPartsStore,
) -> bool:
    """Process a single download request for one backend.

    For each part/chunk in the request:
    1. Ensure meta.json exists on FS (write from DB if missing).
    2. Look up ``backend_identifier`` from the ``chunk_backend`` table.
    3. Skip if no identifier (this backend doesn't hold the chunk).
    4. Skip if already cached on FS.
    5. Call ``fetch_fn(identifier, account_address)`` to obtain ciphertext.
    6. Write to FS atomically.
    7. Publish pub/sub notification via ``obj_cache.notify_chunk``.
    """
    with tracer.start_as_current_span(
        "downloader.process_download",
        attributes={
            "object_id": download_request.object_id,
            "object_key": download_request.object_key,
            "backend": backend_name,
            "num_parts": len(download_request.chunks),
            "hippius.account.main": download_request.address,
        },
    ) as span:
        config = get_config()

        short_id = f"{download_request.bucket_name}/{download_request.object_key}"
        logger.info(
            f"[{backend_name}] Processing download for {short_id} "
            f"parts={[c.part_number for c in download_request.chunks]} "
            f"object_id={download_request.object_id}"
        )

        semaphore = asyncio.Semaphore(config.downloader_semaphore)
        max_attempts = config.downloader_chunk_retries
        base_sleep = config.downloader_retry_base_seconds
        jitter = config.downloader_retry_jitter_seconds

        t_request_start = time.perf_counter()
        ttfb_recorded = False
        ttfb_ms = 0.0
        total_bytes_downloaded = 0

        async def _fetch_chunk(part_number: int, spec: object, span: trace.Span) -> bool:
            """Fetch and cache a single chunk, guarded by the semaphore."""
            nonlocal ttfb_recorded, ttfb_ms, total_bytes_downloaded
            chunk_index = int(spec.index)  # ty: ignore[unresolved-attribute]

            async with semaphore:
                # Check if already cached on FS (may have been filled by a
                # concurrent request / worker)
                cached = await fs_store.chunk_exists(
                    download_request.object_id,
                    int(download_request.object_version),
                    part_number,
                    chunk_index,
                )
                if cached:
                    logger.debug(f"[{backend_name}] Skipping cached chunk part={part_number} ci={chunk_index}")
                    return True

                # Look up backend_identifier (acquire connection from pool)
                async with db_pool.acquire() as conn:
                    row = await conn.fetchrow(
                        get_query("get_chunk_backend_identifier"),
                        backend_name,
                        download_request.object_id,
                        int(download_request.object_version),
                        part_number,
                        chunk_index,
                    )
                if not row or not row["backend_identifier"]:
                    logger.debug(f"[{backend_name}] No identifier for part={part_number} ci={chunk_index}, skipping")
                    return True

                identifier = str(row["backend_identifier"])

                # Fetch with retries
                for attempt in range(1, max_attempts + 1):
                    try:
                        t0 = time.perf_counter()
                        data = await fetch_fn(identifier, download_request.subaccount)
                        fetch_ms = (time.perf_counter() - t0) * 1000.0

                        if not ttfb_recorded:
                            ttfb_ms = (time.perf_counter() - t_request_start) * 1000.0
                            ttfb_recorded = True
                        total_bytes_downloaded += len(data)

                        t_cache = time.perf_counter()
                        await fs_store.set_chunk(
                            download_request.object_id,
                            int(download_request.object_version),
                            part_number,
                            chunk_index,
                            data,
                        )
                        # Notify waiting readers via pub/sub
                        await obj_cache.notify_chunk(
                            download_request.object_id,
                            int(download_request.object_version),
                            part_number,
                            chunk_index,
                        )
                        cache_ms = (time.perf_counter() - t_cache) * 1000.0

                        chunk_mb = len(data) / (1024 * 1024)
                        chunk_throughput = chunk_mb / (fetch_ms / 1000.0) if fetch_ms > 0 else 0
                        logger.info(
                            f"[{backend_name}] CHUNK object_id={download_request.object_id} "
                            f"part={part_number} ci={chunk_index} id={identifier} "
                            f"fetch={fetch_ms:.0f}ms cache={cache_ms:.0f}ms "
                            f"size={chunk_mb:.1f}MB throughput={chunk_throughput:.1f}MB/s"
                        )

                        log_timing(
                            "downloader.chunk_download_and_store",
                            fetch_ms + cache_ms,
                            extra={
                                "backend": backend_name,
                                "object_id": download_request.object_id,
                                "part_number": part_number,
                                "chunk_index": chunk_index,
                                "size_bytes": len(data),
                            },
                        )
                        return True
                    except Exception as exc:
                        if attempt == max_attempts:
                            logger.error(
                                f"[{backend_name}] Failed chunk "
                                f"part={part_number} ci={chunk_index} "
                                f"after {max_attempts} attempts: {exc}"
                            )
                            return False
                        sleep_for = base_sleep * attempt + random.uniform(0, jitter)
                        logger.warning(
                            f"[{backend_name}] Fetch error part={part_number} "
                            f"ci={chunk_index} attempt {attempt}/{max_attempts}: "
                            f"{exc}. Retrying in {sleep_for:.2f}s"
                        )
                        await asyncio.sleep(sleep_for)
            return False  # unreachable, but keeps mypy happy

        async def _process_part(part: PartToDownload) -> bool:
            part_number = int(part.part_number)
            with tracer.start_as_current_span(
                "downloader.download_part",
                attributes={
                    "object_id": download_request.object_id,
                    "part_number": part_number,
                    "num_chunks": len(part.chunks),
                },
            ) as part_span:
                # Ensure meta.json exists up-front so per-chunk existence
                # checks work correctly as chunks land on FS.
                existing_meta = await fs_store.get_meta(
                    download_request.object_id,
                    int(download_request.object_version),
                    part_number,
                )
                if existing_meta is None:
                    try:
                        await _write_part_meta_from_db(
                            db_pool=db_pool,
                            fs_store=fs_store,
                            object_id=download_request.object_id,
                            object_version=int(download_request.object_version),
                            part_number=part_number,
                        )
                    except Exception as meta_exc:
                        logger.warning(f"[{backend_name}] Failed to write eager meta part={part_number}: {meta_exc}")

                # Batch chunks so a pathological single-part (up to ~1280
                # chunks for a 5 GiB part at 4 MiB chunk size) doesn't create
                # thousands of tasks parked on the semaphore. Matches the
                # part-batching budget so the worst case per part is capped.
                chunk_batch_size = max(1, config.downloader_semaphore)
                chunk_results: list[bool] = []
                for i in range(0, len(part.chunks), chunk_batch_size):
                    batch = part.chunks[i : i + chunk_batch_size]
                    chunk_results.extend(
                        await asyncio.gather(*[_fetch_chunk(part_number, spec, part_span) for spec in batch])
                    )

                # Release the coalescing lock (set by build_stream_context on
                # enqueue). Key format must match that callsite exactly.
                with contextlib.suppress(Exception):
                    lock_key = (
                        f"download_in_progress:{download_request.object_id}"
                        f":v:{int(download_request.object_version)}:part:{part_number}"
                    )
                    await obj_cache.redis.delete(lock_key)
                return all(chunk_results)

        try:
            # Process parts in bounded batches to prevent OOM with huge objects
            # (e.g. 5K+ parts = 10K+ tasks). The semaphore still controls concurrent fetches.
            part_batch_size = max(1, config.downloader_semaphore)
            results: list[bool] = []
            for batch_start in range(0, len(download_request.chunks), part_batch_size):
                batch = download_request.chunks[batch_start : batch_start + part_batch_size]
                batch_results = await asyncio.gather(*[_process_part(part) for part in batch])
                results.extend(batch_results)
            success_count = sum(1 for r in results if r)
            total = len(download_request.chunks)
            span.set_attribute("result.success_count", success_count)
            span.set_attribute("result.total_parts", total)
            total_ms = (time.perf_counter() - t_request_start) * 1000.0
            size_mb = total_bytes_downloaded / (1024 * 1024)
            throughput = size_mb / (total_ms / 1000.0) if total_ms > 0 else 0
            logger.info(
                f"[{backend_name}] PERF download {short_id} "
                f"total={total_ms:.0f}ms ttfb={ttfb_ms:.0f}ms "
                f"size={size_mb:.1f}MB throughput={throughput:.1f}MB/s "
                f"parts={success_count}/{total}"
            )
            total_duration = time.perf_counter() - t_request_start
            if success_count == total:
                get_metrics_collector().record_downloader_operation(
                    backend=backend_name,
                    main_account=download_request.address,
                    success=True,
                    duration=total_duration,
                    num_chunks=sum(len(p.chunks) for p in download_request.chunks),
                )
                return True
            get_metrics_collector().record_downloader_operation(
                backend=backend_name,
                main_account=download_request.address,
                success=False,
                duration=total_duration,
            )
            logger.error(f"[{backend_name}] {success_count}/{total} parts OK for {short_id}")
            return False
        except Exception as exc:
            get_metrics_collector().record_downloader_operation(
                backend=backend_name,
                main_account=download_request.address,
                success=False,
            )
            logger.error(f"[{backend_name}] process_download_request error: {exc}", exc_info=True)
            return False


async def run_downloader_loop(
    *,
    backend_name: str,
    queue_name: str,
    fetch_fn: Callable[[str, str], Awaitable[bytes]],
) -> None:
    """Main loop: dequeue from *queue_name* and process via *fetch_fn*."""
    from redis.exceptions import BusyLoadingError
    from redis.exceptions import ConnectionError as RedisConnectionError
    from redis.exceptions import TimeoutError as RedisTimeoutError

    from hippius_s3.monitoring import initialize_metrics_collector
    from hippius_s3.queue import dequeue_download_request
    from hippius_s3.queue import initialize_queue_client
    from hippius_s3.redis_cache import initialize_cache_client
    from hippius_s3.services.ray_id_service import get_logger_with_ray_id
    from hippius_s3.services.ray_id_service import ray_id_context

    config = get_config()

    redis_client: async_redis.Redis = create_redis_client(config.redis_url)  # ty: ignore[invalid-assignment]
    redis_queues_client = async_redis.from_url(config.redis_queues_url)
    db_pool = await asyncpg.create_pool(config.database_url, min_size=2, max_size=20)
    fs_store = create_fs_store(config)

    initialize_queue_client(redis_queues_client)
    initialize_cache_client(redis_client)
    initialize_metrics_collector(redis_client)

    obj_cache = RedisObjectPartsCache(redis_client, queues_client=redis_queues_client, fs_store=fs_store)

    if backend_name == "arion":
        backend_info = f" base_url={config.arion_base_url} verify_ssl={config.arion_verify_ssl}"
        logger.info(f"[{backend_name}] Starting downloader, queue={queue_name}{backend_info}")

    async def _run_job(request: DownloadChainRequest) -> None:
        ray_id = request.ray_id or "no-ray-id"
        ray_id_context.set(ray_id)
        worker_logger = get_logger_with_ray_id(__name__, ray_id)

        with tracer.start_as_current_span(
            "downloader.job",
            attributes={
                "object_id": request.object_id,
                "hippius.ray_id": ray_id,
                "backend": backend_name,
                "hippius.account.main": request.address,
            },
        ) as job_span:
            try:
                ok = await process_download_request(
                    request,
                    backend_name=backend_name,
                    fetch_fn=fetch_fn,
                    db_pool=db_pool,
                    obj_cache=obj_cache,
                    fs_store=fs_store,
                )
                if ok:
                    worker_logger.info(f"[{backend_name}] Done: {request.bucket_name}/{request.object_key}")
                else:
                    job_span.set_status(trace.StatusCode.ERROR, "partial failure")
                    worker_logger.error(f"[{backend_name}] Failed: {request.bucket_name}/{request.object_key}")
            except Exception as exc:
                job_span.record_exception(exc)
                job_span.set_status(trace.StatusCode.ERROR, str(exc))
                worker_logger.error(f"[{backend_name}] job error: {exc}", exc_info=True)
                raise

    max_inflight = max(1, config.downloader_max_inflight)
    inflight: set[asyncio.Task[None]] = set()
    # Flags set by _reap when an inflight task died on an infra error. The
    # main loop picks these up and rebuilds the affected client before
    # dequeuing the next request, so subsequent tasks don't keep failing
    # against a stale connection.
    needs_redis_reconnect = False
    needs_db_reconnect = False

    def _reap(tasks: set[asyncio.Task[None]]) -> None:
        nonlocal needs_redis_reconnect, needs_db_reconnect
        for t in tasks:
            inflight.discard(t)
            err = t.exception()
            if err is None or isinstance(err, asyncio.CancelledError):
                continue
            logger.error(f"[{backend_name}] inflight task error: {err}")
            if isinstance(err, (BusyLoadingError, RedisConnectionError, RedisTimeoutError)):
                needs_redis_reconnect = True
            elif isinstance(err, asyncpg.InterfaceError):
                needs_db_reconnect = True

    logger.info(f"[{backend_name}] Inflight pool size: {max_inflight}")

    try:
        while True:
            _reap({t for t in inflight if t.done()})

            if needs_redis_reconnect:
                logger.warning(f"[{backend_name}] Rebuilding cache redis client after task error")
                with contextlib.suppress(Exception):
                    await redis_client.aclose()  # ty: ignore[unresolved-attribute]
                redis_client = create_redis_client(config.redis_url)  # ty: ignore[invalid-assignment]
                initialize_cache_client(redis_client)
                obj_cache = RedisObjectPartsCache(redis_client, queues_client=redis_queues_client, fs_store=fs_store)
                needs_redis_reconnect = False

            if needs_db_reconnect:
                logger.warning(f"[{backend_name}] Recreating DB pool after task error")
                with contextlib.suppress(Exception):
                    await db_pool.close()
                db_pool = await asyncpg.create_pool(config.database_url, min_size=2, max_size=20)
                needs_db_reconnect = False

            if len(inflight) >= max_inflight:
                done_wait, _ = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
                _reap(done_wait)
                continue

            try:
                request = await dequeue_download_request(queue_name)
            except (BusyLoadingError, RedisConnectionError, RedisTimeoutError) as exc:
                logger.warning(f"[{backend_name}] Redis error dequeuing: {exc}. Reconnecting…")
                with contextlib.suppress(Exception):
                    await redis_queues_client.aclose()  # ty: ignore[unresolved-attribute]
                await asyncio.sleep(2)
                redis_queues_client = async_redis.from_url(config.redis_queues_url)
                initialize_queue_client(redis_queues_client)
                continue
            except Exception as exc:
                logger.error(f"[{backend_name}] Dequeue/parse error, skipping: {exc}", exc_info=True)
                continue

            if request is None:
                # Nothing to pick up. If we have inflight work, wait on it
                # instead of sleeping — keeps the loop responsive to task
                # completions without an idle poll.
                if inflight:
                    done_wait, _ = await asyncio.wait(
                        inflight, return_when=asyncio.FIRST_COMPLETED, timeout=config.downloader_sleep_loop
                    )
                    _reap(done_wait)
                else:
                    await asyncio.sleep(config.downloader_sleep_loop)
                continue

            if request.expire_at and time.time() > request.expire_at:
                logger.warning(f"[{backend_name}] Discarding expired request {request.name}")
                continue

            inflight.add(asyncio.create_task(_run_job(request)))

    except KeyboardInterrupt:
        logger.info(f"[{backend_name}] Downloader stopping…")
        for t in inflight:
            t.cancel()
        if inflight:
            await asyncio.gather(*inflight, return_exceptions=True)
    except Exception as exc:
        logger.error(f"[{backend_name}] Fatal loop error: {exc}", exc_info=True)
        raise
    finally:
        await redis_client.aclose()  # ty: ignore[unresolved-attribute]
        await redis_queues_client.aclose()  # ty: ignore[unresolved-attribute]
        await db_pool.close()
