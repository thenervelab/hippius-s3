# -*- coding: utf-8 -*-
"""Shared per-backend download module.

Each backend entry point (``run_ipfs_downloader_in_loop.py``,
``run_arion_downloader_in_loop.py``) provides:

* ``backend_name`` — e.g. ``"ipfs"`` or ``"arion"``
* ``queue_name``   — e.g. ``"ipfs_download_requests"``
* ``fetch_fn``     — ``async (identifier, account_address) -> bytes``

This module dequeues download requests, looks up the backend-specific
identifier for each chunk, fetches the ciphertext, and stores it in the
Redis chunk cache.
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

from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.config import get_config
from hippius_s3.queue import DownloadChainRequest
from hippius_s3.queue import PartToDownload
from hippius_s3.redis_utils import create_redis_client
from hippius_s3.utils import get_query
from hippius_s3.utils.timing import log_timing


logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


async def process_download_request(
    download_request: DownloadChainRequest,
    *,
    backend_name: str,
    fetch_fn: Callable[[str, str], Awaitable[bytes]],
    db_pool: asyncpg.Pool,
    redis_client: async_redis.Redis,
) -> bool:
    """Process a single download request for one backend.

    For each part/chunk in the request:
    1. Look up ``backend_identifier`` from the ``chunk_backend`` table.
    2. Skip if no identifier (this backend doesn't hold the chunk).
    3. Skip if already cached.
    4. Call *fetch_fn(identifier, account_address)* to obtain ciphertext bytes.
    5. Store in the Redis chunk cache.
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
        obj_cache = RedisObjectPartsCache(redis_client)

        short_id = f"{download_request.bucket_name}/{download_request.object_key}"
        logger.info(
            f"[{backend_name}] Processing download for {short_id} "
            f"parts={[c.part_number for c in download_request.chunks]} "
            f"object_id={download_request.object_id}"
        )

        semaphore = asyncio.Semaphore(10)
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
            chunk_index = int(spec.index)  # type: ignore[attr-defined]

            async with semaphore:
                # Check if already cached
                cached = await obj_cache.chunk_exists(
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
                        await obj_cache.set_chunk(
                            download_request.object_id,
                            int(download_request.object_version),
                            part_number,
                            chunk_index,
                            data,
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
                chunk_results = await asyncio.gather(
                    *[_fetch_chunk(part_number, spec, part_span) for spec in part.chunks]
                )

                # Clear in-progress flag
                with contextlib.suppress(Exception):
                    await redis_client.delete(f"download_in_progress:{download_request.object_id}:{part_number}")
                return all(chunk_results)

        try:
            results = await asyncio.gather(*[_process_part(part) for part in download_request.chunks])
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
            if success_count == total:
                return True
            logger.error(f"[{backend_name}] {success_count}/{total} parts OK for {short_id}")
            return False
        except Exception as exc:
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

    from hippius_s3.queue import dequeue_download_request
    from hippius_s3.queue import initialize_queue_client
    from hippius_s3.redis_cache import initialize_cache_client
    from hippius_s3.services.ray_id_service import get_logger_with_ray_id
    from hippius_s3.services.ray_id_service import ray_id_context

    config = get_config()

    redis_client: async_redis.Redis = create_redis_client(config.redis_url)  # type: ignore[assignment]
    redis_queues_client = async_redis.from_url(config.redis_queues_url)
    db_pool = await asyncpg.create_pool(config.database_url, min_size=2, max_size=20)

    initialize_queue_client(redis_queues_client)
    initialize_cache_client(redis_client)

    logger.info(f"[{backend_name}] Starting downloader, queue={queue_name}")

    try:
        while True:
            try:
                request = await dequeue_download_request(queue_name)
            except (BusyLoadingError, RedisConnectionError, RedisTimeoutError) as exc:
                logger.warning(f"[{backend_name}] Redis error dequeuing: {exc}. Reconnecting…")
                with contextlib.suppress(Exception):
                    await redis_queues_client.aclose()  # type: ignore[attr-defined]
                await asyncio.sleep(2)
                redis_queues_client = async_redis.from_url(config.redis_queues_url)
                initialize_queue_client(redis_queues_client)
                continue
            except Exception as exc:
                logger.error(f"[{backend_name}] Dequeue/parse error, skipping: {exc}", exc_info=True)
                continue

            if request is None:
                await asyncio.sleep(config.downloader_sleep_loop)
                continue

            if request.expire_at and time.time() > request.expire_at:
                logger.warning(f"[{backend_name}] Discarding expired request {request.name}")
                continue

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
                        redis_client=redis_client,
                        # type: ignore
                    )
                    if ok:
                        worker_logger.info(f"[{backend_name}] Done: {request.bucket_name}/{request.object_key}")
                    else:
                        job_span.set_status(trace.StatusCode.ERROR, "partial failure")
                        worker_logger.error(f"[{backend_name}] Failed: {request.bucket_name}/{request.object_key}")
                except (RedisConnectionError, RedisTimeoutError, BusyLoadingError) as exc:
                    job_span.record_exception(exc)
                    job_span.set_status(trace.StatusCode.ERROR, str(exc))
                    logger.warning(f"[{backend_name}] Redis issue during processing: {exc}. Reconnecting…")
                    with contextlib.suppress(Exception):
                        await redis_client.aclose()  # type: ignore[union-attr,attr-defined]
                    redis_client = create_redis_client(config.redis_url)  # type: ignore[assignment]
                    initialize_cache_client(redis_client)
                except asyncpg.InterfaceError as exc:
                    job_span.record_exception(exc)
                    job_span.set_status(trace.StatusCode.ERROR, str(exc))
                    logger.warning(f"[{backend_name}] DB pool issue, recreating…")
                    with contextlib.suppress(Exception):
                        await db_pool.close()
                    db_pool = await asyncpg.create_pool(config.database_url, min_size=2, max_size=20)

    except KeyboardInterrupt:
        logger.info(f"[{backend_name}] Downloader stopping…")
    except Exception as exc:
        logger.error(f"[{backend_name}] Fatal loop error: {exc}", exc_info=True)
        raise
    finally:
        await redis_client.aclose()  # type: ignore[union-attr,attr-defined]
        await redis_queues_client.aclose()  # type: ignore[attr-defined]
        await db_pool.close()
