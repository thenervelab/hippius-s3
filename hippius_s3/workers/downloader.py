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
from hippius_s3.utils import get_query
from hippius_s3.utils.timing import log_timing


logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


async def process_download_request(
    download_request: DownloadChainRequest,
    *,
    backend_name: str,
    fetch_fn: Callable[[str, str], Awaitable[bytes]],
    db: asyncpg.Connection,
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

        async def _process_part(part: PartToDownload) -> bool:
            part_number = int(part.part_number)
            with tracer.start_as_current_span(
                "downloader.download_part",
                attributes={
                    "object_id": download_request.object_id,
                    "part_number": part_number,
                    "num_chunks": len(part.chunks),
                },
            ):
                async with semaphore:
                    for spec in part.chunks:
                        chunk_index = int(spec.index)

                        cached = await obj_cache.chunk_exists(
                            download_request.object_id,
                            int(download_request.object_version),
                            part_number,
                            chunk_index,
                        )
                        if cached:
                            logger.debug(f"[{backend_name}] Skipping cached chunk part={part_number} ci={chunk_index}")
                            continue

                        row = await db.fetchrow(
                            get_query("get_chunk_backend_identifier"),
                            backend_name,
                            download_request.object_id,
                            int(download_request.object_version),
                            part_number,
                            chunk_index,
                        )
                        if not row or not row["backend_identifier"]:
                            logger.debug(
                                f"[{backend_name}] No identifier for part={part_number} ci={chunk_index}, skipping"
                            )
                            continue

                        identifier = str(row["backend_identifier"])

                        for attempt in range(1, max_attempts + 1):
                            try:
                                t0 = time.perf_counter()
                                data = await fetch_fn(identifier, download_request.subaccount)
                                elapsed_ms = (time.perf_counter() - t0) * 1000.0

                                await obj_cache.set_chunk(
                                    download_request.object_id,
                                    int(download_request.object_version),
                                    part_number,
                                    chunk_index,
                                    data,
                                )

                                with contextlib.suppress(Exception):
                                    head8 = data[:8].hex() if data else ""
                                    logger.info(
                                        f"[{backend_name}] STORED part={part_number} "
                                        f"ci={chunk_index} id={identifier[:16]}… "
                                        f"len={len(data)} head8={head8}"
                                    )

                                log_timing(
                                    "downloader.chunk_download_and_store",
                                    elapsed_ms,
                                    extra={
                                        "backend": backend_name,
                                        "object_id": download_request.object_id,
                                        "part_number": part_number,
                                        "chunk_index": chunk_index,
                                        "size_bytes": len(data),
                                    },
                                )
                                break  # success
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

                # Clear in-progress flag
                with contextlib.suppress(Exception):
                    await redis_client.delete(f"download_in_progress:{download_request.object_id}:{part_number}")
                return True

        try:
            results = await asyncio.gather(*[_process_part(part) for part in download_request.chunks])
            success_count = sum(1 for r in results if r)
            total = len(download_request.chunks)
            span.set_attribute("result.success_count", success_count)
            span.set_attribute("result.total_parts", total)
            if success_count == total:
                logger.info(f"[{backend_name}] All {total} parts OK for {short_id}")
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

    redis_client = async_redis.from_url(config.redis_url)
    redis_queues_client = async_redis.from_url(config.redis_queues_url)
    db = await asyncpg.connect(config.database_url)

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
                        db=db,
                        redis_client=redis_client,
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
                        await redis_client.aclose()  # type: ignore[attr-defined]
                    redis_client = async_redis.from_url(config.redis_url)
                    initialize_cache_client(redis_client)
                except asyncpg.InterfaceError as exc:
                    job_span.record_exception(exc)
                    job_span.set_status(trace.StatusCode.ERROR, str(exc))
                    logger.warning(f"[{backend_name}] DB connection lost, reconnecting…")
                    with contextlib.suppress(Exception):
                        await db.close()
                    db = await asyncpg.connect(config.database_url)

    except KeyboardInterrupt:
        logger.info(f"[{backend_name}] Downloader stopping…")
    except Exception as exc:
        logger.error(f"[{backend_name}] Fatal loop error: {exc}", exc_info=True)
        raise
    finally:
        await redis_client.aclose()  # type: ignore[attr-defined]
        await redis_queues_client.aclose()  # type: ignore[attr-defined]
        await db.close()
