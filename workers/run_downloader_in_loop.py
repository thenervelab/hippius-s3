#!/usr/bin/env python3
import asyncio
import contextlib
import logging
import sys
import time
from pathlib import Path

from redis.exceptions import BusyLoadingError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError


# Add parent directory to path to import hippius_s3 modules
sys.path.insert(0, str(Path(__file__).parent))


from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.config import get_config
from hippius_s3.ipfs_service import PendingCIDError
from hippius_s3.ipfs_service import s3_download
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.queue import DownloadChainRequest
from hippius_s3.queue import dequeue_download_request
from hippius_s3.services.ray_id_service import get_logger_with_ray_id
from hippius_s3.services.ray_id_service import ray_id_context
from hippius_s3.utils.timing import log_timing


config = get_config()

setup_loki_logging(config, "downloader")
logger = logging.getLogger(__name__)


async def process_download_request(
    download_request: DownloadChainRequest,
    redis_client,
) -> bool:
    """Process a download request by downloading each chunk and storing in Redis."""

    obj_cache = RedisObjectPartsCache(redis_client)

    # Use shorter identifier for logging
    short_id = f"{download_request.bucket_name}/{download_request.object_key}"
    logger.info(
        f"Processing download request for {short_id} with {len(download_request.chunks)} chunks; "
        f"subaccount={download_request.subaccount[:8]}..."
    )
    logger.info(
        f"Download request ids: object_id={download_request.object_id} request_id={download_request.request_id} parts={[c.part_number for c in download_request.chunks]}"
    )

    try:
        # Process all chunks concurrently with higher concurrency for better IPFS utilization
        semaphore = asyncio.Semaphore(10)  # Increased for better parallel downloads

        def _is_placeholder_cid(cid: str | None) -> bool:
            if cid is None:
                return True
            s = str(cid).strip()
            return (not s) or (s.lower() in {"none", "pending"})

        async def download_chunk(chunk):
            chunk_logger = logging.getLogger(__name__)
            async with semaphore:
                part_number = int(getattr(chunk, "part_number", 0))
                # Filter to entries that actually have a usable CID (IPFS-backed).
                # CID-less entries are expected for some storage_version>=4 flows and should be skipped here.
                cid_plan: list[tuple[int, str, int | None]] = []
                for s in chunk.chunks or []:
                    cid_raw = getattr(s, "cid", None)
                    if _is_placeholder_cid(cid_raw):
                        continue
                    cid_plan.append(
                        (
                            int(s.index),
                            str(cid_raw).strip(),
                            int(s.cipher_size_bytes) if s.cipher_size_bytes is not None else None,
                        )
                    )
                if not cid_plan:
                    chunk_logger.info(
                        f"Skipping part {part_number}: no usable CIDs in download plan (cid-less or not ready)"
                    )
                    return True

            max_attempts = config.downloader_chunk_retries
            base_sleep = config.downloader_retry_base_seconds
            jitter = config.downloader_retry_jitter_seconds

            import random as _random

            # Deduplicate CID fetches to avoid redundant IPFS downloads
            # Define outside the retry loop to avoid B023 loop variable binding issues
            cid_cache: dict[str, bytes] = {}

            for attempt in range(1, max_attempts + 1):
                try:
                    import time as _dtime

                    chunk_start = _dtime.perf_counter()
                    chunk_logger.debug(f"Downloading chunk part={part_number} attempt={attempt}/{max_attempts}")

                    # Download per plan entry (DB-driven chunk layout if available)
                    # Switch to write-on-arrival: persist each chunk as soon as it's fetched

                    # Order plan to prioritize the lowest chunk index for faster first byte
                    ordered_plan = sorted(cid_plan, key=lambda x: int(x[0]))
                    total_size_bytes = 0

                    async def fetch_and_store(entry: tuple[int, str, int | None]) -> int:
                        ci, cid_val, expected_len = entry
                        chunk_logger.debug(f"Downloading part {part_number} ci={ci}")
                        cid_for_ci = str(cid_val).strip()
                        if not cid_for_ci:
                            raise RuntimeError("missing_chunk_cid_for_index")
                        if cid_for_ci in cid_cache:
                            data_i = cid_cache[cid_for_ci]
                            chunk_logger.debug(f"Using cached CID data for ci={ci}")
                        else:
                            download_result = await s3_download(
                                cid=cid_for_ci,
                                account_address=download_request.subaccount,
                                bucket_name=download_request.bucket_name,
                                decrypt=False,
                            )
                            data_i = download_result.data
                            cid_cache[cid_for_ci] = data_i
                        with contextlib.suppress(Exception):
                            head8 = data_i[:8].hex()
                            chunk_logger.info(
                                f"FETCHED cid={cid_val[:10]} part={part_number} ci={int(ci)} len={len(data_i)} head8={head8}"
                            )
                        if expected_len is not None and len(data_i) != int(expected_len):
                            chunk_logger.warning(
                                f"Cipher len mismatch for part {part_number} ci={ci}: got={len(data_i)} expected={expected_len}"
                            )
                        await obj_cache.set_chunk(
                            download_request.object_id,
                            int(download_request.object_version),
                            part_number,
                            int(ci),
                            data_i,
                        )
                        with contextlib.suppress(Exception):
                            chunk_logger.info(
                                f"STORED part={part_number} ci={int(ci)} key=obj:{download_request.object_id}:v:{int(download_request.object_version)}:part:{part_number}:chunk:{int(ci)} head8={data_i[:8].hex()} len={len(data_i)}"
                            )
                        return len(data_i)

                    # Fetch the first (lowest) chunk synchronously and write immediately
                    head_len = 0
                    if ordered_plan:
                        head_len = await fetch_and_store(ordered_plan[0])
                        total_size_bytes += head_len

                    # Fetch and store the remaining chunks concurrently
                    if len(ordered_plan) > 1:
                        tail = ordered_plan[1:]
                        # Limit intra-part concurrency by the same semaphore already held
                        # Note: semaphore guards outer part processing; this inner gather is fine
                        results = await asyncio.gather(*[fetch_and_store(e) for e in tail])
                        total_size_bytes += sum(int(x or 0) for x in results)

                    # Store bytes in Redis cache
                    if cid_plan and len(cid_plan) > 1:
                        chunk_logger.info(
                            f"OBJ-CACHE stored (DB layout) object_id={download_request.object_id} part={part_number} ct_chunks={len(cid_plan)} total_bytes={int(total_size_bytes)}"
                        )
                    else:
                        # Single-chunk case: fetch and write target chunk directly (no whole-part assembly)
                        target_index = int(cid_plan[0][0]) if cid_plan else 0
                        entry = ordered_plan[0] if ordered_plan else (target_index, "", None)
                        _ = await fetch_and_store(entry)
                        chunk_logger.info(
                            f"OBJ-CACHE stored (single) object_id={download_request.object_id} part={part_number} idx={target_index}"
                        )
                    # Log download timing
                    chunk_ms = (_dtime.perf_counter() - chunk_start) * 1000.0
                    log_timing(
                        "downloader.chunk_download_and_store",
                        chunk_ms,
                        extra={
                            "object_id": download_request.object_id,
                            "part_number": part_number,
                            "size_bytes": int(total_size_bytes) if (cid_plan and len(cid_plan) > 1) else None,
                            "num_cids": len(cid_plan),
                        },
                    )
                    # Clear in-progress flag on success
                    try:
                        await redis_client.delete(
                            f"download_in_progress:{download_request.object_id}:{int(part_number)}"
                        )
                    except Exception:
                        chunk_logger.debug("Failed to delete in-progress flag after success", exc_info=True)
                    return True

                except PendingCIDError as e:
                    # Never retry placeholder CIDs (e.g. "pending") - they will always fail.
                    chunk_logger.error(f"Non-retryable CID for chunk part={part_number}: {e}")
                    return False
                except Exception as e:
                    if attempt == max_attempts:
                        chunk_logger.error(
                            f"Failed to download chunk part={part_number} after {max_attempts} attempts: {e}"
                        )
                        # Clear in-progress flag after final failure
                        try:
                            await redis_client.delete(
                                f"download_in_progress:{download_request.object_id}:{int(part_number)}"
                            )
                        except Exception:
                            chunk_logger.debug("Failed to delete in-progress flag after failure", exc_info=True)
                        return False
                    sleep_for = base_sleep * attempt + _random.uniform(0, jitter)
                    chunk_logger.warning(
                        f"Download error for chunk part={part_number} attempt {attempt}/{max_attempts}: {e}. Retrying in {sleep_for:.2f}s"
                    )
                    await asyncio.sleep(sleep_for)
            return None

        # Download all chunks concurrently
        results = await asyncio.gather(*[download_chunk(chunk) for chunk in download_request.chunks])

        success_count = sum(1 for r in results if r is True)
        total_chunks = len(download_request.chunks)

        if success_count == total_chunks:
            logger.info(f"Successfully downloaded all {total_chunks} chunks for {short_id}")
            return True

        logger.error(f"Only downloaded {success_count}/{total_chunks} chunks for {short_id}")
        # Don't delete existing chunks as they might be in use by ongoing downloads
        # Let Redis TTL handle cleanup naturally
        return False
    except Exception as e:
        logger.error(f"process_download_request error: {e}", exc_info=True)
        return False


async def run_downloader_loop():
    """Main loop for downloader service."""
    from hippius_s3.queue import initialize_queue_client
    from hippius_s3.redis_cache import initialize_cache_client
    from hippius_s3.redis_utils import create_redis_client

    redis_client = create_redis_client(config.redis_url)
    from redis.asyncio import Redis

    redis_queues_client = Redis.from_url(config.redis_queues_url)

    initialize_queue_client(redis_queues_client)
    initialize_cache_client(redis_client)

    logger = logging.getLogger(__name__)
    logger.info("Starting downloader service...")
    logger.info(f"Redis URL: {config.redis_url}")
    logger.info(f"Redis Queues URL: {config.redis_queues_url}")

    try:
        while True:
            try:
                download_request = await dequeue_download_request()
            except (BusyLoadingError, RedisConnectionError, RedisTimeoutError) as e:
                from redis.asyncio import Redis

                logger.warning(f"Redis error while dequeuing download request: {e}. Reconnecting in 2s...")
                with contextlib.suppress(Exception):
                    await redis_queues_client.aclose()  # type: ignore[attr-defined]
                await asyncio.sleep(2)
                redis_queues_client = Redis.from_url(config.redis_queues_url)
                initialize_queue_client(redis_queues_client)
                continue
            except Exception as e:
                logger.error(f"Failed to dequeue/parse download request, skipping: {e}", exc_info=True)
                # Skip bad item (already removed by BRPOP) and continue loop
                continue

            if download_request:
                if download_request.expire_at and time.time() > download_request.expire_at:
                    logger.warning(f"Discarding expired {download_request=}")
                    continue

                ray_id = download_request.ray_id or "no-ray-id"
                ray_id_context.set(ray_id)
                worker_logger = get_logger_with_ray_id(__name__, ray_id)

                try:
                    success = await process_download_request(download_request, redis_client)
                    if success:
                        worker_logger.info(
                            f"Successfully processed download request {download_request.bucket_name}/{download_request.object_key}"
                        )
                    else:
                        worker_logger.error(
                            f"Failed to process download request {download_request.bucket_name}/{download_request.object_key}"
                        )
                except (RedisConnectionError, RedisTimeoutError, BusyLoadingError) as e:
                    from hippius_s3.redis_utils import create_redis_client

                    logger.warning(
                        f"Redis connection issue during processing: {e}. Reconnecting in 2s and continuing..."
                    )
                    with contextlib.suppress(Exception):
                        await redis_client.aclose()  # type: ignore[attr-defined]
                    redis_client = create_redis_client(config.redis_url)
                    initialize_cache_client(redis_client)
                    continue
            else:
                # Wait a bit before checking again
                await asyncio.sleep(config.downloader_sleep_loop)

    except KeyboardInterrupt:
        logger.info("Downloader service stopping...")
    except Exception as e:
        logger.error(f"Error in downloader loop: {e}")
        raise
    finally:
        await redis_client.aclose()  # type: ignore[attr-defined]
        await redis_queues_client.aclose()  # type: ignore[attr-defined]


if __name__ == "__main__":
    config = get_config()
    asyncio.run(run_downloader_loop())
