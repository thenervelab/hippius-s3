#!/usr/bin/env python3
import asyncio
import contextlib
import logging
import sys
from pathlib import Path

import redis.asyncio as async_redis
from hippius_sdk.client import HippiusClient


# Add parent directory to path to import hippius_s3 modules
sys.path.insert(0, str(Path(__file__).parent))

import asyncpg

from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.config import get_config
from hippius_s3.queue import DownloadChainRequest
from hippius_s3.queue import dequeue_download_request
from hippius_s3.utils import get_query
from hippius_s3.utils.timing import log_timing


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


config = get_config()


async def process_download_request(
    download_request: DownloadChainRequest,
    redis_client: async_redis.Redis,
) -> bool:
    """Process a download request by downloading each chunk and storing in Redis."""

    obj_cache = RedisObjectPartsCache(redis_client)
    hippius_client = HippiusClient(
        ipfs_gateway=config.ipfs_get_url,
        ipfs_api_url=config.ipfs_store_url,
        substrate_url=config.substrate_url,
        encrypt_by_default=False,
    )

    # Use shorter identifier for logging
    short_id = f"{download_request.bucket_name}/{download_request.object_key}"
    logger.info(
        f"Processing download request for {short_id} with {len(download_request.chunks)} chunks; "
        f"should_decrypt={download_request.should_decrypt} subaccount={download_request.subaccount[:8]}..."
    )
    logger.info(
        f"Download request ids: object_id={download_request.object_id} request_id={download_request.request_id} parts={[c.part_id for c in download_request.chunks]}"
    )

    # DB connection required for part_chunks lookup
    db = await asyncpg.connect(config.database_url)

    try:
        # Process all chunks concurrently with higher concurrency for better IPFS utilization
        semaphore = asyncio.Semaphore(10)  # Increased for better parallel downloads

        async def download_chunk(chunk):
            chunk_logger = logging.getLogger(__name__)
            async with semaphore:
                # Build per-part download plan from DB; optionally restrict to in-range chunk indices
                cid_plan: list[tuple[int, str, int | None]] = []
                try:
                    rows = await db.fetch(
                        get_query("get_part_chunks_by_object_and_number"),
                        download_request.object_id,
                        int(chunk.part_id),
                    )
                    all_entries = [(int(r[0]), str(r[1]), int(r[2]) if r[2] is not None else None) for r in rows or []]
                    # Restrict to requested indices if provided
                    if getattr(chunk, "chunk_indices", None):
                        idx_set = set(int(i) for i in chunk.chunk_indices or [])
                        cid_plan.extend([e for e in all_entries if e[0] in idx_set])
                        # Check for missing indices; do NOT backfill unless flag allows
                        if idx_set:
                            found_indices = {e[0] for e in cid_plan}
                            missing_indices = idx_set - found_indices
                            if missing_indices and chunk.cid:
                                if getattr(config, "downloader_allow_part_backfill", False):
                                    chunk_logger.warning(
                                        f"ALLOW_BACKFILL enabled: attempting to backfill missing indices {sorted(missing_indices)} from part CID for part {chunk.part_id}"
                                    )
                                    try:
                                        from hippius_s3.metadata.meta_reader import read_db_meta
                                        db_meta = await read_db_meta(db, download_request.object_id, int(chunk.part_id))
                                        chunk_size = int(db_meta["chunk_size_bytes"]) if db_meta and db_meta["chunk_size_bytes"] else 0
                                    except Exception:
                                        chunk_size = 0
                                    if chunk_size <= 0:
                                        # Cannot backfill without authoritative chunk size
                                        raise RuntimeError("chunk_size_bytes_missing")
                                    for mi in sorted(missing_indices):
                                        # Plan entries will be fetched as full CID; client lacks range
                                        cid_plan.append((mi, str(chunk.cid), None))
                                else:
                                    # Strict mode: don't guess/backfill; let caller retry later
                                    raise RuntimeError("missing_requested_chunk_indices")
                    else:
                        cid_plan.extend(all_entries)
                except Exception:
                    cid_plan = []

<<<<<<< HEAD
                # Fallback to single CID if no part_chunks in DB
                if not cid_plan:
                    cid_str = str(chunk.cid or "").strip().lower()
                    if cid_str in {"", "none", "pending"}:
                        chunk_logger.error(
                            f"Skipping download for part {chunk.part_id}: invalid CID '{chunk.cid}' and no part_chunks"
                        )
                        # Clear in-progress flag so future attempts aren't blocked by TTL
                        try:
                            await redis_client.delete(
                                f"download_in_progress:{download_request.object_id}:{int(chunk.part_id)}"
                            )
                        except Exception:
                            chunk_logger.debug("Failed to delete in-progress flag for invalid CID", exc_info=True)
                        return False
=======
            # Fallback if no part_chunks rows in DB: honor requested indices if provided, else default to index 0
            if not cid_plan:
                cid_str = str(chunk.cid or "").strip().lower()
                if cid_str in {"", "none", "pending"}:
                    chunk_logger.error(
                        f"Skipping download for part {chunk.part_id}: invalid CID '{chunk.cid}' and no part_chunks"
                    )
                    # Clear in-progress flag so future attempts aren't blocked by TTL
                    try:
                        await redis_client.delete(
                            f"download_in_progress:{download_request.object_id}:{int(chunk.part_id)}"
                        )
                    except Exception:
                        chunk_logger.debug("Failed to delete in-progress flag for invalid CID", exc_info=True)
                    return False
                requested = list(getattr(chunk, "chunk_indices", []) or [])
                if requested:
                    cid_plan = [(int(i), str(chunk.cid), None) for i in requested]
                else:
>>>>>>> d4dc9f7 (fixes)
                    cid_plan = [(0, str(chunk.cid), None)]

            max_attempts = getattr(config, "downloader_chunk_retries", 5)
            base_sleep = getattr(config, "downloader_retry_base_seconds", 0.25)
            jitter = getattr(config, "downloader_retry_jitter_seconds", 0.2)

            import hashlib as _hashlib
            import random as _random

            for attempt in range(1, max_attempts + 1):
                try:
                    import time as _dtime

                    chunk_start = _dtime.perf_counter()
                    chunk_logger.debug(
                        f"Downloading chunk {chunk.part_id} attempt={attempt}/{max_attempts} CID: {chunk.cid}"
                    )

                    # Download per plan entry (DB-driven chunk layout if available)
                    # Deduplicate CID fetches to avoid redundant IPFS downloads
                    cid_cache: dict[str, bytes] = {}
                    assembled: list[tuple[int, bytes]] = []  # (chunk_index, data)

                    # Derive authoritative chunk_size from DB plan if provided (third column)
                    auth_chunk_size = 0
                    try:
                        for _ci, _cidv, _exp in cid_plan:
                            if _exp is not None and int(_exp) > 0:
                                auth_chunk_size = int(_exp)
                                break
                    except Exception:
                        auth_chunk_size = 0

                    for ci, cid_val, expected_len in cid_plan:
                        chunk_logger.debug(
                            f"Downloading part {chunk.part_id} ci={ci} CID={cid_val[:10]}... expected={expected_len}"
                        )
                        # Check if we already fetched this CID
                        if cid_val in cid_cache:
                            data_i = cid_cache[cid_val]
                            chunk_logger.debug(f"Using cached CID data for ci={ci}")
                        else:
                            data_i = await hippius_client.s3_download(
                                cid=cid_val,
                                subaccount_id=download_request.subaccount,
                                bucket_name=download_request.bucket_name,
                                auto_decrypt=False,
                                download_node=download_request.ipfs_node,
                                return_bytes=True,
                            )
                            cid_cache[cid_val] = data_i

                        if expected_len is not None and len(data_i) != int(expected_len):
                            chunk_logger.warning(
                                f"Cipher len mismatch for part {chunk.part_id} ci={ci}: got={len(data_i)} expected={expected_len}"
                            )
                        assembled.append((ci, data_i))

                    # Sort by chunk index and concatenate; also map by index for targeted writes
                    assembled.sort(key=lambda x: x[0])
                    by_index: dict[int, bytes] = {int(i): bytes(d) for i, d in assembled}
                    chunk_data = b"".join([data for _, data in assembled])

                    md5 = _hashlib.md5(chunk_data).hexdigest()
                    head_hex = chunk_data[:8].hex()
                    tail_hex = chunk_data[-8:].hex() if len(chunk_data) >= 8 else head_hex
                    chunk_logger.debug(
                        f"Downloaded chunk {chunk.part_id} cid={str(chunk.cid)}... len={len(chunk_data)} md5={md5} "
                        f"head8={head_hex} tail8={tail_hex}"
                    )

                    # Store bytes in Redis cache with meta-first write order (readiness signal before chunks)
                    part_num = int(chunk.part_id)
                    if cid_plan and len(cid_plan) > 1:
                        # Multiple chunks in DB: write meta first, then chunk keys
                        await obj_cache.set_meta(
                            download_request.object_id,
                            part_num,
                            chunk_size=auth_chunk_size,  # authoritative from DB rows; 0 if unknown
                            num_chunks=len(cid_plan),
                            size_bytes=len(chunk_data),
                        )
                        for ci, _cid, _exp in cid_plan:
                            data_i = by_index.get(int(ci), b"")
                            await obj_cache.set_chunk(download_request.object_id, part_num, int(ci), data_i)
                        chunk_logger.info(
                            f"OBJ-CACHE stored (DB layout) object_id={download_request.object_id} part={part_num} ct_chunks={len(cid_plan)} total_bytes={len(chunk_data)}"
                        )
                    else:
                        # Single-chunk case: write meta first, then chunk at the requested index (default 0)
                        await obj_cache.set_meta(
                            download_request.object_id,
                            part_num,
                            chunk_size=auth_chunk_size,  # authoritative from DB if available
                            num_chunks=1,
                            size_bytes=len(chunk_data),
                        )
                        target_index = int(cid_plan[0][0]) if cid_plan else 0
                        data_i = by_index.get(target_index, chunk_data)
                        await obj_cache.set_chunk(download_request.object_id, part_num, target_index, data_i)
                        chunk_logger.info(
                            f"OBJ-CACHE stored (single) object_id={download_request.object_id} part={part_num} idx={target_index} bytes={len(data_i)}"
                        )
                    # Log download timing
                    chunk_ms = (_dtime.perf_counter() - chunk_start) * 1000.0
                    log_timing(
                        "downloader.chunk_download_and_store",
                        chunk_ms,
                        extra={
                            "object_id": download_request.object_id,
                            "part_id": chunk.part_id,
                            "size_bytes": len(chunk_data),
                            "num_cids": len(cid_plan),
                        },
                    )
                    # Clear in-progress flag on success
                    try:
                        await redis_client.delete(
                            f"download_in_progress:{download_request.object_id}:{int(chunk.part_id)}"
                        )
                    except Exception:
                        chunk_logger.debug("Failed to delete in-progress flag after success", exc_info=True)
                    return True

                except Exception as e:
                    if attempt == max_attempts:
                        chunk_logger.error(
                            f"Failed to download chunk {chunk.part_id} (CID: {chunk.cid}) after {max_attempts} attempts: {e}"
                        )
                        # Clear in-progress flag after final failure
                        try:
                            await redis_client.delete(
                                f"download_in_progress:{download_request.object_id}:{int(chunk.part_id)}"
                            )
                        except Exception:
                            chunk_logger.debug("Failed to delete in-progress flag after failure", exc_info=True)
                        return False
                    sleep_for = base_sleep * attempt + _random.uniform(0, jitter)
                    chunk_logger.warning(
                        f"Download error for chunk {chunk.part_id} (CID: {chunk.cid}) attempt {attempt}/{max_attempts}: {e}. Retrying in {sleep_for:.2f}s"
                    )
                    await asyncio.sleep(sleep_for)
            return None

        # Download all chunks concurrently
        results = await asyncio.gather(*[download_chunk(chunk) for chunk in download_request.chunks])

        success_count = sum(results)
        total_chunks = len(download_request.chunks)

        if success_count == total_chunks:
            logger.info(f"Successfully downloaded all {total_chunks} chunks for {short_id}")
            return True

        logger.error(f"Only downloaded {success_count}/{total_chunks} chunks for {short_id}")
        # Don't delete existing chunks as they might be in use by ongoing downloads
        # Let Redis TTL handle cleanup naturally
        return False
    finally:
        # Clean up DB connection created for this request
        with contextlib.suppress(Exception):
            await db.close()


async def run_downloader_loop():
    """Main loop for downloader service."""
    redis_client = async_redis.from_url(config.redis_url)

    logger = logging.getLogger(__name__)
    logger.info("Starting downloader service...")
    logger.info(f"Redis URL: {config.redis_url}")

    try:
        while True:
            download_request = await dequeue_download_request(redis_client)

            if download_request:
                success = await process_download_request(download_request, redis_client)
                if success:
                    logger.info(
                        f"Successfully processed download request {download_request.bucket_name}/{download_request.object_key}"
                    )
                else:
                    logger.error(
                        f"Failed to process download request {download_request.bucket_name}/{download_request.object_key}"
                    )
            else:
                # Wait a bit before checking again
                await asyncio.sleep(config.downloader_sleep_loop)

    except KeyboardInterrupt:
        logger.info("Downloader service stopping...")
    except Exception as e:
        logger.error(f"Error in downloader loop: {e}")
        raise
    finally:
        await redis_client.aclose()


if __name__ == "__main__":
    config = get_config()
    asyncio.run(run_downloader_loop())
