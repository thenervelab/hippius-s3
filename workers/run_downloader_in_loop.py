#!/usr/bin/env python3
import asyncio
import logging
import os
import sys
from pathlib import Path

import redis.asyncio as async_redis
from hippius_sdk.client import HippiusClient


# Add parent directory to path to import hippius_s3 modules
sys.path.insert(0, str(Path(__file__).parent))

from hippius_s3.config import get_config
from hippius_s3.queue import DownloadChainRequest
from hippius_s3.queue import dequeue_download_request


config = get_config()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def process_download_request(
    download_request: DownloadChainRequest,
    redis_client: async_redis.Redis,
) -> bool:
    """Process a download request by downloading each chunk and storing in Redis."""

    # In e2e test mode, return mock data immediately
    if os.getenv("HIPPIUS_E2E_TESTS") == "true":
        test_content = os.getenv("HIPPIUS_E2E_GET_OBJECT_CONTENT", "test content").encode()
        logger.info(
            f"E2E Mode: Processing download request for {download_request.bucket_name}/{download_request.object_key} with mock data"
        )

        # Store mock data for all chunks
        for chunk in download_request.chunks:
            await redis_client.set(chunk.redis_key, test_content)
            logger.debug(f"E2E Mode: Stored mock data for chunk {chunk.part_id} with key: {chunk.redis_key}")

        logger.info(
            f"E2E Mode: Successfully processed {len(download_request.chunks)} chunks for {download_request.bucket_name}/{download_request.object_key}"
        )
        return True

    # Normal production logic
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

    # Process all chunks concurrently with higher concurrency for better IPFS utilization
    semaphore = asyncio.Semaphore(10)  # Increased for better parallel downloads

    async def download_chunk(chunk):
        async with semaphore:
            # Guard invalid/placeholder CIDs
            cid_str = str(chunk.cid or "").strip().lower()
            if cid_str in {"", "none", "pending"}:
                logger.error(f"Skipping download for chunk {chunk.part_id}: invalid CID '{chunk.cid}'")
                return False

            max_attempts = getattr(config, "downloader_chunk_retries", 5)
            base_sleep = getattr(config, "downloader_retry_base_seconds", 0.25)
            jitter = getattr(config, "downloader_retry_jitter_seconds", 0.2)

            import hashlib as _hashlib
            import random as _random

            for attempt in range(1, max_attempts + 1):
                try:
                    logger.debug(f"Downloading chunk {chunk.part_id} attempt={attempt}/{max_attempts} CID: {chunk.cid}")

                    # Download the chunk using hippius_sdk
                    chunk_data = await hippius_client.s3_download(
                        cid=chunk.cid,
                        subaccount_id=download_request.subaccount,
                        bucket_name=download_request.bucket_name,
                        auto_decrypt=download_request.should_decrypt,
                        download_node=download_request.ipfs_node,
                        return_bytes=True,
                    )

                    md5 = _hashlib.md5(chunk_data).hexdigest()
                    head_hex = chunk_data[:8].hex() if chunk_data else ""
                    tail_hex = chunk_data[-8:].hex() if len(chunk_data) >= 8 else head_hex
                    logger.debug(
                        f"Downloaded chunk {chunk.part_id} cid={str(chunk.cid)[:10]}... len={len(chunk_data)} md5={md5} "
                        f"head8={head_hex} tail8={tail_hex}"
                    )

                    # Store the chunk data in Redis with the expected key
                    await redis_client.set(chunk.redis_key, chunk_data)
                    logger.debug(f"Stored chunk {chunk.part_id} in Redis with key: {chunk.redis_key}")

                    return True

                except Exception as e:
                    if attempt == max_attempts:
                        logger.error(
                            f"Failed to download chunk {chunk.part_id} (CID: {chunk.cid}) after {max_attempts} attempts: {e}"
                        )
                        return False
                    sleep_for = base_sleep * attempt + _random.uniform(0, jitter)
                    logger.warning(
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


async def process_and_log_download(download_request, redis_client):
    """Process download request with logging - runs as background task."""
    success = await process_download_request(download_request, redis_client)
    if success:
        logger.info(
            f"Successfully processed download request {download_request.bucket_name}/{download_request.object_key}"
        )
    else:
        logger.error(f"Failed to process download request {download_request.bucket_name}/{download_request.object_key}")


async def run_downloader_loop():
    """Main loop for downloader service."""
    redis_client = async_redis.from_url(config.redis_url)

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
    asyncio.run(run_downloader_loop())
