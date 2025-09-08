#!/usr/bin/env python3
import asyncio
import logging
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
    hippius_client = HippiusClient(
        ipfs_gateway=config.ipfs_get_url,
        ipfs_api_url=config.ipfs_store_url,
        substrate_url=config.substrate_url,
        encrypt_by_default=False,
    )

    logger.info(f"Processing download request for {download_request.name} with {len(download_request.chunks)} chunks")

    # Process all chunks concurrently with higher concurrency for better IPFS utilization
    semaphore = asyncio.Semaphore(10)  # Increased for better parallel downloads

    async def download_chunk(chunk):
        async with semaphore:
            try:
                logger.debug(f"Downloading chunk {chunk.part_id} with CID: {chunk.cid}")

                # Download the chunk using hippius_sdk
                chunk_data = await hippius_client.s3_download(
                    cid=chunk.cid,
                    subaccount_id=download_request.address,
                    bucket_name=download_request.bucket_name,
                    auto_decrypt=download_request.should_decrypt,
                    download_node=download_request.ipfs_node,
                    return_bytes=True,
                )

                # Store the chunk data in Redis with the expected key
                await redis_client.set(chunk.redis_key, chunk_data)
                logger.debug(f"Stored chunk {chunk.part_id} in Redis with key: {chunk.redis_key}")

                return True

            except Exception as e:
                logger.error(f"Failed to download chunk {chunk.part_id} (CID: {chunk.cid}): {e}")
                return False

    # Download all chunks concurrently
    results = await asyncio.gather(*[download_chunk(chunk) for chunk in download_request.chunks])

    success_count = sum(results)
    total_chunks = len(download_request.chunks)

    if success_count == total_chunks:
        logger.info(f"Successfully downloaded all {total_chunks} chunks for {download_request.name}")
        return True

    logger.error(f"Only downloaded {success_count}/{total_chunks} chunks for {download_request.name}")
    # Clean up any successfully downloaded chunks to prevent partial downloads
    for chunk in download_request.chunks:
        await redis_client.delete(chunk.redis_key)
    return False


async def process_and_log_download(download_request, redis_client):
    """Process download request with logging - runs as background task."""
    success = await process_download_request(download_request, redis_client)
    if success:
        logger.info(f"Successfully processed download request {download_request.name}")
    else:
        logger.error(f"Failed to process download request {download_request.name}")


async def run_downloader_loop():
    """Main loop for downloader service."""
    redis_client = async_redis.from_url(config.redis_url)

    logger.info("Starting downloader service...")
    logger.info(f"Redis URL: {config.redis_url}")

    try:
        while True:
            download_request = await dequeue_download_request(redis_client)

            if download_request:
                # Process download request without blocking the loop
                asyncio.create_task(process_and_log_download(download_request, redis_client))
            else:
                # Tighter loop for faster queue processing
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
