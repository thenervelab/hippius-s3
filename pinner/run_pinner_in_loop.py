#!/usr/bin/env python3
"""
Pinner service that monitors the Redis queue for upload requests and processes them.
"""

import asyncio
import logging
import sys
from pathlib import Path

import redis.asyncio as async_redis
from hippius_sdk.substrate import SubstrateClient, FileInput
from hippius_sdk.errors import HippiusSubstrateError


# Add parent directory to path to import hippius_s3 modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.queue import dequeue_upload_request


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def process_upload_request(upload_request: dict) -> bool:
    """Process a single upload request by creating a storage request using the Hippius SDK."""
    try:
        cid = upload_request["cid"]
        subaccount = upload_request["subaccount"]
        file_path = upload_request["file_path"]
        substrate_url = upload_request["substrate_url"]
        seed_phrase = upload_request["seed_phrase"]

        logger.info(f"Processing upload request for CID={cid}, subaccount={subaccount}")

        # Extract filename from file_path
        filename = Path(file_path).name if file_path else f"{cid}.bin"

        try:
            # Pass the seed phrase directly to avoid password prompts for encrypted config
            substrate_client = SubstrateClient(
                seed_phrase=seed_phrase, url=substrate_url
            )
            logger.info(
                f"Submitting storage request to substrate for file: {filename}, CID: {cid}"
            )

            tx_hash = await substrate_client.storage_request(
                files=[
                    FileInput(
                        file_hash=cid,
                        file_name=filename,
                    )
                ],
                miner_ids=[],
                seed_phrase=seed_phrase,
            )

            logger.debug(f"Substrate call result: {tx_hash}")

            # Check if we got a valid transaction hash
            if not tx_hash or tx_hash == "0x" or len(tx_hash) < 10:
                logger.error(f"Invalid transaction hash received: {tx_hash}")
                raise HippiusSubstrateError(
                    f"Invalid transaction hash received: {tx_hash}. This might indicate insufficient credits or transaction failure."
                )

            logger.info(
                f"Successfully published to substrate with transaction: {tx_hash}"
            )
            return True

        except Exception as e:
            logger.error(f"Substrate call failed: {str(e)}")
            logger.debug(
                "Possible causes: insufficient credits, network issues, invalid seed phrase, or substrate node unavailability"
            )
            raise HippiusSubstrateError(f"Failed to publish to substrate: {str(e)}")

    except Exception as e:
        logger.error(f"Failed to process upload request for CID={upload_request.get('cid', 'unknown')}: {e}")
        return False


async def run_pinner_loop():
    """Main loop that monitors the Redis queue and processes upload requests."""
    config = get_config()
    redis_client = async_redis.from_url(config.redis_url)

    logger.info("Starting pinner service...")
    logger.info(f"Redis URL: {config.redis_url}")

    try:
        while True:
            upload_request = await dequeue_upload_request(redis_client)

            if upload_request:
                success = await process_upload_request(upload_request)
                if success:
                    logger.info("Upload request processed successfully")
                else:
                    logger.error("Failed to process upload request")
            else:
                # No items in queue, wait a bit before checking again
                await asyncio.sleep(0.1)

    except KeyboardInterrupt:
        logger.info("Pinner service stopping...")
    except Exception as e:
        logger.error(f"Error in pinner loop: {e}")
        raise
    finally:
        await redis_client.close()


if __name__ == "__main__":
    asyncio.run(run_pinner_loop())
