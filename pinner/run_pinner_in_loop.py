#!/usr/bin/env python3
import asyncio
import logging
import pathlib
import sys
from pathlib import Path

import redis.asyncio as async_redis

from hippius_sdk.errors import HippiusSubstrateError
from hippius_sdk.substrate import FileInput
from hippius_sdk.substrate import SubstrateClient


# Add parent directory to path to import hippius_s3 modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.queue import dequeue_upload_request


config = get_config()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def process_upload_request(upload_requests: list[dict]) -> bool:
    """Process a single upload request by creating a storage request using the Hippius SDK."""
    seed_phrase = None
    files = []
    for req in upload_requests:
        # Note: SimpleUploadChainRequest doesn't have cid field, it gets generated later
        subaccount = req.subaccount
        seed_phrase = req.subaccount_seed_phrase
        file_name = req.object_key.split('/')[-1]  # Extract filename from object_key

        logger.info(f"Processing upload request for object_key={req.object_key}, subaccount={subaccount}")

        # TODO: CID needs to be retrieved after upload processing is complete
        # files.append(
        #     FileInput(
        #         file_hash=cid,
        #         file_name=file_name,
        #     )
        # )
        logger.info(f"Skipping storage request for now - CID not available in upload request: {file_name}")

    # TODO: Substrate client call disabled until CID retrieval is implemented
    # substrate_client = SubstrateClient(
    #     seed_phrase=seed_phrase,
    #     url=config.substrate_url,
    # )

    # tx_hash = await substrate_client.storage_request(
    #     files=files,
    #     miner_ids=[],
    #     seed_phrase=seed_phrase,
    # )

    logger.info(f"Processed {len(upload_requests)} upload requests (substrate call disabled)")

    # logger.debug(f"Substrate call result: {tx_hash}")

    # TODO: Enable transaction hash validation when substrate calls are re-enabled
    # if not tx_hash or tx_hash == "0x" or len(tx_hash) < 10:
    #     logger.error(f"Invalid transaction hash received: {tx_hash}")
    #     raise HippiusSubstrateError(
    #         f"Invalid transaction hash received: {tx_hash}. This might indicate insufficient credits or transaction failure."
    #     )

    # logger.info(f"Successfully published to substrate with transaction: {tx_hash}")
    return True


async def run_pinner_loop():
    """Main loop that monitors the Redis queue and processes upload requests."""
    redis_client = async_redis.from_url(config.redis_url)

    logger.info("Starting pinner service...")
    logger.info(f"Redis URL: {config.redis_url}")
    user_upload_requests = {}

    try:
        while True:
            upload_request = await dequeue_upload_request(redis_client)

            if upload_request:
                try:
                    user_upload_requests[upload_request.address].append(upload_request)
                except KeyError:
                    user_upload_requests[upload_request.address] = [upload_request]

            else:
                # No items in queue, wait a bit before checking again
                for user in user_upload_requests:
                    success = await process_upload_request(user_upload_requests[user])
                    if success:
                        logger.info(
                            f"SUCCESSFULLY processed user's {user} with {len(user_upload_requests[user])} files"
                        )
                    else:
                        logger.info(
                            f"Failed to batch and serve {len(user_upload_requests[user])} pin requests for user {user}"
                        )

                user_upload_requests = {}
                await asyncio.sleep(60)

    except KeyboardInterrupt:
        logger.info("Pinner service stopping...")
    except Exception as e:
        logger.error(f"Error in pinner loop: {e}")
        raise
    finally:
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(run_pinner_loop())
