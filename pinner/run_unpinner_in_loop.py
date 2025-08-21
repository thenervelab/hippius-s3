#!/usr/bin/env python3
import asyncio
import logging
import sys
from pathlib import Path

import redis.asyncio as async_redis
from hippius_sdk.errors import HippiusSubstrateError
from hippius_sdk.substrate import SubstrateClient


# Add parent directory to path to import hippius_s3 modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.queue import dequeue_unpin_request


config = get_config()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def process_unpin_request(unpin_requests: list[dict]) -> bool:
    """Process unpin requests by canceling storage requests using the Hippius SDK."""
    seed_phrase = None
    cids = []

    for req in unpin_requests:
        cid = req["cid"]
        subaccount = req["subaccount"]
        seed_phrase = req["seed_phrase"]
        file_name = req["file_name"]

        logger.info(f"Processing unpin request for CID={cid}, subaccount={subaccount}")
        cids.append(cid)
        logger.info(f"Submitting storage cancellation to substrate for file: {file_name}, CID: {cid}")

    substrate_client = SubstrateClient(
        seed_phrase=seed_phrase,
        url=config.substrate_url,
    )

    # Process each CID individually since cancel_storage_request takes one CID at a time
    successful_unpins = []
    failed_unpins = []

    for i, cid in enumerate(cids):
        try:
            tx_hash = await substrate_client.cancel_storage_request(
                cid=cid,
                seed_phrase=seed_phrase,
            )

            logger.debug(f"Substrate call result for CID {cid}: {tx_hash}")

            # Check if we got a valid transaction hash
            if not tx_hash or tx_hash == "0x" or len(tx_hash) < 10:
                logger.error(f"Invalid transaction hash received for CID {cid}: {tx_hash}")
                failed_unpins.append(cid)
                continue

            logger.info(f"Successfully unpinned CID {cid} with transaction: {tx_hash}")
            successful_unpins.append(cid)

        except HippiusSubstrateError as e:
            logger.error(f"Failed to unpin CID {cid}: {e}")
            failed_unpins.append(cid)
        except Exception as e:
            logger.error(f"Unexpected error unpinning CID {cid}: {e}")
            failed_unpins.append(cid)

    if successful_unpins:
        logger.info(f"Successfully unpinned {len(successful_unpins)} files: {successful_unpins}")

    if failed_unpins:
        logger.warning(f"Failed to unpin {len(failed_unpins)} files: {failed_unpins}")
        # Return False if any unpins failed to retry later
        return False

    return True


async def run_unpinner_loop():
    """Main loop that monitors the Redis queue and processes unpin requests."""
    redis_client = async_redis.from_url(config.redis_url)

    logger.info("Starting unpinner service...")
    logger.info(f"Redis URL: {config.redis_url}")
    user_unpin_requests = {}

    try:
        while True:
            unpin_request = await dequeue_unpin_request(redis_client)

            if unpin_request:
                try:
                    user_unpin_requests[unpin_request["owner"]].append(unpin_request)
                except KeyError:
                    user_unpin_requests[unpin_request["owner"]] = [unpin_request]

            else:
                # No items in queue, wait a bit before checking again
                for user in list(user_unpin_requests.keys()):
                    success = await process_unpin_request(user_unpin_requests[user])
                    if success:
                        logger.info(
                            f"SUCCESSFULLY processed user's {user} with {len(user_unpin_requests[user])} files"
                        )
                        # Remove that user
                        user_unpin_requests.pop(user)
                    else:
                        logger.warning(f"Some unpins failed for user {user}, will retry later")
                        # Keep the user in the dict to retry later

                await asyncio.sleep(10)

    except KeyboardInterrupt:
        logger.info("Unpinner service stopping...")
    except Exception as e:
        logger.error(f"Error in unpinner loop: {e}")
        raise
    finally:
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(run_unpinner_loop())
