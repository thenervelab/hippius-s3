#!/usr/bin/env python3
import asyncio
import logging
import sys
from pathlib import Path

import redis.asyncio as async_redis

from hippius_sdk.substrate import SubstrateClient


# Add parent directory to path to import hippius_s3 modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.queue import dequeue_unpin_request


config = get_config()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def process_unpin_request(unpin_requests: list[dict]) -> bool:
    """Process unpin requests by creating a manifest and canceling storage request using the Hippius SDK."""
    import json
    import tempfile

    from hippius_sdk import HippiusClient

    seed_phrase = None
    manifest_objects = []

    for req in unpin_requests:
        cid = req["cid"]
        subaccount = req["subaccount"]
        seed_phrase = req["seed_phrase"]
        file_name = req["file_name"]

        logger.info(f"Processing unpin request for CID={cid}, subaccount={subaccount}")

        manifest_objects.append({"cid": cid, "filename": file_name})
        logger.info(f"Added to unpin manifest: {file_name}, CID: {cid}")

    # Create manifest JSON file
    manifest_data = json.dumps(manifest_objects, indent=2)

    # Upload manifest to IPFS
    ipfs_client = HippiusClient(
        get_node=config.ipfs_store_url,
        store_node=config.ipfs_store_url,
    )

    try:
        # Create temporary file with manifest
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as temp_file:
            temp_file.write(manifest_data)
            temp_path = temp_file.name

        # Upload manifest to IPFS using regular file add
        manifest_result = await ipfs_client.add_file(temp_path)

        manifest_cid = manifest_result.cid
        logger.info(f"Created unpin manifest with CID: {manifest_cid}")

        # Clean up temp file
        Path(temp_path).unlink(missing_ok=True)

        # Submit manifest CID to substrate for cancellation
        substrate_client = SubstrateClient(
            seed_phrase=seed_phrase,
            url=config.substrate_url,
        )

        tx_hash = await substrate_client.cancel_storage_request(
            cid=manifest_cid,
            seed_phrase=seed_phrase,
        )

        logger.debug(f"Substrate call result for manifest {manifest_cid}: {tx_hash}")

        # Check if we got a valid transaction hash
        if not tx_hash or tx_hash == "0x" or len(tx_hash) < 10:
            logger.error(f"Invalid transaction hash received for manifest {manifest_cid}: {tx_hash}")
            return False

        logger.info(f"Successfully submitted unpin manifest {manifest_cid} with transaction: {tx_hash}")
        logger.info(f"Unpinned {len(manifest_objects)} files in batch")
        return True

    except Exception as e:
        logger.error(f"Failed to process unpin manifest: {e}")
        return False


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
                        logger.info(f"SUCCESSFULLY processed user's {user} with {len(user_unpin_requests[user])} files")
                        # Remove that user
                        user_unpin_requests.pop(user)
                    else:
                        logger.warning(
                            f"Some unpins failed for user {user}, will retry later"
                        )  # Keep the user in the dict to retry later

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
