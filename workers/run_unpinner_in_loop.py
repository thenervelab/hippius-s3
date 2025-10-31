#!/usr/bin/env python3
import asyncio
import json
import logging
import sys
import tempfile
import time
from pathlib import Path

import aiofiles
import httpx
import redis.asyncio as async_redis
from hippius_sdk.substrate import SubstrateClient


# Add parent directory to path to import hippius_s3 modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.monitoring import initialize_metrics_collector
from hippius_s3.queue import UnpinChainRequest
from hippius_s3.queue import dequeue_unpin_request
from hippius_s3.redis_utils import with_redis_retry


config = get_config()

setup_loki_logging(config, "unpinner")
logger = logging.getLogger(__name__)


async def process_unpin_batch(batch_requests: list[UnpinChainRequest], batch_num: int, total_batches: int) -> bool:
    """Process a single batch of unpin requests (up to 1000 CIDs)."""
    start_time = time.time()
    manifest_objects = []
    user_address = None

    for req in batch_requests:
        if not req.cid:
            continue
        user_address = req.address
        file_name = f"s3-{req.cid}"

        manifest_objects.append(
            {
                "cid": req.cid,
                "filename": file_name,
            },
        )
        logger.debug(f"Added to unpin manifest batch {batch_num}/{total_batches}: {req.address=} {file_name=} {req.cid=}")

    # Check if we have any objects to unpin
    if not manifest_objects:
        logger.info(f"Batch {batch_num}/{total_batches}: No objects to unpin (all requests had empty CIDs)")
        return True

    logger.info(f"Processing batch {batch_num}/{total_batches} with {len(manifest_objects)} CIDs")

    # Create manifest JSON file
    manifest_data = json.dumps(
        manifest_objects,
        indent=2,
    )

    # Create temporary file with manifest
    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".json",
        delete=False,
    ) as temp_file:
        temp_file.write(manifest_data)
        temp_path = temp_file.name

    # Upload manifest to IPFS using httpx with proper redirect handling
    async with httpx.AsyncClient(
        timeout=300.0,
        follow_redirects=True,
    ) as client:
        # Read manifest data
        async with aiofiles.open(temp_path, "rb") as f:
            file_data = await f.read()

        # Ensure we're using HTTPS and the correct endpoint
        base_url = config.ipfs_store_url.rstrip("/")

        upload_url = f"{base_url}/api/v0/add?wrap-with-directory=false&cid-version=1"
        logger.info(f"Uploading manifest to: {upload_url}")

        # Prepare multipart form data
        files = {"file": ("manifest.json", file_data, "application/json")}

        # Use POST with proper redirect handling that maintains method
        response = await client.post(upload_url, files=files)

        logger.info(f"Upload response: {response.status_code} from {response.url}")
        response.raise_for_status()

        # Parse IPFS response (newline-delimited JSON)
        response_text = response.text
        logger.debug(f"IPFS response: {response_text}")

        # Handle newline-delimited JSON response from IPFS
        for line in response_text.strip().split("\n"):
            if line.strip():
                result = json.loads(line)
                if "Hash" in result:
                    manifest_cid = result["Hash"]
                    break
        else:
            raise Exception(f"No valid CID found in IPFS response: {response_text}")

    logger.info(f"Batch {batch_num}/{total_batches}: Uploaded manifest to IPFS with CID: {manifest_cid}")

    # Pin the manifest to local IPFS node
    async with httpx.AsyncClient(timeout=30.0) as client:
        pin_url = f"{base_url}/api/v0/pin/add?arg={manifest_cid}"
        logger.debug(f"Pinning manifest to local IPFS: {pin_url}")
        pin_response = await client.post(pin_url)
        pin_response.raise_for_status()
        logger.info(f"Batch {batch_num}/{total_batches}: Pinned manifest to local IPFS node: {manifest_cid}")

    # Unpin the original CIDs from the IPFS store node
    async with httpx.AsyncClient(timeout=30.0) as client:
        for req in batch_requests:
            if not req.cid:
                continue
            try:
                unpin_url = f"{base_url}/api/v0/pin/rm?arg={req.cid}"
                logger.debug(f"Unpinning CID from IPFS store: {req.cid}")
                unpin_response = await client.post(unpin_url)
                unpin_response.raise_for_status()
                logger.debug(f"Successfully unpinned CID from IPFS store: {req.cid}")
            except Exception as e:
                logger.warning(f"Failed to unpin CID {req.cid} from IPFS store: {e}")

    # Clean up temp file
    Path(temp_path).unlink(missing_ok=True)

    # Submit manifest CID to substrate for cancellation
    seed_phrase = config.resubmission_seed_phrase
    logger.info(f"Initializing SubstrateClient with seed_phrase: {'[PRESENT]' if seed_phrase else '[MISSING]'}")
    if not seed_phrase:
        logger.error("No seed phrase available for SubstrateClient initialization")
        return False

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
        logger.error(f"Batch {batch_num}/{total_batches}: Invalid transaction hash received for manifest {manifest_cid}: {tx_hash}")
        return False

    logger.info(f"Batch {batch_num}/{total_batches}: Successfully submitted unpin manifest {manifest_cid} with transaction: {tx_hash}")
    logger.info(f"Batch {batch_num}/{total_batches}: Unpinned {len(manifest_objects)} files")

    if user_address:
        duration = time.time() - start_time
        get_metrics_collector().record_unpinner_operation(
            main_account=user_address,
            success=True,
            num_files=len(manifest_objects),
            duration=duration,
        )

    return True


async def run_unpinner_loop() -> None:
    """Main loop that monitors the Redis queue and processes unpin requests."""
    redis_client = async_redis.from_url(config.redis_url)
    redis_queues_client = async_redis.from_url(config.redis_queues_url)

    from hippius_s3.queue import initialize_queue_client
    from hippius_s3.redis_cache import initialize_cache_client

    initialize_queue_client(redis_queues_client)
    initialize_cache_client(redis_client)
    initialize_metrics_collector(redis_client)

    logger.info("Starting unpinner service...")
    logger.info(f"Redis URL: {config.redis_url}")
    logger.info(f"Redis Queues URL: {config.redis_queues_url}")

    batch_size = 10000
    user_unpin_requests: dict[str, list[UnpinChainRequest]] = {}

    try:
        while True:
            unpin_request, redis_queues_client = await with_redis_retry(
                lambda rc: dequeue_unpin_request(),
                redis_queues_client,
                config.redis_queues_url,
                "dequeue unpin request",
            )

            if unpin_request:
                try:
                    user_unpin_requests[unpin_request.address].append(unpin_request)
                except KeyError:
                    user_unpin_requests[unpin_request.address] = [unpin_request]

                for user in list(user_unpin_requests.keys()):
                    if len(user_unpin_requests[user]) >= batch_size:
                        batch_to_process = user_unpin_requests[user][:batch_size]
                        user_unpin_requests[user] = user_unpin_requests[user][batch_size:]

                        logger.info(f"Processing batch of {len(batch_to_process)} CIDs for user {user}")
                        try:
                            success = await process_unpin_batch(batch_to_process, 1, 1)
                            if not success:
                                logger.warning(f"Batch processing failed for user {user}")
                        except Exception as e:
                            logger.error(f"Exception processing batch for user {user}: {e}", exc_info=True)

                        if not user_unpin_requests[user]:
                            user_unpin_requests.pop(user)

            else:
                for user in list(user_unpin_requests.keys()):
                    if user_unpin_requests[user]:
                        logger.info(f"Processing remaining {len(user_unpin_requests[user])} CIDs for user {user}")
                        try:
                            success = await process_unpin_batch(user_unpin_requests[user], 1, 1)
                            if success:
                                logger.info(f"SUCCESSFULLY processed remaining requests for user {user}")
                                user_unpin_requests.pop(user)
                            else:
                                logger.warning(f"Some unpins failed for user {user}, will retry later")
                        except Exception as e:
                            logger.error(f"Exception processing remaining batch for user {user}: {e}", exc_info=True)

                await asyncio.sleep(config.unpinner_sleep_loop)

    except KeyboardInterrupt:
        logger.info("Unpinner service stopping...")
    except Exception as e:
        logger.error(f"Fatal error in unpinner loop: {e}", exc_info=True)
        logger.info("Unpinner will restart...")
    finally:
        await redis_client.aclose()  # type: ignore[attr-defined]
        await redis_queues_client.aclose()  # type: ignore[attr-defined]


if __name__ == "__main__":
    while True:
        try:
            asyncio.run(run_unpinner_loop())
        except KeyboardInterrupt:
            logger.info("Unpinner service stopped by user")
            break
        except Exception as e:
            logger.error(f"Unpinner crashed, restarting in 5 seconds: {e}", exc_info=True)
            time.sleep(5)
