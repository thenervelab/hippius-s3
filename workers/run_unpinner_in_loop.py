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


async def process_unpin_request(unpin_requests: list[UnpinChainRequest]) -> bool:
    """Process unpin requests by creating a manifest and canceling storage request using the Hippius SDK."""
    start_time = time.time()
    seed_phrase = None
    manifest_objects = []
    user_address = None

    for req in unpin_requests:
        cid = req.cid
        if not req.cid:
            # skip files that have not yet been pinned
            continue
        subaccount = req.subaccount
        seed_phrase = req.subaccount_seed_phrase
        user_address = req.address
        file_name = f"{req.bucket_name}/{req.object_key}"
        logger.info(f"Processing unpin request for CID={cid}, subaccount={subaccount}")

        manifest_objects.append({"cid": cid, "filename": file_name})
        logger.info(f"Added to unpin manifest: {subaccount=} {file_name=} {cid=}")

    # Check if we have any objects to unpin
    if not manifest_objects:
        logger.info("No objects to unpin (all requests had empty CIDs)")
        return True

    # Create manifest JSON file
    manifest_data = json.dumps(
        manifest_objects,
        indent=2,
    )

    # Upload manifest to IPFS
    try:
        # Create temporary file with manifest
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".json",
            delete=False,
        ) as temp_file:
            temp_file.write(manifest_data)
            temp_path = temp_file.name

        # Upload manifest to IPFS using httpx with proper redirect handling
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
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

        logger.info(f"Uploaded manifest directly to IPFS with CID: {manifest_cid}")

        # Pin the manifest to local IPFS node
        async with httpx.AsyncClient(timeout=30.0) as client:
            pin_url = f"{base_url}/api/v0/pin/add?arg={manifest_cid}"
            logger.debug(f"Pinning manifest to local IPFS: {pin_url}")
            pin_response = await client.post(pin_url)
            pin_response.raise_for_status()
            logger.info(f"Pinned manifest to local IPFS node: {manifest_cid}")

        manifest = {"cid": manifest_cid}

        logger.info(f"Created unpin manifest with CID: {manifest['cid']}")

        # Clean up temp file
        Path(temp_path).unlink(missing_ok=True)

        # Submit manifest CID to substrate for cancellation
        logger.info(f"Initializing SubstrateClient with seed_phrase: {'[PRESENT]' if seed_phrase else '[MISSING]'}")
        if not seed_phrase:
            logger.error("No seed phrase available for SubstrateClient initialization")
            return False

        substrate_client = SubstrateClient(
            seed_phrase=seed_phrase,
            url=config.substrate_url,
        )

        tx_hash = await substrate_client.cancel_storage_request(
            cid=manifest["cid"],
            seed_phrase=seed_phrase,
        )

        logger.debug(f"Substrate call result for manifest {manifest['cid']}: {tx_hash}")

        # Check if we got a valid transaction hash
        if not tx_hash or tx_hash == "0x" or len(tx_hash) < 10:
            logger.error(f"Invalid transaction hash received for manifest {manifest['cid']}: {tx_hash}")
            return False

        logger.info(f"Successfully submitted unpin manifest {manifest['cid']} with transaction: {tx_hash}")
        logger.info(f"Unpinned {len(manifest_objects)} files in batch")

        if user_address:
            duration = time.time() - start_time
            get_metrics_collector().record_unpinner_operation(
                main_account=user_address,
                success=True,
                num_files=len(manifest_objects),
                duration=duration,
            )

        return True

    except Exception as e:
        logger.error(f"Failed to process unpin manifest: {e}")

        if user_address:
            duration = time.time() - start_time
            get_metrics_collector().record_unpinner_operation(
                main_account=user_address,
                success=False,
                num_files=len(manifest_objects),
                duration=duration,
            )

        return False


async def run_unpinner_loop():
    """Main loop that monitors the Redis queue and processes unpin requests."""
    redis_client = async_redis.from_url(config.redis_url)

    initialize_metrics_collector(redis_client)

    logger.info("Starting unpinner service...")
    logger.info(f"Redis URL: {config.redis_url}")
    user_unpin_requests = {}

    try:
        while True:
            unpin_request, redis_client = await with_redis_retry(
                dequeue_unpin_request,
                redis_client,
                config.redis_url,
                "dequeue unpin request",
            )

            if unpin_request:
                try:
                    user_unpin_requests[unpin_request.address].append(unpin_request)
                except KeyError:
                    user_unpin_requests[unpin_request.address] = [unpin_request]

            else:
                for user in list(user_unpin_requests.keys()):
                    success = await process_unpin_request(user_unpin_requests[user])
                    if success:
                        logger.info(f"SUCCESSFULLY processed user's {user} with {len(user_unpin_requests[user])} files")
                        user_unpin_requests.pop(user)
                    else:
                        logger.warning(
                            f"Some unpins failed for user {user}, will retry later"
                        )

                await asyncio.sleep(config.unpinner_sleep_loop)

    except KeyboardInterrupt:
        logger.info("Unpinner service stopping...")
    except Exception as e:
        logger.error(f"Error in unpinner loop: {e}")
        raise
    finally:
        await redis_client.close()


if __name__ == "__main__":
    asyncio.run(run_unpinner_loop())
