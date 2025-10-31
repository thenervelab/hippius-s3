#!/usr/bin/env python3
import asyncio
import json
import logging
import sys
import tempfile
import time
from pathlib import Path
from typing import Dict
from typing import List

import aiofiles
import httpx
import redis.asyncio as async_redis


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.monitoring import initialize_metrics_collector
from hippius_s3.queue import UnpinChainRequest
from hippius_s3.queue import dequeue_unpin_request
from hippius_s3.queue import enqueue_unpin_request
from hippius_s3.redis_utils import with_redis_retry
from hippius_s3.workers.unpinner import UnpinnerWorker


config = get_config()

setup_loki_logging(config, "unpinner")
logger = logging.getLogger(__name__)

EPOCH_SLEEP_SECONDS = 60


async def create_and_upload_manifest(user_address: str, batch_requests: List[UnpinChainRequest]) -> str:
    manifest_objects = []

    for req in batch_requests:
        if not req.cid:
            continue
        file_name = f"s3-{req.cid}"
        manifest_objects.append(
            {
                "cid": req.cid,
                "filename": file_name,
            },
        )

    if not manifest_objects:
        logger.warning(f"No objects to unpin for user {user_address} (all requests had empty CIDs)")
        raise ValueError("No valid CIDs in batch")

    logger.info(f"Creating manifest for user {user_address} with {len(manifest_objects)} CIDs")

    manifest_data = json.dumps(
        manifest_objects,
        indent=2,
    )

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".json",
        delete=False,
    ) as temp_file:
        temp_file.write(manifest_data)
        temp_path = temp_file.name

    async with httpx.AsyncClient(
        timeout=300.0,
        follow_redirects=True,
    ) as client:
        async with aiofiles.open(temp_path, "rb") as f:
            file_data = await f.read()

        base_url = config.ipfs_store_url.rstrip("/")
        upload_url = f"{base_url}/api/v0/add?wrap-with-directory=false&cid-version=1"
        logger.info(f"Uploading manifest to: {upload_url}")

        files = {"file": ("manifest.json", file_data, "application/json")}
        response = await client.post(upload_url, files=files)

        logger.info(f"Upload response: {response.status_code} from {response.url}")
        response.raise_for_status()

        response_text = response.text
        logger.debug(f"IPFS response: {response_text}")

        for line in response_text.strip().split("\n"):
            if line.strip():
                result = json.loads(line)
                if "Hash" in result:
                    manifest_cid = result["Hash"]
                    break
        else:
            raise Exception(f"No valid CID found in IPFS response: {response_text}")

    logger.info(f"Uploaded manifest for user {user_address} to IPFS with CID: {manifest_cid}")

    async with httpx.AsyncClient(timeout=30.0) as client:
        pin_url = f"{base_url}/api/v0/pin/add?arg={manifest_cid}"
        logger.debug(f"Pinning manifest to local IPFS: {pin_url}")
        pin_response = await client.post(pin_url)
        pin_response.raise_for_status()
        logger.info(f"Pinned manifest to local IPFS node: {manifest_cid}")

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

    Path(temp_path).unlink(missing_ok=True)

    return manifest_cid


async def run_unpinner_loop() -> None:
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

    unpinner_worker = UnpinnerWorker(config)

    while True:
        requests = []

        while True:
            unpin_request, redis_queues_client = await with_redis_retry(
                lambda rc: dequeue_unpin_request(),
                redis_queues_client,
                config.redis_queues_url,
                "dequeue unpin request",
            )

            if unpin_request:
                requests.append(unpin_request)
            else:
                break

        if not requests:
            logger.info("No unpin requests in queue, sleeping 10s...")
            await asyncio.sleep(10)
            continue

        logger.info(f"Collected {len(requests)} unpin requests from queue")

        user_request_map: Dict[str, List[UnpinChainRequest]] = {}
        for req in requests:
            if req.address not in user_request_map:
                user_request_map[req.address] = []
            user_request_map[req.address].append(req)

        manifest_cids: Dict[str, str] = {}
        failed_users = []

        for user_addr, user_requests in user_request_map.items():
            try:
                manifest_cid = await create_and_upload_manifest(user_addr, user_requests)
                manifest_cids[user_addr] = manifest_cid
                logger.info(f"Created manifest for user {user_addr}: {manifest_cid}")
            except Exception as e:
                logger.error(f"Failed to create manifest for user {user_addr}: {e}", exc_info=True)
                failed_users.append(user_addr)

        if not manifest_cids:
            logger.warning("No manifests created, requeuing all requests")
            for req in requests:
                await enqueue_unpin_request(req)
            logger.info(f"Sleeping {EPOCH_SLEEP_SECONDS}s until next epoch...")
            await asyncio.sleep(EPOCH_SLEEP_SECONDS)
            continue

        if failed_users:
            logger.warning(f"Failed to create manifests for {len(failed_users)} users, requeuing their requests")
            for user_addr in failed_users:
                for req in user_request_map[user_addr]:
                    await enqueue_unpin_request(req)

        successful_requests = [req for req in requests if req.address not in failed_users]

        try:
            await unpinner_worker.process_batch(successful_requests, manifest_cids)
            logger.info(f"Successfully submitted batch of {len(manifest_cids)} unpin manifests")
        except Exception:
            logger.exception(f"Unpinner batch submission failed for {len(manifest_cids)} manifests")
            for req in successful_requests:
                await enqueue_unpin_request(req)
            logger.info(f"Requeued {len(successful_requests)} failed requests")
        finally:
            logger.info(f"Sleeping {EPOCH_SLEEP_SECONDS}s until next epoch...")
            await asyncio.sleep(EPOCH_SLEEP_SECONDS)


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
