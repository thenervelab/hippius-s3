#!/usr/bin/env python3
import asyncio  # noqa: I001
import logging
import sys
from pathlib import Path
from typing import Union

import asyncpg
import redis.asyncio as async_redis
from hippius_sdk.errors import HippiusSubstrateError
from hippius_sdk.ipfs import S3PublishPin
from hippius_sdk.substrate import FileInput
from hippius_sdk.substrate import SubstrateClient

# Add parent directory to path to import hippius_s3 modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.ipfs_service import IPFSService
from hippius_s3.queue import Chunk
from hippius_s3.queue import MultipartUploadChainRequest
from hippius_s3.queue import SimpleUploadChainRequest
from hippius_s3.queue import dequeue_upload_request
from hippius_s3.utils import upsert_cid_and_get_id


config = get_config()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def _process_simple_upload(
    payload: SimpleUploadChainRequest,
    db: asyncpg.Connection,
    ipfs_service: IPFSService,
    redis_client: async_redis.Redis,
) -> S3PublishPin:
    chunk_data = await redis_client.get(payload.chunk.redis_key)
    if not chunk_data:
        raise ValueError(f"Simple upload chunk not found for key: {payload.chunk.redis_key}")

    # Encrypt chunk using s3_publish
    s3_result = await ipfs_service.client.s3_publish(
        content=chunk_data,
        encrypt=payload.should_encrypt,
        seed_phrase=payload.subaccount_seed_phrase,
        subaccount_id=payload.subaccount,
        bucket_name=payload.bucket_name,
        file_name=payload.object_key,
        store_node=payload.ipfs_node,
        pin_node=payload.ipfs_node,
        substrate_url=payload.substrate_url,
        publish=False,
    )

    logger.info(f"Encrypted simple upload -> CID: {s3_result['cid']}")

    # Update objects table with CID
    await db.execute(
        "UPDATE objects SET cid_id = $1, multipart = FALSE WHERE id = $2",
        await upsert_cid_and_get_id(
            db,
            s3_result["cid"],
        ),
        payload.object_id,
    )

    # Clean up Redis chunk data
    await redis_client.delete(payload.chunk.redis_key)

    return s3_result


async def _process_multipart_chunk(
    payload: SimpleUploadChainRequest,
    chunk: Chunk,
    db: asyncpg.Connection,
    ipfs_service: IPFSService,
    redis_client: async_redis.Redis,
) -> tuple[S3PublishPin, Chunk]:
    chunk_data = await redis_client.get(chunk.redis_key)
    if not chunk_data:
        raise ValueError(f"Multipart chunk not found for key: {chunk.redis_key}")

    # Encrypt chunk using s3_publish
    s3_result = await ipfs_service.client.s3_publish(
        content=chunk_data,
        encrypt=payload.should_encrypt,
        seed_phrase=payload.subaccount_seed_phrase,
        subaccount_id=payload.subaccount,
        bucket_name=payload.bucket_name,
        file_name=payload.object_key,
        store_node=payload.ipfs_node,
        pin_node=payload.ipfs_node,
        substrate_url=payload.substrate_url,
        publish=False,
    )

    logger.info(f"Encrypted multipart chunk {chunk.id} -> CID: {s3_result['cid']}")

    # Clean up Redis chunk data
    await redis_client.delete(chunk.redis_key)

    return s3_result, chunk


async def _process_multipart_upload(
    payload: SimpleUploadChainRequest,
    db: asyncpg.Connection,
    ipfs_service: IPFSService,
    redis_client: async_redis.Redis,
) -> list[S3PublishPin]:
    semaphore = asyncio.Semaphore(5)

    async def process_chunk_with_semaphore(chunk: Chunk) -> tuple[S3PublishPin, Chunk]:
        async with semaphore:
            return await _process_multipart_chunk(
                payload=payload,
                db=db,
                chunk=chunk,
                ipfs_service=ipfs_service,
                redis_client=redis_client,
            )

    tasks = [process_chunk_with_semaphore(chunk) for chunk in payload.chunks]
    chunk_results = await asyncio.gather(*tasks)

    # Update parts table with CIDs for all chunks
    for s3_result, chunk in chunk_results:
        cid_id = await upsert_cid_and_get_id(db, s3_result["cid"])
        await db.execute(
            "UPDATE parts SET cid_id = $1 WHERE object_id = $2 AND part_number = $3",
            cid_id,
            payload.object_id,
            chunk.id,
        )

    # Update objects table to mark as multipart
    await db.execute(
        "UPDATE objects SET multipart = TRUE WHERE id = $1",
        payload.object_id,
    )

    return [result[0] for result in chunk_results]


async def process_upload_request(
    upload_requests: list[
        Union[
            SimpleUploadChainRequest,
            MultipartUploadChainRequest,
        ]
    ],
    db: asyncpg.Connection,
    redis_client: async_redis.Redis,
) -> bool:
    """Process upload requests by processing chunks, encrypting, and updating database."""
    ipfs_service = IPFSService(config, redis_client)
    seed_phrase = None
    files = []

    for payload in upload_requests:
        logger.info(f"Processing upload request for object_key={payload.object_key}, object_id={payload.object_id}")

        if hasattr(payload, "chunks"):  # Multipart upload
            results = await _process_multipart_upload(
                payload=payload,
                db=db,
                ipfs_service=ipfs_service,
                redis_client=redis_client,
            )

            files.extend(
                [
                    FileInput(
                        file_hash=result.cid,
                        file_name=result.file_name,
                    )
                    for result in results
                ]
            )
        else:  # Simple upload
            result = await _process_simple_upload(
                payload=payload,
                db=db,
                ipfs_service=ipfs_service,
                redis_client=redis_client,
            )
            files.append(
                FileInput(
                    file_hash=result.cid,
                    file_name=result.file_name,
                )
            )

    # Create substrate storage request
    substrate_client = SubstrateClient(
        seed_phrase=seed_phrase,
        url=config.substrate_url,
    )

    tx_hash = await substrate_client.storage_request(
        files=files,
        miner_ids=[],
        seed_phrase=seed_phrase,
    )

    logger.info(f"Processed {len(upload_requests)} upload requests")
    logger.debug(f"Substrate call result: {tx_hash}")

    if not tx_hash or tx_hash == "0x" or len(tx_hash) < 10:
        logger.error(f"Invalid transaction hash received: {tx_hash}")
        raise HippiusSubstrateError(
            f"Invalid transaction hash received: {tx_hash}. This might indicate insufficient credits or transaction failure."
        )

    logger.info(f"Successfully published to substrate with transaction: {tx_hash}")
    return True


async def run_pinner_loop():
    """Main loop that monitors the Redis queue and processes upload requests."""
    redis_client = async_redis.from_url(config.redis_url)
    db = await asyncpg.connect(config.database_url)

    logger.info("Starting pinner service...")
    logger.info(f"Redis URL: {config.redis_url}")
    logger.info(f"Database connected: {config.database_url}")
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
                    success = await process_upload_request(user_upload_requests[user], db, redis_client)
                    if success:
                        logger.info(
                            f"SUCCESSFULLY processed user's {user} with {len(user_upload_requests[user])} files"
                        )
                    else:
                        logger.info(
                            f"Failed to batch and serve {len(user_upload_requests[user])} pin requests for user {user}"
                        )

                user_upload_requests = {}
                await asyncio.sleep(config.pinner_sleep_loop)

    except KeyboardInterrupt:
        logger.info("Pinner service stopping...")
    except Exception as e:
        logger.error(f"Error in pinner loop: {e}")
        raise
    finally:
        await redis_client.aclose()
        await db.close()


if __name__ == "__main__":
    asyncio.run(run_pinner_loop())
