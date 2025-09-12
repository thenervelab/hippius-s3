#!/usr/bin/env python3
import asyncio  # noqa: I001
import hashlib
import json
import logging
import os
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

    # Mangle filename if encrypting
    file_name = payload.object_key
    if payload.should_encrypt:
        filename_hash = hashlib.md5(f"{payload.object_key}_md5".encode()).hexdigest()
        file_name = f"{filename_hash}.chunk"

    # Encrypt chunk using s3_publish (debug: before encryption already logged)
    s3_result = await ipfs_service.client.s3_publish(
        content=chunk_data,
        encrypt=payload.should_encrypt,
        seed_phrase=payload.subaccount_seed_phrase,
        subaccount_id=payload.subaccount,
        bucket_name=payload.bucket_name,
        file_name=file_name,
        store_node=payload.ipfs_node,
        pin_node=payload.ipfs_node,
        substrate_url=payload.substrate_url,
        publish=False,
    )

    encryption_status = "encrypted" if payload.should_encrypt else "unencrypted"
    logger.info(f"Processed {encryption_status} simple upload -> CID: {s3_result.cid}")

    # Update objects table with CID
    await db.execute(
        "UPDATE objects SET cid_id = $1, multipart = FALSE WHERE object_id = $2",
        await upsert_cid_and_get_id(
            db,
            s3_result.cid,
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

    # Debug: compute md5 and show first/last bytes for validation
    import hashlib as _hashlib
    import re as _re

    md5_pre = _hashlib.md5(chunk_data).hexdigest()
    head_hex = chunk_data[:8].hex() if chunk_data else ""
    tail_hex = chunk_data[-8:].hex() if len(chunk_data) >= 8 else head_hex
    m = _re.search(r":part:(\d+)$", chunk.redis_key)
    part_number_db = int(m.group(1)) if m else chunk.id
    logger.debug(
        f"Redis multipart read: part={part_number_db} key={chunk.redis_key} len={len(chunk_data)} "
        f"md5={md5_pre} head8={head_hex} tail8={tail_hex}"
    )

    # Mangle filename if encrypting
    file_name = payload.object_key
    if payload.should_encrypt:
        filename_hash = hashlib.md5(f"{payload.object_key}_{chunk.id}".encode()).hexdigest()
        file_name = f"{filename_hash}.chunk"

    # Encrypt chunk using s3_publish
    s3_result = await ipfs_service.client.s3_publish(
        content=chunk_data,
        encrypt=payload.should_encrypt,
        seed_phrase=payload.subaccount_seed_phrase,
        subaccount_id=payload.subaccount,
        bucket_name=payload.bucket_name,
        file_name=file_name,
        store_node=payload.ipfs_node,
        pin_node=payload.ipfs_node,
        substrate_url=payload.substrate_url,
        publish=False,
    )

    import hashlib as _hashlib

    encryption_status = "encrypted" if payload.should_encrypt else "unencrypted"
    md5_post = _hashlib.md5(chunk_data).hexdigest()
    head_hex_post = chunk_data[:8].hex() if chunk_data else ""
    tail_hex_post = chunk_data[-8:].hex() if len(chunk_data) >= 8 else head_hex_post
    logger.debug(
        f"After encrypt/publish: part={part_number_db} status={encryption_status} cid={s3_result.cid} "
        f"md5={md5_post} head8={head_hex_post} tail8={tail_hex_post}"
    )

    # Clean up Redis chunk data
    await redis_client.delete(chunk.redis_key)

    return s3_result, chunk


async def _process_multipart_upload(
    payload: SimpleUploadChainRequest,
    db: asyncpg.Connection,
    ipfs_service: IPFSService,
    redis_client: async_redis.Redis,
) -> tuple[list[S3PublishPin], S3PublishPin]:
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

    # Process first part synchronously to ensure encryption key is generated once,
    # then parallelize the remaining parts to avoid key-generation races.
    chunks_sorted = sorted(payload.chunks, key=lambda c: c.id)
    first_result = await process_chunk_with_semaphore(chunks_sorted[0])
    logger.info(f"Primed encryption key by processing first part synchronously: part={chunks_sorted[0].id}")
    tasks = [process_chunk_with_semaphore(chunk) for chunk in chunks_sorted[1:]]
    other_results = await asyncio.gather(*tasks) if tasks else []
    chunk_results = [first_result] + other_results

    # Update parts table with CIDs for all chunks
    import re as _re

    for s3_result, chunk in chunk_results:
        # Derive authoritative part_number from redis_key to avoid any client-side mismatch
        m = _re.search(r":part:(\d+)$", chunk.redis_key)
        part_number_db = int(m.group(1)) if m else chunk.id
        if part_number_db != chunk.id:
            logger.warning(
                f"Part number mismatch detected: chunk.id={chunk.id} redis_key={chunk.redis_key} -> using {part_number_db}"
            )
        cid_id = await upsert_cid_and_get_id(db, s3_result.cid)
        await db.execute(
            "UPDATE parts SET cid_id = $1 WHERE object_id = $2 AND part_number = $3",
            cid_id,
            payload.object_id,
            part_number_db,
        )
        logger.debug(
            f"Updated parts mapping: object_id={payload.object_id} part_number={part_number_db} cid={s3_result.cid}"
        )

    # Create manifest with chunk indices and CIDs
    manifest = []
    for s3_result, chunk in chunk_results:
        m = _re.search(r":part:(\d+)$", chunk.redis_key)
        part_number_db = int(m.group(1)) if m else chunk.id
        manifest.append([part_number_db, s3_result.cid])

    # Sort manifest by chunk index to ensure proper order
    manifest.sort(key=lambda x: x[0])

    # Create filename_manifest.json and publish it
    manifest_filename = f"{payload.object_key}_manifest.json"
    manifest_data = json.dumps(manifest).encode()

    # Publish the manifest to IPFS
    manifest_result = await ipfs_service.client.s3_publish(
        content=manifest_data,
        encrypt=False,
        seed_phrase=payload.subaccount_seed_phrase,
        subaccount_id=payload.subaccount,
        bucket_name=payload.bucket_name,
        file_name=manifest_filename,
        store_node=payload.ipfs_node,
        pin_node=payload.ipfs_node,
        substrate_url=payload.substrate_url,
        publish=False,
    )

    logger.info(f"Created manifest for multipart upload -> Main CID: {manifest_result.cid}")

    # Save the main object CID
    main_cid_id = await upsert_cid_and_get_id(db, manifest_result.cid)

    # Update objects table with main CID and mark as multipart
    await db.execute(
        "UPDATE objects SET cid_id = $1, multipart = TRUE WHERE object_id = $2",
        main_cid_id,
        payload.object_id,
    )

    return [result[0] for result in chunk_results], manifest_result


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
    seed_phrase = upload_requests[0].subaccount_seed_phrase  # All requests for same user have same seed phrase
    files = []

    # Update all objects status to 'pinning' when processing starts
    for payload in upload_requests:
        await db.execute(
            "UPDATE objects SET status = 'pinning' WHERE object_id = $1",
            payload.object_id,
        )
        logger.info(f"Updated object {payload.object_id} status to 'pinning'")

    for payload in upload_requests:
        logger.info(
            f"Processing upload request for object_key={payload.object_key}, object_id={payload.object_id}, "
            f"should_encrypt={payload.should_encrypt}, chunks={len(getattr(payload, 'chunks', [])) or 1}"
        )

        if hasattr(payload, "chunks"):  # Multipart upload
            try:
                chunk_results, manifest_result = await _process_multipart_upload(
                    payload=payload,
                    db=db,
                    ipfs_service=ipfs_service,
                    redis_client=redis_client,
                )

                # Add individual chunk CIDs
                files.extend(
                    [
                        FileInput(
                            file_hash=result.cid,
                            file_name=result.file_name,
                        )
                        for result in chunk_results
                    ]
                )

                # Add manifest CID
                files.append(
                    FileInput(
                        file_hash=manifest_result.cid,
                        file_name=manifest_result.file_name,
                    )
                )
            except Exception as e:
                logger.error(
                    f"Failed to process multipart upload for object_id={payload.object_id}, object_key={payload.object_key}: {e}"
                )
                # Mark all objects in this batch as failed
                for req in upload_requests:
                    await db.execute(
                        "UPDATE objects SET status = 'failed' WHERE object_id = $1",
                        req.object_id,
                    )
                return False
        else:  # Simple upload
            try:
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
            except Exception as e:
                logger.error(
                    f"Failed to process simple upload for object_id={payload.object_id}, object_key={payload.object_key}: {e}"
                )
                # Mark all objects in this batch as failed
                for req in upload_requests:
                    await db.execute(
                        "UPDATE objects SET status = 'failed' WHERE object_id = $1",
                        req.object_id,
                    )
                return False

    # Optionally skip substrate publish for ipfs-only mode
    if os.getenv("HIPPIUS_PUBLISH_MODE", "full") == "ipfs_only":
        logger.info("Skipping substrate publish because HIPPIUS_PUBLISH_MODE=ipfs_only; marking objects as uploaded")
        for payload in upload_requests:
            await db.execute(
                "UPDATE objects SET status = 'uploaded' WHERE object_id = $1",
                payload.object_id,
            )
            logger.info(f"Updated object {payload.object_id} status to 'uploaded'")
        return True

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
            f"Invalid transaction hash received: {tx_hash}. "
            "This might indicate insufficient credits or transaction failure."
        )

    logger.info(f"Successfully published to substrate with transaction: {tx_hash}")

    # Update all processed objects status to 'uploaded'
    for payload in upload_requests:
        await db.execute(
            "UPDATE objects SET status = 'uploaded' WHERE object_id = $1",
            payload.object_id,
        )
        logger.info(f"Updated object {payload.object_id} status to 'uploaded'")

    return True


async def run_pinner_loop():
    """Main loop that monitors the Redis queue and processes upload requests."""
    redis_client = async_redis.from_url(config.redis_url)
    db = await asyncpg.connect(config.database_url)

    logger.info("Starting workers service...")
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
        logger.error(f"Error in workers loop: {e}")
        raise
    finally:
        await redis_client.aclose()
        await db.close()


if __name__ == "__main__":
    asyncio.run(run_pinner_loop())
