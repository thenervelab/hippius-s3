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
from hippius_sdk.ipfs import S3PublishPin
from hippius_sdk.substrate import FileInput

# Add parent directory to path to import hippius_s3 modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.config import get_config
from hippius_s3.ipfs_service import IPFSService
from hippius_s3.queue import Chunk
from hippius_s3.queue import MultipartUploadChainRequest
from hippius_s3.queue import SimpleUploadChainRequest
from hippius_s3.queue import dequeue_upload_request
from hippius_s3.utils import upsert_cid_and_get_id
from workers.substrate import submit_storage_request


config = get_config()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Keep Redis caches warm after pin by expiring instead of deleting
_CACHE_TTL_SECONDS = int(os.getenv("HIPPIUS_CACHE_TTL", "1800"))


async def _process_simple_upload(
    payload: SimpleUploadChainRequest,
    db: asyncpg.Connection,
    ipfs_service: IPFSService,
    redis_client: async_redis.Redis,
) -> S3PublishPin:
    from hippius_s3.cache import RedisObjectPartsCache

    obj_cache = RedisObjectPartsCache(redis_client)
    # For simple upload, the chunk id should be 0
    chunk_data = await obj_cache.get(payload.object_id, int(payload.chunk.id))
    if not chunk_data:
        raise ValueError(f"Simple upload chunk not found: object_id={payload.object_id} part={int(payload.chunk.id)}")

    # Mangle filename if encrypting
    file_name = payload.object_key
    if payload.should_encrypt:
        filename_hash = hashlib.md5(f"{payload.object_key}_md5".encode()).hexdigest()
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

    # If there is a provisional multipart part 0 row with missing or placeholder CID, backfill it
    try:
        base_cid_id = await upsert_cid_and_get_id(db, s3_result.cid)
        base_md5 = hashlib.md5(chunk_data).hexdigest()
        await db.execute(
            """
            UPDATE parts
            SET ipfs_cid = $1,
                cid_id = $2,
                size_bytes = COALESCE(size_bytes, $3),
                etag = COALESCE(etag, $4)
            WHERE object_id = $5
              AND part_number = 0
              AND (
                    ipfs_cid IS NULL OR ipfs_cid = '' OR ipfs_cid = 'pending' OR ipfs_cid = 'None'
                  )
            """,
            s3_result.cid,
            base_cid_id,
            len(chunk_data),
            base_md5,
            payload.object_id,
        )
        logger.info(f"Backfilled base part(0) CID for object_id={payload.object_id} cid={s3_result.cid}")
    except Exception as e:
        logger.debug(
            f"Failed to backfill base part CID for object_id={payload.object_id}: {e}",
            exc_info=True,
        )

    # Keep cache for a while to ensure immediate consistency on reads (unified multipart)
    await redis_client.expire(f"obj:{payload.object_id}:part:0", _CACHE_TTL_SECONDS)

    return s3_result


async def _process_multipart_chunk(
    payload: SimpleUploadChainRequest,
    chunk: Chunk,
    db: asyncpg.Connection,
    ipfs_service: IPFSService,
    redis_client: async_redis.Redis,
) -> tuple[S3PublishPin, Chunk]:
    obj_cache = RedisObjectPartsCache(redis_client)
    # Derive part_number from key to drive both read and DB updates

    part_number_db = int(chunk.id)

    chunk_data = await obj_cache.get(payload.object_id, part_number_db)
    if not chunk_data:
        raise ValueError(f"Multipart chunk not found for object_id={payload.object_id} part={part_number_db}")

    # Debug: compute md5 and show first/last bytes for validation
    import hashlib as _hashlib

    md5_pre = _hashlib.md5(chunk_data).hexdigest()
    head_hex = chunk_data[:8].hex() if chunk_data else ""
    tail_hex = chunk_data[-8:].hex() if len(chunk_data) >= 8 else head_hex
    # part_number_db already computed above
    logger.debug(
        f"Multipart read: part={part_number_db} len={len(chunk_data)} md5={md5_pre} head8={head_hex} tail8={tail_hex}"
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

    for s3_result, chunk in chunk_results:
        part_number_db = int(chunk.id)
        if part_number_db != chunk.id:
            logger.warning(f"Part number mismatch detected: chunk.id={chunk.id} -> using {part_number_db}")
        cid_id = await upsert_cid_and_get_id(db, s3_result.cid)
        # Set both cid_id and ipfs_cid so tests that check ipfs_cid != 'pending' pass,
        # and pipeline assembly can reference a concrete CID immediately.
        await db.execute(
            "UPDATE parts SET cid_id = $1, ipfs_cid = $2 WHERE object_id = $3 AND part_number = $4",
            cid_id,
            s3_result.cid,
            payload.object_id,
            part_number_db,
        )
        # Update last_append_at timestamp for manifest builder scheduling
        await db.execute(
            "UPDATE objects SET last_append_at = NOW() WHERE object_id = $1",
            payload.object_id,
        )
        logger.debug(
            f"Updated parts mapping: object_id={payload.object_id} part_number={part_number_db} cid={s3_result.cid}"
        )

    # Create manifest with chunk indices and CIDs
    manifest = []
    for s3_result, chunk in chunk_results:
        part_number_db = int(chunk.id)
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

    # Keep per-chunk caches for a while; GETs may still be assembling from cache
    obj_cache = RedisObjectPartsCache(redis_client)
    for _, chunk in chunk_results:
        part_number_db = int(chunk.id)
        await obj_cache.expire(payload.object_id, part_number_db, ttl=_CACHE_TTL_SECONDS)

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

    # Submit storage request to substrate
    cids = [file.file_hash for file in files]
    tx_hash = await submit_storage_request(cids=cids, seed_phrase=seed_phrase, substrate_url=config.substrate_url)

    logger.info(f"Processed {len(upload_requests)} upload requests with transaction: {tx_hash}")

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
        # Batch policy: flush per user when 10 items or 500ms elapsed since first item
        BATCH_MAX_ITEMS = int(os.getenv("PINNER_BATCH_MAX_ITEMS", "10"))
        BATCH_MAX_AGE_SEC = float(os.getenv("PINNER_BATCH_MAX_AGE_SEC", "0.5"))
        batch_first_seen: dict[str, float] = {}

        while True:
            upload_request = await dequeue_upload_request(redis_client)

            now = asyncio.get_event_loop().time()

            if upload_request:
                addr = upload_request.address
                try:
                    user_upload_requests[addr].append(upload_request)
                except KeyError:
                    user_upload_requests[addr] = [upload_request]
                    batch_first_seen[addr] = now

                # Flush conditions: size >= max or age >= max
                for user, items in list(user_upload_requests.items()):
                    age = now - batch_first_seen.get(user, now)
                    if len(items) >= BATCH_MAX_ITEMS or age >= BATCH_MAX_AGE_SEC:
                        success = await process_upload_request(items, db, redis_client)
                        if success:
                            logger.info(f"Processed batch for user {user} with {len(items)} items (age={age:.3f}s)")
                        else:
                            logger.info(f"Failed to process batch with {len(items)} items for user {user}")
                        user_upload_requests.pop(user, None)
                        batch_first_seen.pop(user, None)

            else:
                # No items popped: flush any aged batches
                for user, items in list(user_upload_requests.items()):
                    age = now - batch_first_seen.get(user, now)
                    if items and age >= BATCH_MAX_AGE_SEC:
                        success = await process_upload_request(items, db, redis_client)
                        if success:
                            logger.info(
                                f"Processed aged batch for user {user} with {len(items)} items (age={age:.3f}s)"
                            )
                        else:
                            logger.info(f"Failed to process aged batch with {len(items)} items for user {user}")
                        user_upload_requests.pop(user, None)
                        batch_first_seen.pop(user, None)

    except KeyboardInterrupt:
        logger.info("Pinner service stopping...")
    except Exception as e:
        logger.exception(f"Error in workers loop: {e}")
        # Log and continue instead of exiting
        await asyncio.sleep(5)  # Brief pause before retrying
    finally:
        await redis_client.aclose()
        await db.close()


if __name__ == "__main__":
    asyncio.run(run_pinner_loop())
