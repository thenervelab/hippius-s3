"""Pinner logic with retry handling and batch processing."""

import asyncio
import hashlib
import logging
import random
from typing import Any
from typing import List

# FileInput is from hippius_sdk.substrate.FileInput but we define a simple alias for testing
from typing import NamedTuple
from typing import Tuple
from typing import Union

from hippius_s3.queue import Chunk
from hippius_s3.queue import MultipartUploadChainRequest
from hippius_s3.queue import SimpleUploadChainRequest
from hippius_s3.queue import enqueue_retry_request
from hippius_s3.utils import upsert_cid_and_get_id


class FileInput(NamedTuple):
    file_hash: str
    file_name: str


# Import heavy dependencies only when needed to avoid test import issues
def _get_redis_cache() -> Any:
    from hippius_s3.cache import RedisObjectPartsCache

    return RedisObjectPartsCache


logger = logging.getLogger(__name__)


def classify_error(exc: Exception) -> bool:
    """Classify exception as transient (retryable) or permanent (fail immediately).

    Transient: timeouts, connection errors, 5xx HTTP, reset.
    Permanent: 4xx validation errors, auth failures, etc.
    """
    # Type-based checks for common transient exceptions
    if isinstance(exc, (asyncio.TimeoutError, asyncio.CancelledError, ConnectionError, OSError, BrokenPipeError)):
        return True

    # Try aiohttp-specific checks if available
    try:
        import aiohttp

        if isinstance(exc, (aiohttp.ClientConnectorError, aiohttp.ServerTimeoutError, aiohttp.ClientError)):
            return True
    except ImportError:
        pass

    # String-based pattern matching for other cases
    err_str = str(exc).lower()
    transient_patterns = ["timeout", "temporarily", "connection", "reset", "503", "502", "504", "connect"]
    return any(p in err_str for p in transient_patterns)


def compute_backoff_ms(attempts: int, base_ms: int, max_ms: int) -> int:
    """Compute exponential backoff with jitter.

    Formula: min(max_ms, base_ms * 2^(attempts-1)) with ±20% jitter.
    """
    n = attempts
    raw_delay = base_ms * (2 ** (n - 1))
    jitter = int(0.2 * raw_delay)
    jitter_offset = random.randint(-jitter, jitter)
    delay_ms = max(0, raw_delay + jitter_offset)
    delay_ms = min(max_ms, delay_ms)
    return int(delay_ms)


class Pinner:
    """Handles per-item upload processing with retry logic."""

    def __init__(self, db: Any, ipfs_service: Any, redis_client: Any, config: Any):
        self.db = db
        self.ipfs_service = ipfs_service
        self.redis_client = redis_client
        self.config = config

    async def process_item(
        self, payload: Union[SimpleUploadChainRequest, MultipartUploadChainRequest]
    ) -> Tuple[List[FileInput], bool]:
        """Process a single upload item.

        Returns (files_to_publish, success).
        On transient error: schedules retry and returns ([], False).
        On permanent error: marks failed and returns ([], False).
        On success: returns (files, True).
        """
        try:
            if isinstance(payload, MultipartUploadChainRequest):  # Multipart
                chunk_results = await self._process_multipart_upload(payload)
                files = [result[0] for result in chunk_results]
            else:  # Simple
                result = await self._process_simple_upload(payload)
                files = [result]
            logger.info(
                f"Pinner success: {payload.name} request_id={payload.request_id} attempts={payload.attempts} files={len(files)}"
            )
            return files, True
        except Exception as e:
            err_str = str(e)
            transient = classify_error(e)
            attempts_next = (payload.attempts or 0) + 1
            if transient and attempts_next <= self.config.pinner_max_attempts:
                delay_ms = compute_backoff_ms(
                    attempts_next, self.config.pinner_backoff_base_ms, self.config.pinner_backoff_max_ms
                )
                await enqueue_retry_request(
                    payload, self.redis_client, delay_seconds=delay_ms / 1000.0, last_error=err_str
                )
                logger.warning(
                    f"Pinner transient failure (retry scheduled): {payload.name} request_id={payload.request_id} attempts={payload.attempts} error={err_str[:200]}"
                )
                return [], False
            await self.db.execute("UPDATE objects SET status = 'failed' WHERE object_id = $1", payload.object_id)
            logger.error(
                f"Pinner permanent failure: {payload.name} request_id={payload.request_id} attempts={payload.attempts} error={err_str[:200]}"
            )
            return [], False

    async def process_batch(
        self, payloads: List[Union[SimpleUploadChainRequest, MultipartUploadChainRequest]]
    ) -> Tuple[List[FileInput], List[Union[SimpleUploadChainRequest, MultipartUploadChainRequest]]]:
        """Process a batch of items, isolating failures.

        Returns (successful_files, succeeded_payloads) for substrate publish.
        """
        all_files = []
        succeeded_payloads = []
        for payload in payloads:
            files, success = await self.process_item(payload)
            if success:
                all_files.extend(files)
                succeeded_payloads.append(payload)
        return all_files, succeeded_payloads

    async def _process_simple_upload(self, payload: SimpleUploadChainRequest) -> FileInput:
        """Process a simple upload."""
        logger.info(f"Processing simple upload for object {payload.object_id}")
        RedisObjectPartsCache = _get_redis_cache()
        obj_cache = RedisObjectPartsCache(self.redis_client)
        # For simple upload, the chunk id should be 0
        chunk_data = await obj_cache.get(payload.object_id, int(payload.chunk.id))
        logger.info(
            f"Retrieved chunk data for object {payload.object_id}, size: {len(chunk_data) if chunk_data else 0}"
        )

        # Generate IPFS CID using s3_publish (pin, no chain publish)
        # Keep filename as-is for unencrypted; hash for encrypted to avoid leaking names
        file_name = payload.object_key
        if payload.should_encrypt:
            filename_hash = hashlib.md5(f"{payload.object_key}_md5".encode()).hexdigest()
            file_name = f"{filename_hash}.chunk"

        s3_result = await self.ipfs_service.client.s3_publish(
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
        logger.info(
            f"s3_publish result for object {payload.object_id}: cid={getattr(s3_result, 'cid', 'MISSING')}, type={type(s3_result)}"
        )

        # Backfill the base part CID for unified multipart reads
        logger.info(f"Storing CID {s3_result.cid} for object {payload.object_id} in objects table")
        await self.db.execute(
            """
            UPDATE objects
            SET ipfs_cid = $2
            WHERE object_id = $1 AND ipfs_cid IS NULL
            """,
            payload.object_id,
            s3_result.cid,
        )

        # Ensure base (part 0) exists/updated with concrete ipfs_cid and cid_id for manifest/reads
        try:
            base_md5 = hashlib.md5(chunk_data).hexdigest()
            cid_id = await upsert_cid_and_get_id(self.db, s3_result.cid)
            logger.info(
                f"Storing CID {s3_result.cid} (cid_id={cid_id}) for object {payload.object_id} part 0 in parts table"
            )
            # Try update-first; if no row was updated, insert a new one.
            status = await self.db.execute(
                """
                UPDATE parts
                SET ipfs_cid = $1, cid_id = $2, size_bytes = $3, etag = $4
                WHERE object_id = $5 AND part_number = 0
                """,
                s3_result.cid,
                cid_id,
                len(chunk_data),
                base_md5,
                payload.object_id,
            )
            updated = False
            try:
                updated = isinstance(status, str) and status.split()[-1].isdigit() and int(status.split()[-1]) > 0
            except Exception:
                updated = False
            if not updated:
                await self.db.execute(
                    """
                    INSERT INTO parts (object_id, part_number, ipfs_cid, cid_id, size_bytes, etag)
                    VALUES ($1, 0, $2, $3, $4, $5)
                    """,
                    payload.object_id,
                    s3_result.cid,
                    cid_id,
                    len(chunk_data),
                    base_md5,
                )
            logger.info(f"Successfully stored/updated part 0 in parts table for object {payload.object_id}")
        except Exception:
            logger.exception("Failed to write base part 0 into parts table")

        # Keep cache for a while to ensure immediate consistency on reads (unified multipart)
        RedisObjectPartsCache = _get_redis_cache()
        await RedisObjectPartsCache(self.redis_client).expire(payload.object_id, 0, ttl=self.config.cache_ttl_seconds)

        return FileInput(file_hash=s3_result.cid, file_name=file_name)

    async def _process_multipart_chunk(
        self, payload: MultipartUploadChainRequest, chunk: Chunk
    ) -> Tuple[FileInput, Chunk]:
        """Process a single multipart chunk."""
        RedisObjectPartsCache = _get_redis_cache()
        obj_cache = RedisObjectPartsCache(self.redis_client)
        # Derive part_number from key to drive both read and DB updates

        part_number_db = int(chunk.id)

        # Get chunk data from cache
        chunk_data = await obj_cache.get(payload.object_id, part_number_db)

        # Generate IPFS CID for this chunk using s3_publish (no chain publish)
        file_name = payload.object_key
        if payload.should_encrypt:
            filename_hash = hashlib.md5(f"{payload.object_key}_{chunk.id}".encode()).hexdigest()
            file_name = f"{filename_hash}.chunk"

        s3_result = await self.ipfs_service.client.s3_publish(
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

        # Store part CID in database (update reserved placeholder row)
        cid_id = await upsert_cid_and_get_id(self.db, s3_result.cid)
        await self.db.execute(
            """
            UPDATE parts
            SET ipfs_cid = $1, cid_id = $2, size_bytes = $3
            WHERE object_id = $4 AND part_number = $5
            """,
            s3_result.cid,
            cid_id,
            len(chunk_data),
            payload.object_id,
            part_number_db,
        )

        return FileInput(file_hash=s3_result.cid, file_name=file_name), chunk

    async def _process_multipart_upload(self, payload: MultipartUploadChainRequest) -> List[Tuple[FileInput, Chunk]]:
        """Process multipart upload with concurrent chunk processing."""
        # Concurrency for multipart processing is configurable
        concurrency = int(getattr(self.config, "pinner_multipart_max_concurrency", 5) or 5)
        semaphore = asyncio.Semaphore(concurrency)

        # Ensure base part 0 has a concrete CID (simple PUT may still be pending)
        try:
            row = await self.db.fetchrow(
                "SELECT ipfs_cid FROM parts WHERE object_id = $1 AND part_number = 0",
                payload.object_id,
            )
            needs_base = True
            if row is not None:
                cid_raw = row[0]
                if cid_raw is not None:
                    cid_str = cid_raw if isinstance(cid_raw, str) else str(cid_raw)
                    needs_base = cid_str.strip().lower() in {"", "none", "pending"}

            if needs_base:
                RedisObjectPartsCache = _get_redis_cache()
                obj_cache = RedisObjectPartsCache(self.redis_client)
                base_bytes = await obj_cache.get(payload.object_id, 0)
                if base_bytes:
                    file_name = payload.object_key
                    if payload.should_encrypt:
                        filename_hash = hashlib.md5(f"{payload.object_key}_md5".encode()).hexdigest()
                        file_name = f"{filename_hash}.chunk"
                    base_result = await self.ipfs_service.client.s3_publish(
                        content=base_bytes,
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
                    cid_id = await upsert_cid_and_get_id(self.db, base_result.cid)
                    base_md5 = hashlib.md5(base_bytes).hexdigest()
                    await self.db.execute(
                        """
                        UPDATE parts
                        SET ipfs_cid = $2, cid_id = $3, size_bytes = COALESCE(size_bytes, $4), etag = COALESCE(etag, $5)
                        WHERE object_id = $1 AND part_number = 0
                        """,
                        payload.object_id,
                        base_result.cid,
                        cid_id,
                        len(base_bytes),
                        base_md5,
                    )
                    await obj_cache.expire(payload.object_id, 0, ttl=self.config.cache_ttl_seconds)
        except Exception:
            logger.debug("Failed ensuring base part(0) CID before multipart processing", exc_info=True)

        async def process_chunk_with_semaphore(chunk: Chunk) -> Tuple[FileInput, Chunk]:
            async with semaphore:
                return await self._process_multipart_chunk(payload, chunk)

        # Process first part synchronously to ensure encryption key is generated once,
        # then parallelize the remaining parts to avoid key-generation races.
        chunks_sorted = sorted(payload.chunks, key=lambda c: c.id)
        first_result = await process_chunk_with_semaphore(chunks_sorted[0])

        # Process remaining parts concurrently
        if len(chunks_sorted) > 1:
            remaining_results = await asyncio.gather(
                *[process_chunk_with_semaphore(chunk) for chunk in chunks_sorted[1:]]
            )
            chunk_results = [first_result] + list(remaining_results)
        else:
            chunk_results = [first_result]

        # Keep per-chunk caches for a while; GETs may still be assembling from cache
        RedisObjectPartsCache = _get_redis_cache()
        obj_cache = RedisObjectPartsCache(self.redis_client)
        for _, chunk in chunk_results:
            part_number_db = int(chunk.id)
            await obj_cache.expire(payload.object_id, part_number_db, ttl=self.config.cache_ttl_seconds)

        return chunk_results
