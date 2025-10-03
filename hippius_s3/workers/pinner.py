"""Pinner logic with retry handling and batch processing."""

import asyncio
import json
import logging
import random
from typing import Any
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import Tuple

from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.cache import RedisUploadPartsCache
from hippius_s3.config import get_config
from hippius_s3.dlq.storage import DLQStorage
from hippius_s3.ipfs_service import IPFSService
from hippius_s3.queue import Chunk
from hippius_s3.queue import UploadChainRequest
from hippius_s3.queue import enqueue_retry_request
from hippius_s3.utils import get_query


# FileInput is from hippius_sdk.substrate.FileInput but we define a simple alias for testing
class FileInput(NamedTuple):
    file_hash: str


logger = logging.getLogger(__name__)


def classify_error(error: Exception) -> str:
    """Classify error as transient, permanent, or unknown."""
    err_str = str(error).lower()
    err_type = type(error).__name__.lower()

    # Permanent errors
    if any(
        keyword in err_str
        for keyword in ["malformed", "invalid", "negative size", "missing part", "validation error", "integrity"]
    ):
        return "permanent"

    # Transient errors (retryable)
    if any(
        keyword in err_str
        for keyword in [
            "timeout",
            "connection",
            "network",
            "5xx",
            "503",
            "502",
            "504",
            "unavailable",
            "throttled",
            "rate limit",
        ]
    ) or any(keyword in err_type for keyword in ["connectionerror", "timeouterror", "httperror"]):
        return "transient"

    # Default to transient for unknown errors
    return "transient"


def compute_backoff_ms(attempt: int, base_ms: int = 1000, max_ms: int = 30000) -> float:
    """Compute exponential backoff with jitter."""
    # Exponential backoff: base * 2^(attempt-1) + jitter
    exp_backoff = base_ms * (2 ** (attempt - 1))
    jitter = random.uniform(0, exp_backoff * 0.1)  # 10% jitter
    return float(min(exp_backoff + jitter, max_ms))


# Import heavy dependencies only when needed to avoid test import issues
def _get_redis_cache() -> type:  # type: ignore[no-any-return]
    return RedisObjectPartsCache


def _get_redis_upload_cache() -> type:  # type: ignore[no-any-return]
    return RedisUploadPartsCache


def _get_ipfs_service() -> type:  # type: ignore[no-any-return]
    return IPFSService


class Pinner:
    """Handles per-item upload processing with retry logic."""

    def __init__(self, db: Any, ipfs_service: Any, redis_client: Any, config: Any):
        self.db = db
        self.ipfs_service = ipfs_service
        self.redis_client = redis_client
        self.config = config
        self.dlq_storage = DLQStorage()

    async def process_item(self, payload: UploadChainRequest) -> Tuple[List[FileInput], bool]:
        try:
            logger.debug(
                f"process_item START object_id={payload.object_id} chunks={len(payload.chunks)} upload_id={payload.upload_id} attempts={payload.attempts}"
            )

            # Step 2: Process chunks only (upload to IPFS, upsert parts table)
            chunk_results = await self._process_chunks_only(
                object_id=payload.object_id,
                object_key=payload.object_key,
                chunks=payload.chunks,
                upload_id=payload.upload_id,
                seed_phrase=payload.subaccount_seed_phrase,
            )

            # Step 3: Build and publish manifest (only if all parts have CIDs)
            manifest_result = await self._build_and_publish_manifest(
                object_id=payload.object_id,
                object_key=payload.object_key,
                should_encrypt=payload.should_encrypt,
                seed_phrase=payload.subaccount_seed_phrase,
                account_address=payload.address,
                bucket_name=payload.bucket_name,
            )

            # Return chunk FileInputs, and manifest FileInput only if it was actually built
            files = [result[0] for result in chunk_results]
            if manifest_result.file_hash != "pending":
                files.append(manifest_result)
            return files, True
        except Exception as e:
            logger.debug(f"process_item EXCEPTION object_id={getattr(payload, 'object_id', 'unknown')} err={e}")
            err_str = str(e)
            error_type = classify_error(e)
            attempts_next = (payload.attempts or 0) + 1

            if error_type == "transient" and attempts_next <= self.config.pinner_max_attempts:
                delay_ms = compute_backoff_ms(
                    attempts_next, self.config.pinner_backoff_base_ms, self.config.pinner_backoff_max_ms
                )
                await enqueue_retry_request(
                    payload, self.redis_client, delay_seconds=delay_ms / 1000.0, last_error=err_str
                )
                return [], False
            # Permanent error or max attempts reached - push to DLQ
            await self._push_to_dlq(payload, err_str, error_type)
            await self.db.execute("UPDATE objects SET status = 'failed' WHERE object_id = $1", payload.object_id)
            return [], False

    async def process_batch(
        self, payloads: List[UploadChainRequest]
    ) -> Tuple[List[FileInput], List[UploadChainRequest]]:
        all_files = []
        succeeded_payloads = []
        for payload in payloads:
            files, success = await self.process_item(payload)
            if success:
                all_files.extend(files)
                succeeded_payloads.append(payload)
            else:
                logger.debug(f"process_batch item FAILED object_id={payload.object_id}")
        return all_files, succeeded_payloads

    async def _process_multipart_chunk(
        self,
        *,
        object_id: str,
        object_key: str,
        chunk: Chunk,
        upload_id: Optional[str] = None,
        seed_phrase: Optional[str] = None,
    ) -> Tuple[FileInput, Chunk]:
        """Process a single multipart chunk (upsert into parts) with fallback to upload_id cache."""
        logger.debug(
            f"_process_multipart_chunk START object_id={object_id}, part_number={chunk.id}, upload_id={upload_id}"
        )
        RedisObjectPartsCache = _get_redis_cache()
        obj_cache = RedisObjectPartsCache(self.redis_client)  # type: ignore[call-arg]
        part_number_db = int(chunk.id)
        # Try object_id-based cache first
        chunk_data = await obj_cache.get(object_id, part_number_db)
        # Fallback to upload_id-based key if not found
        if chunk_data is None:
            logger.debug(
                f"_process_multipart_chunk cache miss for object_id={object_id} part={part_number_db} key=obj:{object_id}:part:{part_number_db}"
            )
        if chunk_data is None:
            raise ValueError(f"Chunk data missing for object_id={object_id} part={part_number_db}")

        logger.debug(f"_process_multipart_chunk fetched chunk_data bytes={len(chunk_data)}")
        # For chunks, upload+pin only (no publish) for immediate CID
        upload_result = await self.ipfs_service.upload_file(
            file_data=chunk_data,
            file_name=f"{object_key}.part{part_number_db}",
            content_type="application/octet-stream",
            encrypt=False,
            seed_phrase=seed_phrase,
        )
        chunk_cid = str(upload_result.get("cid"))
        logger.debug(f"_process_multipart_chunk obtained cid={chunk_cid}")
        # Resolve or create cid_id for parts row to override any 'pending' linkage
        cid_id_for_part = None
        try:
            cid_row = await self.db.fetchrow(get_query("upsert_cid"), chunk_cid)
            cid_id_for_part = cid_row["id"] if cid_row else None
        except Exception:
            cid_id_for_part = None
        # Update existing placeholder row, insert if missing
        logger.debug(
            f"_process_multipart_chunk upserting part object_id={object_id}, part_number={part_number_db}, cid={chunk_cid}, size={len(chunk_data)}"
        )
        try:
            updated = await self.db.execute(
                """
                UPDATE parts
                SET ipfs_cid = $3, size_bytes = $4,
                    cid_id = COALESCE($5, cid_id)
                WHERE object_id = $1 AND part_number = $2
                """,
                object_id,
                part_number_db,
                chunk_cid,
                len(chunk_data),
                cid_id_for_part,
            )
            # Some drivers return a string like "UPDATE 1"; normalize to int check
            updated_str = str(updated or "")
            rows_affected = 0
            try:
                # asyncpg returns number of rows as string prefix
                if updated_str.upper().startswith("UPDATE"):
                    rows_affected = int(updated_str.split(" ")[-1])
                else:
                    rows_affected = int(updated_str)  # best effort if driver returns int
            except Exception:
                rows_affected = 0

            logger.debug(f"_process_multipart_chunk update rows_affected={rows_affected}")
            if rows_affected == 0:
                await self.db.execute(
                    """
                    INSERT INTO parts (part_id, object_id, part_number, ipfs_cid, size_bytes, cid_id, uploaded_at)
                    VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, NOW())
                    ON CONFLICT (object_id, part_number) DO UPDATE SET
                        ipfs_cid = EXCLUDED.ipfs_cid,
                        size_bytes = EXCLUDED.size_bytes,
                        cid_id = COALESCE(EXCLUDED.cid_id, parts.cid_id)
                    """,
                    object_id,
                    part_number_db,
                    chunk_cid,
                    len(chunk_data),
                    cid_id_for_part,
                )
                logger.debug(
                    f"_process_multipart_chunk inserted/upserted part object_id={object_id} part_number={part_number_db}"
                )
        except Exception:
            # Fallback to upsert if update failed for any reason
            logger.debug("PINNER _process_multipart_chunk update failed; falling back to upsert", exc_info=True)
            await self.db.execute(
                """
                INSERT INTO parts (part_id, object_id, part_number, ipfs_cid, size_bytes, cid_id, uploaded_at)
                VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, NOW())
                ON CONFLICT (object_id, part_number) DO UPDATE SET
                    ipfs_cid = EXCLUDED.ipfs_cid,
                    size_bytes = EXCLUDED.size_bytes,
                    cid_id = COALESCE(EXCLUDED.cid_id, parts.cid_id)
                """,
                object_id,
                part_number_db,
                chunk_cid,
                len(chunk_data),
                cid_id_for_part,
            )
            logger.debug(
                f"_process_multipart_chunk upserted part after exception object_id={object_id} part_number={part_number_db}"
            )
        logger.debug(
            f"_process_multipart_chunk COMPLETE object_id={object_id}, part_number={part_number_db}, cid={chunk_cid}"
        )
        return FileInput(file_hash=chunk_cid), chunk

    async def _process_chunks_only(
        self,
        *,
        object_id: str,
        object_key: str,
        chunks: List[Chunk],
        upload_id: Optional[str] = None,
        seed_phrase: Optional[str] = None,
    ) -> List[Tuple[FileInput, Chunk]]:
        """Process chunks only: read from Redis, upload to IPFS, upsert parts table with CIDs."""
        logger.debug(
            f"_process_chunks_only START object_id={object_id}, object_key={object_key}, chunks={[c.id for c in chunks]}, upload_id={upload_id}"
        )
        cfg = get_config()
        concurrency = int(getattr(cfg, "pinner_multipart_max_concurrency", 5) or 5)
        semaphore = asyncio.Semaphore(concurrency)

        async def process_chunk_with_semaphore(chunk: Chunk) -> Tuple[FileInput, Chunk]:
            async with semaphore:
                return await self._process_multipart_chunk(
                    object_id=object_id,
                    object_key=object_key,
                    chunk=chunk,
                    upload_id=upload_id,
                    seed_phrase=seed_phrase,
                )

        chunks_sorted = sorted(chunks, key=lambda c: c.id)
        # Process first chunk synchronously
        first_result = await process_chunk_with_semaphore(chunks_sorted[0])
        # Process remaining concurrently
        if len(chunks_sorted) > 1:
            remaining_results = await asyncio.gather(*[process_chunk_with_semaphore(c) for c in chunks_sorted[1:]])
            chunk_results = [first_result] + list(remaining_results)
        else:
            chunk_results = [first_result]

        # Touch caches for present parts
        RedisCacheCls = _get_redis_cache()
        obj_cache = RedisCacheCls(self.redis_client)  # type: ignore[call-arg]
        for _, chunk in chunk_results:
            await obj_cache.expire(object_id, int(chunk.id), ttl=self.config.cache_ttl_seconds)

        logger.debug(f"_process_chunks_only COMPLETE object_id={object_id}, processed {len(chunk_results)} chunks")
        return chunk_results

    async def _build_and_publish_manifest(
        self,
        *,
        object_id: str,
        object_key: str,
        should_encrypt: bool,
        seed_phrase: Optional[str] = None,
        account_address: Optional[str] = None,
        bucket_name: Optional[str] = None,
    ) -> FileInput:
        """Build manifest from parts table and publish/pin it. Only builds if all parts have CIDs."""
        logger.debug(f"_build_and_publish_manifest START object_id={object_id}, object_key={object_key}")

        # Build manifest JSON from DB parts
        rows = await self.db.fetch(
            """
            SELECT p.part_number,
                   COALESCE(c.cid, p.ipfs_cid) AS cid,
                   p.size_bytes::bigint AS size_bytes
            FROM parts p
            LEFT JOIN cids c ON p.cid_id = c.id
            WHERE p.object_id = $1
            ORDER BY part_number
            """,
            object_id,
        )

        # Check if all parts have concrete CIDs
        parts_data = []
        all_parts_have_cids = True
        for r in rows:
            pn = int(r[0])
            cid_raw = r[1]
            cid: str | None = None
            if cid_raw is not None:
                cid_str = cid_raw if isinstance(cid_raw, str) else str(cid_raw)
                cid_str = cid_str.strip()
                if cid_str and cid_str.lower() not in {"", "none", "pending"}:
                    cid = cid_str
                else:
                    all_parts_have_cids = False

            size = int(r[2] or 0)
            parts_data.append({"part_number": pn, "cid": cid, "size_bytes": size})

        # Only build manifest if all parts have concrete CIDs
        if not all_parts_have_cids:
            logger.debug(
                f"_build_and_publish_manifest SKIP - not all parts have CIDs yet (found {len(parts_data)} parts)"
            )
            return FileInput(file_hash="pending")

        # Get object info for manifest metadata
        obj_row = await self.db.fetchrow("SELECT content_type FROM objects WHERE object_id = $1", object_id)
        content_type = obj_row["content_type"] if obj_row else "application/octet-stream"

        manifest_data = {
            "object_id": object_id,
            "object_key": object_key,
            "appendable": True,  # All objects are appendable in this system
            "content_type": content_type,
            "parts": parts_data,
        }

        manifest_json = json.dumps(manifest_data)
        logger.debug(f"_build_and_publish_manifest built manifest with {len(parts_data)} parts")

        # Publish/pin the manifest
        manifest_result = await self.ipfs_service.pin_file_with_encryption(
            file_data=manifest_json.encode(),
            file_name=f"{object_key}.manifest",
            should_encrypt=should_encrypt,
            seed_phrase=seed_phrase,
            account_address=account_address,
            bucket_name=bucket_name,
        )

        # Update object with manifest CID and set status to uploaded
        try:
            cid_row = await self.db.fetchrow(get_query("upsert_cid"), manifest_result.cid)
            cid_id = cid_row["id"] if cid_row else None
        except Exception:
            cid_id = None

        await self.db.execute(
            """
            UPDATE objects
            SET ipfs_cid = $2,
                cid_id = COALESCE($3, cid_id),
                status = 'uploaded'
            WHERE object_id = $1
            """,
            object_id,
            manifest_result.cid,
            cid_id,
        )

        logger.debug(f"_build_and_publish_manifest COMPLETE object_id={object_id}, manifest_cid={manifest_result.cid}")
        return FileInput(file_hash=manifest_result.cid)

    async def _push_to_dlq(self, payload: UploadChainRequest, last_error: str, error_type: str) -> None:
        """Push a failed request to the Dead-Letter Queue and persist chunks to disk."""
        import time

        # First, persist chunks from cache to disk
        await self._persist_chunks_to_dlq(payload)

        # Normalize error_type to string for DLQ entries
        etype = error_type
        if isinstance(etype, bool):
            etype = "transient" if etype else "permanent"

        dlq_entry = {
            "payload": payload.model_dump(),
            "object_id": payload.object_id,
            "upload_id": payload.upload_id,
            "bucket_name": payload.bucket_name,
            "object_key": payload.object_key,
            "attempts": payload.attempts or 0,
            "first_enqueued_at": getattr(payload, "first_enqueued_at", time.time()),
            "last_attempt_at": time.time(),
            "last_error": last_error,
            "error_type": etype,
        }

        try:
            await self.redis_client.lpush("upload_requests:dlq", json.dumps(dlq_entry))
            logger.warning(f"Pushed to DLQ: object_id={payload.object_id}, error_type={error_type}, error={last_error}")
        except Exception as e:
            logger.error(f"Failed to push to DLQ: {e}")

    async def _persist_chunks_to_dlq(self, payload: UploadChainRequest) -> None:
        """Persist all available chunks from cache to DLQ filesystem."""
        object_id = payload.object_id
        RedisCache = _get_redis_cache()
        obj_cache = RedisCache(self.redis_client)

        try:
            # Get all chunks for this object from cache
            for chunk in payload.chunks:
                part_number = int(chunk.id)

                # Try to get chunk data from cache
                chunk_data = await obj_cache.get(object_id, part_number)
                if chunk_data is None:
                    logger.warning(
                        f"No cached data for object {object_id} part {part_number}, skipping DLQ persistence"
                    )
                    continue

                # Save to DLQ filesystem
                try:
                    self.dlq_storage.save_chunk(object_id, part_number, chunk_data)
                    logger.debug(f"Persisted chunk {part_number} for object {object_id} to DLQ")
                except Exception as e:
                    logger.error(f"Failed to persist chunk {part_number} for object {object_id} to DLQ: {e}")

        except Exception as e:
            logger.error(f"Failed to persist chunks for object {object_id} to DLQ: {e}")
            # Don't fail the DLQ push if persistence fails
