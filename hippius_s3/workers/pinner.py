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
from hippius_s3.ipfs_service import IPFSService
from hippius_s3.queue import Chunk
from hippius_s3.queue import UploadChainRequest
from hippius_s3.queue import enqueue_retry_request
from hippius_s3.utils import get_query


# FileInput is from hippius_sdk.substrate.FileInput but we define a simple alias for testing
class FileInput(NamedTuple):
    file_hash: str


logger = logging.getLogger(__name__)


# Import heavy dependencies only when needed to avoid test import issues
def _get_redis_cache() -> type:  # type: ignore[no-any-return]
    return RedisObjectPartsCache


def _get_redis_upload_cache() -> type:  # type: ignore[no-any-return]
    return RedisUploadPartsCache


def _get_ipfs_service() -> type:  # type: ignore[no-any-return]
    return IPFSService


def classify_error(exc: Exception) -> bool:
    """Classify exception as transient (retryable) or permanent (fail immediately)."""
    err_str = str(exc).lower()
    transient_patterns = ["timeout", "temporarily", "connection", "reset", "503", "502", "504", "connect"]
    return any(p in err_str for p in transient_patterns)


def compute_backoff_ms(attempts: int, base_ms: int, max_ms: int) -> int:
    """Compute exponential backoff with jitter."""
    n = attempts
    raw_delay = base_ms * (2 ** (n - 1))
    jitter = int(0.2 * raw_delay)
    jitter_offset = random.randint(-jitter, jitter)
    result = min(max_ms, max(0, raw_delay + jitter_offset))
    return int(result)


class Pinner:
    """Handles per-item upload processing with retry logic."""

    def __init__(self, db: Any, ipfs_service: Any, redis_client: Any, config: Any):
        self.db = db
        self.ipfs_service = ipfs_service
        self.redis_client = redis_client
        self.config = config

    async def process_item(self, payload: UploadChainRequest) -> Tuple[List[FileInput], bool]:
        try:
            logger.debug(
                f"process_item START object_id={payload.object_id} chunks={len(payload.chunks)} upload_id={payload.upload_id} attempts={payload.attempts}"
            )

            # Step 2: Process chunks only (upload to IPFS, upsert parts table)
            chunk_results = await self._process_chunks_only(
                object_id=payload.object_id,
                object_key=payload.object_key,
                should_encrypt=payload.should_encrypt,
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
                subaccount=payload.subaccount,
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
            transient = classify_error(e)
            attempts_next = (payload.attempts or 0) + 1
            if transient and attempts_next <= self.config.pinner_max_attempts:
                delay_ms = compute_backoff_ms(
                    attempts_next, self.config.pinner_backoff_base_ms, self.config.pinner_backoff_max_ms
                )
                await enqueue_retry_request(
                    payload, self.redis_client, delay_seconds=delay_ms / 1000.0, last_error=err_str
                )
                return [], False
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
        should_encrypt: bool,
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
        should_encrypt: bool,
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
                    should_encrypt=should_encrypt,
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
        subaccount: Optional[str] = None,
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
            subaccount_id=subaccount,
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

    async def _process_upload_common(
        self,
        *,
        object_id: str,
        object_key: str,
        should_encrypt: bool,
        chunks: List[Chunk],
        appendable: bool,
        upload_id: Optional[str] = None,
        seed_phrase: Optional[str] = None,
        subaccount: Optional[str] = None,
        bucket_name: Optional[str] = None,
    ) -> Tuple[List[Tuple[FileInput, Chunk]], FileInput]:
        """Pin chunks, upsert parts, build/pin manifest, set object ipfs_cid."""
        logger.debug(
            f"_process_upload_common START object_id={object_id}, object_key={object_key}, chunks={[c.id for c in chunks]}, upload_id={upload_id}"
        )

        # Process chunks only (Step 2)
        chunk_results = await self._process_chunks_only(
            object_id=object_id,
            object_key=object_key,
            should_encrypt=should_encrypt,
            chunks=chunks,
            upload_id=upload_id,
            seed_phrase=seed_phrase,
        )

        # TODO: Step 3 - Build manifest from current parts
        # For now, return a placeholder manifest result
        manifest_result = FileInput(file_hash="pending")  # Placeholder for step 3

        logger.debug(
            f"_process_upload_common COMPLETE object_id={object_id}, chunks processed (manifest deferred to step 3)"
        )
        return chunk_results, manifest_result
