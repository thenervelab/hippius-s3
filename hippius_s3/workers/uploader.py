import asyncio
import json
import logging
import random
import time
from typing import Any
from typing import List
from typing import Tuple

from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.dlq.storage import DLQStorage
from hippius_s3.ipfs_service import IPFSService
from hippius_s3.queue import Chunk
from hippius_s3.queue import SubstratePinningRequest
from hippius_s3.queue import UploadChainRequest
from hippius_s3.queue import enqueue_substrate_request
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)


def classify_error(error: Exception) -> str:
    """Classify error as transient, permanent, or unknown."""
    err_str = str(error).lower()
    err_type = type(error).__name__.lower()

    if any(
        keyword in err_str
        for keyword in ["malformed", "invalid", "negative size", "missing part", "validation error", "integrity"]
    ):
        return "permanent"

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

    return "transient"


def compute_backoff_ms(attempt: int, base_ms: int = 1000, max_ms: int = 30000) -> float:
    """Compute exponential backoff with jitter."""
    exp_backoff = base_ms * (2 ** (attempt - 1))
    jitter = random.uniform(0, exp_backoff * 0.1)
    return float(min(exp_backoff + jitter, max_ms))


class Uploader:
    def __init__(self, db: Any, ipfs_service: IPFSService, redis_client: Any, config: Any):
        self.db = db
        self.ipfs_service = ipfs_service
        self.redis_client = redis_client
        self.config = config
        self.obj_cache = RedisObjectPartsCache(redis_client)
        self.dlq_storage = DLQStorage()

    async def process_upload(self, payload: UploadChainRequest) -> List[str]:
        start_time = time.time()
        logger.info(f"Processing upload object_id={payload.object_id} chunks={len(payload.chunks)}")

        chunk_start = time.time()
        chunk_cids = await self._upload_chunks(
            object_id=payload.object_id,
            object_key=payload.object_key,
            chunks=payload.chunks,
            seed_phrase=payload.subaccount_seed_phrase,
        )
        chunk_duration = time.time() - chunk_start

        manifest_start = time.time()
        manifest_cid = await self._build_and_upload_manifest(
            object_id=payload.object_id,
            object_key=payload.object_key,
            should_encrypt=payload.should_encrypt,
            seed_phrase=payload.subaccount_seed_phrase,
            account_address=payload.address,
            bucket_name=payload.bucket_name,
        )
        manifest_duration = time.time() - manifest_start

        all_cids = chunk_cids + [manifest_cid]

        await enqueue_substrate_request(
            SubstratePinningRequest(
                cids=all_cids,
                address=payload.address,
                object_id=payload.object_id,
            ),
            self.redis_client,
        )

        total_duration = time.time() - start_time
        logger.info(
            f"Upload complete object_id={payload.object_id} cids={len(all_cids)} "
            f"chunks={chunk_duration:.2f}s manifest={manifest_duration:.2f}s total={total_duration:.2f}s"
        )
        return all_cids

    async def _upload_chunks(
        self,
        object_id: str,
        object_key: str,
        chunks: List[Chunk],
        seed_phrase: str,
    ) -> List[str]:
        logger.debug(f"Uploading {len(chunks)} chunks for object_id={object_id}")

        concurrency = self.config.uploader_multipart_max_concurrency
        semaphore = asyncio.Semaphore(concurrency)

        async def upload_chunk(chunk: Chunk) -> Tuple[str, int]:
            async with semaphore:
                return await self._upload_single_chunk(
                    object_id=object_id,
                    object_key=object_key,
                    chunk=chunk,
                    seed_phrase=seed_phrase,
                )

        chunks_sorted = sorted(chunks, key=lambda c: c.id)

        first_cid, first_part = await upload_chunk(chunks_sorted[0])

        if len(chunks_sorted) > 1:
            remaining_results = await asyncio.gather(*[upload_chunk(c) for c in chunks_sorted[1:]])
            all_results = [(first_cid, first_part)] + list(remaining_results)
        else:
            all_results = [(first_cid, first_part)]

        for _cid, part_number in all_results:
            await self.obj_cache.expire(object_id, part_number, ttl=self.config.cache_ttl_seconds)

        return [cid for cid, _ in all_results]

    async def _upload_single_chunk(
        self,
        object_id: str,
        object_key: str,
        chunk: Chunk,
        seed_phrase: str,
    ) -> Tuple[str, int]:
        part_number = int(chunk.id)
        logger.debug(f"Uploading chunk object_id={object_id} part={part_number}")

        chunk_data = await self.obj_cache.get(object_id, part_number)
        if chunk_data is None:
            raise ValueError(f"Chunk data missing for object_id={object_id} part={part_number}")

        upload_result = await self.ipfs_service.upload_file(
            file_data=chunk_data,
            file_name=f"{object_key}.part{part_number}",
            content_type="application/octet-stream",
            encrypt=False,
        )

        chunk_cid = str(upload_result["cid"])
        logger.debug(f"Chunk uploaded object_id={object_id} part={part_number} cid={chunk_cid}")

        cid_id = None
        try:
            cid_row = await self.db.fetchrow(get_query("upsert_cid"), chunk_cid)
            cid_id = cid_row["id"] if cid_row else None
        except Exception:
            cid_id = None

        try:
            updated = await self.db.execute(
                """
                UPDATE parts
                SET ipfs_cid = $3, size_bytes = $4, cid_id = COALESCE($5, cid_id)
                WHERE object_id = $1 AND part_number = $2
                """,
                object_id,
                part_number,
                chunk_cid,
                len(chunk_data),
                cid_id,
            )
            updated_str = str(updated or "")
            rows_affected = 0
            if updated_str.upper().startswith("UPDATE"):
                rows_affected = int(updated_str.split(" ")[-1])
            else:
                rows_affected = int(updated_str) if updated_str else 0

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
                    part_number,
                    chunk_cid,
                    len(chunk_data),
                    cid_id,
                )
        except Exception:
            logger.debug("Uploader update failed; falling back to upsert", exc_info=True)
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
                part_number,
                chunk_cid,
                len(chunk_data),
                cid_id,
            )

        return chunk_cid, part_number

    async def _build_and_upload_manifest(
        self,
        object_id: str,
        object_key: str,
        should_encrypt: bool,
        seed_phrase: str,
        account_address: str,
        bucket_name: str,
    ) -> str:
        logger.debug(f"Building manifest for object_id={object_id}")

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

        parts_data = []
        for r in rows:
            part_number = int(r[0])
            cid_raw = r[1]
            cid = str(cid_raw).strip() if cid_raw else None
            size = int(r[2] or 0)

            if not cid or cid.lower() in {"", "none", "pending"}:
                raise ValueError(f"Part {part_number} missing CID for object_id={object_id}")

            parts_data.append({"part_number": part_number, "cid": cid, "size_bytes": size})

        obj_row = await self.db.fetchrow("SELECT content_type FROM objects WHERE object_id = $1", object_id)
        content_type = obj_row["content_type"] if obj_row else "application/octet-stream"

        manifest_data = {
            "object_id": object_id,
            "object_key": object_key,
            "appendable": True,
            "content_type": content_type,
            "parts": parts_data,
        }

        manifest_json = json.dumps(manifest_data)
        logger.debug(f"Manifest built with {len(parts_data)} parts for object_id={object_id}")

        manifest_result = await self.ipfs_service.pin_file_with_encryption(
            file_data=manifest_json.encode(),
            file_name=f"{object_key}.manifest",
            should_encrypt=should_encrypt,
            seed_phrase=seed_phrase,
            account_address=account_address,
            bucket_name=bucket_name,
            publish_to_chain=False,
        )

        manifest_cid = manifest_result.cid

        cid_id = None
        cid_row = await self.db.fetchrow(get_query("upsert_cid"), manifest_cid)
        cid_id = cid_row["id"] if cid_row else None

        await self.db.execute(
            """
            UPDATE objects
            SET ipfs_cid = $2,
                cid_id = COALESCE($3, cid_id),
                status = 'pinning'
            WHERE object_id = $1
            """,
            object_id,
            manifest_cid,
            cid_id,
        )

        logger.info(f"Manifest uploaded object_id={object_id} cid={manifest_cid}")
        return manifest_cid

    async def _push_to_dlq(self, payload: UploadChainRequest, last_error: str, error_type: str) -> None:
        """Push a failed request to the Dead-Letter Queue and persist chunks to disk."""
        await self._persist_chunks_to_dlq(payload)

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

        await self.redis_client.lpush("upload_requests:dlq", json.dumps(dlq_entry))
        logger.warning(f"Pushed to DLQ: object_id={payload.object_id}, error_type={error_type}, error={last_error}")

    async def _persist_chunks_to_dlq(self, payload: UploadChainRequest) -> None:
        """Persist all available chunks from cache to DLQ filesystem."""
        object_id = payload.object_id

        for chunk in payload.chunks:
            part_number = int(chunk.id)

            chunk_data = await self.obj_cache.get(object_id, part_number)
            if chunk_data is None:
                logger.warning(f"No cached data for object {object_id} part {part_number}, skipping DLQ persistence")
                continue

            self.dlq_storage.save_chunk(object_id, part_number, chunk_data)
            logger.debug(f"Persisted chunk {part_number} for object {object_id} to DLQ")
