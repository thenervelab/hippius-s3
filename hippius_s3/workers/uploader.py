import asyncio
import json
import logging
import random
import time
from typing import Any
from typing import List
from typing import Optional
from typing import Tuple

from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.dlq.storage import DLQStorage
from hippius_s3.ipfs_service import IPFSService
from hippius_s3.monitoring import get_metrics_collector
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
            upload_id=payload.upload_id,
        )
        chunk_duration = time.time() - chunk_start

        manifest_start = time.time()
        manifest_cid = await self._build_and_upload_manifest(
            object_id=payload.object_id,
            object_key=payload.object_key,
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

        get_metrics_collector().record_uploader_operation(
            main_account=payload.address,
            success=True,
            num_chunks=len(payload.chunks),
            duration=total_duration,
        )

        return all_cids

    async def _upload_chunks(
        self,
        object_id: str,
        object_key: str,
        chunks: List[Chunk],
        upload_id: Optional[str],
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
                    upload_id=upload_id,
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
        upload_id: Optional[str],
    ) -> Tuple[str, int]:
        part_number = int(chunk.id)
        logger.debug(f"Uploading chunk object_id={object_id} part={part_number}")
        # Prefer chunked path when cache meta indicates multiple ciphertext chunks
        meta = await self.obj_cache.get_meta(object_id, part_number)
        num_chunks_meta = int(meta.get("num_chunks", 0)) if isinstance(meta, dict) else 0

        # Ensure we have a non-null upload_id for parts table operations
        resolved_upload_id: Optional[str] = upload_id
        if not resolved_upload_id:
            try:
                row = await self.db.fetchrow(
                    "SELECT upload_id FROM multipart_uploads WHERE object_id = $1 ORDER BY initiated_at DESC LIMIT 1",
                    object_id,
                )
                if row and row[0] is not None:
                    resolved_upload_id = str(row[0])
            except Exception:
                resolved_upload_id = None
        if not resolved_upload_id or (isinstance(resolved_upload_id, str) and resolved_upload_id.strip() == ""):
            logger.warning(
                f"uploader: missing upload_id; using object_id as fallback (object_id={object_id} part={part_number})"
            )
            resolved_upload_id = object_id

        # Look up part_id and sizing metadata for part_chunks upsert
        part_row = await self.db.fetchrow(
            """
            SELECT part_id, COALESCE(chunk_size_bytes, 0) AS chunk_size_bytes, COALESCE(size_bytes, 0) AS size_bytes
            FROM parts
            WHERE object_id = $1 AND part_number = $2
            LIMIT 1
            """,
            object_id,
            part_number,
        )
        part_id: Optional[str] = str(part_row[0]) if part_row else None
        part_chunk_size: int = int(part_row[1] or 0) if part_row else 0
        part_plain_size: int = int(part_row[2] or 0) if part_row else 0

        if num_chunks_meta > 0 and part_id:
            # Upload each ciphertext chunk and upsert part_chunks rows
            first_chunk_cid = ""
            for ci in range(num_chunks_meta):
                piece = await self.obj_cache.get_chunk(object_id, part_number, ci)
                if not isinstance(piece, (bytes, bytearray)):
                    raise RuntimeError("missing_cipher_chunk")
                up_res = await self.ipfs_service.upload_file(
                    file_data=bytes(piece),
                    file_name=f"{object_key}.part{part_number}.chunk{ci}",
                    content_type="application/octet-stream",
                    encrypt=False,
                )
                piece_cid = str(up_res["cid"])  # textual CID
                if ci == 0:
                    first_chunk_cid = piece_cid

                # Compute plaintext size for this chunk if possible
                if part_chunk_size > 0 and part_plain_size > 0 and num_chunks_meta > 0:
                    if ci < num_chunks_meta - 1:
                        pt_len = int(part_chunk_size)
                    else:
                        pt_len = max(0, int(part_plain_size) - int(part_chunk_size) * int(num_chunks_meta - 1))
                else:
                    pt_len = None  # type: ignore[assignment]

                await self.db.execute(
                    get_query("upsert_part_chunk"),
                    part_id,
                    int(ci),
                    piece_cid,
                    int(len(piece)),
                    int(pt_len) if isinstance(pt_len, int) else None,
                    None,
                )

            # Set parts.ipfs_cid to first chunk CID for manifest compatibility
            cid_row = await self.db.fetchrow(get_query("upsert_cid"), first_chunk_cid) if first_chunk_cid else None
            cid_id = cid_row["id"] if cid_row else None
            await self.db.execute(
                """
                UPDATE parts
                SET ipfs_cid = $3, cid_id = COALESCE($4, cid_id)
                WHERE object_id = $1 AND part_number = $2
                """,
                object_id,
                part_number,
                first_chunk_cid,
                cid_id,
            )

            logger.debug(
                f"Uploaded {num_chunks_meta} chunk pieces for object_id={object_id} part={part_number}; first_cid={first_chunk_cid}"
            )
            return first_chunk_cid or "", part_number

        # Fallback: whole-part upload (legacy)
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
            cid_row2 = await self.db.fetchrow(get_query("upsert_cid"), chunk_cid)
            cid_id = cid_row2["id"] if cid_row2 else None
        except Exception:
            cid_id = None

        try:
            updated = await self.db.execute(
                """
                UPDATE parts
                SET ipfs_cid = $3, cid_id = COALESCE($4, cid_id)
                WHERE object_id = $1 AND part_number = $2
                """,
                object_id,
                part_number,
                chunk_cid,
                cid_id,
            )
            updated_str = str(updated or "")
            rows_affected = 0
            if updated_str.upper().startswith("UPDATE"):
                rows_affected = int(updated_str.split(" ")[-1])
            else:
                rows_affected = int(updated_str) if updated_str else 0

            if rows_affected == 0:
                # Fallback: try update by upload_id
                updated2 = await self.db.execute(
                    """
                    UPDATE parts
                    SET ipfs_cid = $3, cid_id = COALESCE($4, cid_id)
                    WHERE upload_id = $1 AND part_number = $2
                    """,
                    resolved_upload_id,
                    part_number,
                    chunk_cid,
                    cid_id,
                )
                updated2_str = str(updated2 or "")
                rows_affected2 = 0
                if updated2_str.upper().startswith("UPDATE"):
                    rows_affected2 = int(updated2_str.split(" ")[-1])
                else:
                    rows_affected2 = int(updated2_str) if updated2_str else 0
                if rows_affected2 == 0:
                    # No existing parts row yet; treat as transient and let caller requeue
                    raise RuntimeError("parts_row_missing_for_update")
        except Exception:
            logger.debug("Uploader update failed; will requeue (no synthesize)", exc_info=True)
            raise

        return chunk_cid, part_number

    async def _build_and_upload_manifest(
        self,
        object_id: str,
        object_key: str,
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
        # If any part is missing a concrete CID, skip manifest build for now
        parts_data = []
        for r in rows:
            part_number = int(r[0])
            cid_raw = r[1]
            cid = str(cid_raw).strip() if cid_raw else None
            size = int(r[2] or 0)
            if not cid or cid.lower() in {"", "none", "pending"}:
                logger.debug(f"Manifest deferred: missing CID for part {part_number} (object_id={object_id})")
                return "pending"
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

        manifest_result = await self.ipfs_service.upload_file(
            file_data=manifest_json.encode(),
            file_name=f"{object_key}.manifest",
            content_type="application/json",
            encrypt=False,
        )
        manifest_cid = str(
            manifest_result.get("cid") if isinstance(manifest_result, dict) else getattr(manifest_result, "cid", None)
        )
        if not manifest_cid:
            raise ValueError("manifest_publish_missing_cid")

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

        get_metrics_collector().record_uploader_operation(
            main_account=payload.address,
            success=False,
            error_type=etype,
        )

    async def _persist_chunks_to_dlq(self, payload: UploadChainRequest) -> None:
        """Persist all available chunks + metadata from cache to DLQ filesystem."""
        object_id = payload.object_id

        try:
            for chunk in payload.chunks:
                part_number = int(chunk.id)

                meta = await self.obj_cache.get_meta(object_id, part_number)
                if not meta:
                    logger.warning(
                        f"No cached meta for object {object_id} part {part_number}, skipping DLQ persistence"
                    )
                    continue

                try:
                    self.dlq_storage.save_meta(object_id, part_number, meta)
                    logger.debug(f"Persisted meta for part {part_number} of object {object_id} to DLQ")
                except Exception as e:
                    logger.error(f"Failed to persist meta for part {part_number} of object {object_id} to DLQ: {e}")
                    continue

                num_chunks = int(meta.get("num_chunks", 0))
                if num_chunks == 0:
                    logger.warning(f"Meta has num_chunks=0 for object {object_id} part {part_number}")
                    continue

                pieces_saved = 0
                for ci in range(num_chunks):
                    piece = await self.obj_cache.get_chunk(object_id, part_number, ci)
                    if isinstance(piece, (bytes, bytearray)) and len(piece) > 0:
                        try:
                            self.dlq_storage.save_chunk_piece(object_id, part_number, ci, bytes(piece))
                            pieces_saved += 1
                        except Exception as e:
                            logger.error(
                                f"Failed to persist chunk piece {ci} for object {object_id} part {part_number}: {e}"
                            )
                    else:
                        logger.warning(f"Missing chunk piece {ci} for object {object_id} part {part_number} in Redis")

                logger.debug(
                    f"Persisted {pieces_saved}/{num_chunks} pieces for part {part_number} of object {object_id} to DLQ"
                )

        except Exception as e:
            logger.error(f"Failed to persist chunks for object {object_id} to DLQ: {e}")
