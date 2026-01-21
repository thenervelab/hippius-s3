import asyncio
import hashlib
import json
import logging
import random
import time
from types import TracebackType
from typing import Any
from typing import List
from typing import Optional

import redis.asyncio as async_redis
from pydantic import BaseModel

from hippius_s3.cache import FileSystemPartsStore
from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.dlq.upload_dlq import UploadDLQManager
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.queue import Chunk
from hippius_s3.queue import UploadChainRequest
from hippius_s3.services.hippius_api_service import HippiusApiClient
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)


class ChunkUploadResult(BaseModel):
    cids: List[str]
    part_number: int


def classify_error(error: Exception) -> str:
    """Classify error as transient, permanent, or unknown."""
    err_str = str(error).lower()
    err_type = type(error).__name__.lower()

    if any(
        keyword in err_str
        for keyword in ["malformed", "invalid", "negative size", "missing part", "validation error", "integrity"]
    ):
        return "permanent"

    if any(keyword in err_str for keyword in ["pin", "unpin", "hippius api", "hippiusapi"]) or "hippiusapi" in err_type:
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
            "manifest_pending",
            "part_meta_not_ready",
            "part_row_missing",
        ]
    ) or any(keyword in err_type for keyword in ["connectionerror", "timeouterror", "httperror"]):
        return "transient"

    return "unknown"


def compute_backoff_ms(attempt: int, base_ms: int = 1000, max_ms: int = 30000) -> float:
    """Compute exponential backoff with jitter."""
    exp_backoff = base_ms * (2 ** (attempt - 1))
    jitter = random.uniform(0, exp_backoff * 0.1)
    return float(min(exp_backoff + jitter, max_ms))


class Uploader:
    def __init__(
        self,
        db_pool: Any,
        redis_client: async_redis.Redis,  # type: ignore[type-arg]
        redis_queues_client: async_redis.Redis,  # type: ignore[type-arg]
        config: Any,
    ) -> None:
        # Support either a Pool (has acquire) or a single Connection
        self.db = db_pool
        self.redis_client = redis_client
        self.redis_queues_client = redis_queues_client
        self.config = config
        self.obj_cache = RedisObjectPartsCache(redis_client)
        # Primary source: filesystem store (authoritative, survives Redis eviction)
        self.fs_store = FileSystemPartsStore(config.object_cache_dir)
        self.dlq_manager = UploadDLQManager(redis_queues_client)

    class _ConnCtx:
        def __init__(self, conn: Any) -> None:
            self._conn = conn

        async def __aenter__(self) -> Any:
            return self._conn

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None,
        ) -> bool:
            return False

    def _acquire_conn(self) -> Any:
        """Return an async context manager yielding a connection.

        Works with either a Pool (has acquire) or a single Connection.
        """
        acquire = getattr(self.db, "acquire", None)
        if callable(acquire):
            return acquire()
        return Uploader._ConnCtx(self.db)

    async def process_upload(self, payload: UploadChainRequest) -> List[str]:
        start_time = time.time()
        logger.info(f"Processing upload object_id={payload.object_id} chunks={len(payload.chunks)}")

        chunk_start = time.time()
        _ = await self._upload_chunks(
            object_id=payload.object_id,
            object_key=payload.object_key,
            chunks=payload.chunks,
            upload_id=payload.upload_id,
            object_version=int(payload.object_version or 1),
            account_ss58=payload.address,
        )
        chunk_duration = time.time() - chunk_start

        manifest_start = time.time()
        manifest_data = await self._build_and_upload_manifest(
            object_id=payload.object_id,
            object_key=payload.object_key,
            object_version=int(payload.object_version or 1),
            account_ss58=payload.address,
        )
        manifest_duration = time.time() - manifest_start

        if manifest_data.get("status") == "pending":
            logger.warning(f"Manifest pending for object_id={payload.object_id}, parts missing CIDs - will retry")
            raise RuntimeError(f"manifest_pending_missing_cids: object_id={payload.object_id}")

        cids_and_sizes = {part["cid"]: part["size_bytes"] for part in manifest_data["parts"]}  # noqa: C416
        cids_and_sizes[manifest_data["manifest_cid"]] = manifest_data["manifest_size_bytes"]

        logger.info(
            f"All uploads complete for object_id={payload.object_id}, {len(cids_and_sizes)} CIDs uploaded and pinned"
        )

        async with self._acquire_conn() as conn:
            await conn.execute(
                "UPDATE object_versions SET status = 'uploaded' WHERE object_id = $1 AND object_version = $2",
                payload.object_id,
                int(payload.object_version or 1),
            )
        logger.info(f"Updated object status to 'uploaded' object_id={payload.object_id}")

        total_duration = time.time() - start_time
        logger.info(
            f"Upload complete object_id={payload.object_id} cids={len(cids_and_sizes)} "
            f"chunks={chunk_duration:.2f}s manifest={manifest_duration:.2f}s total={total_duration:.2f}s"
        )

        get_metrics_collector().record_uploader_operation(
            main_account=payload.address,
            success=True,
            num_chunks=len(payload.chunks),
            duration=total_duration,
        )

        return list(cids_and_sizes.keys())

    async def _upload_chunks(
        self,
        object_id: str,
        object_key: str,
        chunks: List[Chunk],
        upload_id: Optional[str],
        object_version: int,
        account_ss58: str,
    ) -> List[str]:
        logger.debug(f"Uploading {len(chunks)} chunks for object_id={object_id}")

        concurrency = self.config.uploader_multipart_max_concurrency
        semaphore = asyncio.Semaphore(concurrency)

        async def upload_chunk(chunk: Chunk) -> ChunkUploadResult:
            async with semaphore:
                return await self._upload_single_chunk(
                    object_id=object_id,
                    object_key=object_key,
                    chunk=chunk,
                    upload_id=upload_id,
                    object_version=int(object_version),
                    account_ss58=account_ss58,
                )

        chunks_sorted = sorted(chunks, key=lambda c: c.id)

        if not chunks_sorted:
            logger.info(f"No chunks for object_id={object_id}; skipping upload")
            return []

        first_result = await upload_chunk(chunks_sorted[0])

        if len(chunks_sorted) > 1:
            remaining_results = await asyncio.gather(*[upload_chunk(c) for c in chunks_sorted[1:]])
            all_results = [first_result] + list(remaining_results)
        else:
            all_results = [first_result]

        for result in all_results:
            await self.obj_cache.expire(
                object_id, int(object_version), result.part_number, ttl=self.config.cache_ttl_seconds
            )

        all_cids = []
        for result in all_results:
            all_cids.extend(result.cids)
        return all_cids

    async def _upload_single_chunk(
        self,
        object_id: str,
        object_key: str,
        chunk: Chunk,
        upload_id: Optional[str],
        object_version: int,
        account_ss58: str,
    ) -> ChunkUploadResult:
        part_number = int(chunk.id)
        logger.debug(f"Uploading chunk object_id={object_id} part={part_number}")
        # Read from FS store (authoritative source that survives Redis eviction)
        meta = await self.fs_store.get_meta(object_id, int(object_version), part_number)
        num_chunks_meta = int(meta.get("num_chunks", 0)) if isinstance(meta, dict) else 0

        # Ensure we have a non-null upload_id for parts table operations
        resolved_upload_id: Optional[str] = upload_id
        if not resolved_upload_id:
            try:
                async with self._acquire_conn() as conn:
                    row = await conn.fetchrow(
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
        async with self._acquire_conn() as conn:
            part_row = await conn.fetchrow(
                """
                SELECT p.part_id,
                       COALESCE(p.chunk_size_bytes, 0) AS chunk_size_bytes,
                       COALESCE(p.size_bytes, 0) AS size_bytes
                FROM parts p
                WHERE p.object_id = $1 AND p.part_number = $2 AND p.object_version = $3
                LIMIT 1
                """,
                object_id,
                part_number,
                int(object_version),
            )
        part_id: Optional[str] = str(part_row[0]) if part_row else None
        part_chunk_size: int = int(part_row[1] or 0) if part_row else 0
        part_plain_size: int = int(part_row[2] or 0) if part_row else 0

        if num_chunks_meta > 0 and not part_id:
            logger.warning(
                f"Chunked upload meta indicates {num_chunks_meta} pieces but no part_id found "
                f"(object_id={object_id} part={part_number} object_version={object_version}); requeue"
            )
            raise RuntimeError("part_row_missing_for_chunked_upload")

        if num_chunks_meta == 0:
            logger.debug(f"Empty file upload: object_id={object_id} part={part_number}")
            return ChunkUploadResult(cids=[], part_number=part_number)

        if num_chunks_meta > 0 and part_id:
            all_chunk_cids: list[str] = []
            for ci in range(num_chunks_meta):
                # Read from FS store (authoritative source)
                piece = await self.fs_store.get_chunk(object_id, int(object_version), part_number, ci)
                if not isinstance(piece, (bytes, bytearray)):
                    raise RuntimeError("missing_cipher_chunk")
                async with HippiusApiClient() as api_client:
                    mangled_file_name = f"{object_id}.part{part_number}.chunk{ci}"
                    mangled_file_name = hashlib.md5(mangled_file_name.encode()).hexdigest()

                    chunk_upload_result = await api_client.upload_file_and_get_cid(
                        file_data=bytes(piece),
                        file_name=f"s3-{mangled_file_name}",
                        content_type="application/octet-stream",
                        account_ss58=account_ss58,
                    )
                piece_cid = str(chunk_upload_result.cid)
                piece_file_id = str(chunk_upload_result.id)
                all_chunk_cids.append(piece_cid)

                logger.info(
                    f"Uploaded chunk: object_id={object_id} part={part_number} chunk={ci} "
                    f"file_id={piece_file_id} cid={piece_cid} status={chunk_upload_result.status}"
                )

                if part_chunk_size > 0 and part_plain_size > 0 and num_chunks_meta > 0:
                    if ci < num_chunks_meta - 1:
                        pt_len = int(part_chunk_size)
                    else:
                        pt_len = max(0, int(part_plain_size) - int(part_chunk_size) * int(num_chunks_meta - 1))
                else:
                    pt_len = None

                async with self._acquire_conn() as conn:
                    await conn.execute(
                        get_query("upsert_part_chunk"),
                        part_id,
                        int(ci),
                        piece_cid,
                        int(len(piece)),
                        int(pt_len) if isinstance(pt_len, int) else None,
                        None,
                        piece_file_id,
                    )

                    # Increment storage backend counter after successful upload
                    backend_count = await conn.fetchval(
                        get_query("increment_chunk_backend_count"),
                        part_id,
                        int(ci),
                    )
                    logger.debug(
                        f"Incremented backend count to {backend_count}: "
                        f"object_id={object_id} part={part_number} chunk={ci}"
                    )

            # Set parts.ipfs_cid to first chunk CID for manifest compatibility
            first_chunk_cid = all_chunk_cids[0] if all_chunk_cids else ""
            async with self._acquire_conn() as conn:
                cid_row = await conn.fetchrow(get_query("upsert_cid"), first_chunk_cid) if first_chunk_cid else None
            cid_id = cid_row["id"] if cid_row else None
            async with self._acquire_conn() as conn:
                await conn.execute(
                    """
                    UPDATE parts p
                    SET ipfs_cid = $3, cid_id = COALESCE($4, cid_id)
                    WHERE p.object_id = $1 AND p.part_number = $2 AND p.object_version = $5
                    """,
                    object_id,
                    part_number,
                    first_chunk_cid,
                    cid_id,
                    int(object_version),
                )

            logger.debug(
                f"Uploaded {num_chunks_meta} chunk pieces for object_id={object_id} part={part_number}; all_cids={len(all_chunk_cids)}"
            )
            return ChunkUploadResult(cids=all_chunk_cids, part_number=part_number)

        raise RuntimeError("part_meta_not_ready")

    async def _build_and_upload_manifest(
        self,
        object_id: str,
        object_key: str,
        object_version: int,
        account_ss58: str,
    ) -> dict:
        async with self._acquire_conn() as conn:
            logger.debug(f"Building manifest for object_id={object_id}")

            rows = await conn.fetch(
                """
                SELECT p.part_number,
                       COALESCE(c.cid, p.ipfs_cid) AS cid,
                       p.size_bytes::bigint AS size_bytes
                FROM parts p
                LEFT JOIN cids c ON p.cid_id = c.id
                WHERE p.object_id = $1 AND p.object_version = $2
                ORDER BY p.part_number
                """,
                object_id,
                object_version,
            )
            parts_data = []
            for r in rows:
                part_number = int(r[0])
                cid_raw = r[1]
                cid = str(cid_raw).strip() if cid_raw else None
                size = int(r[2] or 0)
                if size == 0:
                    cid = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"
                elif not cid or cid.lower() in {"", "none", "pending"}:
                    logger.debug(f"Manifest deferred: missing CID for part {part_number} (object_id={object_id})")
                    return {"status": "pending"}
                parts_data.append({"part_number": part_number, "cid": cid, "size_bytes": size})

            obj_row = await conn.fetchrow(
                """
                SELECT content_type
                FROM object_versions
                WHERE object_id = $1 AND object_version = $2
                """,
                object_id,
                object_version,
            )
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

            async with HippiusApiClient() as api_client:
                mangled_file_name = f"{object_id}.manifest"
                mangled_file_name = hashlib.md5(mangled_file_name.encode()).hexdigest()

                manifest_upload_result = await api_client.upload_file_and_get_cid(
                    file_data=manifest_json.encode(),
                    file_name=f"s3-{mangled_file_name}",
                    content_type="application/json",
                    account_ss58=account_ss58,
                )
            if not manifest_upload_result.cid:
                raise ValueError("manifest_publish_missing_cid")

            manifest_cid = str(manifest_upload_result.cid)
            manifest_file_id = str(manifest_upload_result.id)

            logger.info(
                f"Uploaded manifest: object_id={object_id} file_id={manifest_file_id} "
                f"cid={manifest_cid} status={manifest_upload_result.status}"
            )

            cid_row = await conn.fetchrow(get_query("upsert_cid"), manifest_cid)
            cid_id = cid_row["id"] if cid_row else None

            await conn.execute(
                """
                UPDATE object_versions
                SET ipfs_cid = $3,
                    cid_id = COALESCE($4, cid_id),
                    manifest_api_file_id = $5,
                    status = 'pinning'
                WHERE object_id = $1 AND object_version = $2
                """,
                object_id,
                object_version,
                manifest_cid,
                cid_id,
                manifest_file_id,
            )

            manifest_data["manifest_cid"] = manifest_cid
            manifest_data["manifest_size_bytes"] = manifest_upload_result.size_bytes

            return manifest_data

    async def _push_to_dlq(self, payload: UploadChainRequest, last_error: str, error_type: str) -> None:
        """Push a failed request to the Dead-Letter Queue."""
        await self.dlq_manager.push(payload, last_error, error_type)

        etype = error_type
        if isinstance(etype, bool):
            etype = "transient" if etype else "permanent"

        get_metrics_collector().record_uploader_operation(
            main_account=payload.address,
            success=False,
            error_type=etype,
        )
