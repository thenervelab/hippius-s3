import asyncio
import logging
import random
import time
from abc import ABC
from abc import abstractmethod
from collections.abc import AsyncIterator
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
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)


class UploadResponse(BaseModel):
    id: str
    cid: str
    status: str
    size_bytes: int = 0


class BackendClient(ABC):
    @abstractmethod
    async def upload_file_and_get_cid(
        self,
        file_data: bytes,
        file_name: str,
        content_type: str,
        account_ss58: str,
    ) -> UploadResponse:
        pass

    @abstractmethod
    async def download_file(
        self,
        file_id: str,
        account_ss58: str,
        chunk_size: int = 65536,
    ) -> AsyncIterator[bytes]:
        pass

    @abstractmethod
    async def delete_file(
        self,
        file_id: str,
    ) -> Any:
        pass


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
        backend_name: str,
        backend_client: BackendClient,
    ) -> None:
        self.db = db_pool
        self.redis_client = redis_client
        self.redis_queues_client = redis_queues_client
        self.config = config
        self.backend_name = backend_name
        self.backend_client = backend_client
        self.obj_cache = RedisObjectPartsCache(redis_client)
        self.fs_store = FileSystemPartsStore(config.object_cache_dir)
        self.dlq_manager = UploadDLQManager(redis_queues_client, backend_name=backend_name)

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
        # Skip upload if object or version was deleted
        async with self._acquire_conn() as conn:
            is_deleted = await conn.fetchval(
                get_query("is_object_deleted"),
                payload.object_id,
                payload.object_version,
            )
        if is_deleted:
            logger.info(
                f"Skipping upload for deleted object/version: backend={self.backend_name} "
                f"object_id={payload.object_id} version={payload.object_version}"
            )
            return []

        start_time = time.time()
        logger.info(
            f"Processing upload backend={self.backend_name} object_id={payload.object_id} chunks={len(payload.chunks)}"
        )

        all_chunk_cids = await self._upload_chunks(
            object_id=payload.object_id,
            object_key=payload.object_key,
            chunks=payload.chunks,
            upload_id=payload.upload_id,
            object_version=int(payload.object_version or 1),
            account_ss58=payload.address,
        )

        total_duration = time.time() - start_time
        logger.info(
            f"Upload complete backend={self.backend_name} object_id={payload.object_id} "
            f"chunks={len(all_chunk_cids)} duration={total_duration:.2f}s"
        )

        get_metrics_collector().record_uploader_operation(
            main_account=payload.address,
            success=True,
            num_chunks=len(payload.chunks),
            duration=total_duration,
        )

        return all_chunk_cids

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

        # Look up part_id for chunk_backend insert
        async with self._acquire_conn() as conn:
            part_row = await conn.fetchrow(
                """
                SELECT p.part_id
                FROM parts p
                WHERE p.object_id = $1 AND p.part_number = $2 AND p.object_version = $3
                LIMIT 1
                """,
                object_id,
                part_number,
                int(object_version),
            )
        part_id: Optional[str] = str(part_row[0]) if part_row else None

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
                piece = await self.fs_store.get_chunk(object_id, int(object_version), part_number, ci)
                if not isinstance(piece, (bytes, bytearray)):
                    raise RuntimeError("missing_cipher_chunk")

                # Look up the chunk UUID from part_chunks so Arion can address it
                async with self._acquire_conn() as conn:
                    chunk_id = await conn.fetchval(
                        "SELECT id FROM part_chunks WHERE part_id = $1 AND chunk_index = $2",
                        part_id,
                        int(ci),
                    )
                if not chunk_id:
                    raise RuntimeError("part_chunk_row_missing")

                chunk_upload_result = await self.backend_client.upload_file_and_get_cid(
                    file_data=bytes(piece),
                    file_name=str(chunk_id),
                    content_type="application/octet-stream",
                    account_ss58=account_ss58,
                )

                piece_cid = str(chunk_upload_result.cid)
                piece_file_id = str(chunk_upload_result.id)
                all_chunk_cids.append(piece_cid)

                logger.info(
                    f"Uploaded chunk: backend={self.backend_name} object_id={object_id} part={part_number} "
                    f"chunk={ci} file_id={piece_file_id} cid={piece_cid} status={chunk_upload_result.status}"
                )

                async with self._acquire_conn() as conn:
                    await conn.fetchval(
                        get_query("insert_chunk_backend"),
                        part_id,
                        int(ci),
                        self.backend_name,
                        piece_cid,
                    )

            logger.debug(
                f"Uploaded {num_chunks_meta} chunks backend={self.backend_name} object_id={object_id} "
                f"part={part_number} cids={len(all_chunk_cids)}"
            )
            return ChunkUploadResult(cids=all_chunk_cids, part_number=part_number)

        raise RuntimeError("part_meta_not_ready")

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
