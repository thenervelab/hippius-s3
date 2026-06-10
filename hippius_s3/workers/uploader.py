import asyncio
import logging
import time
from abc import ABC
from abc import abstractmethod
from collections.abc import AsyncIterator
from types import TracebackType
from typing import Any
from typing import List
from typing import Optional

import redis.asyncio as async_redis
from opentelemetry import trace
from pydantic import BaseModel

from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.cache import create_fs_store
from hippius_s3.dlq.upload_dlq import UploadDLQManager
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.queue import Chunk
from hippius_s3.queue import UploadChainRequest
from hippius_s3.utils import get_query
from hippius_s3.workers.errors import is_billing_error


logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


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
        extra_headers: dict[str, str] | None = None,
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


class Uploader:
    def __init__(
        self,
        db_pool: Any,
        redis_client: async_redis.Redis,
        redis_queues_client: async_redis.Redis,
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
        self.fs_store = create_fs_store(config)
        self.dlq_manager = UploadDLQManager(redis_queues_client, backend_name=backend_name)
        # Single shared per-pod bound on concurrent backend POSTs across ALL in-flight
        # requests, so outer request-concurrency can't stampede the backend. Replaces
        # the old per-request chunk semaphore.
        self._put_semaphore = asyncio.Semaphore(max(1, int(getattr(config, "arion_upload_concurrency", 8))))

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
        with tracer.start_as_current_span(
            "uploader.process_upload",
            attributes={
                "object_id": payload.object_id,
                "object_key": payload.object_key,
                "object_version": str(payload.object_version or 1),
                "backend": self.backend_name,
                "num_chunks": len(payload.chunks),
                "hippius.account.main": payload.address,
            },
        ) as span:
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
                span.set_attribute("skipped", True)
                return []

            start_time = time.time()
            logger.info(
                f"Processing upload backend={self.backend_name} object_id={payload.object_id} chunks={len(payload.chunks)}"
            )

            extra_headers: dict[str, str] | None = None
            if payload.bypass_billing and self.config.arion_billing_bypass_key:
                extra_headers = {"X-Billing-Bypass": self.config.arion_billing_bypass_key}

            all_chunk_cids = await self._upload_chunks(
                object_id=payload.object_id,
                object_key=payload.object_key,
                chunks=payload.chunks,
                upload_id=payload.upload_id,
                object_version=int(payload.object_version or 1),
                account_ss58=payload.address,
                extra_headers=extra_headers,
            )

            total_duration = time.time() - start_time
            logger.info(
                f"Upload complete backend={self.backend_name} object_id={payload.object_id} "
                f"chunks={len(all_chunk_cids)} duration={total_duration:.2f}s"
            )

            span.set_attribute("result.chunks_uploaded", len(all_chunk_cids))
            span.set_attribute("result.duration_s", total_duration)

            get_metrics_collector().record_uploader_operation(
                main_account=payload.address,
                success=True,
                backend=self.backend_name,
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
        extra_headers: dict[str, str] | None = None,
    ) -> List[str]:
        concurrency = self.config.uploader_multipart_max_concurrency
        with tracer.start_as_current_span(
            "uploader.upload_chunks",
            attributes={
                "object_id": object_id,
                "num_chunks": len(chunks),
                "concurrency": concurrency,
            },
        ):
            logger.debug(f"Uploading {len(chunks)} chunks for object_id={object_id}")

            semaphore = asyncio.Semaphore(concurrency)
            # Once one chunk hits a 402, stop firing the rest at Arion — the whole object is doomed
            # until the account tops up, so there's no point billing every remaining chunk.
            billing_abort = asyncio.Event()

            async def upload_chunk(chunk: Chunk) -> ChunkUploadResult:
                async with semaphore:
                    if billing_abort.is_set():
                        raise RuntimeError("billing_aborted")
                    try:
                        return await self._upload_single_chunk(
                            object_id=object_id,
                            object_key=object_key,
                            chunk=chunk,
                            upload_id=upload_id,
                            object_version=int(object_version),
                            account_ss58=account_ss58,
                            extra_headers=extra_headers,
                        )
                    except Exception as e:
                        if is_billing_error(e):
                            billing_abort.set()
                        raise

            chunks_sorted = sorted(chunks, key=lambda c: c.id)

            if not chunks_sorted:
                logger.info(f"No chunks for object_id={object_id}; skipping upload")
                return []

            first_result = await upload_chunk(chunks_sorted[0])

            if len(chunks_sorted) > 1:
                results = await asyncio.gather(*[upload_chunk(c) for c in chunks_sorted[1:]], return_exceptions=True)
                errors = [r for r in results if isinstance(r, Exception)]
                if errors:
                    raise next((e for e in errors if is_billing_error(e)), errors[0])
                all_results = [first_result] + [r for r in results if isinstance(r, ChunkUploadResult)]
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
        extra_headers: dict[str, str] | None = None,
    ) -> ChunkUploadResult:
        part_number = int(chunk.id)
        with tracer.start_as_current_span(
            "uploader.upload_chunk",
            attributes={
                "object_id": object_id,
                "part_number": part_number,
            },
        ) as span:
            logger.debug(f"Uploading chunk object_id={object_id} part={part_number}")
            meta_wait_s = float(getattr(self.config, "fs_meta_wait_seconds", 30.0))
            meta = await self.fs_store.get_meta_with_wait(
                object_id, int(object_version), part_number, deadline_seconds=meta_wait_s
            )
            if meta is None:
                raise RuntimeError(
                    f"Missing meta in filesystem cache after {meta_wait_s}s for upload: "
                    f"object_id={object_id} version={int(object_version)} part={part_number}"
                )
            num_chunks_meta = int(meta.get("num_chunks", 0)) if isinstance(meta, dict) else 0

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
                # Upload a part's cipher chunks concurrently. Each chunk's backend POST is
                # bounded by the shared per-pod `_put_semaphore` (all in-flight requests on
                # this pod share one Arion concurrency budget). Chunk order is preserved in
                # the returned hashes.
                async def upload_one_chunk(ci: int) -> tuple[int, str]:
                    async with self._put_semaphore:
                        piece = await self.fs_store.get_chunk(object_id, int(object_version), part_number, ci)
                        if not isinstance(piece, (bytes, bytearray)):
                            raise RuntimeError("missing_cipher_chunk")

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
                            extra_headers=extra_headers,
                        )

                        file_hash = str(chunk_upload_result.id)
                        logger.info(
                            f"Uploaded chunk: backend={self.backend_name} object_id={object_id} part={part_number} "
                            f"chunk={ci} file_id={file_hash} upload_id={chunk_upload_result.cid} "
                            f"status={chunk_upload_result.status}"
                        )

                        async with self._acquire_conn() as conn:
                            await conn.fetchval(
                                get_query("insert_chunk_backend"),
                                part_id,
                                int(ci),
                                self.backend_name,
                                file_hash,
                            )
                        return ci, file_hash

                results = await asyncio.gather(
                    *[upload_one_chunk(ci) for ci in range(num_chunks_meta)],
                    return_exceptions=True,
                )
                errors = [r for r in results if isinstance(r, Exception)]
                if errors:
                    raise next((e for e in errors if is_billing_error(e)), errors[0])

                ok_results = sorted((r for r in results if isinstance(r, tuple)), key=lambda t: t[0])
                all_file_hashes: list[str] = [fh for _, fh in ok_results]

                span.set_attribute("result.num_piece_cids", len(all_file_hashes))
                span.set_attribute("result.cids", ",".join(all_file_hashes))

                return ChunkUploadResult(
                    cids=all_file_hashes,
                    part_number=part_number,
                )

            raise RuntimeError("part_meta_not_ready")

    async def _push_to_dlq(
        self, payload: UploadChainRequest, last_error: str, error_type: str, status_code: str = ""
    ) -> None:
        """Push a failed request to the Dead-Letter Queue."""
        await self.dlq_manager.push(payload, last_error, error_type)

        etype = error_type
        if isinstance(etype, bool):
            etype = "transient" if etype else "permanent"

        get_metrics_collector().record_uploader_operation(
            main_account=payload.address,
            success=False,
            backend=self.backend_name,
            error_type=etype,
            status_code=status_code,
        )
