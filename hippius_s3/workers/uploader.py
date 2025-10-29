import asyncio
import json
import logging
import random
import time
from pathlib import Path
from typing import Any
from typing import List
from typing import Optional

from pydantic import BaseModel

from hippius_s3.cache import FileSystemPartsStore
from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.ipfs_service import IPFSService
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.queue import BaseUploadRequest
from hippius_s3.queue import Chunk
from hippius_s3.queue import DataUploadRequest
from hippius_s3.queue import SubstratePinningRequest
from hippius_s3.queue import enqueue_substrate_request
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

    return "unknown"


def compute_backoff_ms(attempt: int, base_ms: int = 1000, max_ms: int = 30000) -> float:
    """Compute exponential backoff with jitter."""
    exp_backoff = base_ms * (2 ** (attempt - 1))
    jitter = random.uniform(0, exp_backoff * 0.1)
    return float(min(exp_backoff + jitter, max_ms))


class Uploader:
    def __init__(self, db_pool: Any, ipfs_service: IPFSService, redis_client: Any, config: Any):
        # Support either a Pool (has acquire) or a single Connection
        self.db = db_pool
        self.ipfs_service = ipfs_service
        self.redis_client = redis_client
        self.config = config
        self.obj_cache = RedisObjectPartsCache(redis_client)
        self.fs_store = FileSystemPartsStore(getattr(config, "object_cache_dir", "/var/lib/hippius/object_cache"))

    class _ConnCtx:
        def __init__(self, conn: Any):
            self._conn = conn

        async def __aenter__(self):
            return self._conn

        async def __aexit__(self, exc_type, exc, tb):
            return False

    def _acquire_conn(self):
        """Return an async context manager yielding a connection.

        Works with either a Pool (has acquire) or a single Connection.
        """
        acquire = getattr(self.db, "acquire", None)
        if callable(acquire):
            return acquire()
        return Uploader._ConnCtx(self.db)

    async def process_upload(self, payload: BaseUploadRequest) -> List[str]:
        start_time = time.time()
        num_items = payload.get_items_count()
        logger.info(
            f"Processing upload object_id={payload.object_id} items={num_items} kind={getattr(payload, 'kind', 'data')}"
        )

        # Handle redundancy staged uploads
        kind = str(getattr(payload, "kind", "data") or "data").lower()
        if kind in ("replica", "parity"):
            cids = await self._process_redundancy_upload(payload)
            total_duration = time.time() - start_time
            get_metrics_collector().record_uploader_operation(
                main_account=payload.address,
                success=True,
                num_chunks=payload.get_items_count(),
                duration=total_duration,
            )
            return cids

        chunk_start = time.time()
        data_payload: DataUploadRequest = (
            payload
            if isinstance(payload, DataUploadRequest)
            else DataUploadRequest.model_validate(payload.model_dump())
        )
        chunk_cids = await self._upload_chunks(
            object_id=payload.object_id,
            object_key=payload.object_key,
            chunks=data_payload.chunks,
            upload_id=data_payload.upload_id,
            object_version=int(payload.object_version or 1),
        )
        chunk_duration = time.time() - chunk_start

        manifest_start = time.time()
        manifest_cid = await self._build_and_upload_manifest(
            object_id=payload.object_id,
            object_key=payload.object_key,
            object_version=int(payload.object_version or 1),
        )
        manifest_duration = time.time() - manifest_start

        valid_cids = [c for c in (chunk_cids + [manifest_cid]) if c and c != "pending"]

        if valid_cids:
            await enqueue_substrate_request(
                SubstratePinningRequest(
                    cids=valid_cids,
                    address=payload.address,
                    object_id=payload.object_id,
                    object_version=int(payload.object_version or 1),
                ),
            )

        total_duration = time.time() - start_time
        logger.info(
            f"Upload complete object_id={payload.object_id} cids={len(valid_cids)} "
            f"chunks={chunk_duration:.2f}s manifest={manifest_duration:.2f}s total={total_duration:.2f}s"
        )

        get_metrics_collector().record_uploader_operation(
            main_account=payload.address,
            success=True,
            num_chunks=len(data_payload.chunks),
            duration=total_duration,
        )

        return valid_cids

    async def _process_redundancy_upload(self, payload: BaseUploadRequest) -> List[str]:
        """Process staged redundancy uploads (replica/parity)."""
        kind = str(getattr(payload, "kind", "data") or "data").lower()
        staged = list(getattr(payload, "staged", []) or [])
        if not staged:
            return []

        # Resolve part_id once
        async with self._acquire_conn() as conn:
            row = await conn.fetchrow(
                """
                SELECT p.part_id
                  FROM parts p
                 WHERE p.object_id = $1 AND p.object_version = $2 AND p.part_number = $3
                 LIMIT 1
                """,
                payload.object_id,
                int(payload.object_version or 1),
                int(getattr(payload, "part_number", 0) or 1),
            )
        if not row:
            logger.warning("uploader: redundancy part_id not found; skipping")
            return []
        part_id = str(row[0])
        policy_version = int(getattr(payload, "policy_version", 1) or 1)

        # Ensure part_ec row exists for FK safety (rep-v1 pending_upload)
        async with self._acquire_conn() as conn:
            await conn.execute(
                """
                INSERT INTO part_ec (part_id, policy_version, scheme, k, m, shard_size_bytes, stripes, state)
                VALUES ($1, $2, 'rep-v1', 1, 0, 0, 1, 'pending_upload')
                ON CONFLICT (part_id, policy_version)
                DO NOTHING
                """,
                part_id,
                policy_version,
            )

        uploaded_cids: list[str] = []
        max_replica_index = -1
        for item in staged:
            data: Optional[bytes] = None
            file_path = item.get("file_path")
            if isinstance(file_path, str) and file_path:
                try:
                    p = Path(file_path)
                    if p.exists():
                        data = p.read_bytes()
                except Exception:
                    logger.warning(f"uploader: failed to read staged file {file_path}")
            if data is None:
                redis_key = str(item.get("redis_key", ""))
                if redis_key:
                    data = await self.redis_client.get(redis_key)
            if not isinstance(data, (bytes, bytearray)):
                logger.warning("uploader: staged item missing data; skipping")
                continue

            res = await self.ipfs_service.upload_file(
                file_data=bytes(data),
                file_name=(Path(file_path).name if isinstance(file_path, str) and file_path else "redundancy.bin"),
                content_type="application/octet-stream",
                encrypt=False,
            )
            cid = str(res["cid"]) if isinstance(res, dict) else str(res)
            uploaded_cids.append(cid)

            # Persist into part_parity_chunks
            async with self._acquire_conn() as conn:
                if kind == "replica":
                    max_replica_index = max(max_replica_index, int(item.get("replica_index", 0)))
                    await conn.execute(
                        """
                        INSERT INTO part_parity_chunks (part_id, policy_version, stripe_index, parity_index, cid)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (part_id, policy_version, stripe_index, parity_index)
                        DO UPDATE SET cid=EXCLUDED.cid
                        """,
                        part_id,
                        policy_version,
                        int(item.get("chunk_index", 0)),
                        int(item.get("replica_index", 0)),
                        cid,
                    )
                else:  # parity path (future)
                    await conn.execute(
                        """
                        INSERT INTO part_parity_chunks (part_id, policy_version, stripe_index, parity_index, cid)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (part_id, policy_version, stripe_index, parity_index)
                        DO UPDATE SET cid=EXCLUDED.cid
                        """,
                        part_id,
                        policy_version,
                        int(item.get("stripe_index", 0)),
                        int(item.get("parity_index", 0)),
                        cid,
                    )

        # Mark part_ec complete for replication and set m to at least max_replica_index+1
        if kind == "replica":
            async with self._acquire_conn() as conn:
                await conn.execute(
                    """
                    INSERT INTO part_ec (part_id, policy_version, scheme, k, m, shard_size_bytes, stripes, state)
                    VALUES ($1, $2, 'rep-v1', 1, $3, 0, 1, 'complete')
                    ON CONFLICT (part_id, policy_version)
                    DO UPDATE SET state='complete', m=GREATEST(part_ec.m, $3), updated_at=now()
                    """,
                    part_id,
                    policy_version,
                    int(max_replica_index + 1 if max_replica_index >= 0 else 0),
                )

        # Enqueue substrate pinning for redundancy CIDs
        if uploaded_cids:
            await enqueue_substrate_request(
                SubstratePinningRequest(
                    cids=uploaded_cids,
                    address=payload.address,
                    object_id=payload.object_id,
                    object_version=int(payload.object_version or 1),
                )
            )

        return uploaded_cids

    # part_number is explicitly supplied in UploadChainRequest for redundancy

    async def _upload_chunks(
        self,
        object_id: str,
        object_key: str,
        chunks: List[Chunk],
        upload_id: Optional[str],
        object_version: int,
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

        if num_chunks_meta > 0 and part_id:
            all_chunk_cids: list[str] = []
            for ci in range(num_chunks_meta):
                # Read from FS store (authoritative source)
                piece = await self.fs_store.get_chunk(object_id, int(object_version), part_number, ci)
                if not isinstance(piece, (bytes, bytearray)):
                    raise RuntimeError("missing_cipher_chunk")
                up_res = await self.ipfs_service.upload_file(
                    file_data=bytes(piece),
                    file_name=f"{object_key}.part{part_number}.chunk{ci}",
                    content_type="application/octet-stream",
                    encrypt=False,
                )
                piece_cid = str(up_res["cid"])
                all_chunk_cids.append(piece_cid)

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
    ) -> str:
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
                if not cid or cid.lower() in {"", "none", "pending"}:
                    logger.debug(f"Manifest deferred: missing CID for part {part_number} (object_id={object_id})")
                    return "pending"
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

            manifest_result = await self.ipfs_service.upload_file(
                file_data=manifest_json.encode(),
                file_name=f"{object_key}.manifest",
                content_type="application/json",
                encrypt=False,
            )
            manifest_cid = str(
                manifest_result.get("cid")
                if isinstance(manifest_result, dict)
                else getattr(manifest_result, "cid", None)
            )
            if not manifest_cid:
                raise ValueError("manifest_publish_missing_cid")

            cid_row = await conn.fetchrow(get_query("upsert_cid"), manifest_cid)
            cid_id = cid_row["id"] if cid_row else None

            await conn.execute(
                """
                UPDATE object_versions
                SET ipfs_cid = $3,
                    cid_id = COALESCE($4, cid_id),
                    status = 'pinning'
                WHERE object_id = $1 AND object_version = $2
                """,
                object_id,
                object_version,
                manifest_cid,
                cid_id,
            )

            logger.info(f"Manifest uploaded object_id={object_id} cid={manifest_cid}")
            return manifest_cid

    async def _push_to_dlq(self, payload: BaseUploadRequest, last_error: str, error_type: str) -> None:
        """Push a failed request to the Dead-Letter Queue."""

        etype = error_type
        if isinstance(etype, bool):
            etype = "transient" if etype else "permanent"

        dlq_entry = {
            "payload": payload.model_dump(),
            "object_id": payload.object_id,
            "upload_id": getattr(payload, "upload_id", None),
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
