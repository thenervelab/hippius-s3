from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from typing import Any
from typing import AsyncIterator

from opentelemetry import trace  # type: ignore[import-not-found]

from hippius_s3.api.middlewares.tracing import set_span_attributes
from hippius_s3.cache import FileSystemPartsStore
from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.config import get_config
from hippius_s3.services.crypto_service import CryptoService
from hippius_s3.services.key_service import get_or_create_encryption_key_bytes
from hippius_s3.services.parts_service import upsert_part_placeholder
from hippius_s3.utils import get_query
from hippius_s3.writer.db import ensure_upload_row
from hippius_s3.writer.db import upsert_object_basic
from hippius_s3.writer.types import AppendPreconditionFailed
from hippius_s3.writer.types import CompleteResult
from hippius_s3.writer.types import EmptyAppendError
from hippius_s3.writer.types import ObjectNotFound
from hippius_s3.writer.types import PartResult
from hippius_s3.writer.types import PutResult
from hippius_s3.writer.write_through_writer import WriteThroughPartsWriter


tracer = trace.get_tracer(__name__)


class ObjectWriter:
    def __init__(self, *, db: Any, redis_client: Any, fs_store: FileSystemPartsStore | None = None) -> None:
        self.db = db
        self.redis_client = redis_client
        self.config = get_config()
        self.obj_cache = RedisObjectPartsCache(redis_client)
        # Initialize FS store for write-through (optional for backward compat during rollout)
        self.fs_store = fs_store or FileSystemPartsStore(self.config.object_cache_dir)

    async def create_version_for_migration(
        self,
        *,
        object_id: str,
        content_type: str,
        metadata: dict[str, Any],
        storage_version_target: int,
    ) -> int:
        row = await self.db.fetchrow(
            get_query("create_migration_version"),
            object_id,
            content_type,
            json.dumps(metadata),
            int(storage_version_target),
        )
        if not row:
            raise RuntimeError("Failed to create migration version")
        return int(row[0])

    async def swap_current_version_cas(
        self,
        *,
        object_id: str,
        expected_old_version: int,
        new_version: int,
    ) -> bool:
        row = await self.db.fetchrow(
            get_query("swap_current_version_cas"),
            object_id,
            int(expected_old_version),
            int(new_version),
        )
        return bool(row)

    async def put_simple(
        self,
        *,
        bucket_id: str,
        bucket_name: str,
        object_id: str,
        object_key: str,
        object_version: int,
        account_address: str,
        seed_phrase: str,
        content_type: str,
        metadata: dict[str, Any],
        body_bytes: bytes,
    ) -> PutResult:
        """Deprecated: prefer put_simple_full which will upsert + write.

        This method assumes the object row/version already exists in DB.
        """
        # Encrypt into chunks
        chunk_size = self.config.object_chunk_size_bytes
        key_bytes = await get_or_create_encryption_key_bytes(
            main_account_id=account_address,
            bucket_name=bucket_name,
        )
        ct_chunks = CryptoService.encrypt_part_to_chunks(
            body_bytes,
            object_id=object_id,
            part_number=1,
            seed_phrase=seed_phrase,
            chunk_size=chunk_size,
            key=key_bytes,
        )

        ttl = self.config.cache_ttl_seconds
        writer = WriteThroughPartsWriter(self.fs_store, self.obj_cache, ttl_seconds=ttl)
        await writer.write_chunks(object_id, int(object_version), 1, ct_chunks)
        await writer.write_meta(
            object_id,
            int(object_version),
            1,
            chunk_size=chunk_size,
            num_chunks=len(ct_chunks),
            plain_size=len(body_bytes),
        )

        md5_hash = hashlib.md5(body_bytes).hexdigest()

        # Ensure upload row and insert part placeholder
        upload_id = await ensure_upload_row(
            self.db,
            object_id=object_id,
            bucket_id=bucket_id,
            object_key=object_key,
            content_type=content_type,
            metadata=metadata,
        )
        await upsert_part_placeholder(
            self.db,
            object_id=object_id,
            upload_id=str(upload_id),
            part_number=1,
            size_bytes=int(len(body_bytes)),
            etag=md5_hash,
            chunk_size_bytes=int(chunk_size),
            object_version=int(object_version),
        )

        return PutResult(etag=md5_hash, size_bytes=len(body_bytes), upload_id=str(upload_id))

    async def put_simple_stream_full(
        self,
        *,
        bucket_id: str,
        bucket_name: str,
        object_id: str,
        object_key: str,
        account_address: str,
        content_type: str,
        metadata: dict[str, Any],
        storage_version: int | None = None,
        body_iter: AsyncIterator[bytes],
    ) -> PutResult:
        """Upsert destination object and write content (single-part) using a streaming iterator.

        - Consumes an AsyncIterator[bytes] and encrypts/writes chunk-by-chunk to cache.
        - Keeps peak memory bounded to configured chunk size.
        - Uses server-side keys; seed phrases are not required.
        """
        chunk_size = self.config.object_chunk_size_bytes
        ttl = self.config.cache_ttl_seconds

        with tracer.start_as_current_span(
            "put_simple_stream_full.key_retrieval",
            attributes={
                "account_address": account_address,
                "bucket_name": bucket_name,
            },
        ):
            key_bytes = await get_or_create_encryption_key_bytes(
                main_account_id=account_address,
                bucket_name=bucket_name,
            )

        part_number = 1
        # Resolve current object_version for this object_id if exists; default to 1 for new objects
        try:
            row = await self.db.fetchrow(
                "SELECT current_object_version FROM objects WHERE object_id = $1",
                object_id,
            )
            object_version = int(row["current_object_version"]) if row and row.get("current_object_version") else 1
        except Exception:
            object_version = 1

        hasher = hashlib.md5()
        total_size = 0
        writer = WriteThroughPartsWriter(self.fs_store, self.obj_cache, ttl_seconds=ttl)

        pt_buf = bytearray()
        next_chunk_index = 0

        async def _encrypt_and_write(buf: bytes) -> None:
            nonlocal total_size, next_chunk_index
            if not buf:
                return
            hasher.update(buf)
            total_size += len(buf)
            ct_list = CryptoService.encrypt_part_to_chunks(
                buf,
                object_id=object_id,
                part_number=part_number,
                seed_phrase="",
                chunk_size=chunk_size,
                key=key_bytes,
            )
            # Write each produced ciphertext chunk immediately with increasing indices
            for ct in ct_list:
                # Write-through: FS first (fatal), then Redis (best-effort)
                await self.fs_store.set_chunk(
                    object_id,
                    int(object_version),
                    int(part_number),
                    int(next_chunk_index),
                    ct,
                )
                try:
                    await self.obj_cache.set_chunk(
                        object_id,
                        int(object_version),
                        int(part_number),
                        int(next_chunk_index),
                        ct,
                        ttl=ttl,
                    )
                except Exception as e:
                    import logging

                    logging.getLogger(__name__).warning(
                        f"Redis chunk write failed (best-effort) in streaming: object_id={object_id} v={object_version} part={part_number} chunk={next_chunk_index}: {e}"
                    )
                next_chunk_index += 1

        with tracer.start_as_current_span("put_simple_stream_full.encrypt_and_cache") as span:
            async for piece in body_iter:
                if not piece:
                    continue
                pt_buf.extend(piece)
                while len(pt_buf) >= chunk_size:
                    to_write = bytes(pt_buf[:chunk_size])
                    del pt_buf[:chunk_size]
                    await _encrypt_and_write(to_write)

            if pt_buf:
                await _encrypt_and_write(bytes(pt_buf))
                pt_buf.clear()

            md5_hash = hasher.hexdigest()
            set_span_attributes(
                span,
                {
                    "total_size": total_size,
                    "num_chunks": next_chunk_index,
                    "md5_hash": md5_hash,
                },
            )

        # Upsert DB row with final md5/size and storage version
        resolved_storage_version = int(
            storage_version if storage_version is not None else self.config.target_storage_version
        )
        with tracer.start_as_current_span(
            "put_simple_stream_full.upsert_metadata",
            attributes={
                "object_id": object_id,
                "has_object_id": True,
                "size_bytes": int(total_size),
                "md5_hash": md5_hash,
                "storage_version": resolved_storage_version,
            },
        ):
            await upsert_object_basic(
                self.db,
                object_id=object_id,
                bucket_id=bucket_id,
                object_key=object_key,
                content_type=content_type,
                metadata=metadata,
                md5_hash=md5_hash,
                size_bytes=int(total_size),
                storage_version=resolved_storage_version,
            )

        # Now that we know counts and sizes, write meta last using exact chunk count
        num_chunks = int(next_chunk_index)
        await writer.write_meta(
            object_id,
            int(object_version),
            int(part_number),
            chunk_size=chunk_size,
            num_chunks=int(num_chunks),
            plain_size=int(total_size),
        )

        # Ensure upload row and return PutResult-compatible values
        upload_id = await ensure_upload_row(
            self.db,
            object_id=object_id,
            bucket_id=bucket_id,
            object_key=object_key,
            content_type=content_type,
            metadata=metadata,
        )

        # Ensure parts table has placeholder for base part (part 1) so append/read paths see it
        import contextlib

        with contextlib.suppress(Exception):
            await upsert_part_placeholder(
                self.db,
                object_id=object_id,
                upload_id=str(upload_id),
                part_number=int(part_number),
                size_bytes=int(total_size),
                etag=md5_hash,
                chunk_size_bytes=int(chunk_size),
                object_version=int(object_version),
            )

        return PutResult(etag=md5_hash, size_bytes=int(total_size), upload_id=str(upload_id))

    async def mpu_upload_part(
        self,
        *,
        upload_id: str,
        object_id: str,
        object_version: int,
        bucket_name: str,
        account_address: str,
        seed_phrase: str,
        part_number: int,
        body_bytes: bytes,
    ) -> PartResult:
        file_size = len(body_bytes)
        if file_size == 0:
            raise ValueError("Zero-length part not allowed")

        chunk_size = self.config.object_chunk_size_bytes
        key_bytes = await get_or_create_encryption_key_bytes(
            main_account_id=account_address,
            bucket_name=bucket_name,
        )
        ct_chunks = CryptoService.encrypt_part_to_chunks(
            body_bytes,
            object_id=str(object_id),
            part_number=int(part_number),
            seed_phrase=seed_phrase,
            chunk_size=chunk_size,
            key=key_bytes,
        )

        ttl = self.config.cache_ttl_seconds
        writer = WriteThroughPartsWriter(self.fs_store, self.obj_cache, ttl_seconds=ttl)
        await writer.write_chunks(str(object_id), int(object_version), int(part_number), ct_chunks)
        await writer.write_meta(
            str(object_id),
            int(object_version),
            int(part_number),
            chunk_size=chunk_size,
            num_chunks=len(ct_chunks),
            plain_size=len(body_bytes),
        )

        # AWS parity: md5 of the part only
        loop = asyncio.get_event_loop()
        md5_hash = await loop.run_in_executor(None, lambda: hashlib.md5(body_bytes).hexdigest())

        await upsert_part_placeholder(
            self.db,
            object_id=str(object_id),
            upload_id=str(upload_id),
            part_number=int(part_number),
            size_bytes=int(file_size),
            etag=str(md5_hash),
            chunk_size_bytes=int(chunk_size),
            object_version=int(object_version),
        )

        return PartResult(etag=md5_hash, size_bytes=file_size, part_number=int(part_number))

    async def mpu_complete(
        self,
        *,
        bucket_name: str,
        object_id: str,
        object_key: str,
        upload_id: str,
        object_version: int,
        address: str,
        seed_phrase: str,
    ) -> CompleteResult:
        # Compute combined ETag from part etags for this version
        parts = await self.db.fetch(
            get_query("get_parts_etags_for_version"),
            object_id,
            int(object_version),
        )
        etags = [p["etag"].split("-")[0] for p in parts]
        binary = b"".join(bytes.fromhex(e) for e in etags) if etags else b""
        final_md5 = hashlib.md5(binary).hexdigest() + f"-{len(etags)}"

        # Total size
        all_parts = await self.db.fetch(
            get_query("list_parts_for_version"),
            object_id,
            int(object_version),
        )
        total_size = sum(int(p["size_bytes"]) for p in all_parts)

        async with self.db.transaction():
            # Update object_versions
            await self.db.execute(
                """
                UPDATE object_versions ov
                SET md5_hash = $1,
                    size_bytes = $2,
                    last_modified = NOW(),
                    status = 'publishing'
                WHERE ov.object_id = $3 AND ov.object_version = $4
                """,
                final_md5,
                int(total_size),
                object_id,
                int(object_version),
            )

            # Mark MPU completed
            await self.db.execute(
                "UPDATE multipart_uploads SET is_completed = TRUE WHERE upload_id = $1",
                upload_id,
            )

        return CompleteResult(etag=final_md5, size_bytes=int(total_size))

    async def append(
        self,
        *,
        bucket_id: str,
        bucket_name: str,
        object_key: str,
        expected_version: int,
        account_address: str,
        seed_phrase: str,
        incoming_bytes: bytes,
    ) -> dict:
        """Append bytes with CAS, cache write-through, and enqueue.

        Returns: {"object_id", "new_append_version", "etag", "part_number"}
        """
        if not incoming_bytes:
            raise EmptyAppendError("Empty append not allowed")

        row = await self.db.fetchrow(
            """
            SELECT o.object_id, o.current_object_version AS cov
            FROM objects o
            WHERE o.bucket_id = $1 AND o.object_key = $2
            """,
            bucket_id,
            object_key,
        )
        if not row:
            raise ObjectNotFound("NoSuchKey")

        object_id = str(row["object_id"])  # type: ignore
        cov = int(row["cov"])  # type: ignore

        # Reserve part and update version state
        delta_md5 = hashlib.md5(incoming_bytes).hexdigest()
        delta_size = len(incoming_bytes)
        next_part = None
        upload_id = None
        composite_etag = None
        new_append_version_val = None
        async with self.db.transaction():
            # 1) Lock the current version row and re-check CAS atomically
            locked = await self.db.fetchrow(
                """
                SELECT append_version
                  FROM object_versions
                 WHERE object_id = $1 AND object_version = $2
                 FOR UPDATE
                """,
                object_id,
                cov,
            )
            if not locked:
                raise ObjectNotFound("NoSuchKey")
            current_version = int(locked["append_version"])  # type: ignore
            if expected_version != current_version:
                raise AppendPreconditionFailed(current_version)

            # 2) Compute next part number AFTER holding the lock
            next_part = await self.db.fetchval(
                """
                SELECT COALESCE(MAX(part_number), 0) + 1
                  FROM parts
                 WHERE object_id = $1 AND object_version = $2
                """,
                object_id,
                cov,
            )
            upload_row = await self.db.fetchrow(
                "SELECT upload_id FROM multipart_uploads WHERE object_id = $1 ORDER BY initiated_at DESC LIMIT 1",
                object_id,
            )
            if upload_row:
                upload_id = str(upload_row["upload_id"])  # type: ignore
            else:
                upload_id = str(uuid.uuid4())
                await self.db.execute(
                    """
                    INSERT INTO multipart_uploads (upload_id, bucket_id, object_key, initiated_at, is_completed, content_type, metadata, object_id)
                    VALUES ($1, (SELECT bucket_id FROM buckets WHERE bucket_name = $2 LIMIT 1), $3, NOW(), TRUE, 'application/octet-stream', '{}', $4)
                    ON CONFLICT (upload_id) DO NOTHING
                    """,
                    upload_id,
                    bucket_name,
                    object_key,
                    object_id,
                )
            await upsert_part_placeholder(
                self.db,
                object_id=object_id,
                upload_id=str(upload_id),
                part_number=int(next_part),
                size_bytes=int(delta_size),
                etag=delta_md5,
                chunk_size_bytes=self.config.object_chunk_size_bytes,
                object_version=int(cov),
            )

            # Recompute composite ETag
            parts = await self.db.fetch(
                "SELECT part_number, etag FROM parts WHERE object_id = $1 AND object_version = $2 ORDER BY part_number",
                object_id,
                cov,
            )
            # Prefer DB md5_hash for current version; fallback to cache base part
            base_md5_row = await self.db.fetchrow(
                "SELECT md5_hash FROM object_versions WHERE object_id = $1 AND object_version = $2",
                object_id,
                cov,
            )
            base_md5 = (base_md5_row and (base_md5_row["md5_hash"] or "").strip()) or ""
            if len(base_md5) != 32:
                try:
                    base_bytes = await self.obj_cache.get(object_id, int(cov), 1)
                    # If no base bytes, use MD5 of empty content for correctness
                    base_md5 = hashlib.md5(base_bytes or b"").hexdigest()
                except Exception:
                    # As a last resort, treat as empty content
                    base_md5 = hashlib.md5(b"").hexdigest()
            md5s = [bytes.fromhex(base_md5)]
            for p in parts:
                e = str(p["etag"]).strip('"').split("-")[0]  # type: ignore
                if len(e) == 32:
                    md5s.append(bytes.fromhex(e))
            composite_etag = hashlib.md5(b"".join(md5s)).hexdigest() + f"-{len(md5s)}"

            # 3) CAS update on append_version to ensure no other writer slipped in
            updated = await self.db.fetchrow(
                """
                UPDATE object_versions
                   SET size_bytes     = size_bytes + $3,
                       md5_hash       = $4,
                       multipart      = TRUE,
                       append_version = append_version + 1,
                       last_modified  = NOW(),
                       last_append_at = NOW()
                 WHERE object_id = $1 AND object_version = $2 AND append_version = $5
                RETURNING append_version
                """,
                object_id,
                cov,
                int(delta_size),
                composite_etag,
                current_version,
            )
            if not updated:
                fresh = await self.db.fetchval(
                    "SELECT append_version FROM object_versions WHERE object_id = $1 AND object_version = $2",
                    object_id,
                    cov,
                )
                raise AppendPreconditionFailed(int(fresh or 0))
            new_append_version_val = int(updated["append_version"])  # type: ignore

        # Cache write-through
        chunk_size = self.config.object_chunk_size_bytes
        key_bytes = await get_or_create_encryption_key_bytes(
            main_account_id=account_address,
            bucket_name=bucket_name,
        )
        ct_chunks = CryptoService.encrypt_part_to_chunks(
            incoming_bytes,
            object_id=object_id,
            part_number=int(next_part),
            seed_phrase=seed_phrase,
            chunk_size=chunk_size,
            key=key_bytes,
        )
        ttl = self.config.cache_ttl_seconds
        writer = WriteThroughPartsWriter(self.fs_store, self.obj_cache, ttl_seconds=ttl)
        await writer.write_chunks(object_id, int(cov), int(next_part), ct_chunks)
        await writer.write_meta(
            object_id,
            int(cov),
            int(next_part),
            chunk_size=chunk_size,
            num_chunks=len(ct_chunks),
            plain_size=int(delta_size),
        )

        return {
            "object_id": object_id,
            "object_version": int(cov),
            "upload_id": str(upload_id),
            "new_append_version": int(new_append_version_val) if new_append_version_val is not None else 0,
            "etag": composite_etag,
            "part_number": int(next_part),
        }
