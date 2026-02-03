from __future__ import annotations

import asyncio
import logging
import hashlib
import json
import uuid
from datetime import datetime
from datetime import timezone
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
from hippius_s3.storage_version import require_supported_storage_version
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
        upload_backends: list[str],
    ) -> int:
        row = await self.db.fetchrow(
            get_query("create_migration_version"),
            object_id,
            content_type,
            json.dumps(metadata),
            int(storage_version_target),
            upload_backends,
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
        # Resolve per-object key: v5 uses DEK, older versions use per-bucket key.
        storage_version_row = await self.db.fetchrow(
            "SELECT storage_version FROM object_versions WHERE object_id=$1 AND object_version=$2",
            object_id,
            int(object_version),
        )
        storage_version = int(storage_version_row["storage_version"]) if storage_version_row else 2
        suite_id: str | None = None
        if storage_version >= 5:
            suite_id = "hip-enc/aes256gcm"
            key_bytes = await self._ensure_and_get_v5_dek(
                bucket_id=bucket_id,
                object_id=object_id,
                object_version=int(object_version),
                chunk_size=int(chunk_size),
                suite_id=suite_id,
                rotate=True,
            )
        else:
            suite_id = "hip-enc/legacy"
            key_bytes = await get_or_create_encryption_key_bytes(
                main_account_id=account_address, bucket_name=bucket_name
            )
        ct_chunks = CryptoService.encrypt_part_to_chunks(
            body_bytes,
            object_id=object_id,
            part_number=1,
            seed_phrase=seed_phrase,
            chunk_size=chunk_size,
            key=key_bytes,
            suite_id=suite_id,
            bucket_id=str(bucket_id),
            upload_id="",
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
            chunk_cipher_sizes=[len(ct) for ct in ct_chunks],
        )

        return PutResult(
            object_id=str(object_id),
            etag=md5_hash,
            size_bytes=len(body_bytes),
            upload_id=str(upload_id),
            object_version=int(object_version),
        )

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

        resolved_storage_version = int(
            storage_version if storage_version is not None else self.config.target_storage_version
        )
        resolved_storage_version = require_supported_storage_version(resolved_storage_version)
        suite_id: str | None = None
        kek_id = None
        wrapped_dek = None

        part_number = 1

        # Reserve the version upfront by upserting with placeholder values
        # This ensures we write chunks to the correct version from the start
        with tracer.start_as_current_span(
            "put_simple_stream_full.reserve_version",
            attributes={
                "object_id": object_id,
                "has_object_id": True,
            },
        ):
            reserve_row = await upsert_object_basic(
                self.db,
                object_id=object_id,
                bucket_id=bucket_id,
                object_key=object_key,
                content_type=content_type,
                metadata=metadata,
                md5_hash="",
                size_bytes=0,
                storage_version=resolved_storage_version,
                upload_backends=self.config.upload_backends,
            )
            # IMPORTANT: `object_id` is authoritative from DB. Under concurrent creates,
            # our candidate UUID may conflict with an existing (bucket_id, object_key).
            # We must use the DB-returned object_id for cache keys, crypto binding, and enqueue.
            if reserve_row and reserve_row.get("object_id") is None:
                raise RuntimeError("reserve_version_missing_object_id")
            object_id = str(reserve_row.get("object_id") or object_id) if reserve_row else str(object_id)
            object_version = int(reserve_row.get("current_object_version") or 1) if reserve_row else 1

        if resolved_storage_version >= 5:
            suite_id = "hip-enc/aes256gcm"
            # New DEK per overwrite/write (this code path reserves a new object_version upfront)
            from hippius_s3.services.envelope_service import generate_dek
            from hippius_s3.services.envelope_service import wrap_dek
            from hippius_s3.services.kek_service import get_or_create_active_bucket_kek

            dek = generate_dek()
            kek_id, kek_bytes = await get_or_create_active_bucket_kek(bucket_id=bucket_id)
            aad = f"hippius-dek:{bucket_id}:{object_id}:{int(object_version)}".encode("utf-8")
            wrapped_dek = wrap_dek(kek=kek_bytes, dek=dek, aad=aad)
            key_bytes = dek
        else:
            suite_id = "hip-enc/legacy"
            with tracer.start_as_current_span(
                "put_simple_stream_full.key_retrieval",
                attributes={
                    "account_address": account_address,
                    "bucket_name": bucket_name,
                },
            ):
                key_bytes = await get_or_create_encryption_key_bytes(
                    main_account_id=account_address, bucket_name=bucket_name
                )

        hasher = hashlib.md5()
        total_size = 0
        writer = WriteThroughPartsWriter(self.fs_store, self.obj_cache, ttl_seconds=ttl)

        pt_buf = bytearray()
        next_chunk_index = 0
        chunk_cipher_sizes: list[int] = []

        async def _encrypt_and_write(buf: bytes) -> None:
            nonlocal total_size, next_chunk_index
            if not buf:
                return
            hasher.update(buf)
            total_size += len(buf)
            # IMPORTANT: For AEAD suites that bind chunk_index (e.g. AES-GCM with deterministic nonces),
            # we must encrypt with the *global* chunk index. We therefore encrypt one chunk at a time.
            adapter = CryptoService.get_adapter(suite_id)
            ct = adapter.encrypt_chunk(
                buf,
                key=key_bytes,
                bucket_id=str(bucket_id),
                object_id=object_id,
                part_number=int(part_number),
                chunk_index=int(next_chunk_index),
                upload_id="",
            )
            chunk_cipher_sizes.append(len(ct))
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

        # Update the reserved object_versions row with final md5/size
        with tracer.start_as_current_span(
            "put_simple_stream_full.update_metadata",
            attributes={
                "object_id": object_id,
                "has_object_id": True,
                "size_bytes": int(total_size),
                "md5_hash": md5_hash,
                "object_version": object_version,
                "storage_version": resolved_storage_version,
            },
        ):
            await self.db.execute(
                get_query("update_object_version_metadata"),
                int(total_size),
                md5_hash,
                content_type,
                json.dumps(metadata),
                datetime.now(timezone.utc),
                object_id,
                int(object_version),
            )

            if resolved_storage_version >= 5 and kek_id and wrapped_dek:
                aad = f"hippius-dek:{bucket_id}:{object_id}:{object_version}".encode("utf-8")
                # Re-wrap with correct object_version AAD (in case object_version differs)
                from hippius_s3.services.envelope_service import wrap_dek
                from hippius_s3.services.kek_service import get_bucket_kek_bytes

                kek_bytes = await get_bucket_kek_bytes(bucket_id=bucket_id, kek_id=kek_id)
                wrapped_dek = wrap_dek(kek=kek_bytes, dek=key_bytes, aad=aad)
                await self.db.execute(
                    """
                    UPDATE object_versions
                       SET encryption_version = 5,
                           enc_suite_id = $1,
                           enc_chunk_size_bytes = $2,
                           kek_id = $3,
                           wrapped_dek = $4
                     WHERE object_id = $5 AND object_version = $6
                    """,
                    str(suite_id),
                    int(chunk_size),
                    kek_id,
                    wrapped_dek,
                    object_id,
                    int(object_version),
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
        await upsert_part_placeholder(
            self.db,
            object_id=object_id,
            upload_id=str(upload_id),
            part_number=int(part_number),
            size_bytes=int(total_size),
            etag=md5_hash,
            chunk_size_bytes=int(chunk_size),
            object_version=int(object_version),
            chunk_cipher_sizes=chunk_cipher_sizes,
        )

        return PutResult(
            object_id=str(object_id),
            etag=md5_hash,
            size_bytes=int(total_size),
            upload_id=str(upload_id),
            object_version=int(object_version),
        )

    async def _ensure_and_get_v5_dek(
        self,
        *,
        bucket_id: str,
        object_id: str,
        object_version: int,
        chunk_size: int,
        suite_id: str,
        rotate: bool,
    ) -> bytes:
        """Ensure v5 envelope metadata exists for a given object version and return the DEK.

        rotate=True forces generating a new DEK (used for overwrite PutObject where object_version doesn't bump).
        """
        from hippius_s3.services.envelope_service import generate_dek
        from hippius_s3.services.envelope_service import unwrap_dek
        from hippius_s3.services.envelope_service import wrap_dek
        from hippius_s3.services.kek_service import get_bucket_kek_bytes
        from hippius_s3.services.kek_service import get_or_create_active_bucket_kek

        async with self.db.transaction():
            row = await self.db.fetchrow(
                """
                SELECT storage_version, kek_id, wrapped_dek
                  FROM object_versions
                 WHERE object_id = $1 AND object_version = $2
                 FOR UPDATE
                """,
                object_id,
                int(object_version),
            )
            if not row:
                raise RuntimeError("object_version_missing")
            storage_version = int(row.get("storage_version") or 0)
            if storage_version < 5:
                raise RuntimeError("not_v5_object_version")

            kek_id = row.get("kek_id")
            wrapped = row.get("wrapped_dek")
            if (not rotate) and kek_id and wrapped:
                kek_bytes = await get_bucket_kek_bytes(bucket_id=bucket_id, kek_id=kek_id)
                aad = f"hippius-dek:{bucket_id}:{object_id}:{int(object_version)}".encode("utf-8")
                return unwrap_dek(kek=kek_bytes, wrapped_dek=bytes(wrapped), aad=aad)

            dek = generate_dek()
            kek_id_new, kek_bytes = await get_or_create_active_bucket_kek(bucket_id=bucket_id)
            aad = f"hippius-dek:{bucket_id}:{object_id}:{int(object_version)}".encode("utf-8")
            wrapped_new = wrap_dek(kek=kek_bytes, dek=dek, aad=aad)
            await self.db.execute(
                """
                UPDATE object_versions
                   SET encryption_version = 5,
                       enc_suite_id = $1,
                       enc_chunk_size_bytes = $2,
                       kek_id = $3,
                       wrapped_dek = $4
                 WHERE object_id = $5 AND object_version = $6
                """,
                str(suite_id),
                int(chunk_size),
                kek_id_new,
                wrapped_new,
                object_id,
                int(object_version),
            )
            return dek

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
        # Resolve bucket_id and storage_version for this version
        meta = await self.db.fetchrow(
            """
            SELECT o.bucket_id, ov.storage_version
              FROM objects o
              JOIN object_versions ov ON ov.object_id = o.object_id AND ov.object_version = $2
             WHERE o.object_id = $1
             LIMIT 1
            """,
            object_id,
            int(object_version),
        )
        bucket_id = str(meta["bucket_id"]) if meta and meta.get("bucket_id") else ""
        storage_version = int(meta["storage_version"]) if meta and meta.get("storage_version") is not None else 2
        suite_id: str | None = "hip-enc/aes256gcm" if storage_version >= 5 else "hip-enc/legacy"
        if storage_version >= 5:
            key_bytes = await self._ensure_and_get_v5_dek(
                bucket_id=bucket_id,
                object_id=str(object_id),
                object_version=int(object_version),
                chunk_size=int(chunk_size),
                suite_id=str(suite_id),
                rotate=False,
            )
        else:
            key_bytes = await get_or_create_encryption_key_bytes(
                main_account_id=account_address, bucket_name=bucket_name
            )
        ct_chunks = CryptoService.encrypt_part_to_chunks(
            body_bytes,
            object_id=str(object_id),
            part_number=int(part_number),
            seed_phrase=seed_phrase,
            chunk_size=chunk_size,
            key=key_bytes,
            suite_id=suite_id,
            bucket_id=bucket_id,
            upload_id=str(upload_id),
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
            chunk_cipher_sizes=[len(ct) for ct in ct_chunks],
        )

        return PartResult(etag=md5_hash, size_bytes=file_size, part_number=int(part_number))

    async def mpu_upload_part_stream(
        self,
        *,
        upload_id: str,
        object_id: str,
        object_version: int,
        bucket_name: str,
        account_address: str,
        seed_phrase: str,
        part_number: int,
        body_iter: AsyncIterator[bytes],
        max_size_bytes: int | None = None,
    ) -> PartResult:
        chunk_size = self.config.object_chunk_size_bytes
        ttl = self.config.cache_ttl_seconds
        max_size = int(self.config.max_multipart_part_size)
        if max_size_bytes is not None:
            max_size = int(max_size_bytes)
        if max_size <= 0:
            max_size = 0

        # Resolve bucket_id and storage_version for this version
        meta = await self.db.fetchrow(
            """
            SELECT o.bucket_id, ov.storage_version
              FROM objects o
              JOIN object_versions ov ON ov.object_id = o.object_id AND ov.object_version = $2
             WHERE o.object_id = $1
             LIMIT 1
            """,
            object_id,
            int(object_version),
        )
        bucket_id = str(meta["bucket_id"]) if meta and meta.get("bucket_id") else ""
        storage_version = int(meta["storage_version"]) if meta and meta.get("storage_version") is not None else 2
        suite_id: str | None = "hip-enc/aes256gcm" if storage_version >= 5 else "hip-enc/legacy"
        if storage_version >= 5:
            key_bytes = await self._ensure_and_get_v5_dek(
                bucket_id=bucket_id,
                object_id=str(object_id),
                object_version=int(object_version),
                chunk_size=int(chunk_size),
                suite_id=str(suite_id),
                rotate=False,
            )
        else:
            key_bytes = await get_or_create_encryption_key_bytes(
                main_account_id=account_address, bucket_name=bucket_name
            )

        writer = WriteThroughPartsWriter(self.fs_store, self.obj_cache, ttl_seconds=ttl)
        adapter = CryptoService.get_adapter(suite_id)

        hasher = hashlib.md5()
        total_size = 0
        next_chunk_index = 0
        chunk_cipher_sizes: list[int] = []
        pt_buf = bytearray()
        written_chunk_indices: list[int] = []
        meta_written = False

        async def _cleanup_partial() -> None:
            try:
                await self.fs_store.delete_part(str(object_id), int(object_version), int(part_number))
            except Exception:
                logger = logging.getLogger(__name__)
                logger.warning(
                    "Failed to cleanup partial part on FS: object_id=%s v=%s part=%s",
                    object_id,
                    int(object_version),
                    int(part_number),
                )
            if written_chunk_indices:
                try:
                    keys = [
                        self.obj_cache.build_chunk_key(
                            str(object_id), int(object_version), int(part_number), int(idx)
                        )
                        for idx in written_chunk_indices
                    ]
                    keys.append(self.obj_cache.build_meta_key(str(object_id), int(object_version), int(part_number)))
                    await self.redis_client.delete(*keys)
                except Exception:
                    logger = logging.getLogger(__name__)
                    logger.warning(
                        "Failed to cleanup partial part in Redis: object_id=%s v=%s part=%s",
                        object_id,
                        int(object_version),
                        int(part_number),
                    )

        async def _encrypt_and_write(buf: bytes) -> None:
            nonlocal total_size, next_chunk_index
            if not buf:
                return
            hasher.update(buf)
            total_size += len(buf)
            ct = adapter.encrypt_chunk(
                buf,
                key=key_bytes,
                bucket_id=bucket_id,
                object_id=str(object_id),
                part_number=int(part_number),
                chunk_index=int(next_chunk_index),
                upload_id=str(upload_id),
            )
            chunk_cipher_sizes.append(len(ct))
            await self.fs_store.set_chunk(
                str(object_id),
                int(object_version),
                int(part_number),
                int(next_chunk_index),
                ct,
            )
            try:
                await self.obj_cache.set_chunk(
                    str(object_id),
                    int(object_version),
                    int(part_number),
                    int(next_chunk_index),
                    ct,
                    ttl=ttl,
                )
            except Exception:
                logger = logging.getLogger(__name__)
                logger.warning(
                    "Redis chunk write failed (best-effort) in mpu stream: object_id=%s v=%s part=%s chunk=%s",
                    object_id,
                    int(object_version),
                    int(part_number),
                    int(next_chunk_index),
                )
            written_chunk_indices.append(int(next_chunk_index))
            next_chunk_index += 1

        try:
            async for piece in body_iter:
                if not piece:
                    continue
                pt_buf.extend(piece)
                if max_size and total_size + len(pt_buf) > int(max_size):
                    raise ValueError("part_size_exceeds_max")
                while len(pt_buf) >= chunk_size:
                    to_write = bytes(pt_buf[:chunk_size])
                    del pt_buf[:chunk_size]
                    await _encrypt_and_write(to_write)

            if pt_buf:
                await _encrypt_and_write(bytes(pt_buf))
                pt_buf.clear()

            if total_size == 0:
                raise ValueError("Zero-length part not allowed")

            await writer.write_meta(
                str(object_id),
                int(object_version),
                int(part_number),
                chunk_size=chunk_size,
                num_chunks=int(next_chunk_index),
                plain_size=int(total_size),
            )
            meta_written = True
        except Exception:
            if not meta_written:
                await _cleanup_partial()
            raise

        md5_hash = hasher.hexdigest()

        await upsert_part_placeholder(
            self.db,
            object_id=str(object_id),
            upload_id=str(upload_id),
            part_number=int(part_number),
            size_bytes=int(total_size),
            etag=str(md5_hash),
            chunk_size_bytes=int(chunk_size),
            object_version=int(object_version),
            chunk_cipher_sizes=chunk_cipher_sizes,
        )

        return PartResult(etag=md5_hash, size_bytes=int(total_size), part_number=int(part_number))

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

    async def append_stream(
        self,
        *,
        bucket_id: str,
        bucket_name: str,
        object_key: str,
        expected_version: int,
        account_address: str,
        seed_phrase: str,
        body_iter: AsyncIterator[bytes],
    ) -> dict:
        """Append bytes with CAS, cache write-through, and enqueue (streaming)."""
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
        chunk_size = self.config.object_chunk_size_bytes

        next_part = None
        upload_id = None

        async with self.db.transaction():
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

            await self.db.execute(
                """
                INSERT INTO parts (part_id, upload_id, part_number, ipfs_cid, size_bytes, etag, uploaded_at, object_id, object_version, chunk_size_bytes)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (object_id, object_version, part_number) DO NOTHING
                """,
                str(uuid.uuid4()),
                str(upload_id),
                int(next_part),
                None,
                0,
                "pending",
                datetime.now(timezone.utc),
                object_id,
                int(cov),
                int(chunk_size),
            )

        if next_part is None or upload_id is None:
            raise RuntimeError("append_reservation_failed")

        async def _delete_part_row() -> None:
            await self.db.execute(
                "DELETE FROM parts WHERE object_id = $1 AND object_version = $2 AND part_number = $3",
                object_id,
                int(cov),
                int(next_part),
            )

        async def _cleanup_part(num_chunks: int | None) -> None:
            try:
                await self.fs_store.delete_part(str(object_id), int(cov), int(next_part))
            except Exception:
                logging.getLogger(__name__).warning(
                    "Failed to cleanup append part on FS: object_id=%s v=%s part=%s",
                    object_id,
                    int(cov),
                    int(next_part),
                )
            try:
                meta_key = self.obj_cache.build_meta_key(str(object_id), int(cov), int(next_part))
                keys = [meta_key]
                if num_chunks is not None:
                    keys.extend(
                        [
                            self.obj_cache.build_chunk_key(str(object_id), int(cov), int(next_part), int(i))
                            for i in range(int(num_chunks))
                        ]
                    )
                    await self.redis_client.delete(*keys)
                else:
                    await self.redis_client.delete(*keys)
                    pattern = f"obj:{object_id}:v:{int(cov)}:part:{int(next_part)}:chunk:*"
                    cursor = 0
                    while True:
                        cursor, found = await self.redis_client.scan(cursor, match=pattern, count=100)
                        if found:
                            await self.redis_client.delete(*found)
                        if cursor == 0:
                            break
            except Exception:
                logging.getLogger(__name__).warning(
                    "Failed to cleanup append part in Redis: object_id=%s v=%s part=%s",
                    object_id,
                    int(cov),
                    int(next_part),
                )

        try:
            part_res = await self.mpu_upload_part_stream(
                upload_id=str(upload_id),
                object_id=str(object_id),
                object_version=int(cov),
                bucket_name=str(bucket_name),
                account_address=account_address,
                seed_phrase=seed_phrase,
                part_number=int(next_part),
                body_iter=body_iter,
                max_size_bytes=0,
            )
        except ValueError as exc:
            await _delete_part_row()
            if "Zero-length part" in str(exc):
                raise EmptyAppendError("Empty append not allowed")
            raise
        except Exception:
            await _delete_part_row()
            raise

        delta_size = int(part_res.size_bytes)
        meta = await self.fs_store.get_meta(str(object_id), int(cov), int(next_part))
        num_chunks = int(meta.get("num_chunks", 0)) if meta else None

        try:
            async with self.db.transaction():
                locked = await self.db.fetchrow(
                    """
                    SELECT append_version,
                           md5_hash,
                           (append_etag_md5s IS NOT NULL AND octet_length(append_etag_md5s) > 0) AS has_etag_md5s
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

                seed_md5s = b""
                if not bool(locked.get("has_etag_md5s")):
                    parts = await self.db.fetch(
                        "SELECT part_number, etag FROM parts WHERE object_id = $1 AND object_version = $2 ORDER BY part_number",
                        object_id,
                        cov,
                    )
                    base_md5 = str((locked.get("md5_hash") or "")).strip()
                    if len(base_md5) != 32:
                        try:
                            base_bytes = await self.obj_cache.get(object_id, int(cov), 1)
                            base_md5 = hashlib.md5(base_bytes or b"").hexdigest()
                        except Exception:
                            base_md5 = hashlib.md5(b"").hexdigest()
                    md5s = [bytes.fromhex(base_md5)]
                    for p in parts:
                        if int(p.get("part_number") or 0) == int(next_part):
                            continue
                        e = str(p["etag"]).strip('"').split("-")[0]  # type: ignore
                        if len(e) == 32:
                            md5s.append(bytes.fromhex(e))
                    seed_md5s = b"".join(md5s)

                updated = await self.db.fetchrow(
                    """
                    UPDATE object_versions
                       SET size_bytes     = size_bytes + $3,
                           md5_hash       = md5(
                               (CASE
                                  WHEN append_etag_md5s IS NULL OR octet_length(append_etag_md5s) = 0
                                  THEN $5
                                  ELSE append_etag_md5s
                                END) || decode($6, 'hex')
                           ) || '-' || (
                               (octet_length(
                                  CASE
                                    WHEN append_etag_md5s IS NULL OR octet_length(append_etag_md5s) = 0
                                    THEN $5
                                    ELSE append_etag_md5s
                                  END
                               ) + 16) / 16
                           )::text,
                           append_etag_md5s = (
                               CASE
                                  WHEN append_etag_md5s IS NULL OR octet_length(append_etag_md5s) = 0
                                  THEN $5
                                  ELSE append_etag_md5s
                               END
                           ) || decode($6, 'hex'),
                           multipart      = TRUE,
                           append_version = append_version + 1,
                           last_modified  = NOW(),
                           last_append_at = NOW()
                     WHERE object_id = $1 AND object_version = $2 AND append_version = $4
                    RETURNING append_version, md5_hash
                    """,
                    object_id,
                    cov,
                    int(delta_size),
                    current_version,
                    seed_md5s,
                    str(part_res.etag),
                )
                if not updated:
                    fresh = await self.db.fetchval(
                        "SELECT append_version FROM object_versions WHERE object_id = $1 AND object_version = $2",
                        object_id,
                        cov,
                    )
                    raise AppendPreconditionFailed(int(fresh or 0))
                new_append_version_val = int(updated["append_version"])  # type: ignore
                composite_etag = str(updated.get("md5_hash") or "")
        except AppendPreconditionFailed:
            await _cleanup_part(num_chunks)
            await _delete_part_row()
            raise

        return {
            "object_id": object_id,
            "object_version": int(cov),
            "upload_id": str(upload_id),
            "new_append_version": int(new_append_version_val),
            "etag": composite_etag,
            "part_number": int(next_part),
            "size_bytes": int(delta_size),
        }

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
        async def _iter_once() -> AsyncIterator[bytes]:
            if incoming_bytes:
                yield incoming_bytes

        return await self.append_stream(
            bucket_id=bucket_id,
            bucket_name=bucket_name,
            object_key=object_key,
            expected_version=int(expected_version),
            account_address=account_address,
            seed_phrase=seed_phrase,
            body_iter=_iter_once(),
        )
