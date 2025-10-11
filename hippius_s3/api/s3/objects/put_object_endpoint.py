from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import datetime
from datetime import timezone
from typing import Any

from fastapi import Request
from fastapi import Response

from hippius_s3 import utils
from hippius_s3.api.s3 import errors
from hippius_s3.api.s3.extensions.append import handle_append
from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.config import get_config
from hippius_s3.metadata.meta_writer import write_cache_meta
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.queue import Chunk
from hippius_s3.queue import UploadChainRequest
from hippius_s3.queue import enqueue_upload_request
from hippius_s3.services.crypto_service import CryptoService
from hippius_s3.services.parts_service import upsert_part_placeholder
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)
config = get_config()


async def handle_put_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: Any,
    ipfs_service: Any,
    redis_client: Any,
) -> Response:
    try:
        # Get or create user and bucket for this main account
        main_account_id = request.state.account.main_account
        await db.fetchrow(
            get_query("get_or_create_user_by_main_account"),
            main_account_id,
            datetime.now(timezone.utc),
        )

        bucket = await db.fetchrow(
            get_query("get_bucket_by_name_and_owner"),
            bucket_name,
            main_account_id,
        )

        if not bucket:
            return errors.s3_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        bucket_id = bucket["bucket_id"]

        # Read request body properly handling chunked encoding
        incoming_bytes = await utils.get_request_body(request)
        content_type = request.headers.get("Content-Type", "application/octet-stream")

        # Detect S4 append semantics via metadata
        meta_append = request.headers.get("x-amz-meta-append", "").lower() == "true"
        if meta_append:
            return await handle_append(
                request,
                db,
                ipfs_service,
                redis_client,
                bucket=bucket,
                bucket_id=bucket_id,
                bucket_name=bucket_name,
                object_key=object_key,
                incoming_bytes=incoming_bytes,
            )

        # Regular non-append PutObject path
        file_data = incoming_bytes
        file_size = len(file_data)
        md5_hash = hashlib.md5(file_data).hexdigest()
        logger.info(f"PUT {bucket_name}/{object_key}: size={len(file_data)}, md5={md5_hash}")

        created_at = datetime.now(timezone.utc)

        metadata = {}
        for key, value in request.headers.items():
            if key.lower().startswith("x-amz-meta-"):
                meta_key = key[11:]
                # Do not persist append control metadata as user metadata
                if meta_key not in {"append", "append-id", "append-if-version"}:
                    metadata[meta_key] = value

        # Capture previous object (to clean up multipart parts if overwriting)
        prev = await db.fetchrow(
            get_query("get_object_by_path"),
            bucket_id,
            object_key,
        )

        # Generate object_id or reuse existing one
        if prev:
            object_id = str(prev["object_id"])  # ensure string for model validation and cache keys
            logger.debug(f"Reusing existing object_id {object_id} for overwrite")
        else:
            object_id = str(uuid.uuid4())

        should_encrypt = not bucket["is_public"]

        # Unified cache: write base part as part 1 (1-based indexing)
        obj_cache = RedisObjectPartsCache(redis_client)
        if not bucket["is_public"]:
            # Encrypt-before-Redis: store ciphertext meta first (readiness signal), then chunks
            chunk_size = int(getattr(config, "object_chunk_size_bytes", 4 * 1024 * 1024))

            from hippius_s3.services.key_service import get_or_create_encryption_key_bytes

            try:
                key_bytes = await get_or_create_encryption_key_bytes(
                    subaccount_id=request.state.account.main_account,
                    bucket_name=bucket_name,
                )
                ct_chunks = CryptoService.encrypt_part_to_chunks(
                    file_data,
                    object_id=object_id,
                    part_number=1,
                    seed_phrase=request.state.seed_phrase,
                    chunk_size=chunk_size,
                    key=key_bytes,
                )
            except Exception:
                from hippius_s3.api.s3.errors import s3_error_response

                return s3_error_response(
                    "InternalError",
                    "Failed to resolve encryption key for private bucket",
                    status_code=500,
                )
            total_ct = sum(len(ct) for ct in ct_chunks)
            # Write meta first using unified writer; store plaintext size for readers
            await write_cache_meta(
                obj_cache,
                object_id,
                1,
                chunk_size=chunk_size,
                num_chunks=len(ct_chunks),
                plain_size=len(file_data),
            )
            # Then write chunk data
            for i, ct in enumerate(ct_chunks):
                await obj_cache.set_chunk(object_id, 1, i, ct)
            logger.info(
                f"PUT cache encrypted write object_id={object_id} part=1 chunks={len(ct_chunks)} bytes={total_ct}"
            )
        else:
            await obj_cache.set(object_id, 1, file_data)
            logger.info(f"PUT cache unified write object_id={object_id} part=1 bytes={len(file_data)}")

        async with db.transaction():
            await db.fetchrow(
                get_query("upsert_object_basic"),
                object_id,
                bucket_id,
                object_key,
                content_type,
                json.dumps(metadata),
                md5_hash,
                file_size,
                created_at,
            )

            # If overwriting a previous object, remove its parts since we're replacing it
            try:
                if prev:
                    await db.execute("DELETE FROM parts WHERE object_id = $1", prev["object_id"])  # type: ignore[index]
            except Exception:
                logger.debug("Failed to cleanup previous parts on overwrite", exc_info=True)

        # Ensure upload row and parts(1) placeholder exist atomically before enqueueing
        async with db.transaction():
            # Generate a fresh upload_id per simple PUT to avoid conflicts with previous sessions
            new_upload_id = uuid.uuid4()
            upload_row = await db.fetchrow(
                get_query("create_multipart_upload"),
                new_upload_id,
                bucket_id,
                object_key,
                created_at,
                content_type,
                json.dumps(metadata),
                created_at,
                uuid.UUID(object_id),
            )
            upload_id = upload_row["upload_id"] if upload_row else new_upload_id

            # Insert parts(1) placeholder for simple objects (1-based indexing)
            await upsert_part_placeholder(
                db,
                object_id=object_id,
                upload_id=str(upload_id),
                part_number=1,
                size_bytes=int(file_size),
                etag=md5_hash,
                chunk_size_bytes=int(getattr(config, "object_chunk_size_bytes", 4 * 1024 * 1024)),
            )

        # Only enqueue after DB state (object, upload row, and part 1) is persisted to avoid race with pinner
        await enqueue_upload_request(
            payload=UploadChainRequest(
                substrate_url=config.substrate_url,
                ipfs_node=config.ipfs_store_url,
                address=request.state.account.main_account,
                subaccount=request.state.account.main_account,
                subaccount_seed_phrase=request.state.seed_phrase,
                bucket_name=bucket_name,
                object_key=object_key,
                should_encrypt=should_encrypt,
                object_id=object_id,
                upload_id=str(upload_id),
                chunks=[Chunk(id=1)],
            ),
            redis_client=redis_client,
        )

        get_metrics_collector().record_s3_operation(
            operation="put_object",
            bucket_name=bucket_name,
            main_account=main_account_id,
            success=True,
        )
        get_metrics_collector().record_data_transfer(
            operation="put_object",
            bytes_transferred=file_size,
            bucket_name=bucket_name,
            main_account=main_account_id,
        )

        # New or overwrite base object: expose append-version so clients can start append flow without HEAD
        return Response(
            status_code=200,
            headers={
                "ETag": f'"{md5_hash}"',
                "x-amz-meta-append-version": "0",
            },
        )

    except Exception as e:
        logger.exception(f"Error uploading object: {e}")
        get_metrics_collector().record_error(
            error_type="internal_error",
            operation="put_object",
            bucket_name=bucket_name,
            main_account=getattr(request.state, "account", None) and request.state.account.main_account,
        )
        return errors.s3_error_response(
            "InternalError",
            "We encountered an internal error. Please try again.",
            status_code=500,
        )
