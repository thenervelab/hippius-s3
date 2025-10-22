from __future__ import annotations

import hashlib
import logging
import uuid
from datetime import datetime
from datetime import timezone
from typing import Any
from typing import AsyncIterator

from fastapi import Request
from fastapi import Response

from hippius_s3 import utils
from hippius_s3.api.s3 import errors
from hippius_s3.api.s3.extensions.append import handle_append
from hippius_s3.config import get_config
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.utils import get_query
from hippius_s3.writer.object_writer import ObjectWriter
from hippius_s3.writer.queue import enqueue_upload as writer_enqueue_upload


logger = logging.getLogger(__name__)
config = get_config()


async def handle_put_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: Any,
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

        metadata: dict[str, Any] = {}
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

        # Use ObjectWriter streaming upsert/write (single-part)
        writer = ObjectWriter(db=db, redis_client=redis_client)

        async def _iter_once() -> AsyncIterator[bytes]:
            yield file_data

        put_res = await writer.put_simple_stream_full(
            bucket_id=bucket_id,
            bucket_name=bucket_name,
            object_id=object_id,
            object_key=object_key,
            account_address=request.state.account.main_account,
            content_type=content_type,
            metadata=metadata,
            storage_version=int(getattr(config, "target_storage_version", 3)),
            body_iter=_iter_once(),
        )

        # Fetch current version to enqueue background publish
        object_row = await db.fetchrow(
            get_query("get_object_by_path"),
            bucket_id,
            object_key,
        )
        # get_object_by_path exposes 'object_version' (the current version), not 'current_object_version'
        current_object_version = int(object_row["object_version"] or 1) if object_row else 1

        # Only enqueue after DB state is persisted; use writer queue helper
        await writer_enqueue_upload(
            redis_client=redis_client,
            address=request.state.account.main_account,
            subaccount_seed_phrase=request.state.seed_phrase,
            subaccount=request.state.account.main_account,
            bucket_name=bucket_name,
            object_key=object_key,
            object_id=object_id,
            object_version=int(current_object_version),
            upload_id=str(put_res.upload_id),
            chunk_ids=[1],
            substrate_url=config.substrate_url,
            ipfs_node=config.ipfs_store_url,
        )

        get_metrics_collector().record_s3_operation(
            operation="put_object",
            bucket_name=bucket_name,
            main_account=main_account_id,
            subaccount_id=request.state.account.id,
            success=True,
        )
        get_metrics_collector().record_data_transfer(
            operation="put_object",
            bytes_transferred=file_size,
            bucket_name=bucket_name,
            main_account=main_account_id,
            subaccount_id=request.state.account.id,
        )

        # New or overwrite base object: expose append-version so clients can start append flow without HEAD
        return Response(
            status_code=200,
            headers={
                "ETag": f'"{put_res.etag}"',
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
