from __future__ import annotations

import logging
import uuid
from datetime import datetime
from datetime import timezone
from typing import Any

from fastapi import Request
from fastapi import Response
from opentelemetry import trace

from hippius_s3 import utils
from hippius_s3.api.middlewares.tracing import set_span_attributes
from hippius_s3.api.s3 import errors
from hippius_s3.api.s3.extensions.append import handle_append
from hippius_s3.config import get_config
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.utils import get_query
from hippius_s3.writer.object_writer import ObjectWriter
from hippius_s3.writer.queue import enqueue_upload as writer_enqueue_upload


logger = logging.getLogger(__name__)
config = get_config()
tracer = trace.get_tracer(__name__)


async def handle_put_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: Any,
) -> Response:
    try:
        # Get or create user and bucket for this main account
        main_account_id = request.state.account.main_account

        with tracer.start_as_current_span(
            "put_object.get_or_create_user", attributes={"hippius.account.main": main_account_id}
        ):
            await db.fetchrow(
                get_query("get_or_create_user_by_main_account"),
                main_account_id,
                datetime.now(timezone.utc),
            )

        with tracer.start_as_current_span("put_object.get_bucket", attributes={"bucket_name": bucket_name}):
            bucket = await db.fetchrow(
                get_query("get_bucket_by_name"),
                bucket_name,
            )

        if not bucket:
            return errors.s3_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        bucket_id = bucket["bucket_id"]

        content_type = request.headers.get("Content-Type", "application/octet-stream")

        # Detect S4 append semantics via metadata
        meta_append = request.headers.get("x-amz-meta-append", "").lower() == "true"
        if meta_append:
            return await handle_append(
                request,
                db,
                request.app.state.redis_queues_client,
                bucket=bucket,
                bucket_id=bucket_id,
                bucket_name=bucket_name,
                object_key=object_key,
                body_iter=utils.iter_request_body(request),
            )

        # Regular non-append PutObject path (streamed)
        content_length = request.headers.get("content-length")
        try:
            hinted_size = int(content_length) if content_length is not None else 0
        except Exception:
            hinted_size = 0

        metadata: dict[str, Any] = {}
        for key, value in request.headers.items():
            if key.lower().startswith("x-amz-meta-"):
                meta_key = key[11:]
                # Do not persist append control metadata as user metadata
                if meta_key not in {"append", "append-id", "append-if-version"}:
                    metadata[meta_key] = value

        # Capture previous object (to clean up multipart parts if overwriting)
        with tracer.start_as_current_span("put_object.check_existing_object") as span:
            prev = await db.fetchrow(
                get_query("get_object_by_path"),
                bucket_id,
                object_key,
            )
            set_span_attributes(span, {"is_overwrite": prev is not None})

        # Candidate object_id: DB may override on (bucket_id, object_key) conflict
        # TODO: Make object identity/version allocation fully DB-atomic by removing this
        #       pre-check and always passing a generated candidate UUID. The writer already
        #       treats the DB-returned object_id/object_version as authoritative.
        if prev:
            candidate_object_id = str(prev["object_id"])
            logger.debug(f"Reusing existing object_id {candidate_object_id} for overwrite")
        else:
            candidate_object_id = str(uuid.uuid4())

        # Use ObjectWriter streaming upsert/write (single-part)
        # Note: No transaction wrapper needed here. The upsert_object_basic query is atomic
        # (uses CTEs). Holding a transaction open during the entire upload would cause lock
        # contention under concurrent uploads. If chunk writing fails after the upsert, the
        # object remains with status='publishing' which prevents serving and gets cleaned up.
        with tracer.start_as_current_span(
            "put_object.stream_and_upsert",
            attributes={
                "object_id": candidate_object_id,
                "has_object_id": True,
                "file_size_bytes": hinted_size,
                "content_type": content_type,
                "storage_version": config.target_storage_version,
            },
        ) as span:
            writer = ObjectWriter(db=db, fs_store=request.app.state.fs_store)

            put_res = await writer.put_simple_stream_full(
                bucket_id=bucket_id,
                bucket_name=bucket_name,
                object_id=candidate_object_id,
                object_key=object_key,
                account_address=request.state.account.main_account,
                content_type=content_type,
                metadata=metadata,
                storage_version=config.target_storage_version,
                body_iter=utils.iter_request_body(request),
            )

            set_span_attributes(
                span,
                {
                    "upload_id": put_res.upload_id,
                    "has_upload_id": True,
                    "etag": put_res.etag,
                    "object_id_db": put_res.object_id,
                    "returned_size_bytes": put_res.size_bytes,
                },
            )

        logger.info(f"PUT {bucket_name}/{object_key}: size={put_res.size_bytes}, md5={put_res.etag}")

        # Only enqueue after DB state is persisted; use writer queue helper
        with tracer.start_as_current_span(
            "put_object.enqueue_upload",
            attributes={"upload_id": str(put_res.upload_id), "has_upload_id": True},
        ):
            await writer_enqueue_upload(
                address=request.state.account.main_account,
                bucket_name=bucket_name,
                object_key=object_key,
                object_id=str(put_res.object_id),
                object_version=int(put_res.object_version),
                upload_id=str(put_res.upload_id),
                chunk_ids=[1],
                ray_id=getattr(request.state, "ray_id", "no-ray-id"),
            )

        # Mark upload completed to prevent CASCADE deletion of chunk_backend on DELETE
        # Maintains parity with append (sets TRUE immediately) and multipart (sets TRUE in mpu_complete)
        await db.execute(
            "UPDATE multipart_uploads SET is_completed = TRUE WHERE upload_id = $1",
            put_res.upload_id,
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
            bytes_transferred=int(put_res.size_bytes),
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
