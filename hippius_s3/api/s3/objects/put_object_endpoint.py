from __future__ import annotations

import logging
import uuid
from datetime import datetime
from datetime import timezone
from typing import Any

import asyncpg
from fastapi import Request
from fastapi import Response
from opentelemetry import trace
from redis.exceptions import RedisError

from hippius_s3 import utils
from hippius_s3.api.middlewares.tracing import set_span_attributes
from hippius_s3.api.s3 import errors
from hippius_s3.api.s3.extensions.append import handle_append
from hippius_s3.config import get_config
from hippius_s3.db_pool import acquire_with_timeout
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.utils import get_query
from hippius_s3.writer.db import set_object_version_address
from hippius_s3.writer.object_writer import ObjectWriter


logger = logging.getLogger(__name__)
config = get_config()
tracer = trace.get_tracer(__name__)

USER_SEEN_CACHE_TTL_SECONDS = 86400


async def _user_needs_upsert(redis_client: Any, main_account_id: str) -> bool:
    """Whether this main account needs the get_or_create_user DB upsert.

    Uses Redis SET NX so only the first PUT per account within the TTL window touches Postgres
    (SET NX returns truthy only when the key was absent). Best-effort: a Redis hiccup fails open
    so we never skip a genuinely-needed upsert.
    """
    try:
        was_set = await redis_client.set(f"user_seen:{main_account_id}", "1", nx=True, ex=USER_SEEN_CACHE_TTL_SECONDS)
    except RedisError as exc:
        logger.warning(f"user-seen cache: redis SET failed, running upsert: {exc}")
        return True
    return bool(was_set)


async def handle_put_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    pool: asyncpg.Pool,
    redis_client: Any,
) -> Response:
    try:
        main_account_id = request.state.account.main_account

        # Detect S4 append semantics via metadata (header-only, no DB).
        meta_append = request.headers.get("x-amz-meta-append", "").lower() == "true"

        # The gateway's ACL middleware already resolved this bucket and forwards its id as
        # X-Hippius-Bucket-Id, so the common PUT path can skip a duplicate get_bucket_by_name.
        # Append still needs the full bucket row, so it always does the lookup.
        forwarded_bucket_id = getattr(request.state, "bucket_id", "") or ""
        needs_bucket_row = meta_append or not forwarded_bucket_id

        # Skip the per-PUT user upsert for accounts we've seen recently (Redis NX cache).
        user_needs_upsert = await _user_needs_upsert(redis_client, main_account_id)

        # Acquire a pooled connection only when there is DB work. For a known account on a
        # forwarded-bucket PUT, both reads are skipped and we never touch the pool here. We do NOT
        # hold the connection across the append branch below, so handle_append acquires its own.
        bucket = None
        if user_needs_upsert or needs_bucket_row:
            async with acquire_with_timeout(pool, config.db_pool_acquire_timeout) as conn:
                if user_needs_upsert:
                    with tracer.start_as_current_span(
                        "put_object.get_or_create_user", attributes={"hippius.account.main": main_account_id}
                    ):
                        await conn.fetchrow(
                            get_query("get_or_create_user_by_main_account"),
                            main_account_id,
                            datetime.now(timezone.utc),
                        )

                if needs_bucket_row:
                    with tracer.start_as_current_span("put_object.get_bucket", attributes={"bucket_name": bucket_name}):
                        bucket = await conn.fetchrow(
                            get_query("get_bucket_by_name"),
                            bucket_name,
                        )

        content_type = request.headers.get("Content-Type", "application/octet-stream")

        if needs_bucket_row:
            if not bucket:
                return errors.s3_error_response(
                    "NoSuchBucket",
                    f"The specified bucket {bucket_name} does not exist",
                    status_code=404,
                    BucketName=bucket_name,
                )
            bucket_id = bucket["bucket_id"]
            # Append is handled here, where `bucket` is known non-None (meta_append forces
            # needs_bucket_row, so the row was fetched above) and handle_append requires the full row.
            if meta_append:
                return await handle_append(
                    request,
                    pool,
                    redis_client,
                    bucket=bucket,
                    bucket_id=bucket_id,
                    bucket_name=bucket_name,
                    object_key=object_key,
                    body_iter=utils.iter_request_body(request),
                )
        else:
            bucket_id = forwarded_bucket_id

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
            async with acquire_with_timeout(pool, config.db_pool_acquire_timeout) as conn:
                prev = await conn.fetchrow(
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
            writer = ObjectWriter(pool=pool, redis_client=redis_client, fs_store=request.app.state.fs_store)

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

        # Drain-direct (s3-2.1 PR-11): the api does NOT enqueue the backend upload. It
        # persists the main-account address; the Rust drain reads it and LPUSHes the
        # UploadChainRequest itself once the part has replicated to ceph (so the uploader
        # only ever dequeues ceph-ready data). The drain is the sole upload producer.
        with tracer.start_as_current_span(
            "put_object.persist_upload_address",
            attributes={"upload_id": str(put_res.upload_id), "has_upload_id": True},
        ):
            await set_object_version_address(
                request.app.state.postgres_pool,
                object_id=str(put_res.object_id),
                object_version=int(put_res.object_version),
                address=request.state.account.main_account,
            )

        # Mark upload completed only AFTER the address is persisted. If that fails above, this is
        # skipped so the row stays is_completed=FALSE and remains eligible for the DELETE cascade
        # cleanup, rather than becoming a durably-"complete" object the drain can't enqueue.
        async with acquire_with_timeout(pool, config.db_pool_acquire_timeout) as conn:
            await conn.execute(
                "UPDATE multipart_uploads SET is_completed = TRUE WHERE upload_id = $1",
                str(put_res.upload_id),
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
        # DB connection-pool saturation (acquire timeout / too-many-connections) is retryable:
        # return 503 SlowDown, not a 500. This catch-all would otherwise mask it before the
        # global handler in main.py can map it.
        pool_busy = errors.pool_saturation_response(e)
        if pool_busy is not None:
            return pool_busy
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
