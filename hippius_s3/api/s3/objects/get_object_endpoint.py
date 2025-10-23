from __future__ import annotations

import contextlib
import json
import logging
from datetime import datetime
from datetime import timezone
from typing import Any

from fastapi import Request
from fastapi import Response
from opentelemetry import trace

from hippius_s3.api.middlewares.tracing import set_span_attributes
from hippius_s3.api.s3 import errors
from hippius_s3.api.s3.common import parse_range
from hippius_s3.api.s3.common import parse_read_mode
from hippius_s3.api.s3.range_utils import parse_range_header
from hippius_s3.config import get_config
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.services.manifest_service import ManifestService
from hippius_s3.services.object_reader import DownloadNotReadyError
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)
config = get_config()
tracer = trace.get_tracer(__name__)


async def handle_get_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: Any,
    redis_client: Any,
    object_reader: Any | None = None,
) -> Response:
    """Isolated GET object endpoint handler."""
    # If tagging is in query params, handle object tags request
    if "tagging" in request.query_params:
        with tracer.start_as_current_span("get_object.check_tagging_request"):
            from hippius_s3.api.s3.objects.tagging_endpoint import get_object_tags  # local import to avoid cycles

            account = getattr(request.state, "account", None)
            return await get_object_tags(
                bucket_name,
                object_key,
                db,
                getattr(request.state, "seed_phrase", ""),
                account.main_account if account else "",
            )

    # List parts for an ongoing multipart upload
    if "uploadId" in request.query_params:
        upload_id = request.query_params.get("uploadId", "")
        with tracer.start_as_current_span(
            "get_object.check_list_parts_request",
            attributes={"upload_id": upload_id, "has_upload_id": bool(upload_id)},
        ):
            from hippius_s3.api.s3.multipart import list_parts_internal

            return await list_parts_internal(bucket_name, object_key, request, db)

    # Parse read mode and range
    hdr_mode = parse_read_mode(request)
    rng_obj, range_header = parse_range(request, total_size=10**12)
    with tracer.start_as_current_span(
        "get_object.parse_request_headers",
        attributes={
            "read_mode": hdr_mode or "auto",
            "has_range_header": bool(range_header),
        },
    ):
        logger.info(f"GET start {bucket_name}/{object_key} read_mode={hdr_mode or 'auto'} range={bool(range_header)}")

    try:
        # For anonymous access, skip user creation and pass NULL account for permission check
        is_anonymous = getattr(request.state, "access_mode", None) == "anon"
        account = getattr(request.state, "account", None)
        account_id = None if is_anonymous else (account.main_account if account else None)

        if not is_anonymous and account:
            with tracer.start_as_current_span(
                "get_object.get_or_create_user",
                attributes={
                    "main_account_id": account.main_account,
                    "is_anonymous": False,
                },
            ):
                await db.fetchrow(
                    get_query("get_or_create_user_by_main_account"),
                    account.main_account,
                    datetime.now(timezone.utc),
                )

        # Get object info for download with permission checks
        with tracer.start_as_current_span(
            "get_object.get_object_with_permissions",
            attributes={"is_anonymous": is_anonymous},
        ) as span:
            object_info = await db.fetchrow(
                get_query("get_object_for_download_with_permissions"),
                bucket_name,
                object_key,
                account_id,
            )
            if object_info:
                set_span_attributes(
                    span,
                    {
                        "object_id": str(object_info["object_id"]),
                        "has_object_id": True,
                        "size_bytes": int(object_info.get("size_bytes") or 0),
                        "multipart": bool(object_info.get("multipart")),
                        "storage_version": int(object_info.get("storage_version") or 2),
                        "is_public": bool(object_info.get("is_public")),
                    },
                )

        if not object_info:
            return errors.s3_error_response(
                code="NoSuchKey",
                message=f"The specified key {object_key} does not exist or you don't have permission to access it",
                status_code=404,
                Key=object_key,
            )

        # Build manifest purely from DB parts, 0-based; prefer ObjectReader if provided
        with tracer.start_as_current_span("get_object.build_manifest") as span:
            download_chunks = json.loads(object_info["download_chunks"]) if object_info.get("download_chunks") else []
            manifest_source = "unknown"
            try:
                built_chunks = await ManifestService.build_initial_download_chunks(
                    db, object_info if isinstance(object_info, dict) else dict(object_info)
                )
                if built_chunks:
                    download_chunks = built_chunks
                    manifest_source = "db"
                else:
                    manifest_source = "cached"
            except Exception:
                logger.debug("Failed to build manifest from ManifestService", exc_info=True)
                manifest_source = "cached_fallback"

            set_span_attributes(
                span,
                {
                    "manifest_source": manifest_source,
                    "num_download_chunks": len(download_chunks),
                },
            )

            # Attach manifest to object_info for cache assembly
            try:
                if not isinstance(object_info, dict):
                    object_info = dict(object_info)
                object_info["download_chunks"] = json.dumps(download_chunks)
            except Exception:
                pass
        # Validate/resolve range with effective size (objects.size_bytes or sum of chunks)
        start_byte = end_byte = None
        range_was_invalid = False
        if range_header:
            with tracer.start_as_current_span("get_object.parse_range") as span:
                try:
                    effective_size = int(object_info.get("size_bytes") or 0)
                    if (not effective_size) and download_chunks:
                        try:
                            effective_size = sum(int(c.get("size_bytes", 0)) for c in download_chunks)
                        except Exception:
                            effective_size = 0
                    start_byte, end_byte = parse_range_header(range_header, effective_size)

                    original_parts = range_header.lower().strip()
                    if original_parts.startswith("bytes="):
                        spec = original_parts[len("bytes=") :]
                        range_parts = spec.split("-", 1)
                        if len(range_parts) == 2 and range_parts[0].isdigit() and range_parts[1].isdigit():
                            orig_start = int(range_parts[0])
                            orig_end = int(range_parts[1])
                            if orig_end < orig_start:
                                range_was_invalid = True

                    set_span_attributes(
                        span,
                        {
                            "start_byte": start_byte,
                            "end_byte": end_byte,
                            "effective_size": effective_size,
                            "range_size_bytes": (end_byte - start_byte + 1)
                            if start_byte is not None and end_byte is not None
                            else effective_size,
                            "range_was_invalid": range_was_invalid,
                        },
                    )
                except ValueError:
                    return Response(
                        status_code=416,
                        headers={
                            "Content-Range": f"bytes */{effective_size}",
                            "Accept-Ranges": "bytes",
                            "Content-Length": "0",
                        },
                    )

        with contextlib.suppress(Exception):
            logger.debug(
                f"GET manifest-built multipart={object_info.get('multipart')} parts={[c if isinstance(c, dict) else c for c in download_chunks]}"
            )

        # Use new reader (flat chunk plan; blocks between parts; no downloader meta dependency)
        from hippius_s3.reader.types import RangeRequest as V2Range
        from hippius_s3.services.object_reader import read_response

        v2_rng = None
        if range_header and start_byte is not None and end_byte is not None:
            v2_rng = V2Range(start=int(start_byte), end=int(end_byte))

        # Decide decryption internally based on storage_version (v3+: always decrypt)
        storage_version = int(object_info.get("storage_version") or 2)
        is_public_bucket = bool(object_info.get("is_public"))
        bucket_owner_id = str(object_info.get("bucket_owner_id") or "")

        # Resolve key address: for public buckets always use bucket owner id (works for anon and authenticated)
        # Otherwise, use the caller's main account when authenticated
        resolved_address = bucket_owner_id if is_public_bucket else (account.main_account if account else "")

        info_dict = {
            "object_id": str(object_info["object_id"]),
            "bucket_name": object_info["bucket_name"],
            "object_key": object_key,
            "size_bytes": int(object_info["size_bytes"]),
            "content_type": object_info["content_type"],
            "md5_hash": object_info["md5_hash"],
            "created_at": object_info["created_at"],
            "metadata": object_info.get("metadata") or {},
            "multipart": bool(object_info["multipart"]),
            # should_decrypt derived in reader from storage_version; keep for now to avoid breaking
            "should_decrypt": storage_version >= 3 or (not is_public_bucket),
            "storage_version": storage_version,
        }

        with tracer.start_as_current_span(
            "get_object.read_response",
            attributes={
                "resolved_address": resolved_address,
                "read_mode": hdr_mode or "auto",
                "has_range": v2_rng is not None,
            },
        ) as span:
            response = await read_response(
                db=db,
                redis=request.app.state.redis_client,
                obj_cache=request.app.state.obj_cache,
                info=info_dict,
                read_mode=hdr_mode or "auto",
                rng=v2_rng,
                address=resolved_address,
                range_was_invalid=range_was_invalid,
            )
            set_span_attributes(span, {"http.status_code": response.status_code})

        if response.status_code in (200, 206):
            bytes_transferred = int(object_info["size_bytes"])
            if range_header and start_byte is not None and end_byte is not None:
                bytes_transferred = end_byte - start_byte + 1

            get_metrics_collector().record_s3_operation(
                operation="get_object",
                bucket_name=bucket_name,
                main_account=account.main_account if account else None,
                subaccount_id=account.id if account else None,
                success=True,
            )
            get_metrics_collector().record_data_transfer(
                operation="get_object",
                bytes_transferred=bytes_transferred,
                bucket_name=bucket_name,
                main_account=account.main_account if account else None,
                subaccount_id=account.id if account else None,
            )

        return response

    except errors.S3Error as e:
        logger.exception(f"S3 Error getting object {bucket_name}/{object_key}: {e.message}")
        account = getattr(request.state, "account", None)
        get_metrics_collector().record_error(
            error_type=e.code,
            operation="get_object",
            bucket_name=bucket_name,
            main_account=account.main_account if account else None,
        )
        return errors.s3_error_response(
            code=e.code,
            message=e.message,
            status_code=e.status_code,
        )

    except DownloadNotReadyError as e:
        error_msg = str(e)
        if "Parts not ready" in error_msg:
            logger.warning(f"GET {bucket_name}/{object_key}: parts not ready for download: {error_msg}")
        else:
            logger.warning(f"GET {bucket_name}/{object_key}: download not ready: {error_msg}")
        account = getattr(request.state, "account", None)
        get_metrics_collector().record_error(
            error_type="download_not_ready",
            operation="get_object",
            bucket_name=bucket_name,
            main_account=account.main_account if account else None,
        )
        return errors.s3_error_response(
            code="SlowDown",
            message="Object not ready for download yet. Please retry.",
            status_code=503,
            extra_headers={"Retry-After": "1"},
        )

    except Exception as e:
        logger.exception(f"Error getting object {bucket_name}/{object_key}: {e}")
        account = getattr(request.state, "account", None)
        get_metrics_collector().record_error(
            error_type="internal_error",
            operation="get_object",
            bucket_name=bucket_name,
            main_account=account.main_account if account else None,
        )
        return errors.s3_error_response(
            code="InternalError",
            message=f"We encountered an internal error: {str(e)}. Please try again.",
            status_code=500,
        )
