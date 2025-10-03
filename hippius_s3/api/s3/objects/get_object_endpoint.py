from __future__ import annotations

import contextlib
import json
import logging
from datetime import datetime
from datetime import timezone
from typing import Any
from typing import cast

from fastapi import Request
from fastapi import Response

from hippius_s3.api.s3 import errors
from hippius_s3.api.s3.common import parse_range
from hippius_s3.api.s3.common import parse_read_mode
from hippius_s3.api.s3.range_utils import parse_range_header
from hippius_s3.config import get_config
from hippius_s3.services.manifest_service import ManifestService
from hippius_s3.services.object_reader import DownloadNotReadyError
from hippius_s3.services.object_reader import ObjectInfo as ORObjectInfo
from hippius_s3.services.object_reader import ObjectReader
from hippius_s3.services.object_reader import Range as ORRange
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)
config = get_config()


async def handle_get_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: Any,
    ipfs_service: Any,
    redis_client: Any,
    object_reader: ObjectReader | None = None,
) -> Response:
    """Isolated GET object endpoint handler (extracted from endpoints.py)."""
    # If tagging is in query params, handle object tags request
    if "tagging" in request.query_params:
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
        from hippius_s3.api.s3.multipart import list_parts_internal  # local import to avoid cycles

        return await list_parts_internal(bucket_name, object_key, request, db)

    # Parse read mode and range
    hdr_mode = parse_read_mode(request)
    rng_obj, range_header = parse_range(request, total_size=10**12)  # provisional; validated below with real size
    logger.info(f"GET start {bucket_name}/{object_key} read_mode={hdr_mode or 'auto'} range={bool(range_header)}")

    try:
        # For anonymous access, skip user creation and pass NULL account for permission check
        is_anonymous = getattr(request.state, "access_mode", None) == "anon"
        account = getattr(request.state, "account", None)
        account_id = None if is_anonymous else (account.main_account if account else None)

        if not is_anonymous and account:
            # Get user for user-scoped bucket lookup (creates user if not exists)
            await db.fetchrow(
                get_query("get_or_create_user_by_main_account"),
                account.main_account,
                datetime.now(timezone.utc),
            )

        # Get object info for download with permission checks
        object_info = await db.fetchrow(
            get_query("get_object_for_download_with_permissions"),
            bucket_name,
            object_key,
            account_id,
        )

        if not object_info:
            return errors.s3_error_response(
                code="NoSuchKey",
                message=f"The specified key {object_key} does not exist or you don't have permission to access it",
                status_code=404,
                Key=object_key,
            )

        # Build manifest purely from DB parts, 0-based; prefer ObjectReader if provided
        download_chunks = json.loads(object_info["download_chunks"]) if object_info.get("download_chunks") else []
        if object_reader is not None:
            try:
                md = object_info.get("metadata") or {}
                if isinstance(md, str):
                    md = json.loads(md)
                info = ORObjectInfo(
                    object_id=str(object_info["object_id"]),
                    bucket_name=object_info["bucket_name"],
                    object_key=object_info["object_key"],
                    size_bytes=int(object_info["size_bytes"]),
                    content_type=object_info["content_type"],
                    md5_hash=object_info["md5_hash"],
                    created_at=object_info["created_at"],
                    metadata=md,
                    multipart=bool(object_info["multipart"]),
                    should_decrypt=bool(object_info["should_decrypt"]),
                    simple_cid=object_info.get("simple_cid"),
                    upload_id=object_info.get("upload_id"),
                )
                parts = await object_reader.build_manifest(db, info)
                download_chunks = [
                    {"part_number": p.part_number, "cid": p.cid, "size_bytes": p.size_bytes} for p in parts
                ]
            except Exception:
                logger.debug("Failed to build manifest via ObjectReader", exc_info=True)
        else:
            try:
                built_chunks = await ManifestService.build_initial_download_chunks(
                    db, object_info if isinstance(object_info, dict) else dict(object_info)
                )
                if built_chunks:
                    download_chunks = built_chunks
            except Exception:
                logger.debug("Failed to build manifest from ManifestService", exc_info=True)

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
            try:
                effective_size = int(object_info.get("size_bytes") or 0)
                if (not effective_size) and download_chunks:
                    try:
                        effective_size = sum(int(c.get("size_bytes", 0)) for c in download_chunks)
                    except Exception:
                        effective_size = 0
                start_byte, end_byte = parse_range_header(range_header, effective_size)
                # Check if this was originally an invalid range (start > end) that got converted to full range
                original_parts = range_header.lower().strip()
                if original_parts.startswith("bytes="):
                    spec = original_parts[len("bytes=") :]
                    range_parts = spec.split("-", 1)
                    if len(range_parts) == 2 and range_parts[0].isdigit() and range_parts[1].isdigit():
                        orig_start = int(range_parts[0])
                        orig_end = int(range_parts[1])
                        if orig_end < orig_start:
                            # This was originally an invalid range (start > end) converted to full range
                            range_was_invalid = True
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

        # Delegate entire read to ObjectReader (cache or pipeline)
        info = ORObjectInfo(
            object_id=str(object_info["object_id"]),
            bucket_name=object_info["bucket_name"],
            object_key=object_info["object_key"],
            size_bytes=int(object_info["size_bytes"]),
            content_type=object_info["content_type"],
            md5_hash=object_info["md5_hash"],
            created_at=object_info["created_at"],
            metadata=object_info.get("metadata") or {},
            multipart=bool(object_info["multipart"]),
            should_decrypt=bool(object_info["should_decrypt"]),
            simple_cid=object_info.get("simple_cid"),
            upload_id=object_info.get("upload_id"),
        )
        if object_reader is None:
            object_reader = ObjectReader(config)

        return cast(
            Response,
            await object_reader.read_response(
                db,
                request.app.state.redis_client,
                request.app.state.obj_cache,
                info,
                read_mode=hdr_mode,
                rng=None
                if not range_header
                else ORRange(start=start_byte, end=end_byte)
                if start_byte is not None and end_byte is not None
                else None,
                address=account.main_account if account else "",
                seed_phrase=getattr(request.state, "seed_phrase", ""),  # Empty for anonymous
                range_was_invalid=range_was_invalid,
            ),
        )

    except errors.S3Error as e:
        logger.exception(f"S3 Error getting object {bucket_name}/{object_key}: {e.message}")
        return errors.s3_error_response(
            code=e.code,
            message=e.message,
            status_code=e.status_code,
        )

    except DownloadNotReadyError as e:
        # Map download readiness issues to 503 SlowDown response
        error_msg = str(e)
        if "Parts not ready" in error_msg:
            logger.warning(f"GET {bucket_name}/{object_key}: parts not ready for download: {error_msg}")
        else:
            logger.warning(f"GET {bucket_name}/{object_key}: download not ready: {error_msg}")
        return errors.s3_error_response(
            code="SlowDown",
            message="Object not ready for download yet. Please retry.",
            status_code=503,
            extra_headers={"Retry-After": "1"},
        )

    except Exception as e:
        logger.exception(f"Error getting object {bucket_name}/{object_key}: {e}")
        return errors.s3_error_response(
            code="InternalError",
            message=f"We encountered an internal error: {str(e)}. Please try again.",
            status_code=500,
        )
