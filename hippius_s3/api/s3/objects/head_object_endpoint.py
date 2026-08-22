from __future__ import annotations

import json
import logging
from typing import Any
from typing import Optional

import asyncpg
from fastapi import Request
from fastapi import Response
from opentelemetry import trace

from hippius_s3.api.middlewares.tracing import set_span_attributes
from hippius_s3.api.s3 import errors
from hippius_s3.api.s3.common import apply_response_overrides
from hippius_s3.api.s3.common import if_none_match_matches
from hippius_s3.api.s3.common import parse_response_overrides
from hippius_s3.repositories.objects import ObjectRepository
from hippius_s3.repositories.users import UserRepository
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

# Distinguishes "the light HEAD query returned a (possibly NULL) arion_file_hash" from "the column
# isn't in this row" (the versioned path's heavy query) — asyncpg Record.get returns this only when
# the key is absent.
_MISSING = object()


async def _get_object_with_permissions_min(
    bucket_name: str,
    object_key: str,
    db: Any,
    main_account_id: Optional[str],
    version: Optional[int] = None,
) -> Any:
    """Lightweight existence and metadata check (HEAD). Gateway handles permissions."""
    # Ensure user exists — read-first (HD-2) so the common case stays off the write path, and skip
    # anonymous (mirrors the GET guard; avoids creating a bogus "anonymous" user row).
    if main_account_id and main_account_id != "anonymous":
        await UserRepository(db).ensure_by_main_account_read_first(main_account_id)

    # Gateway already checked permissions, just fetch the object
    if version is not None:
        row = await ObjectRepository(db).get_for_download_with_permissions_by_version(
            bucket_name, object_key, version, main_account_id
        )
        if not row:
            bucket_exists = await db.fetchval(
                "SELECT 1 FROM buckets WHERE bucket_name = $1 AND deleted_at IS NULL",
                bucket_name,
            )
            if not bucket_exists:
                raise errors.S3Error(
                    code="NoSuchBucket",
                    status_code=404,
                    message=f"The specified bucket {bucket_name} does not exist",
                )
            object_exists = await db.fetchval(
                """
                SELECT 1 FROM objects o
                JOIN buckets b ON o.bucket_id = b.bucket_id
                WHERE b.bucket_name = $1 AND o.object_key = $2 AND b.deleted_at IS NULL
                """,
                bucket_name,
                object_key,
            )
            if object_exists:
                raise errors.S3Error(
                    code="NoSuchVersion",
                    status_code=404,
                    message=f"The specified version does not exist: {version}",
                )
            raise errors.S3Error(
                code="NoSuchKey",
                status_code=404,
                message=f"The specified key {object_key} does not exist",
            )
    else:
        # HD-4: HEAD uses the light by-path query (no download_chunks/mpu joins; carries append_version
        # and the Arion hash). The versioned path above keeps the heavy query.
        row = await ObjectRepository(db).get_head_by_path(bucket_name, object_key, main_account_id)
        if not row:
            bucket_exists = await db.fetchval(
                "SELECT 1 FROM buckets WHERE bucket_name = $1 AND deleted_at IS NULL",
                bucket_name,
            )
            if not bucket_exists:
                raise errors.S3Error(
                    code="NoSuchBucket",
                    status_code=404,
                    message=f"The specified bucket {bucket_name} does not exist",
                )
            raise errors.S3Error(
                code="NoSuchKey",
                status_code=404,
                message=f"The specified key {object_key} does not exist",
            )
    return row


async def handle_head_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    pool: asyncpg.Pool,
) -> Response:
    # Gateway now handles all ACL/permission checks
    # Backend trusts the account information from gateway
    account = getattr(request.state, "account", None)

    # Storage attribution is the bucket owner (caller fallback), not the caller — the account
    # split moved this off the rebound account.main_account onto its own state key.
    main_account_id = request.state.main_account_id

    # Parse versionId query parameter
    version_id = None
    if "versionId" in request.query_params:
        try:
            version_id = int(request.query_params["versionId"])
            if version_id <= 0:
                raise ValueError("Version must be positive")
        except (ValueError, TypeError):
            return Response(
                status_code=400,
                headers={
                    "x-amz-error-code": "InvalidArgument",
                    "x-amz-error-message": f"Invalid version ID: {request.query_params.get('versionId')}",
                },
            )

    # Tagging HEAD: only verify existence
    if "tagging" in request.query_params:
        with tracer.start_as_current_span("head_object.check_tagging_request"):
            try:
                async with pool.acquire() as conn:
                    await _get_object_with_permissions_min(bucket_name, object_key, conn, main_account_id, version_id)
                return Response(status_code=200)
            except errors.S3Error as e:
                return Response(
                    status_code=e.status_code,
                    headers={
                        "x-amz-error-code": e.code,
                        "x-amz-error-message": e.message,
                    },
                )
            except Exception:
                logger.exception("Error in HEAD tagging request")
                return Response(status_code=500)

    # Anonymous reads on a public bucket still carry the bucket owner in
    # state.main_account_id, so we gate on the caller's account.id. The account middleware
    # sets it to literal "anonymous" for unsigned requests; an empty string means it never
    # ran (request_context's stand-in) — treat as anon.
    is_anonymous = account is None or account.id in ("", "anonymous")
    try:
        response_overrides = parse_response_overrides(request.query_params, is_anonymous=is_anonymous)
    except ValueError as e:
        return Response(
            status_code=400,
            headers={
                "x-amz-error-code": "InvalidArgument",
                "x-amz-error-message": str(e),
            },
        )

    db = await pool.acquire()
    try:
        with tracer.start_as_current_span("head_object.get_object_metadata") as span:
            row = await _get_object_with_permissions_min(bucket_name, object_key, db, main_account_id, version_id)
            set_span_attributes(
                span,
                {
                    "object_id": str(row["object_id"]),
                    "has_object_id": True,
                    "size_bytes": int(row.get("size_bytes") or 0),
                    "multipart": bool(row.get("multipart")),
                    "content_type": row.get("content_type", ""),
                },
            )
        # A delete marker has no bytes. HEAD carries no body, so the signal is the status plus
        # x-amz-delete-marker: 404 for a plain HEAD, 405 when the marker was addressed by version.
        if row.get("is_delete_marker"):
            marker_headers = {
                "x-amz-delete-marker": "true",
                "Last-Modified": row["created_at"].strftime("%a, %d %b %Y %H:%M:%S GMT"),
                "x-amz-version-id": str(int(row.get("object_version") or 1)),
            }
            return Response(status_code=404 if version_id is None else 405, headers=marker_headers)

        # Build headers
        created_at = row["created_at"]
        size_bytes = int(row["size_bytes"]) if row.get("size_bytes") is not None else 0
        md5_hash = row["md5_hash"]
        # Fallback: if md5_hash is missing/empty for multipart object, compute combined ETag from parts
        if (not md5_hash) and bool(row.get("multipart")):
            with tracer.start_as_current_span("head_object.compute_multipart_etag") as span:
                try:
                    object_version = int(row.get("object_version"))
                    parts = await db.fetch(get_query("get_parts_etags_for_version"), row["object_id"], object_version)
                    etags = [p["etag"].split("-")[0] for p in parts]
                    import hashlib as _hashlib

                    if etags:
                        binary = b"".join(bytes.fromhex(e) for e in etags)
                        md5_hash = f"{_hashlib.md5(binary).hexdigest()}-{len(etags)}"
                        set_span_attributes(
                            span,
                            {
                                "num_parts": len(etags),
                                "computed_etag": md5_hash,
                            },
                        )
                except Exception:
                    md5_hash = md5_hash or ""
        if md5_hash and if_none_match_matches(request.headers.get("if-none-match"), md5_hash):
            return Response(status_code=304, headers={"ETag": f'"{md5_hash}"'})

        content_type = row["content_type"]
        headers: dict[str, str] = {
            "Content-Type": content_type,
            "Content-Length": str(size_bytes),
            "ETag": f'"{md5_hash}"',
            "Last-Modified": created_at.strftime("%a, %d %b %Y %H:%M:%S GMT"),
            "Accept-Ranges": "bytes",
        }
        # Source hint: cache vs pipeline
        with tracer.start_as_current_span("head_object.check_cache_status") as span:
            source = "pipeline"
            try:
                # HD-6: exists() takes (object_id, object_version, part_number); the old 2-arg call
                # raised a TypeError swallowed below, so the header was always "pipeline". Pass the
                # version + part 1 so the hint is truthful. Costs one meta stat per HEAD.
                obj_id_str = str(row["object_id"])
                oc = request.app.state.obj_cache
                has1 = await oc.exists(obj_id_str, int(row["object_version"]), 1)
                source = "cache" if has1 else "pipeline"
                headers["x-hippius-source"] = source
            except Exception:
                headers["x-hippius-source"] = "pipeline"
            set_span_attributes(span, {"source": source})

        # Add x-amz-version-id header
        object_version = int(row.get("object_version") or 1)
        headers["x-amz-version-id"] = str(object_version)

        # Add Arion file hash (first chunk of first part). HD-5: the light HEAD query returns it via a
        # LATERAL join, so skip the extra fetchval when present; the versioned path still fetches it.
        # Sentinel (not `in row`) because asyncpg Record membership tests values, not column names.
        arion_hash = row.get("arion_file_hash", _MISSING)
        if arion_hash is _MISSING:
            arion_hash = await db.fetchval(
                get_query("get_chunk_backend_identifier"),
                "arion",
                row["object_id"],
                object_version,
                1,  # part_number (1-based)
                0,  # chunk_index (0-based)
            )
        headers["X-Hippius-Arion-File-Hash"] = arion_hash or "pending"

        # Append version header if present. HD-3: the download query's outer SELECT now returns
        # append_version, so no fallback JOIN is needed.
        with tracer.start_as_current_span("head_object.fetch_append_version") as span:
            try:
                append_version = row.get("append_version")
                if append_version is not None:
                    headers["x-amz-meta-append-version"] = str(int(append_version))
                    set_span_attributes(span, {"append_version": int(append_version)})
            except Exception:
                pass
        # Metadata passthrough
        meta_val = row.get("metadata") or {}
        if isinstance(meta_val, str):
            try:
                meta_val = json.loads(meta_val)
            except Exception:
                meta_val = {}
        if isinstance(meta_val, dict):
            for k, v in meta_val.items():
                if k != "ipfs" and not isinstance(v, dict):
                    headers[f"x-amz-meta-{k}"] = str(v)
        apply_response_overrides(headers, response_overrides)
        return Response(status_code=200, headers=headers)

    except errors.S3Error as e:
        return Response(
            status_code=e.status_code,
            headers={
                "x-amz-error-code": e.code,
                "x-amz-error-message": e.message,
            },
        )
    except Exception as e:
        logger.exception(f"Error getting object metadata: {e}")
        return Response(status_code=500)

    finally:
        await pool.release(db)
