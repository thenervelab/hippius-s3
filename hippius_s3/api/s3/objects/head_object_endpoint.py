from __future__ import annotations

import json
import logging
from typing import Any
from typing import Optional

from fastapi import Request
from fastapi import Response
from opentelemetry import trace

from hippius_s3.api.middlewares.tracing import set_span_attributes
from hippius_s3.api.s3 import errors
from hippius_s3.repositories.objects import ObjectRepository
from hippius_s3.repositories.users import UserRepository
from hippius_s3.services.object_reader import ObjectReader
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


async def _get_object_with_permissions_min(
    bucket_name: str,
    object_key: str,
    db: Any,
    main_account_id: Optional[str],
) -> Any:
    """Lightweight existence and metadata check (HEAD). Gateway handles permissions."""
    # Ensure user exists
    if main_account_id:
        await UserRepository(db).ensure_by_main_account(main_account_id)

    # Gateway already checked permissions, just fetch the object
    row = await ObjectRepository(db).get_for_download_with_permissions(bucket_name, object_key, main_account_id)
    if not row:
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
    db: Any,
    *,
    object_reader: ObjectReader | None = None,
) -> Response:
    # Gateway now handles all ACL/permission checks
    # Backend trusts the account information from gateway
    account = getattr(request.state, "account", None)

    main_account_id = account.main_account if account else "anonymous"

    # Tagging HEAD: only verify existence
    if "tagging" in request.query_params:
        with tracer.start_as_current_span("head_object.check_tagging_request"):
            try:
                await _get_object_with_permissions_min(bucket_name, object_key, db, main_account_id)
                return Response(status_code=200)
            except errors.S3Error as e:
                return Response(status_code=e.status_code)
            except Exception:
                logger.exception("Error in HEAD tagging request")
                return Response(status_code=500)

    try:
        with tracer.start_as_current_span("head_object.get_object_metadata") as span:
            row = await _get_object_with_permissions_min(bucket_name, object_key, db, main_account_id)
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
        content_type = row["content_type"]
        headers: dict[str, str] = {
            "Content-Type": content_type,
            "Content-Length": str(size_bytes),
            "ETag": f'"{md5_hash}"',
            "Last-Modified": created_at.strftime("%a, %d %b %Y %H:%M:%S GMT"),
        }
        # Source hint: cache vs pipeline
        with tracer.start_as_current_span("head_object.check_cache_status") as span:
            source = "pipeline"
            try:
                obj_id_str = str(row["object_id"])
                oc = request.app.state.obj_cache
                has1 = await oc.exists(obj_id_str, 1)
                source = "cache" if has1 else "pipeline"
                headers["x-hippius-source"] = source
            except Exception:
                headers["x-hippius-source"] = "pipeline"
            set_span_attributes(span, {"source": source})

        # Append version header if present
        with tracer.start_as_current_span("head_object.fetch_append_version") as span:
            try:
                append_version = row.get("append_version")
                if append_version is None:
                    append_version = await db.fetchval(
                        """
                        SELECT ov.append_version
                        FROM objects o
                        JOIN object_versions ov
                          ON ov.object_id = o.object_id
                         AND ov.object_version = o.current_object_version
                        WHERE o.object_id = $1
                        """,
                        row["object_id"],
                    )
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
        # CID hint if available
        simple_cid = (row.get("simple_cid") or row.get("ipfs_cid") or "").strip()
        headers["x-amz-ipfs-cid"] = simple_cid or "pending"
        return Response(status_code=200, headers=headers)

    except errors.S3Error as e:
        return Response(status_code=e.status_code)
    except Exception as e:
        logger.exception(f"Error getting object metadata: {e}")
        return Response(status_code=500)
