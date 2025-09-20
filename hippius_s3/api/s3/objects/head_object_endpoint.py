from __future__ import annotations

import json
import logging
from typing import Any
from typing import Optional

from fastapi import Request
from fastapi import Response

from hippius_s3.api.s3 import errors
from hippius_s3.repositories.objects import ObjectRepository
from hippius_s3.repositories.users import UserRepository
from hippius_s3.services.object_reader import ObjectReader


logger = logging.getLogger(__name__)


async def _get_object_with_permissions_min(
    bucket_name: str,
    object_key: str,
    db: Any,
    main_account_id: Optional[str],
) -> Any:
    """Lightweight existence and metadata check with permissions (HEAD)."""
    # For anonymous access (empty account), skip user creation
    if main_account_id:
        # Ensure user exists (align with GET behavior)
        await UserRepository(db).ensure_by_main_account(main_account_id)
    # Prefer the same query used by GET with permissions baked in
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
    # Tagging HEAD: only verify existence
    if "tagging" in request.query_params:
        try:
            await _get_object_with_permissions_min(bucket_name, object_key, db, request.state.account.main_account)
            return Response(status_code=200)
        except errors.S3Error as e:
            return Response(status_code=e.status_code)
        except Exception:
            logger.exception("Error in HEAD tagging request")
            return Response(status_code=500)

    try:
        row = await _get_object_with_permissions_min(bucket_name, object_key, db, request.state.account.main_account)
        # Build headers
        created_at = row["created_at"]
        size_bytes = int(row["size_bytes"]) if row.get("size_bytes") is not None else 0
        md5_hash = row["md5_hash"]
        content_type = row["content_type"]
        headers: dict[str, str] = {
            "Content-Type": content_type,
            "Content-Length": str(size_bytes),
            "ETag": f'"{md5_hash}"',
            "Last-Modified": created_at.strftime("%a, %d %b %Y %H:%M:%S GMT"),
        }
        # Source hint: cache vs pipeline
        try:
            obj_id_str = str(row["object_id"])  # type: ignore[index]
            oc = request.app.state.obj_cache
            has0 = await oc.exists(obj_id_str, 0)
            has1 = await oc.exists(obj_id_str, 1)
            headers["x-hippius-source"] = "cache" if (has0 or has1) else "pipeline"
        except Exception:
            headers["x-hippius-source"] = "pipeline"
        # Append version header if present
        try:
            append_version = row.get("append_version")
            if append_version is None:
                # Fetch explicitly if not present in row
                append_version = await db.fetchval(
                    "SELECT append_version FROM objects WHERE object_id = $1",
                    row["object_id"],
                )
            if append_version is not None:
                headers["x-amz-meta-append-version"] = str(int(append_version))
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
