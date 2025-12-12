from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime
from datetime import timezone
from typing import Any
from urllib.parse import unquote

from fastapi import Request
from fastapi import Response
from lxml import etree as ET

from hippius_s3.api.s3 import errors
from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.config import get_config
from hippius_s3.repositories.buckets import BucketRepository
from hippius_s3.repositories.objects import ObjectRepository
from hippius_s3.repositories.users import UserRepository
from hippius_s3.services.object_reader import stream_object
from hippius_s3.storage_version import require_supported_storage_version
from hippius_s3.writer.object_writer import ObjectWriter


logger = logging.getLogger(__name__)
config = get_config()


async def handle_copy_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: Any,
    redis_client: Any,
    *,
    object_reader: Any | None = None,
) -> Response:
    copy_source = request.headers.get("x-amz-copy-source")
    if not copy_source:
        return errors.s3_error_response("InvalidArgument", "x-amz-copy-source missing", status_code=400)

    # Trim query (e.g., versionId) and normalize leading slash; allow URL-encoded
    copy_source_path = copy_source.split("?", 1)[0]
    copy_source_path = unquote(copy_source_path)
    if copy_source_path.startswith("/"):
        copy_source_path = copy_source_path[1:]
    path_parts = copy_source_path.split("/", 1)
    if len(path_parts) != 2:
        return errors.s3_error_response(
            "InvalidArgument",
            "x-amz-copy-source must be in format /source-bucket/source-key",
            status_code=400,
        )
    source_bucket_name, source_object_key = path_parts

    # Resolve user and buckets (same owner for src/dst)
    user = await UserRepository(db).ensure_by_main_account(request.state.account.main_account)
    source_bucket = await BucketRepository(db).get_by_name_and_owner(source_bucket_name, user["main_account_id"])
    if not source_bucket:
        return errors.s3_error_response(
            "NoSuchBucket", f"The specified source bucket {source_bucket_name} does not exist", status_code=404
        )
    dest_bucket = await BucketRepository(db).get_by_name_and_owner(bucket_name, user["main_account_id"])
    if not dest_bucket:
        return errors.s3_error_response(
            "NoSuchBucket", f"The specified destination bucket {bucket_name} does not exist", status_code=404
        )

    # Fetch source object
    source_object = await ObjectRepository(db).get_by_path(source_bucket["bucket_id"], source_object_key)
    if not source_object:
        return errors.s3_error_response(
            "NoSuchKey", f"The specified key {source_object_key} does not exist", status_code=404
        )

    # Multipart copy not supported (parity with existing behavior)
    try:
        source_metadata = json.loads(source_object["metadata"])  # type: ignore[index]
    except Exception:
        source_metadata = {}
    if source_metadata.get("multipart"):
        return errors.s3_error_response(
            "NotImplemented", "Copying multipart objects is not currently supported", status_code=501
        )

    object_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc)

    # Prefer ObjectReader to manage readiness and cache hydration
    # Resolve source object via repository (avoid reader shim)
    src_obj_row = await ObjectRepository(db).get_by_path(source_bucket["bucket_id"], source_object_key)
    if not src_obj_row:
        return errors.s3_error_response(
            "NoSuchKey", f"The specified key {source_object_key} does not exist", status_code=404
        )
    # Build src_info with safe fallbacks (metadata may be JSON string)
    src_info = type("_Src", (), {})()
    src_info.object_id = str(src_obj_row.get("object_id"))
    src_info.bucket_name = source_bucket_name
    src_info.object_key = source_object_key
    src_info.size_bytes = int(src_obj_row.get("size_bytes") or 0)
    src_info.content_type = str(src_obj_row.get("content_type") or "application/octet-stream")
    # metadata column may be missing or be JSON string
    raw_meta = src_obj_row.get("metadata") if hasattr(src_obj_row, "get") else None
    if isinstance(raw_meta, str):
        try:
            raw_meta = json.loads(raw_meta)
        except Exception:
            raw_meta = {}
    src_info.metadata = raw_meta or {}
    src_info.multipart = bool((src_info.metadata or {}).get("multipart", False))
    # Assemble bytes via high-level reader API (internally handles downloader/cache)
    logger.info("CopyObject assembling bytes via object_reader.stream_object")
    obj_cache = RedisObjectPartsCache(redis_client)
    storage_version = require_supported_storage_version(int(src_obj_row["storage_version"]))
    chunks_iter = await stream_object(
        db,
        redis_client,
        obj_cache,
        {
            "object_id": src_info.object_id,
            "bucket_name": source_bucket_name,
            "object_key": source_object_key,
            "storage_version": storage_version,
            "object_version": int(src_obj_row.get("object_version") or 1),
            "multipart": bool(src_info.multipart),
            "metadata": src_info.metadata,
            "ray_id": getattr(request.state, "ray_id", None),
        },
        rng=None,
        address=request.state.account.main_account,
    )
    # Stream to destination via ObjectWriter to avoid buffering entire object
    content_type = str(source_object["content_type"])  # type: ignore[index]
    metadata: dict[str, Any] = {}

    ow = ObjectWriter(db=db, redis_client=redis_client, fs_store=request.app.state.fs_store)
    put_res = await ow.put_simple_stream_full(
        bucket_id=str(dest_bucket["bucket_id"]),
        bucket_name=dest_bucket["bucket_name"],
        object_id=object_id,
        object_key=object_key,
        account_address=request.state.account.main_account,
        content_type=content_type,
        metadata=metadata,
        storage_version=int(getattr(config, "target_storage_version", 4)),
        body_iter=chunks_iter,
    )

    # Success XML
    root = ET.Element("CopyObjectResult")
    etag = ET.SubElement(root, "ETag")
    etag.text = put_res.etag
    last_modified = ET.SubElement(root, "LastModified")
    last_modified.text = created_at.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    xml_response = ET.tostring(root, encoding="utf-8", xml_declaration=True)

    return Response(
        content=xml_response,
        media_type="application/xml",
        status_code=200,
        headers={
            "ETag": f'"{put_res.etag}"',
        },
    )
