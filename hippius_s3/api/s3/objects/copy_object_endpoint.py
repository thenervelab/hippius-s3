from __future__ import annotations

import json
import logging
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime
from datetime import timezone
from typing import Any
from urllib.parse import unquote

from fastapi import Request
from fastapi import Response

from hippius_s3.api.s3 import errors
from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.config import get_config
from hippius_s3.repositories.buckets import BucketRepository
from hippius_s3.repositories.objects import ObjectRepository
from hippius_s3.repositories.users import UserRepository
from hippius_s3.services.object_reader import stream_object
from hippius_s3.services.parts_service import upsert_part_placeholder
from hippius_s3.storage_version import require_supported_storage_version
from hippius_s3.utils import get_query
from hippius_s3.writer.db import ensure_upload_row
from hippius_s3.writer.db import upsert_object_basic
from hippius_s3.writer.object_writer import ObjectWriter


logger = logging.getLogger(__name__)
config = get_config()


async def handle_copy_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: Any,
    redis_client: Any,
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
    metadata_multipart = bool(source_metadata.get("multipart", False))
    row_multipart = source_object.get("multipart")
    is_multipart = bool(row_multipart) if row_multipart is not None else metadata_multipart
    if is_multipart:
        return errors.s3_error_response(
            "NotImplemented", "Copying multipart objects is not currently supported", status_code=501
        )

    object_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc)

    # Prefer reader service to manage readiness and cache hydration
    # Resolve source object via repository (avoid reader shim)
    src_obj_row = await ObjectRepository(db).get_by_path(source_bucket["bucket_id"], source_object_key)
    if not src_obj_row:
        return errors.s3_error_response(
            "NoSuchKey", f"The specified key {source_object_key} does not exist", status_code=404
        )
    src_object_id = str(src_obj_row.get("object_id"))
    # metadata column may be missing or be JSON string
    raw_meta = src_obj_row.get("metadata") if hasattr(src_obj_row, "get") else None
    if isinstance(raw_meta, str):
        try:
            raw_meta = json.loads(raw_meta)
        except Exception:
            raw_meta = {}
    src_metadata: dict[str, Any] = raw_meta if isinstance(raw_meta, dict) else {}
    row_multipart = src_obj_row.get("multipart")
    src_multipart = bool(row_multipart) if row_multipart is not None else bool(src_metadata.get("multipart", False))

    raw_storage_version = src_obj_row.get("storage_version")
    if raw_storage_version is None:
        return errors.s3_error_response("InternalError", "Missing storage version", status_code=500)
    src_storage_version = require_supported_storage_version(int(raw_storage_version))
    src_object_version = int(src_obj_row.get("object_version") or 1)

    # v5 fast path: envelope rewrap + CID reuse (no decrypt/re-encrypt of content)
    if src_storage_version >= 5:
        # CopyObject currently does not support multipart objects in this endpoint
        if src_multipart:
            return errors.s3_error_response(
                "NotImplemented", "Copying multipart objects is not currently supported", status_code=501
            )

        # Require chunk CIDs for v5 no-download copy
        rows = await db.fetch(
            get_query("get_part_chunks_by_object_and_number"),
            src_object_id,
            int(src_object_version),
            1,
        )
        if not rows:
            logger.info("CopyObject v5 fallback: missing part_chunks metadata; using streaming copy")
        else:
            # Ensure every chunk has a concrete CID
            bad = [int(r[0]) for r in rows if not r[1] or str(r[1]).strip().lower() in {"", "none", "pending"}]
            if bad:
                logger.info("CopyObject v5 fallback: missing chunk CID(s) indices=%s; using streaming copy", bad)
            else:
                # Create destination object+version row
                content_type = str(source_object["content_type"])  # type: ignore[index]
                size_bytes = int(src_obj_row.get("size_bytes") or 0)
                md5_hash = str(src_obj_row.get("md5_hash") or "")
                dest_metadata: dict[str, Any] = dict(src_metadata) if src_metadata else {}

                row = await upsert_object_basic(
                    db,
                    object_id=object_id,
                    bucket_id=str(dest_bucket["bucket_id"]),
                    object_key=object_key,
                    content_type=content_type,
                    metadata=dest_metadata,
                    md5_hash=md5_hash,
                    size_bytes=size_bytes,
                    storage_version=src_storage_version,
                )
                dest_object_version = int(row.get("current_object_version") or 1) if row else 1

                # Create upload row and part placeholder for part 1
                upload_id = await ensure_upload_row(
                    db,
                    object_id=object_id,
                    bucket_id=str(dest_bucket["bucket_id"]),
                    object_key=object_key,
                    content_type=content_type,
                    metadata=dest_metadata,
                )
                # Part CID is optional for v4+ objects; keep pending while chunk CIDs drive hydration.
                await upsert_part_placeholder(
                    db,
                    object_id=object_id,
                    upload_id=str(upload_id),
                    part_number=1,
                    size_bytes=size_bytes,
                    etag=md5_hash or "",
                    placeholder_cid="pending",
                    chunk_size_bytes=int(src_obj_row.get("enc_chunk_size_bytes") or config.object_chunk_size_bytes),
                    object_version=int(dest_object_version),
                )
                dest_part = await db.fetchrow(
                    """
                    SELECT part_id FROM parts
                     WHERE object_id=$1 AND object_version=$2 AND part_number=1
                     LIMIT 1
                    """,
                    object_id,
                    int(dest_object_version),
                )
                if not dest_part:
                    return errors.s3_error_response(
                        "InternalError", "Failed to create destination part row", status_code=500
                    )
                dest_part_id = dest_part["part_id"]

                # Copy chunk CID rows
                for r in rows:
                    chunk_index = int(r[0])
                    cid = str(r[1])
                    cipher_size = int(r[2]) if len(r) > 2 and r[2] is not None else 0
                    plain_size = int(r[3]) if len(r) > 3 and r[3] is not None else None
                    await db.execute(
                        get_query("upsert_part_chunk"),
                        dest_part_id,
                        chunk_index,
                        cid,
                        int(cipher_size),
                        plain_size,
                        None,
                        None,
                    )

                # Rewrap DEK under destination bucket KEK
                from hippius_s3.services.envelope_service import unwrap_dek
                from hippius_s3.services.envelope_service import wrap_dek
                from hippius_s3.services.kek_service import get_bucket_kek_bytes
                from hippius_s3.services.kek_service import get_or_create_active_bucket_kek

                src_bucket_id = str(source_bucket["bucket_id"])
                src_kek_id = src_obj_row.get("kek_id")
                src_wrapped_dek = src_obj_row.get("wrapped_dek")
                if not src_kek_id or not src_wrapped_dek:
                    return errors.s3_error_response(
                        "InternalError", "Missing v5 envelope metadata on source", status_code=500
                    )
                src_kek_bytes = await get_bucket_kek_bytes(bucket_id=src_bucket_id, kek_id=src_kek_id)
                src_aad = f"hippius-dek:{src_bucket_id}:{src_object_id}:{src_object_version}".encode("utf-8")
                dek = unwrap_dek(kek=src_kek_bytes, wrapped_dek=bytes(src_wrapped_dek), aad=src_aad)

                dest_bucket_id = str(dest_bucket["bucket_id"])
                dest_kek_id, dest_kek_bytes = await get_or_create_active_bucket_kek(bucket_id=dest_bucket_id)
                dest_aad = f"hippius-dek:{dest_bucket_id}:{object_id}:{dest_object_version}".encode("utf-8")
                dest_wrapped = wrap_dek(kek=dest_kek_bytes, dek=dek, aad=dest_aad)

                await db.execute(
                    """
                    UPDATE object_versions
                       SET encryption_version = 5,
                           enc_suite_id = COALESCE($1, 'hip-enc/aes256gcm'),
                           enc_chunk_size_bytes = COALESCE($2, $3),
                           kek_id = $4,
                           wrapped_dek = $5
                     WHERE object_id = $6 AND object_version = $7
                    """,
                    str(src_obj_row.get("enc_suite_id") or "hip-enc/aes256gcm"),
                    src_obj_row.get("enc_chunk_size_bytes"),
                    int(config.object_chunk_size_bytes),
                    dest_kek_id,
                    dest_wrapped,
                    object_id,
                    int(dest_object_version),
                )

                # Success XML (AWS-style)
                root = ET.Element("CopyObjectResult")
                etag = ET.SubElement(root, "ETag")
                etag.text = md5_hash or ""
                last_modified = ET.SubElement(root, "LastModified")
                last_modified.text = created_at.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                xml_response = ET.tostring(root, encoding="utf-8", xml_declaration=True)

                return Response(
                    content=xml_response,
                    media_type="application/xml",
                    status_code=200,
                    headers={
                        "ETag": f'"{md5_hash}"' if md5_hash else '""',
                    },
                )

    # Assemble bytes via high-level reader API (internally handles downloader/cache)
    logger.info("CopyObject assembling bytes via object_reader.stream_object")
    obj_cache = RedisObjectPartsCache(redis_client)
    storage_version = require_supported_storage_version(int(src_obj_row["storage_version"]))
    chunks_iter = await stream_object(
        db,
        redis_client,
        obj_cache,
        {
            "object_id": src_object_id,
            "bucket_id": str(src_obj_row.get("bucket_id") or str(source_bucket.get("bucket_id") or "")),
            "bucket_name": source_bucket_name,
            "object_key": source_object_key,
            "storage_version": storage_version,
            "object_version": int(src_obj_row.get("object_version") or 1),
            "is_public": bool(source_bucket.get("is_public", False)),
            "multipart": src_multipart,
            "metadata": src_metadata,
            "encryption_version": src_obj_row.get("encryption_version"),
            "enc_suite_id": src_obj_row.get("enc_suite_id"),
            "enc_chunk_size_bytes": src_obj_row.get("enc_chunk_size_bytes"),
            "kek_id": src_obj_row.get("kek_id"),
            "wrapped_dek": src_obj_row.get("wrapped_dek"),
            "ray_id": getattr(request.state, "ray_id", None),
        },
        rng=None,
        address=request.state.account.main_account,
    )
    # Stream to destination via ObjectWriter to avoid buffering entire object
    content_type = str(source_object["content_type"])  # type: ignore[index]
    metadata: dict[str, Any] = dict(src_metadata) if src_metadata else {}

    ow = ObjectWriter(db=db, redis_client=redis_client, fs_store=request.app.state.fs_store)
    put_res = await ow.put_simple_stream_full(
        bucket_id=str(dest_bucket["bucket_id"]),
        bucket_name=dest_bucket["bucket_name"],
        object_id=object_id,
        object_key=object_key,
        account_address=request.state.account.main_account,
        content_type=content_type,
        metadata=metadata,
        storage_version=config.target_storage_version,
        body_iter=chunks_iter,
    )

    # Success XML (AWS-style; keep declaration for client compatibility)
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
