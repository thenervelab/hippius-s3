from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from fastapi import Response

from hippius_s3.api.s3 import errors
from hippius_s3.api.s3.copy_helpers import build_copy_success_response
from hippius_s3.api.s3.copy_helpers import parse_object_metadata
from hippius_s3.api.s3.copy_helpers import resolve_chunk_size
from hippius_s3.config import Config
from hippius_s3.services.parts_service import upsert_part_placeholder
from hippius_s3.storage_version import require_supported_storage_version
from hippius_s3.utils import get_query
from hippius_s3.writer.db import ensure_upload_row
from hippius_s3.writer.db import upsert_object_basic


logger = logging.getLogger(__name__)


async def execute_v5_fast_path_copy(
    db: Any,
    source_bucket: dict,
    dest_bucket: dict,
    source_object: dict,
    src_obj_row: Any,
    object_id: str,
    object_key: str,
    chunk_rows: list,
    copy_created_at: datetime,
    config: Config,
) -> Response:
    content_type = str(source_object["content_type"])
    size_bytes = int(src_obj_row.get("size_bytes") or 0)
    md5_hash = str(src_obj_row.get("md5_hash") or "")
    metadata = parse_object_metadata(src_obj_row.get("metadata"))

    dest_object_version = await create_destination_objects(
        db=db,
        object_id=object_id,
        dest_bucket=dest_bucket,
        object_key=object_key,
        content_type=content_type,
        metadata=metadata,
        md5_hash=md5_hash,
        size_bytes=size_bytes,
        src_obj_row=src_obj_row,
        config=config,
    )

    await copy_chunk_cids(db, chunk_rows, object_id, dest_object_version)

    dest_kek_id, dest_wrapped_dek = await rewrap_encryption_envelope(
        db=db,
        source_bucket=source_bucket,
        dest_bucket=dest_bucket,
        src_obj_row=src_obj_row,
        object_id=object_id,
        dest_object_version=dest_object_version,
    )

    chunk_size_bytes = resolve_chunk_size(src_obj_row, config)
    await db.execute(
        get_query("update_object_version_envelope"),
        str(src_obj_row.get("enc_suite_id") or "hip-enc/aes256gcm"),
        chunk_size_bytes,
        dest_kek_id,
        dest_wrapped_dek,
        object_id,
        dest_object_version,
    )

    return build_copy_success_response(md5_hash, copy_created_at)


async def create_destination_objects(
    db: Any,
    object_id: str,
    dest_bucket: dict,
    object_key: str,
    content_type: str,
    metadata: dict[str, Any],
    md5_hash: str,
    size_bytes: int,
    src_obj_row: Any,
    config: Config,
) -> int:
    src_storage_version = require_supported_storage_version(int(src_obj_row.get("storage_version")))

    row = await upsert_object_basic(
        db,
        object_id=object_id,
        bucket_id=str(dest_bucket["bucket_id"]),
        object_key=object_key,
        content_type=content_type,
        metadata=metadata,
        md5_hash=md5_hash,
        size_bytes=size_bytes,
        storage_version=src_storage_version,
    )
    dest_object_version = int(row.get("current_object_version") or 1) if row else 1

    upload_id = await ensure_upload_row(
        db,
        object_id=object_id,
        bucket_id=str(dest_bucket["bucket_id"]),
        object_key=object_key,
        content_type=content_type,
        metadata=metadata,
    )

    chunk_size_bytes = resolve_chunk_size(src_obj_row, config)
    await upsert_part_placeholder(
        db,
        object_id=object_id,
        upload_id=str(upload_id),
        part_number=1,
        size_bytes=size_bytes,
        etag=md5_hash or "",
        chunk_size_bytes=chunk_size_bytes,
        object_version=dest_object_version,
    )

    dest_part = await db.fetchrow(
        get_query("get_destination_part_for_copy"),
        object_id,
        dest_object_version,
    )
    if not dest_part:
        raise errors.S3Error(
            code="InternalError",
            message="Failed to create destination part row",
            status_code=500,
        )

    return dest_object_version


async def copy_chunk_cids(
    db: Any,
    chunk_rows: list,
    object_id: str,
    dest_object_version: int,
) -> None:
    dest_part = await db.fetchrow(
        get_query("get_destination_part_for_copy"),
        object_id,
        dest_object_version,
    )
    if not dest_part:
        raise errors.S3Error(
            code="InternalError",
            message="Destination part not found for chunk copy",
            status_code=500,
        )

    dest_part_id = dest_part["part_id"]

    for row in chunk_rows:
        chunk_index = int(row[0])
        cid = str(row[1])
        cipher_size = int(row[2]) if len(row) > 2 and row[2] is not None else 0
        plain_size = int(row[3]) if len(row) > 3 and row[3] is not None else None

        await db.execute(
            get_query("upsert_part_chunk"),
            dest_part_id,
            chunk_index,
            cid,
            cipher_size,
            plain_size,
            None,
            None,
        )


async def rewrap_encryption_envelope(
    db: Any,
    source_bucket: dict,
    dest_bucket: dict,
    src_obj_row: Any,
    object_id: str,
    dest_object_version: int,
) -> tuple[str, bytes]:
    from hippius_s3.services.envelope_service import unwrap_dek
    from hippius_s3.services.envelope_service import wrap_dek
    from hippius_s3.services.kek_service import get_bucket_kek_bytes
    from hippius_s3.services.kek_service import get_or_create_active_bucket_kek

    src_kek_id = src_obj_row.get("kek_id")
    src_wrapped_dek = src_obj_row.get("wrapped_dek")
    if not src_kek_id or not src_wrapped_dek:
        raise errors.S3Error(
            code="InternalError",
            message="Missing v5 envelope metadata on source",
            status_code=500,
        )

    src_bucket_id = str(source_bucket["bucket_id"])
    src_object_id = str(src_obj_row.get("object_id"))
    src_object_version = int(src_obj_row.get("object_version") or 1)

    src_kek_bytes = await get_bucket_kek_bytes(
        bucket_id=src_bucket_id,
        kek_id=src_kek_id,
    )
    src_aad = f"hippius-dek:{src_bucket_id}:{src_object_id}:{src_object_version}".encode("utf-8")
    dek = unwrap_dek(kek=src_kek_bytes, wrapped_dek=bytes(src_wrapped_dek), aad=src_aad)

    dest_bucket_id = str(dest_bucket["bucket_id"])
    dest_kek_id, dest_kek_bytes = await get_or_create_active_bucket_kek(bucket_id=dest_bucket_id)
    dest_aad = f"hippius-dek:{dest_bucket_id}:{object_id}:{dest_object_version}".encode("utf-8")
    dest_wrapped = wrap_dek(kek=dest_kek_bytes, dek=dek, aad=dest_aad)

    return str(dest_kek_id), dest_wrapped
