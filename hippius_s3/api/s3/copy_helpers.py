from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any
from urllib.parse import unquote

from fastapi import Request
from fastapi import Response
from lxml import etree as ET  # ty: ignore[unresolved-import]

from hippius_s3.api.s3 import errors
from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.config import Config
from hippius_s3.repositories.buckets import BucketRepository
from hippius_s3.repositories.objects import ObjectRepository
from hippius_s3.repositories.users import UserRepository
from hippius_s3.services.object_reader import stream_object
from hippius_s3.storage_version import require_supported_storage_version
from hippius_s3.writer.object_writer import ObjectWriter


logger = logging.getLogger(__name__)


def parse_copy_source(copy_source: str | None) -> tuple[str, str]:
    if not copy_source:
        raise errors.S3Error(
            code="InvalidArgument",
            message="x-amz-copy-source missing",
            status_code=400,
        )

    copy_source_path = copy_source.split("?", 1)[0]
    copy_source_path = unquote(copy_source_path).lstrip("/")

    path_parts = copy_source_path.split("/", 1)
    if len(path_parts) != 2:
        raise errors.S3Error(
            code="InvalidArgument",
            message="x-amz-copy-source must be in format /source-bucket/source-key",
            status_code=400,
        )

    return path_parts[0], path_parts[1]


async def resolve_copy_resources(
    db: Any,
    main_account: str,
    source_bucket_name: str,
    source_object_key: str,
    dest_bucket_name: str,
) -> tuple[dict, dict, dict, dict]:
    user = await UserRepository(db).ensure_by_main_account(main_account)
    user_id = user["main_account_id"]

    source_bucket = await BucketRepository(db).get_by_name_and_owner(source_bucket_name, user_id)
    dest_bucket = await BucketRepository(db).get_by_name_and_owner(dest_bucket_name, user_id)

    if not source_bucket:
        raise errors.S3Error(
            code="NoSuchBucket",
            message=f"The specified source bucket {source_bucket_name} does not exist",
            status_code=404,
        )

    if not dest_bucket:
        raise errors.S3Error(
            code="NoSuchBucket",
            message=f"The specified destination bucket {dest_bucket_name} does not exist",
            status_code=404,
        )

    source_object = await ObjectRepository(db).get_by_path(source_bucket["bucket_id"], source_object_key)
    if not source_object:
        raise errors.S3Error(
            code="NoSuchKey",
            message=f"The specified key {source_object_key} does not exist",
            status_code=404,
        )

    return user, source_bucket, dest_bucket, source_object


def parse_object_metadata(raw_meta: Any) -> dict[str, Any]:
    if isinstance(raw_meta, dict):
        return raw_meta

    if isinstance(raw_meta, str):
        try:
            parsed = json.loads(raw_meta)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    return {}


def is_multipart_object(obj_row: Any) -> bool:
    row_multipart = obj_row.get("multipart")
    if row_multipart is not None:
        return bool(row_multipart)

    metadata = parse_object_metadata(obj_row.get("metadata"))
    return bool(metadata.get("multipart", False))


def resolve_chunk_size(obj_row: Any, config: Config) -> int:
    chunk_size = obj_row.get("enc_chunk_size_bytes")
    if chunk_size is not None:
        return int(chunk_size)
    return int(config.object_chunk_size_bytes)


async def should_use_v5_fast_path(
    db: Any,
    src_obj_row: Any,
    existing_dest: Any,
    src_storage_version: int,
    src_multipart: bool,
) -> tuple[bool, list | None, str]:
    # NOTE: Disabled for now.
    #
    # For storage_version>=5, ciphertext chunks are bound to identifiers (at least bucket_id/object_id)
    # via deterministic nonce/AAD (see `hippius_s3/services/crypto_service.py`).
    #
    # That means "CID reuse" copy across different destination `object_id`s is not decryptable
    # even if we rewrap the DEK. We'll revisit in a future storage version (e.g. v6) with a
    # copy-friendly binding scheme.
    return False, None, "v5_fast_path_disabled_object_id_binding"


def build_copy_success_response(etag: str, last_modified: datetime) -> Response:
    root = ET.Element("CopyObjectResult")
    etag_elem = ET.SubElement(root, "ETag")
    etag_elem.text = etag or ""
    last_modified_elem = ET.SubElement(root, "LastModified")
    last_modified_elem.text = last_modified.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    xml_bytes = ET.tostring(
        root,
        encoding="utf-8",
        xml_declaration=True,
    )

    return Response(
        content=xml_bytes,
        media_type="application/xml",
        status_code=200,
        headers={
            "ETag": f'"{etag}"' if etag else '""',
        },
    )


async def handle_streaming_copy(
    db: Any,
    redis_client: Any,
    request: Request,
    source_bucket: dict,
    dest_bucket: dict,
    source_object: dict,
    src_obj_row: Any,
    object_id: str,
    object_key: str,
    copy_created_at: datetime,
    config: Config,
) -> Response:
    logger.info("CopyObject assembling bytes via object_reader.stream_object")

    metadata = parse_object_metadata(src_obj_row.get("metadata"))
    src_multipart = is_multipart_object(src_obj_row)

    obj_cache = RedisObjectPartsCache(redis_client)
    storage_version = require_supported_storage_version(int(src_obj_row.get("storage_version")))

    src_object_id = str(src_obj_row.get("object_id"))
    source_bucket_name = source_bucket["bucket_name"]
    source_object_key = source_object["object_key"]

    chunks_iter = await stream_object(
        db,
        redis_client,
        obj_cache,
        {
            "object_id": src_object_id,
            "bucket_id": str(src_obj_row.get("bucket_id") or source_bucket["bucket_id"]),
            "bucket_name": source_bucket_name,
            "object_key": source_object_key,
            "storage_version": storage_version,
            "object_version": int(src_obj_row.get("object_version") or 1),
            "is_public": bool(source_bucket.get("is_public", False)),
            "multipart": src_multipart,
            "metadata": metadata,
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

    content_type = str(source_object["content_type"])
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

    return build_copy_success_response(put_res.etag, copy_created_at)
