from __future__ import annotations

import contextlib
import json
import logging
from datetime import datetime
from typing import Any
from urllib.parse import parse_qs
from urllib.parse import unquote

import asyncpg
from fastapi import Request
from fastapi import Response
from lxml import etree as ET  # ty: ignore[unresolved-import]

from hippius_s3.api.s3 import errors
from hippius_s3.config import Config
from hippius_s3.db_pool import acquire_with_timeout
from hippius_s3.repositories.buckets import BucketRepository
from hippius_s3.repositories.objects import ObjectRepository
from hippius_s3.repositories.users import UserRepository
from hippius_s3.services.object_reader import stream_object
from hippius_s3.storage_version import require_supported_storage_version
from hippius_s3.writer.db import set_object_version_address
from hippius_s3.writer.object_writer import ObjectWriter


logger = logging.getLogger(__name__)


def parse_copy_source(copy_source: str | None) -> tuple[str, str, int | None]:
    """Split `x-amz-copy-source` into (bucket, key, version_id).

    The version id used to be discarded along with the rest of the query string, so
    `CopySource={"VersionId": N}` silently copied the CURRENT version instead — the standard
    "restore an old version" move returned the wrong bytes with no error.
    """
    if not copy_source:
        raise errors.S3Error(
            code="InvalidArgument",
            message="x-amz-copy-source missing",
            status_code=400,
        )

    copy_source_path, _, query = copy_source.partition("?")
    copy_source_path = unquote(copy_source_path).lstrip("/")

    path_parts = copy_source_path.split("/", 1)
    if len(path_parts) != 2:
        raise errors.S3Error(
            code="InvalidArgument",
            message="x-amz-copy-source must be in format /source-bucket/source-key",
            status_code=400,
        )

    return path_parts[0], path_parts[1], _parse_source_version_id(query)


def _parse_source_version_id(query: str) -> int | None:
    raw = parse_qs(query).get("versionId", [""])[0]
    # "null" is the version id AWS reports for objects predating versioning; we never mint it, so
    # it means "current". Other query params (partNumber, ...) are not ours to interpret.
    if not raw or raw == "null":
        return None
    try:
        version_id = int(raw)
    except ValueError:
        version_id = 0
    if version_id <= 0:
        raise errors.S3Error(
            code="InvalidArgument",
            message=f"Invalid version ID: {raw}",
            status_code=400,
        )
    return version_id


async def resolve_copy_resources(
    db: Any,
    main_account: str,
    source_bucket_name: str,
    source_object_key: str,
    dest_bucket_name: str,
    source_version_id: int | None = None,
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

    if source_version_id is None:
        source_object = await ObjectRepository(db).get_by_path(source_bucket["bucket_id"], source_object_key)
    else:
        source_object = await ObjectRepository(db).get_by_path_and_version(
            source_bucket["bucket_id"], source_object_key, source_version_id
        )
        if not source_object:
            raise errors.S3Error(
                code="NoSuchVersion",
                message=f"The specified version does not exist: {source_version_id}",
                status_code=404,
            )

    if not source_object:
        raise errors.S3Error(
            code="NoSuchKey",
            message=f"The specified key {source_object_key} does not exist",
            status_code=404,
        )

    # A delete marker has no bytes to copy; AWS answers a GET on one with 405, and a copy from one
    # is the same category of mistake.
    if source_object.get("is_delete_marker"):
        raise errors.S3Error(
            code="MethodNotAllowed",
            message="The specified version is a delete marker and has no content",
            status_code=405,
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


def build_copy_success_response(etag: str, last_modified: datetime, version_id: int | None = None) -> Response:
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

    headers = {"ETag": f'"{etag}"' if etag else '""'}
    if version_id is not None:
        headers["x-amz-version-id"] = str(version_id)

    return Response(
        content=xml_bytes,
        media_type="application/xml",
        status_code=200,
        headers=headers,
    )


async def handle_streaming_copy(
    pool: asyncpg.Pool,
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

    # Reuse the lifespan-built cache: it carries the standalone queues client used for
    # chunk-ready pub/sub. Constructing our own here fell back to `redis_client`, which in
    # prod is a RedisCluster — and redis-py's async RedisCluster has no `.pubsub()`, so any
    # copy that had to wait for a chunk died with AttributeError -> 500.
    obj_cache = request.app.state.obj_cache
    storage_version = require_supported_storage_version(int(src_obj_row.get("storage_version")))

    src_object_id = str(src_obj_row.get("object_id"))
    source_bucket_name = source_bucket["bucket_name"]
    source_object_key = source_object["object_key"]

    chunks_iter = await stream_object(
        pool,
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
        address=request.state.main_account_id,
        bound_first_chunk=True,  # A2: fail fast (503) if the source is still draining, before writing the destination
    )

    content_type = str(source_object["content_type"])
    ow = ObjectWriter(pool=pool, redis_client=redis_client, fs_store=request.app.state.fs_store)
    put_res = await ow.put_simple_stream_full(
        bucket_id=str(dest_bucket["bucket_id"]),
        bucket_name=dest_bucket["bucket_name"],
        object_id=object_id,
        object_key=object_key,
        account_address=request.state.main_account_id,
        content_type=content_type,
        metadata=metadata,
        storage_version=config.target_storage_version,
        body_iter=chunks_iter,
    )

    # Drain-direct (s3-2.1 PR-11): same contract as PutObject. Without the address the Rust drain
    # never rebuilds an UploadChainRequest for the destination, so the copy would live only in the
    # FS cache — no chunk_backend rows, never replicated, never evictable.
    try:
        await set_object_version_address(
            pool,
            object_id=str(put_res.object_id),
            object_version=int(put_res.object_version),
            address=request.state.main_account_id,
        )
    except Exception:
        # B4: put_simple_stream_full already made the version serveable (size/md5 written). If the
        # address never lands, a GET would serve bytes the drain can never back. Revert to the
        # reserved-row shape so reads skip it and the sweep reclaims its parts, then surface.
        with contextlib.suppress(Exception):
            async with acquire_with_timeout(pool, config.db_pool_acquire_timeout) as conn:
                await conn.execute(
                    "UPDATE object_versions SET size_bytes = 0, md5_hash = '' "
                    "WHERE object_id = $1 AND object_version = $2",
                    str(put_res.object_id),
                    int(put_res.object_version),
                )
        raise

    # put_simple_stream_full creates the multipart_uploads row (ensure_upload_row) but deliberately
    # leaves is_completed=FALSE; flipping it only after the address lands keeps a failed copy
    # eligible for the DELETE-cascade cleanup.
    async with acquire_with_timeout(pool, config.db_pool_acquire_timeout) as conn:
        await conn.execute(
            "UPDATE multipart_uploads SET is_completed = TRUE WHERE upload_id = $1",
            str(put_res.upload_id),
        )

    return build_copy_success_response(put_res.etag, copy_created_at, int(put_res.object_version))
