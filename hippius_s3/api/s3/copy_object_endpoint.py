from __future__ import annotations

import hashlib
import json
import logging
import os
import uuid
from datetime import datetime
from datetime import timezone
from typing import Any
from urllib.parse import unquote

from fastapi import Request
from fastapi import Response
from lxml import etree as ET

from hippius_s3 import utils
from hippius_s3.api.s3 import errors
from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.config import get_config
from hippius_s3.repositories.buckets import BucketRepository
from hippius_s3.repositories.objects import ObjectRepository
from hippius_s3.repositories.users import UserRepository
from hippius_s3.services.object_reader import ObjectInfo as ORObjectInfo
from hippius_s3.services.object_reader import ObjectReader


logger = logging.getLogger(__name__)
config = get_config()


async def handle_copy_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: Any,
    ipfs_service: Any,
    redis_client: Any,
    *,
    object_reader: ObjectReader | None = None,
) -> Response:
    copy_source = request.headers.get("x-amz-copy-source")
    if not copy_source:
        return errors.s3_error_response("InvalidArgument", "x-amz-copy-source missing", status_code=400)

    # Trim query (e.g., versionId) and normalize leading slash; allow URL-encoded
    copy_source_path = copy_source.split("?", 1)[0]
    copy_source_path = unquote(copy_source_path)
    if copy_source_path.startswith("/"):
        copy_source_path = copy_source_path[1:]
    parts = copy_source_path.split("/", 1)
    if len(parts) != 2:
        return errors.s3_error_response(
            "InvalidArgument",
            "x-amz-copy-source must be in format /source-bucket/source-key",
            status_code=400,
        )
    source_bucket_name, source_object_key = parts

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

    # Determine encryption context
    source_is_public = source_bucket["is_public"]
    dest_is_public = dest_bucket["is_public"]
    same_bucket = source_bucket["bucket_id"] == dest_bucket["bucket_id"]
    same_encryption_context = source_is_public == dest_is_public and same_bucket

    object_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc)

    if same_encryption_context:
        # Fast path: reuse CID & metadata
        logger.info(f"CopyObject fast path: same encryption (same_bucket={same_bucket}, public={source_is_public})")
        ipfs_cid = source_object["ipfs_cid"]
        file_size = source_object["size_bytes"]
        content_type = source_object["content_type"]
        md5_hash = source_object["md5_hash"]
        metadata: dict[str, Any] = {
            "ipfs": source_metadata.get("ipfs", {}),
            "hippius": source_metadata.get("hippius", {}),
            **{k: v for k, v in source_metadata.items() if k not in ["ipfs", "hippius"]},
        }
    else:
        # Slow path: fetch bytes and republish for destination bucket context
        logger.info(f"CopyObject slow path: source public={source_is_public}, dest public={dest_is_public}")
        # If CID missing, try cache/ObjectReader before 503
        source_cid = (source_object.get("ipfs_cid") or "").strip()
        if not source_cid:
            src_bytes: bytes | None = None
            # Try unified cache via ObjectReader, then direct cache as fallback
            try:
                if object_reader is None:
                    object_reader = ObjectReader(config)
                # Create ObjectInfo from source_object database record
                md = source_object.get("metadata") or {}
                if isinstance(md, str):
                    try:
                        md = json.loads(md)
                    except Exception:
                        md = {}
                info = ORObjectInfo(
                    object_id=str(source_object["object_id"]),
                    bucket_name=source_object["bucket_name"],
                    object_key=source_object["object_key"],
                    size_bytes=int(source_object["size_bytes"]),
                    content_type=source_object["content_type"],
                    md5_hash=source_object["md5_hash"],
                    created_at=source_object["created_at"],
                    metadata=md,
                    multipart=bool(source_object.get("multipart", False)),
                    should_decrypt=bool(source_object.get("should_decrypt", False)),
                    simple_cid=source_object.get("simple_cid"),
                    upload_id=source_object.get("upload_id"),
                )
                src_bytes = await object_reader.read_base_bytes(db, redis_client, info)
            except Exception:
                try:
                    src_bytes = await RedisObjectPartsCache(redis_client).get(str(source_object["object_id"]), 0)
                except Exception:
                    src_bytes = None
            if src_bytes:
                md5_hash = hashlib.md5(src_bytes).hexdigest()  # type: ignore[arg-type]
                should_encrypt = not dest_is_public
                s3_result = await ipfs_service.client.s3_publish(
                    content=src_bytes,  # type: ignore[arg-type]
                    encrypt=should_encrypt,
                    seed_phrase=request.state.seed_phrase,
                    subaccount_id=request.state.account.main_account,
                    bucket_name=dest_bucket["bucket_name"],
                    file_name=source_object_key,
                    store_node=config.ipfs_store_url,
                    pin_node=config.ipfs_store_url,
                    substrate_url=config.substrate_url,
                    publish=(os.getenv("HIPPIUS_PUBLISH_MODE", "full") != "ipfs_only"),
                )
                ipfs_cid = s3_result.cid
                file_size = len(src_bytes)  # type: ignore[arg-type]
                content_type = source_object["content_type"]
                metadata = {}
            else:
                return errors.s3_error_response(
                    "ServiceUnavailable",
                    "Source object is not yet available for copying. Please retry shortly.",
                    status_code=503,
                )
        else:
            # Download by CID and re-publish with destination encryption
            src_bytes = await ipfs_service.download_file(
                cid=source_cid,
                subaccount_id=request.state.account.main_account,
                bucket_name=source_bucket_name,
                decrypt=not source_is_public,
            )
            md5_hash = hashlib.md5(src_bytes).hexdigest()  # type: ignore[arg-type]
            should_encrypt = not dest_is_public
            s3_result = await ipfs_service.client.s3_publish(
                content=src_bytes,
                encrypt=should_encrypt,
                seed_phrase=request.state.seed_phrase,
                subaccount_id=request.state.account.main_account,
                bucket_name=dest_bucket["bucket_name"],
                file_name=source_object_key,
                store_node=config.ipfs_store_url,
                pin_node=config.ipfs_store_url,
                substrate_url=config.substrate_url,
                publish=(os.getenv("HIPPIUS_PUBLISH_MODE", "full") != "ipfs_only"),
            )
            ipfs_cid = s3_result.cid
            file_size = len(src_bytes)  # type: ignore[arg-type]
            content_type = source_object["content_type"]
            metadata = {}

    # Upsert destination object (CID table)
    cid_id = await utils.upsert_cid_and_get_id(db, ipfs_cid)
    await ObjectRepository(db).upsert_with_cid(
        object_id,
        dest_bucket["bucket_id"],
        object_key,
        cid_id,
        file_size,
        content_type,
        created_at,
        json.dumps(metadata),
        md5_hash,
    )

    # Success XML
    root = ET.Element("CopyObjectResult")
    etag = ET.SubElement(root, "ETag")
    etag.text = md5_hash
    last_modified = ET.SubElement(root, "LastModified")
    last_modified.text = created_at.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    xml_response = ET.tostring(root, encoding="utf-8", xml_declaration=True)

    return Response(
        content=xml_response,
        media_type="application/xml",
        status_code=200,
        headers={
            "ETag": f'"{md5_hash}"',
            "x-amz-ipfs-cid": ipfs_cid,
        },
    )
