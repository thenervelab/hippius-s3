from __future__ import annotations

import hashlib
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

from hippius_s3 import utils
from hippius_s3.api.s3 import errors
from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.config import get_config
from hippius_s3.repositories.buckets import BucketRepository
from hippius_s3.repositories.objects import ObjectRepository
from hippius_s3.repositories.users import UserRepository
from hippius_s3.services.crypto_service import CryptoService
from hippius_s3.services.object_reader import DownloadNotReadyError
from hippius_s3.services.object_reader import ObjectReader
from hippius_s3.services.object_reader import Part


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

    # Determine encryption context
    source_is_public = source_bucket["is_public"]
    dest_is_public = dest_bucket["is_public"]
    same_bucket = source_bucket["bucket_id"] == dest_bucket["bucket_id"]
    same_encryption_context = source_is_public == dest_is_public and same_bucket

    object_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc)

    # Prefer ObjectReader to manage readiness and cache hydration
    obj_reader = object_reader or ObjectReader(config)
    try:
        src_info = await obj_reader.fetch_object_info(
            db, source_bucket_name, source_object_key, request.state.account.main_account
        )
    except Exception:
        return errors.s3_error_response(
            "NoSuchKey", f"The specified key {source_object_key} does not exist", status_code=404
        )

    # Fast path reuse if contexts match and a CID exists (allow during publishing too)
    if same_encryption_context and (str(source_object.get("ipfs_cid") or "").strip()):
        logger.info(f"CopyObject fast path: reuse CID (same_bucket={same_bucket}, public={source_is_public})")
        ipfs_cid = str(source_object["ipfs_cid"])  # type: ignore[index]
        file_size = int(source_object["size_bytes"])  # type: ignore[index]
        content_type = str(source_object["content_type"])  # type: ignore[index]
        md5_hash = str(source_object["md5_hash"])  # type: ignore[index]
        metadata: dict[str, Any] = {
            "ipfs": source_metadata.get("ipfs", {}),
            "hippius": source_metadata.get("hippius", {}),
            **{k: v for k, v in source_metadata.items() if k not in ["ipfs", "hippius"]},
        }
    else:
        # Assemble bytes via ObjectReader (hydrate cache and read parts in order)
        logger.info("CopyObject assembling bytes via ObjectReader")
        obj_cache = RedisObjectPartsCache(redis_client)
        # Build full manifest and split by cid presence
        parts: list[Part] = await obj_reader.build_manifest(db, src_info)
        parts = sorted(parts, key=lambda p: p.part_number)
        cid_backed: list[Part] = [p for p in parts if p.cid]
        pending_only: list[Part] = [p for p in parts if not p.cid]

        # Hydrate CID-backed parts
        if cid_backed:
            await obj_reader.hydrate_object_cache(
                redis_client,
                obj_cache,
                src_info,
                cid_backed,
                address=request.state.account.main_account,
                seed_phrase=request.state.seed_phrase,
            )

        # Handle pending parts with bounded polling
        if pending_only:
            try:
                await obj_reader._handle_pending_parts_with_cache_fallback(  # type: ignore[attr-defined]
                    db,
                    obj_cache,
                    src_info,
                    pending_only,
                    redis_client,
                    request.state.account.main_account,
                    request.state.seed_phrase,
                )
            except DownloadNotReadyError:
                return errors.s3_error_response(
                    "SlowDown",
                    "Source object is not yet available for copying. Please retry shortly.",
                    status_code=503,
                )

        # Read all parts from cache in order (assemble plaintext for private buckets)
        data_chunks: list[bytes] = []
        for p in parts:
            # Prefer chunked meta
            meta = None
            try:
                meta = await obj_cache.get_meta(src_info.object_id, int(p.part_number))  # type: ignore[attr-defined]
            except Exception:
                meta = None
            if meta:
                num_chunks = int(meta.get("num_chunks", 0))
                if num_chunks <= 0:
                    data_chunks.append(b"")
                    continue
                chunks: list[bytes] = []
                for ci in range(num_chunks):
                    c = await obj_cache.get_chunk(src_info.object_id, int(p.part_number), ci)  # type: ignore[attr-defined]
                    if c is None:
                        return errors.s3_error_response(
                            "SlowDown",
                            "Source object is not yet available for copying. Please retry shortly.",
                            status_code=503,
                        )
                    chunks.append(c)
                if not source_is_public:
                    # Decrypt assembled ciphertext
                    ct_all = b"".join(chunks)
                    try:
                        pt = CryptoService.decrypt_part_auto(
                            ct_all,
                            seed_phrase=request.state.seed_phrase,
                            object_id=src_info.object_id,
                            part_number=int(p.part_number),
                            chunk_count=len(chunks),
                            chunk_loader=lambda i, arr=chunks: arr[i],  # type: ignore[misc]
                        )
                    except Exception:
                        # fallback per-chunk
                        parts_pt: list[bytes] = []
                        for ci, c in enumerate(chunks):
                            parts_pt.append(
                                CryptoService.decrypt_chunk(
                                    c,
                                    seed_phrase=request.state.seed_phrase,
                                    object_id=src_info.object_id,
                                    part_number=int(p.part_number),
                                    chunk_index=ci,
                                )
                            )
                        pt = b"".join(parts_pt)
                    data_chunks.append(pt)
                else:
                    data_chunks.append(b"".join(chunks))
            else:
                # Fallback to whole-part key (may be plaintext for public only)
                chunk = await obj_cache.get(src_info.object_id, int(p.part_number))
                if chunk is None:
                    return errors.s3_error_response(
                        "SlowDown",
                        "Source object is not yet available for copying. Please retry shortly.",
                        status_code=503,
                    )
                data_chunks.append(chunk)
        src_bytes = b"".join(data_chunks)

        # Publish with destination encryption context (or reuse CID if contexts match and CID exists)
        md5_hash = hashlib.md5(src_bytes).hexdigest()
        if same_encryption_context and (str(source_object.get("ipfs_cid") or "").strip()):
            ipfs_cid = str(source_object["ipfs_cid"])  # type: ignore[index]
        else:
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
                publish=config.publish_to_chain,
            )
            ipfs_cid = s3_result.cid
        file_size = len(src_bytes)
        content_type = str(source_object["content_type"])  # type: ignore[index]
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
