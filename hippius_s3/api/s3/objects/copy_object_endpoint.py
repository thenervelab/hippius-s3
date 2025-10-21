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
from hippius_s3.reader.db_meta import read_parts_manifest
from hippius_s3.reader.planner import build_chunk_plan
from hippius_s3.reader.streamer import stream_plan
from hippius_s3.repositories.buckets import BucketRepository
from hippius_s3.repositories.objects import ObjectRepository
from hippius_s3.repositories.users import UserRepository


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

    # Determine encryption context
    source_is_public = source_bucket["is_public"]
    dest_is_public = dest_bucket["is_public"]
    same_bucket = source_bucket["bucket_id"] == dest_bucket["bucket_id"]
    same_encryption_context = (source_is_public and dest_is_public) or same_bucket

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
    src_info.should_decrypt = not source_is_public

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
        logger.info("CopyObject assembling bytes via flat-plan reader")
        obj_cache = RedisObjectPartsCache(redis_client)
        src_ver = int(src_obj_row.get("object_version") or 1)
        # Build plan and stream
        # Build manifest from DB (part_number, cid, plain_size, chunk_size_bytes)
        manifest = await read_parts_manifest(db, src_info.object_id)
        # Enqueue missing indices per part
        plan = await build_chunk_plan(db, src_info.object_id, manifest, None)
        # Minimal enqueue: determine missing indices
        indices_by_part: dict[int, list[int]] = {}
        for it in plan:
            exists = await obj_cache.chunk_exists(src_info.object_id, src_ver, int(it.part_number), int(it.chunk_index))
            if not exists:
                arr = indices_by_part.setdefault(int(it.part_number), [])
                arr.append(int(it.chunk_index))
        if indices_by_part:
            from hippius_s3.queue import ChunkToDownload
            from hippius_s3.queue import DownloadChainRequest
            from hippius_s3.queue import enqueue_download_request

            dl_parts = [
                ChunkToDownload(
                    cid=str(next((c["cid"] for c in manifest if int(c.get("part_number", 0)) == pn), "")),
                    part_id=int(pn),
                    redis_key=None,
                    chunk_indices=sorted(set(idxs)),
                )
                for pn, idxs in indices_by_part.items()
            ]
            req = DownloadChainRequest(
                request_id=f"{src_info.object_id}::copy",
                object_id=src_info.object_id,
                object_version=src_ver,
                object_key=source_object_key,
                bucket_name=source_bucket_name,
                address=request.state.account.main_account,
                subaccount=request.state.account.main_account,
                subaccount_seed_phrase=request.state.seed_phrase,
                substrate_url=config.substrate_url,
                ipfs_node=config.ipfs_get_url,
                should_decrypt=not source_is_public,
                size=src_info.size_bytes,
                multipart=bool(src_info.multipart),
                chunks=dl_parts,
            )
            await enqueue_download_request(req, redis_client)

        # Stream bytes from plan
        chunks_iter = stream_plan(
            obj_cache=obj_cache,
            object_id=src_info.object_id,
            object_version=src_ver,
            plan=plan,
            should_decrypt=not source_is_public,
            seed_phrase=request.state.seed_phrase,
            sleep_seconds=float(config.http_download_sleep_loop),
            address=request.state.account.main_account,
            bucket_name=source_bucket_name,
            storage_version=int(src_obj_row.get("storage_version") or 2),
        )
        # Accumulate bytes (TODO: switch to streaming publish if supported)
        src_bytes = b"".join([piece async for piece in chunks_iter])

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
        storage_version=int(getattr(config, "target_storage_version", 3)),
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
