"""S3-compatible multipart upload implementation for handling large file uploads."""

import contextlib
import hashlib
import json
import logging
import re
import time
import uuid
from datetime import UTC
from datetime import datetime
from typing import Any
from urllib.parse import unquote

import lxml.etree as _ET  # type: ignore[import-untyped]
from fastapi import APIRouter
from fastapi import Depends
from fastapi import Request
from fastapi import Response
from opentelemetry import trace
from redis.asyncio import Redis
from starlette.requests import ClientDisconnect

from hippius_s3 import dependencies
from hippius_s3 import utils
from hippius_s3.api.s3.errors import s3_error_response
from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.config import get_config
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.queue import Chunk
from hippius_s3.queue import UploadChainRequest
from hippius_s3.queue import enqueue_upload_request
from hippius_s3.storage_version import require_supported_storage_version
from hippius_s3.utils import get_query
from hippius_s3.writer.object_writer import ObjectWriter


logger = logging.getLogger(__name__)
router = APIRouter(tags=["s3-multipart"])

config = get_config()
tracer = trace.get_tracer(__name__)

# Type checkers don't understand lxml.etree's runtime API well (Element/SubElement/tostring kwargs).
# Treat the module as Any to avoid hundreds of noisy type errors.
ET: Any = _ET


async def get_request_body(request: Request) -> bytes:
    """Get request body properly handling chunked encoding from HAProxy."""
    return await utils.get_request_body(request)


@router.api_route(
    "/{bucket_name}/{object_key:path}/",
    methods=["POST"],
    status_code=200,
    include_in_schema=True,
)
@router.api_route(
    "/{bucket_name}/{object_key:path}",
    methods=["POST"],
    status_code=200,
    include_in_schema=True,
)
async def handle_post_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
) -> Response:
    """
    Handle POST requests for objects:
    1. InitiateMultipartUpload (if ?uploads is in query params)
    2. CompleteMultipartUpload (if ?uploadId=X is in query params)
    """
    logger.info(f"[POST] {bucket_name}/{object_key} - {dict(request.query_params)}")

    # Check for uploads parameter (Initiate Multipart Upload)
    if "uploads" in request.query_params:
        with tracer.start_as_current_span("multipart.route_initiate"):
            return await initiate_multipart_upload(
                bucket_name,
                object_key,
                request,
                db,
            )

    # Check for uploadId parameter (Complete Multipart Upload)
    if "uploadId" in request.query_params:
        upload_id = request.query_params.get("uploadId")
        if upload_id is not None:
            with tracer.start_as_current_span(
                "multipart.route_complete",
                attributes={"upload_id": upload_id, "has_upload_id": True},
            ):
                return await complete_multipart_upload(
                    bucket_name,
                    object_key,
                    upload_id,
                    request,
                    db,
                )

    # Not a multipart operation we handle
    return s3_error_response("InvalidRequest", "Unsupported multipart POST request", status_code=400)


async def list_parts_internal(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: dependencies.DBConnection,
) -> Response:
    """List parts for an ongoing multipart upload (?uploadId=...)."""
    upload_id = request.query_params.get("uploadId")
    if not upload_id:
        return s3_error_response("InvalidRequest", "Missing uploadId", status_code=400)

    # Validate the multipart upload exists and matches bucket/key
    mpu = await db.fetchrow(get_query("get_multipart_upload"), upload_id)
    if not mpu:
        return s3_error_response("NoSuchUpload", "The specified upload does not exist", status_code=404)
    if mpu["is_completed"]:
        return s3_error_response("InvalidRequest", "Upload already completed", status_code=400)

    # Get object_id from multipart upload
    object_id = mpu["object_id"]

    # Check bucket and key
    if mpu["object_key"] != object_key:
        return s3_error_response("InvalidRequest", "Object key does not match upload", status_code=400)

    _ = await db.fetchrow(
        get_query("get_or_create_user_by_main_account"), request.state.account.main_account, datetime.now(UTC)
    )
    bucket = await db.fetchrow(get_query("get_bucket_by_name"), bucket_name)
    if not bucket or bucket["bucket_id"] != mpu["bucket_id"]:
        return s3_error_response("NoSuchBucket", f"Bucket {bucket_name} does not exist", status_code=404)

    # Pagination params
    max_parts_str = request.query_params.get("max-parts")
    part_marker_str = request.query_params.get("part-number-marker")
    try:
        max_parts = int(max_parts_str) if max_parts_str else 1000
    except ValueError:
        max_parts = 1000
    try:
        part_marker = int(part_marker_str) if part_marker_str else 0
    except ValueError:
        part_marker = 0

    # Fetch all parts and then apply simple pagination (DB already orders)
    all_parts = await db.fetch(
        get_query("list_parts_for_version"),
        object_id,
        int(mpu.get("current_object_version") or 1),
    )
    visible_parts = [p for p in all_parts if p["part_number"] > part_marker][:max_parts]

    is_truncated = len(visible_parts) < len([p for p in all_parts if p["part_number"] > part_marker])
    next_part_marker = visible_parts[-1]["part_number"] if visible_parts else part_marker

    # Build XML per S3 ListParts
    root = ET.Element("ListPartsResult", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")
    ET.SubElement(root, "Bucket").text = bucket_name
    ET.SubElement(root, "Key").text = object_key
    ET.SubElement(root, "UploadId").text = upload_id
    ET.SubElement(root, "PartNumberMarker").text = str(part_marker)
    ET.SubElement(root, "NextPartNumberMarker").text = str(next_part_marker)
    ET.SubElement(root, "MaxParts").text = str(max_parts)
    ET.SubElement(root, "IsTruncated").text = "true" if is_truncated else "false"

    for part in visible_parts:
        p = ET.SubElement(root, "Part")
        ET.SubElement(p, "PartNumber").text = str(part["part_number"])
        ET.SubElement(p, "ETag").text = f'"{part["etag"]}"'
        ET.SubElement(p, "Size").text = str(part["size_bytes"])
        # Use CreatedAt as LastModified if available
        ts = part.get("created_at")
        if ts:
            ET.SubElement(p, "LastModified").text = ts.isoformat()

    xml_content = ET.tostring(root, encoding="UTF-8", xml_declaration=True, pretty_print=True)
    return Response(
        content=xml_content,
        media_type="application/xml",
        headers={
            "Content-Type": "application/xml; charset=utf-8",
            "x-amz-request-id": str(uuid.uuid4()),
            "Content-Length": str(len(xml_content)),
        },
    )


async def initiate_multipart_upload(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: dependencies.DBConnection,
) -> Response:
    """Initiate a multipart upload (POST /{bucket_name}/{object_key}?uploads)."""
    try:
        # Get user for user-scoped bucket lookup
        _ = await db.fetchrow(
            get_query("get_or_create_user_by_main_account"),
            request.state.account.main_account,
            datetime.now(UTC),
        )

        # Check if bucket exists
        bucket = await db.fetchrow(
            get_query("get_bucket_by_name"),
            bucket_name,
        )
        if not bucket:
            return s3_error_response(
                "NoSuchBucket",
                f"Bucket {bucket_name} does not exist",
                status_code=404,
            )

        # Create a new multipart upload
        upload_id = str(uuid.uuid4())
        object_id = str(uuid.uuid4())  # Create object_id immediately
        initiated_at = datetime.now(UTC)
        content_type = request.headers.get(
            "Content-Type",
            "application/octet-stream",
        )

        # Extract metadata from headers and check for file size
        metadata = {}
        file_size = None
        file_mtime = None

        for key, value in request.headers.items():
            if key.lower().startswith("x-amz-meta-"):
                meta_key = key[11:]
                metadata[meta_key] = value
                # Extract mtime for file timestamp preservation
                if meta_key.lower() == "mtime":
                    with contextlib.suppress(ValueError):
                        file_mtime = float(value)
            elif key.lower() in [
                "content-length",
                "x-amz-content-length",
                "x-amz-decoded-content-length",
            ]:
                with contextlib.suppress(ValueError):
                    file_size = int(value)

        if file_size and file_size > config.max_multipart_file_size:
            return s3_error_response(
                "EntityTooLarge",
                f"Your proposed upload size {file_size} bytes exceeds the maximum "
                f"allowed object size of {config.max_multipart_file_size} bytes",
                status_code=400,
            )

        # Create initial objects row for this multipart upload
        upsert_result = await db.fetchrow(
            get_query("upsert_object_multipart"),
            object_id,
            bucket["bucket_id"],
            object_key,
            content_type,
            json.dumps(metadata),
            "",  # initial md5_hash (will be updated on completion)
            0,  # initial size_bytes (will be updated on completion)
            initiated_at,  # created_at
            config.target_storage_version,
        )

        # Use the returned object_id (will be existing one if conflict occurred)
        object_id = str(upsert_result["object_id"])

        # Create the multipart upload in the database with object_id
        await db.fetchrow(
            get_query("create_multipart_upload"),
            upload_id,
            bucket["bucket_id"],
            object_key,
            initiated_at,
            content_type,
            json.dumps(metadata),
            datetime.fromtimestamp(file_mtime, UTC) if file_mtime is not None else None,
            uuid.UUID(object_id),
        )

        # Create XML response using a hardcoded template
        xml_string = f"""<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>{bucket_name}</Bucket>
  <Key>{object_key}</Key>
  <UploadId>{upload_id}</UploadId>
</InitiateMultipartUploadResult>
"""
        xml_bytes = xml_string.encode("utf-8")

        get_metrics_collector().record_multipart_operation(
            operation="initiate_upload",
            main_account=request.state.account.main_account,
        )

        # Return response with proper headers
        return Response(
            content=xml_bytes,
            media_type="application/xml",
            headers={
                "Content-Type": "application/xml; charset=utf-8",
                "x-amz-request-id": str(uuid.uuid4()),
                "Content-Length": str(len(xml_bytes)),
            },
        )
    except Exception as e:
        logger.exception(f"Error initiating multipart upload: {e}")
        get_metrics_collector().record_error(
            error_type="internal_error",
            operation="initiate_multipart_upload",
            bucket_name=bucket_name,
            main_account=getattr(request.state, "account", None) and request.state.account.main_account,
        )
        return s3_error_response(
            "InternalError",
            f"Error initiating multipart upload: {str(e)}",
            status_code=500,
        )


async def get_all_cached_chunks(
    object_id: str,
    redis_client: Redis,
):
    """Find all cached part meta keys for an object using non-blocking SCAN."""
    try:
        keys_pattern = f"obj:{object_id}:part:*:meta"
        return [key async for key in redis_client.scan_iter(keys_pattern, count=1000)]
    except Exception as e:
        logger.error(f"Failed to find any cached parts for {object_id=}: {e}")
        return []


async def upload_part(
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
) -> Response:
    """Upload a part for a multipart upload (PUT with partNumber & uploadId)."""
    # These two parameters are required for multipart upload parts
    upload_id = request.query_params.get("uploadId")
    part_number_str = request.query_params.get("partNumber")

    # If this doesn't have both uploadId and partNumber, it's not a multipart upload part.
    if not upload_id or not part_number_str:
        return s3_error_response("InvalidRequest", "Missing uploadId or partNumber", status_code=400)

    # Validate part number format
    try:
        part_number = int(part_number_str)
    except ValueError:
        logger.error(f"Invalid part number format: '{part_number_str}'")
        return s3_error_response(
            "InvalidArgument",
            "Part number must be an integer between 1 and 10000",
            status_code=400,
        )

    if part_number < 1 or part_number > 10000:
        logger.error(f"Part number {part_number} out of range 1-10000")
        return s3_error_response(
            "InvalidArgument",
            "Part number must be an integer between 1 and 10000",
            status_code=400,
        )

    # Check if the multipart upload exists
    ongoing_multipart_upload = await db.fetchrow(
        get_query("get_multipart_upload"),
        upload_id,
    )
    if not ongoing_multipart_upload:
        return s3_error_response(
            "NoSuchUpload",
            "The specified upload does not exist",
            status_code=404,
        )

    if ongoing_multipart_upload["is_completed"]:
        return s3_error_response(
            "InvalidRequest",
            "The specified multipart upload has already been completed",
            status_code=400,
        )

    # Get object_id and current_object_version from multipart upload
    object_id = ongoing_multipart_upload["object_id"]
    current_object_version = int(ongoing_multipart_upload.get("current_object_version") or 1)

    start_time = time.time()
    logger.info(f"Starting part {part_number} upload for upload {upload_id} (object_id={object_id})")

    # Support UploadPartCopy via x-amz-copy-source
    copy_source = request.headers.get("x-amz-copy-source")
    if copy_source:
        # Parse source bucket/key (may be /bucket/key or bucket/key, URL-encoded)
        src = unquote(copy_source.strip())
        if src.startswith("/"):
            src = src[1:]
        if "/" not in src:
            return s3_error_response(
                "InvalidArgument", "x-amz-copy-source must be in format /bucket/key", status_code=400
            )
        source_bucket_name, source_object_key = src.split("/", 1)

        # Optional range header: x-amz-copy-source-range: bytes=start-end
        range_header = request.headers.get("x-amz-copy-source-range")
        range_start = None
        range_end = None
        if range_header:
            m = re.match(r"bytes=(\d+)-(\d+)$", range_header)
            if not m:
                return s3_error_response("InvalidArgument", "Invalid copy range", status_code=400)
            range_start = int(m.group(1))
            range_end = int(m.group(2))

        # Resolve source object and fetch bytes from IPFS (require CID available)
        _ = await db.fetchrow(
            get_query("get_or_create_user_by_main_account"), request.state.account.main_account, datetime.now(UTC)
        )
        source_bucket = await db.fetchrow(get_query("get_bucket_by_name"), source_bucket_name)
        if not source_bucket:
            return s3_error_response("NoSuchBucket", f"Bucket {source_bucket_name} does not exist", status_code=404)

        source_obj = await db.fetchrow(get_query("get_object_by_path"), source_bucket["bucket_id"], source_object_key)
        if not source_obj:
            return s3_error_response("NoSuchKey", f"Key {source_object_key} not found", status_code=404)

        # Always read source from IPFS service to obtain plaintext when needed
        source_bytes = None

        if source_bytes is None:
            # Read plaintext via reader pipeline (manifest → plan → stream decrypt)
            try:
                from hippius_s3.queue import DownloadChainRequest  # local import
                from hippius_s3.queue import PartToDownload  # local import
                from hippius_s3.queue import enqueue_download_request  # local import
                from hippius_s3.reader.db_meta import read_parts_manifest  # local import to avoid cycles
                from hippius_s3.reader.planner import build_chunk_plan  # local import
                from hippius_s3.reader.streamer import stream_plan  # local import
            except Exception:
                return s3_error_response("InternalError", "Reader pipeline unavailable", status_code=500)

            object_id_str = str(source_obj["object_id"])  # type: ignore[index]
            src_ver = int(source_obj.get("object_version") or 1)
            manifest = await read_parts_manifest(db, object_id_str, src_ver)
            plan = await build_chunk_plan(db, object_id_str, manifest, None, object_version=src_ver)

            # Enqueue downloader for any missing chunk indices in cache
            obj_cache = RedisObjectPartsCache(request.app.state.redis_client)
            indices_by_part: dict[int, list[int]] = {}
            for it in plan:
                exists = await obj_cache.chunk_exists(object_id_str, src_ver, int(it.part_number), int(it.chunk_index))
                if not exists:
                    arr = indices_by_part.setdefault(int(it.part_number), [])
                    arr.append(int(it.chunk_index))
            if indices_by_part:
                dl_parts: list[PartToDownload] = []
                for pn, idxs in indices_by_part.items():
                    try:
                        rows = await db.fetch(
                            get_query("get_part_chunks_by_object_and_number"),
                            object_id_str,
                            src_ver,
                            int(pn),
                        )
                        all_entries = [
                            (int(r[0]), str(r[1]), int(r[2]) if r[2] is not None else None) for r in rows or []
                        ]
                        chunk_specs = []
                        include = {int(i) for i in idxs}
                        for ci, cid, clen in all_entries:
                            if int(ci) in include:
                                chunk_specs.append(
                                    {
                                        "index": int(ci),
                                        "cid": str(cid),
                                        "cipher_size_bytes": int(clen) if clen is not None else None,
                                    }
                                )
                        if not chunk_specs:
                            continue
                        dl_parts.append(
                            PartToDownload(part_number=int(pn), chunks=chunk_specs)  # type: ignore[arg-type]
                        )
                    except Exception:
                        continue
                req = DownloadChainRequest(
                    request_id=f"{object_id_str}::upload_part_copy",
                    object_id=object_id_str,
                    object_version=src_ver,
                    object_storage_version=int(source_obj.get("storage_version") or 0),
                    object_key=source_object_key,
                    bucket_name=source_bucket_name,
                    address=request.state.account.main_account,
                    subaccount=request.state.account.main_account,
                    subaccount_seed_phrase=request.state.seed_phrase,
                    substrate_url=config.substrate_url,
                    size=int(source_obj.get("size_bytes") or 0),
                    multipart=bool((json.loads(source_obj.get("metadata") or "{}") or {}).get("multipart", False)),
                    chunks=dl_parts,
                )
                await enqueue_download_request(req)

            # Stream plaintext bytes
            source_storage_version = require_supported_storage_version(int(source_obj["storage_version"]))
            chunks_iter = stream_plan(
                obj_cache=obj_cache,
                object_id=object_id_str,
                object_version=src_ver,
                plan=plan,
                sleep_seconds=config.http_download_sleep_loop,
                address=request.state.account.main_account,
                bucket_name=source_bucket_name,
                storage_version=source_storage_version,
            )
            try:
                source_bytes = b"".join([piece async for piece in chunks_iter])
            except Exception as e:
                logger.warning(f"UploadPartCopy failed to stream source via reader: {e}")
                return s3_error_response("ServiceUnavailable", "Failed to read source", status_code=503)

        if range_start is not None and range_end is not None:
            if range_start < 0 or range_end < range_start or range_end >= len(source_bytes):
                return s3_error_response("InvalidRange", "Copy range invalid", status_code=416)
            part_bytes = source_bytes[range_start : range_end + 1]
        else:
            part_bytes = source_bytes

        file_data = part_bytes
        # Debug: log copy slice info
        try:
            md5_copy = hashlib.md5(file_data).hexdigest()
            logger.info(
                f"UploadPartCopy slice: upload_id={upload_id} part={part_number} src={source_bucket_name}/{source_object_key} "
                f"range={range_start}-{range_end} len={len(file_data)} md5={md5_copy}"
            )
        except Exception:
            logger.debug("Failed to compute md5 for copy slice", exc_info=True)
        # Proceed as normal part write below
    else:
        # Read request body for regular UploadPart
        try:
            body_start = time.time()
            file_data = await get_request_body(request)
            body_time = time.time() - body_start
            logger.debug(f"Part {part_number}: Got request body in {body_time:.3f}s, size: {len(file_data)} bytes")
        except ClientDisconnect:
            logger.warning(f"Client disconnected during part {part_number} upload for upload {upload_id}")

            # Clean up any cached parts for this upload when client disconnects
            keys = await get_all_cached_chunks(
                upload_id,
                request.app.state.redis_client,
            )
            if keys:
                await request.app.state.redis_client.delete(*keys)
                logger.info(f"Cleaned up {len(keys)} cached parts for disconnected upload {upload_id}")

            return s3_error_response(
                "RequestTimeout",
                "Client disconnected during upload",
                status_code=408,
            )

    # (file_data is set either by copy path or regular body read)

    # Enforce AWS S3 minimum part size (except for final part)
    # Allow parts smaller than 5MB - AWS S3 allows this for the final part
    # We don't have a way to know if this is the final part until completion,
    # so we'll be permissive here and let completion validation handle it
    file_size = len(file_data)

    if file_size == 0:
        return s3_error_response(
            "InvalidArgument",
            "Zero-length part not allowed",
            status_code=400,
        )

    if file_size > config.max_multipart_chunk_size:
        return s3_error_response(
            "EntityTooLarge",
            f"Part size {file_size} bytes exceeds maximum {config.max_multipart_chunk_size} bytes",
            status_code=400,
        )

    # Cache part data in chunked layout via ObjectWriter (no IPFS upload for parts)
    redis_client = request.app.state.redis_client
    obj_cache = RedisObjectPartsCache(redis_client)

    try:
        # Check if client is still connected before proceeding
        disconnect_start = time.time()
        if await request.is_disconnected():
            logger.warning(f"Client disconnected before caching part {part_number} for upload {upload_id}")
            return s3_error_response(
                "RequestTimeout",
                "Client disconnected during upload",
                status_code=408,
            )
        disconnect_time = time.time() - disconnect_start
        logger.debug(f"Part {part_number}: Disconnect check took {disconnect_time:.3f}s")

        # Store in Redis (debug: log about to cache)
        try:
            import hashlib as _hashlib

            md5_pre_cache = _hashlib.md5(file_data).hexdigest()
            head_hex = file_data[:8].hex() if file_data else ""
            tail_hex = file_data[-8:].hex() if len(file_data) >= 8 else head_hex
            logger.debug(
                f"About to cache part: upload_id={upload_id} part={part_number} len={len(file_data)} md5={md5_pre_cache} head8={head_hex} tail8={tail_hex}"
            )
        except Exception:
            logger.debug("Failed to compute md5 for pre-cache log", exc_info=True)
        # Resolve destination bucket name for key lookup
        try:
            row = await db.fetchrow(
                """
                SELECT b.bucket_name
                FROM multipart_uploads mu
                JOIN buckets b ON b.bucket_id = mu.bucket_id
                WHERE mu.upload_id = $1
                LIMIT 1
                """,
                upload_id,
            )
        except Exception:
            row = None
        if not row:
            logger.error(f"Upload row not found for upload_id={upload_id}; refusing to cache part")
            return s3_error_response("NoSuchUpload", "The specified upload does not exist.", status_code=404)
        dest_bucket_name = row.get("bucket_name")

        # Store in Redis via chunked cache API (encrypt for private, meta-first for readiness)
        redis_start = time.time()
        # Route through ObjectWriter for standardized behavior
        writer = ObjectWriter(db=db, redis_client=redis_client, fs_store=request.app.state.fs_store)
        part_res = await writer.mpu_upload_part(
            upload_id=str(upload_id),
            object_id=str(object_id),
            object_version=int(current_object_version),
            bucket_name=str(dest_bucket_name or ""),
            account_address=request.state.account.main_account,
            seed_phrase=request.state.seed_phrase,
            part_number=int(part_number),
            body_bytes=file_data,
        )
        redis_time = time.time() - redis_start
        logger.debug(
            f"Part {part_number}: Cached via RedisObjectPartsCache in {redis_time:.3f}s (object_id={object_id}, encrypted=True)"
        )

        # Create mock part result (no IPFS upload needed for individual parts)
        etag = part_res.etag

        part_result = {
            "size_bytes": file_size,
            "etag": etag,
            "part_number": part_number,
        }

        # Save the part information in the database
        db_start = time.time()
        # (placeholder upsert already handled by writer)
        db_time = time.time() - db_start
        logger.debug(f"Part {part_number}: Database insert took {db_time:.3f}s")

        total_time = time.time() - start_time
        logger.debug(f"Part {part_number}: TOTAL processing time: {total_time:.3f}s")

        get_metrics_collector().record_multipart_operation(
            operation="upload_part",
            main_account=request.state.account.main_account,
        )
        get_metrics_collector().record_data_transfer(
            operation="upload_part",
            bytes_transferred=file_size,
            bucket_name=ongoing_multipart_upload.get("bucket_name", ""),
            main_account=request.state.account.main_account,
            subaccount_id=request.state.account.id,
        )

        # Return response
        if copy_source:
            # AWS-style XML body for UploadPartCopy
            xml = f"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<CopyPartResult>
  <ETag>\"{part_result["etag"]}\"</ETag>
  <LastModified>{datetime.now(UTC).isoformat()}</LastModified>
</CopyPartResult>
""".encode("utf-8")
            return Response(content=xml, media_type="application/xml", status_code=200)
        return Response(
            status_code=200,
            headers={"ETag": f'"{part_result["etag"]}"'},
        )

    except Exception:
        # If any error occurs, clean up the Redis keys for this part
        try:
            # Delete meta and any chunk keys
            try:
                # Delete versioned meta and chunk keys using cache helpers
                object_version = int(ongoing_multipart_upload.get("current_object_version") or 1)
                delegate = RedisObjectPartsCache(redis_client)
                meta_key = delegate.build_meta_key(str(object_id), object_version, int(part_number))
                await redis_client.delete(meta_key)
                base_key = delegate.build_key(str(object_id), object_version, int(part_number))
                async for k in redis_client.scan_iter(f"{base_key}:chunk:*", count=1000):
                    await redis_client.delete(k)
            except Exception:
                pass
        except Exception as cleanup_error:
            logger.error(f"Failed to cleanup Redis key after error: {cleanup_error}")

        raise


@router.delete("/{bucket_name}/{object_key:path}", status_code=204)
async def abort_multipart_upload(
    _: str,
    __: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
) -> Response:
    """Abort a multipart upload (DELETE with uploadId)."""
    upload_id = request.query_params.get("uploadId")
    if not upload_id:
        return s3_error_response(
            "NoSuchUpload",
            "The specified upload does not exist",
            status_code=404,
        )
    try:
        # Get the multipart upload information
        multipart_upload = await db.fetchrow(
            get_query("get_multipart_upload"),
            upload_id,
        )
        if not multipart_upload:
            return s3_error_response(
                "NoSuchUpload",
                "The specified upload does not exist",
                status_code=404,
            )

        # Get object_id from multipart upload
        object_id = multipart_upload["object_id"]

        # Clean up Redis keys for cached parts (meta + chunks)
        object_version = int(multipart_upload.get("current_object_version") or 1)
        parts = await db.fetch(
            get_query("list_parts_for_version"),
            object_id,
            object_version,
        )
        if parts:
            redis_client = request.app.state.redis_client
            delegate = RedisObjectPartsCache(redis_client)
            for part in parts:
                part_num = int(part["part_number"])
                meta_key = delegate.build_meta_key(str(object_id), object_version, part_num)
                await redis_client.delete(meta_key)
                base_key = delegate.build_key(str(object_id), object_version, part_num)
                async for key in redis_client.scan_iter(f"{base_key}:chunk:*"):
                    await redis_client.delete(key)

        # Fully remove the multipart upload (and cascade parts) so it disappears from listings immediately
        async with db.transaction():
            await db.fetchrow(
                get_query("abort_multipart_upload"),
                upload_id,
            )
        # Mark aborted in Redis so listings immediately hide this upload (defensive against read lag)
        with contextlib.suppress(Exception):
            await request.app.state.redis_client.setex(f"aborted_mpu:{upload_id}", 300, "1")
        return Response(status_code=204)

    except Exception as e:
        logger.error(f"Error aborting multipart upload: {e}")
        return s3_error_response(
            "InternalError",
            "We encountered an internal error",
            status_code=500,
        )


# Multipart uploads function properly organized in dedicated multipart module
# This maintains separation of concerns and avoids conflicts with other GET handlers


async def list_multipart_uploads(
    bucket_name: str,
    request: Request,
    db: dependencies.DBConnection,
) -> Response:
    """List multipart uploads in a bucket (GET with ?uploads)."""

    try:
        # Get user for user-scoped bucket lookup
        _ = await db.fetchrow(
            get_query("get_or_create_user_by_main_account"),
            request.state.account.main_account,
            datetime.now(UTC),
        )

        bucket = await db.fetchrow(
            get_query("get_bucket_by_name"),
            bucket_name,
        )
        if not bucket:
            return s3_error_response(
                "NoSuchBucket",
                f"Bucket {bucket_name} does not exist",
                status_code=404,
            )

        # List multipart uploads
        uploads = await db.fetch(
            get_query("list_multipart_uploads"), bucket["bucket_id"], request.query_params.get("prefix")
        )

        # Filter out any uploads that were very recently aborted (defensive cache for race conditions)
        try:
            redis_client: Redis = request.app.state.redis_client
            filtered_uploads = []
            for upload in uploads:
                aborted_flag = await redis_client.get(f"aborted_mpu:{str(upload['upload_id'])}")
                if aborted_flag:
                    continue
                filtered_uploads.append(upload)
            uploads = filtered_uploads
        except Exception as _:
            # If Redis not available, proceed with DB results
            pass

        # Generate the response XML
        root = ET.Element("ListMultipartUploadsResult", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")
        ET.SubElement(root, "Bucket").text = bucket_name
        ET.SubElement(root, "KeyMarker").text = ""
        ET.SubElement(root, "UploadIdMarker").text = ""
        ET.SubElement(root, "NextKeyMarker").text = ""
        ET.SubElement(root, "NextUploadIdMarker").text = ""
        ET.SubElement(root, "MaxUploads").text = "1000"
        ET.SubElement(root, "IsTruncated").text = "false"

        # Add Upload elements
        for upload in uploads:
            upload_elem = ET.SubElement(root, "Upload")
            ET.SubElement(upload_elem, "Key").text = (
                str(upload["object_key"]) if upload.get("object_key") is not None else ""
            )
            # Ensure UploadId is a string (DB may return uuid.UUID)
            ET.SubElement(upload_elem, "UploadId").text = (
                str(upload["upload_id"]) if upload.get("upload_id") is not None else ""
            )
            ET.SubElement(upload_elem, "Initiated").text = upload["initiated_at"].isoformat()

        # Generate XML with proper declaration
        xml_content = ET.tostring(
            root,
            encoding="UTF-8",
            xml_declaration=True,
            pretty_print=True,
        )

        # Return with proper headers
        return Response(
            content=xml_content,
            media_type="application/xml",
            headers={
                "Content-Type": "application/xml; charset=utf-8",
                "x-amz-request-id": str(uuid.uuid4()),
                "Content-Length": str(len(xml_content)),
            },
        )
    except Exception as e:
        logger.error(f"Error listing multipart uploads: {e}")
        return s3_error_response(
            "InternalError",
            "Error listing multipart uploads",
            status_code=500,
        )


async def hash_all_etags(
    object_id: str,
    object_version: int,
    db: dependencies.DBConnection,
) -> str:
    parts = await db.fetch(
        get_query("get_parts_etags_for_version"),
        object_id,
        object_version,
    )

    etags = [part["etag"].split("-")[0] for part in parts]
    # Convert hex ETags to binary and concatenate them (S3 multipart algorithm)
    binary_etags = b"".join(bytes.fromhex(etag) for etag in etags)
    combined_etag = hashlib.md5(binary_etags).hexdigest()

    return f"{combined_etag}-{len(parts)}"


async def complete_multipart_upload(
    bucket_name: str,
    object_key: str,
    upload_id: str,
    request: Request,
    db: dependencies.DBConnection,
) -> Response:
    """Internal implementation of multipart upload completion logic."""
    try:
        # Validate the multipart upload exists
        multipart_upload = await db.fetchrow(get_query("get_multipart_upload"), upload_id)
        if not multipart_upload:
            return s3_error_response(
                "NoSuchUpload",
                "The specified upload does not exist",
                status_code=404,
            )

        # Get object_id from multipart upload
        object_id = multipart_upload["object_id"]

        # Get bucket info
        bucket = await db.fetchrow(
            get_query("get_bucket_by_name"),
            bucket_name,
        )
        if not bucket:
            return s3_error_response("NoSuchBucket", f"Bucket {bucket_name} does not exist", status_code=404)

        if multipart_upload["is_completed"]:
            # Return success response for already completed uploads (idempotent)
            # This prevents AWS CLI retries from failing
            # Get the final ETag from the objects table
            completed_object = await db.fetchrow(
                get_query("get_object_by_path"),
                bucket["bucket_id"],
                object_key,
            )
            final_etag = None
            if completed_object and completed_object.get("md5_hash"):
                final_etag = completed_object["md5_hash"]
            else:
                # Fallback: recompute combined ETag from parts by object_id
                try:
                    object_version = int(multipart_upload.get("current_object_version") or 1)
                    final_etag = await hash_all_etags(object_id, object_version, db)
                except Exception:
                    final_etag = "completed"
            xml_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Location>http://localhost:8000/{bucket_name}/{object_key}</Location>
    <Bucket>{bucket_name}</Bucket>
    <Key>{object_key}</Key>
    <ETag>"{final_etag}"</ETag>
</CompleteMultipartUploadResult>"""
            return Response(
                content=xml_content,
                media_type="application/xml",
                status_code=200,
            )

        # Parse the XML request body to get parts list
        body_text = ""
        try:
            body_text = (await get_request_body(request)).decode("utf-8")
        except Exception:
            body_text = "<CompleteMultipartUpload></CompleteMultipartUpload>"

        # Parse part numbers and ETags
        part_numbers = re.findall(r"<PartNumber>(\d+)</PartNumber>", body_text)
        etags = re.findall(r"<ETag>([^<]+)</ETag>", body_text)

        if not part_numbers or not etags or len(part_numbers) != len(etags):
            return s3_error_response(
                "InvalidRequest",
                "The XML provided was not well-formed",
                status_code=400,
            )

        # Create part info list
        part_info = []
        for num, tag in zip(
            part_numbers,
            etags,
            strict=True,
        ):
            part_info.append(
                (
                    int(num),
                    tag.replace('"', "").strip(),
                ),
            )
        part_info.sort(key=lambda x: x[0])

        # Get the parts from the database for the version captured on MPU initiation
        object_version = int(multipart_upload.get("current_object_version") or 1)
        db_parts = await db.fetch(
            get_query("list_parts_for_version"),
            object_id,
            object_version,
        )
        logger.info(f"Found {len(db_parts)} parts for upload {upload_id} (object_id={object_id})")
        db_parts_dict = {p["part_number"]: p for p in db_parts}

        if not db_parts_dict:
            logger.error(f"No parts found for multipart upload {upload_id}")
            return s3_error_response(
                "InvalidRequest",
                "No parts found for this multipart upload",
                status_code=400,
            )

        # Check that all parts exist
        missing_parts = [pn for pn, _ in part_info if pn not in db_parts_dict]
        if missing_parts:
            return s3_error_response(
                "InvalidPart",
                f"One or more parts could not be found: {', '.join(map(str, missing_parts))}",
                status_code=400,
            )

        writer = ObjectWriter(db=db, redis_client=request.app.state.redis_client, fs_store=request.app.state.fs_store)
        complete_res = await writer.mpu_complete(
            bucket_name=bucket_name,
            object_id=str(object_id),
            object_key=object_key,
            upload_id=str(upload_id),
            object_version=int(object_version),
            address=request.state.account.main_account,
            seed_phrase=request.state.seed_phrase,
        )

        # After commit, enqueue background publish
        parts = await db.fetch(
            get_query("list_parts_for_version"),
            object_id,
            object_version,
        )
        ray_id = getattr(request.state, "ray_id", None)
        await enqueue_upload_request(
            UploadChainRequest(
                address=request.state.account.id,
                bucket_name=bucket_name,
                object_key=object_key,
                object_id=str(object_id),
                object_version=int(object_version),
                chunks=[
                    Chunk(
                        id=part["part_number"],
                    )
                    for part in parts
                ],
                upload_id=upload_id,
                ray_id=ray_id,
            ),
        )

        # Create XML response
        xml_bytes = f"""<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Location>http://{request.headers.get("Host", "")}/{bucket_name}/{object_key}</Location>
  <Bucket>{bucket_name}</Bucket>
  <Key>{object_key}</Key>
  <ETag>"{complete_res.etag}"</ETag>
</CompleteMultipartUploadResult>
""".encode("utf-8")

        get_metrics_collector().record_multipart_operation(
            operation="complete_upload",
            main_account=request.state.account.main_account,
        )
        get_metrics_collector().record_s3_operation(
            operation="complete_multipart_upload",
            bucket_name=bucket_name,
            main_account=request.state.account.main_account,
            subaccount_id=request.state.account.id,
            success=True,
        )
        get_metrics_collector().record_data_transfer(
            operation="complete_multipart_upload",
            bytes_transferred=int(complete_res.size_bytes),
            bucket_name=bucket_name,
            main_account=request.state.account.main_account,
            subaccount_id=request.state.account.id,
        )

        # Return with proper headers
        return Response(
            content=xml_bytes,
            media_type="application/xml",
            headers={
                "Content-Type": "application/xml; charset=utf-8",
                "Content-Length": str(len(xml_bytes)),
            },
        )
    except Exception as e:
        logger.exception(f"Error completing multipart upload: {e}")
        get_metrics_collector().record_error(
            error_type="internal_error",
            operation="complete_multipart_upload",
            bucket_name=bucket_name,
            main_account=getattr(request.state, "account", None) and request.state.account.main_account,
        )
        return s3_error_response(
            "InternalError",
            f"Error completing multipart upload: {str(e)}",
            status_code=500,
        )
