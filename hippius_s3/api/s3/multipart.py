"""S3-compatible multipart upload implementation for handling large file uploads."""

import asyncio
import contextlib
import hashlib
import json
import logging
import re
import time
import uuid
from datetime import UTC
from datetime import datetime
from urllib.parse import unquote

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Request
from fastapi import Response
from lxml import etree as ET
from redis.asyncio import Redis
from starlette.requests import ClientDisconnect

from hippius_s3 import dependencies
from hippius_s3 import utils
from hippius_s3.api.s3.errors import s3_error_response
from hippius_s3.config import get_config
from hippius_s3.dependencies import get_object_reader
from hippius_s3.queue import Chunk
from hippius_s3.queue import MultipartUploadChainRequest
from hippius_s3.queue import enqueue_upload_request
from hippius_s3.services.object_reader import ObjectReader
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)
router = APIRouter(tags=["s3-multipart"])

config = get_config()


async def get_all_chunk_ids(
    upload_id: str,
    db: dependencies.DBConnection,
) -> list[Chunk]:
    """Get all chunk IDs and Redis keys for a multipart upload."""
    parts = await db.fetch(
        get_query("list_parts"),
        upload_id,
    )

    chunks = []
    for part in parts:
        chunk = Chunk(
            id=part["part_id"],
            redis_key=f"obj:{upload_id}:part:{part['part_number']}",
        )
        chunks.append(chunk)

    return chunks


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
            return await complete_multipart_upload(
                bucket_name,
                object_key,
                upload_id,
                request,
                db,
            )

    # Not a multipart operation we handle
    return None


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

    # Check bucket and key
    if mpu["object_key"] != object_key:
        return s3_error_response("InvalidRequest", "Object key does not match upload", status_code=400)

    user = await db.fetchrow(
        get_query("get_or_create_user_by_main_account"), request.state.account.main_account, datetime.now(UTC)
    )
    bucket = await db.fetchrow(get_query("get_bucket_by_name_and_owner"), bucket_name, user["main_account_id"])
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
    all_parts = await db.fetch(get_query("list_parts"), upload_id)
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
        user = await db.fetchrow(
            get_query("get_or_create_user_by_main_account"),
            request.state.account.main_account,
            datetime.now(UTC),
        )

        # Check if bucket exists
        bucket = await db.fetchrow(
            get_query("get_bucket_by_name_and_owner"),
            bucket_name,
            user["main_account_id"],
        )
        if not bucket:
            return s3_error_response(
                "NoSuchBucket",
                f"Bucket {bucket_name} does not exist",
                status_code=404,
            )

        # Create a new multipart upload
        upload_id = str(uuid.uuid4())
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

        # Create the multipart upload in the database
        await db.fetchrow(
            get_query("create_multipart_upload"),
            upload_id,
            bucket["bucket_id"],
            object_key,
            initiated_at,
            content_type,
            json.dumps(metadata),
            datetime.fromtimestamp(file_mtime, UTC) if file_mtime is not None else None,
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
        return s3_error_response(
            "InternalError",
            f"Error initiating multipart upload: {str(e)}",
            status_code=500,
        )


async def get_all_cached_chunks(
    upload_id: str,
    redis_client: Redis,
):
    try:
        keys_pattern = f"obj:{upload_id}:part:*"
        return await redis_client.keys(keys_pattern)
    except Exception as e:
        logger.error(f"Failed to find any cached parts for {upload_id=}: {e}")


@router.put(
    "/{bucket_name}/{object_key:path}/",
    status_code=200,
    include_in_schema=True,
)
@router.put(
    "/{bucket_name}/{object_key:path}",
    status_code=200,
    include_in_schema=True,
)
async def upload_part(
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
    ipfs_service=Depends(dependencies.get_ipfs_service),
    object_reader: ObjectReader = Depends(get_object_reader),
) -> Response:
    """Upload a part for a multipart upload (PUT with partNumber & uploadId)."""
    # These two parameters are required for multipart upload parts
    upload_id = request.query_params.get("uploadId")
    part_number_str = request.query_params.get("partNumber")

    # If this doesn't have both uploadId and partNumber, it's not a multipart upload part.
    # We should return None to allow the main S3 router to handle it
    if not upload_id or not part_number_str:
        return None  # Allow the request to fall through to the main S3 router

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

    start_time = time.time()
    logger.info(f"Starting part {part_number} upload for upload {upload_id}")

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
        user = await db.fetchrow(
            get_query("get_or_create_user_by_main_account"), request.state.account.main_account, datetime.now(UTC)
        )
        source_bucket = await db.fetchrow(
            get_query("get_bucket_by_name_and_owner"), source_bucket_name, user["main_account_id"]
        )
        if not source_bucket:
            return s3_error_response("NoSuchBucket", f"Bucket {source_bucket_name} does not exist", status_code=404)

        source_obj = await db.fetchrow(get_query("get_object_by_path"), source_bucket["bucket_id"], source_object_key)
        if not source_obj:
            return s3_error_response("NoSuchKey", f"Key {source_object_key} not found", status_code=404)

        # Prefer unified cache via ObjectReader, fallback to IPFS through service
        source_bytes = None
        try:
            source_bytes = await request.app.state.obj_cache.get(str(source_obj["object_id"]), 0)
        except Exception as e:
            logger.debug(f"ObjectReader cache base read miss: {e}")
            # Fallback: try unified object-parts cache directly (part 0)
            try:
                source_bytes = await request.app.state.obj_cache.get(str(source_obj["object_id"]), 0)
                if source_bytes:
                    logger.info(
                        f"UploadPartCopy fallback cache hit object_id={source_obj['object_id']} part=0 bytes={len(source_bytes)}"
                    )
            except Exception:
                pass

        if source_bytes is None:
            source_cid = (source_obj.get("ipfs_cid") or "").strip()
            if not source_cid:
                return s3_error_response(
                    "ServiceUnavailable",
                    "Source object not yet available for copy",
                    status_code=503,
                )

            # Download full bytes then slice per range if provided
            # Do not support UploadPartCopy from encrypted sources yet
            if not source_bucket.get("is_public"):
                return s3_error_response(
                    "NotImplemented",
                    "UploadPartCopy is not supported for encrypted source objects yet",
                    status_code=501,
                )
            decrypt = False
            try:
                source_bytes = await ipfs_service.download_file(
                    cid=source_cid,
                    subaccount_id=request.state.account.main_account,
                    bucket_name=source_bucket_name,
                    decrypt=decrypt,
                    seed_phrase=request.state.seed_phrase,
                )
            except Exception as e:
                logger.warning(f"UploadPartCopy failed to download source: {e}")
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

    # Cache raw part data for concatenation at completion (no IPFS upload for parts)
    redis_client = request.app.state.redis_client
    part_key = f"obj:{upload_id}:part:{part_number}"

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
        # Store in Redis
        redis_start = time.time()
        await redis_client.setex(
            part_key,
            1800,
            file_data,
        )  # 30 minute TTL
        redis_time = time.time() - redis_start
        logger.debug(f"Part {part_number}: Redis setex took {redis_time:.3f}s (key={part_key})")

        # Create mock part result (no IPFS upload needed for individual parts)
        md5_start = time.time()
        # Move MD5 calculation to thread pool to avoid blocking event loop
        loop = asyncio.get_event_loop()
        md5_hash = await loop.run_in_executor(None, lambda: hashlib.md5(file_data).hexdigest())
        md5_time = time.time() - md5_start
        logger.debug(f"Part {part_number}: MD5 calculation took {md5_time:.3f}s (threaded) md5={md5_hash}")

        etag = f"{md5_hash}-{part_number}" if not copy_source else md5_hash

        part_result = {
            "size_bytes": file_size,
            "etag": etag,
            "part_number": part_number,
        }

        # Save the part information in the database
        db_start = time.time()
        await db.fetchrow(
            get_query("upload_part"),
            str(uuid.uuid4()),
            upload_id,
            part_number,
            "",
            # cid not yet available
            part_result["size_bytes"],
            part_result["etag"],
            datetime.now(UTC),
        )
        db_time = time.time() - db_start
        logger.debug(f"Part {part_number}: Database insert took {db_time:.3f}s")

        total_time = time.time() - start_time
        logger.debug(f"Part {part_number}: TOTAL processing time: {total_time:.3f}s")

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
        # If any error occurs, clean up the Redis key
        try:
            await redis_client.delete(part_key)
        except Exception as cleanup_error:
            logger.error(f"Failed to cleanup Redis key after error: {cleanup_error}")

        raise


@router.delete("/{bucket_name}/{object_key:path}", status_code=204)
async def abort_multipart_upload(
    _: str,
    __: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
    ___=Depends(dependencies.get_ipfs_service),
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

        # Clean up Redis keys for cached parts
        parts = await db.fetch(
            get_query("list_parts"),
            upload_id,
        )
        if parts:
            redis_client = request.app.state.redis_client
            redis_keys_to_delete = [f"obj:{upload_id}:part:{part['part_number']}" for part in parts]
            if redis_keys_to_delete:
                await redis_client.delete(
                    *redis_keys_to_delete,
                )

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
        user = await db.fetchrow(
            get_query("get_or_create_user_by_main_account"),
            request.state.account.main_account,
            datetime.now(UTC),
        )

        bucket = await db.fetchrow(
            get_query("get_bucket_by_name_and_owner"),
            bucket_name,
            user["main_account_id"],
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
    upload_id: str,
    db: dependencies.DBConnection,
) -> str:
    parts = await db.fetch(
        get_query("get_parts_etags"),
        upload_id,
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

        if multipart_upload["is_completed"]:
            # Return success response for already completed uploads (idempotent)
            # This prevents AWS CLI retries from failing
            xml_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Location>http://localhost:8000/{bucket_name}/{object_key}</Location>
    <Bucket>{bucket_name}</Bucket>
    <Key>{object_key}</Key>
    <ETag>"{multipart_upload.get("final_etag", "completed")}"</ETag>
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

        # Get the parts from the database
        db_parts = await db.fetch(
            get_query("list_parts"),
            upload_id,
        )
        logger.info(f"Found {len(db_parts)} parts for upload {upload_id}")
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

        # Get content type
        content_type = multipart_upload["content_type"] or "application/octet-stream"

        # Get bucket info to check if it's public
        bucket = await db.fetchrow(
            get_query("get_bucket_by_name_and_owner"),
            bucket_name,
            request.state.account.main_account,
        )

        final_md5_hash = await hash_all_etags(
            upload_id,
            db,
        )
        logger.info(f"MPU complete: upload_id={upload_id} key={object_key} final_etag={final_md5_hash}")

        # Upload concatenated file to IPFS
        # Re-enable encryption for multipart uploads based on bucket privacy
        should_encrypt = not bucket["is_public"]

        # Prepare metadata
        bucket_id = multipart_upload["bucket_id"]

        metadata = multipart_upload["metadata"] if multipart_upload["metadata"] else {}
        if isinstance(metadata, str):
            metadata = json.loads(metadata)

        # Add metadata for final concatenated file
        metadata.update(
            {
                "multipart": True,
                "encrypted": should_encrypt,
            }
        )

        # Set file creation time to current timestamp
        file_created_at = datetime.now(UTC)

        # Mark the upload as completed
        await db.fetchrow(
            get_query("complete_multipart_upload"),
            upload_id,
        )

        # Delete any existing object with same key
        await db.execute(
            "DELETE FROM objects WHERE bucket_id = $1 AND object_key = $2",
            bucket_id,
            object_key,
        )

        # Calculate total size from all parts
        parts = await db.fetch(
            get_query("list_parts"),
            upload_id,
        )
        total_size = sum(part["size_bytes"] for part in parts)
        logger.info(
            f"MPU sizes: upload_id={upload_id} total_size={total_size} parts={[p['size_bytes'] for p in parts]}"
        )
        # Also log each part's ETag and size for debugging
        try:
            part_rows = await db.fetch(get_query("list_parts"), upload_id)
            dbg = [
                {
                    "part": r["part_number"],
                    "size": r["size_bytes"],
                    "etag": r["etag"],
                }
                for r in part_rows
            ]
            logger.info(f"MPU parts detail: upload_id={upload_id} parts={dbg}")
        except Exception:
            logger.debug("Failed to log parts detail after MPU", exc_info=True)

        # Create the object in database (without CID initially for multipart uploads)
        object_result = await db.fetchrow(
            get_query("upsert_object_multipart"),
            upload_id,
            bucket_id,
            object_key,
            content_type,
            json.dumps(metadata),
            final_md5_hash,
            total_size,
            file_created_at,
        )

        # Update multipart_uploads table with the object_id
        await db.execute(
            "UPDATE multipart_uploads SET object_id = $1 WHERE upload_id = $2",
            object_result["object_id"],
            upload_id,
        )

        # Update all parts for this upload to have the correct object_id
        await db.execute(
            "UPDATE parts SET object_id = $1 WHERE upload_id = $2",
            object_result["object_id"],
            upload_id,
        )

        await enqueue_upload_request(
            MultipartUploadChainRequest(
                object_key=object_key,
                bucket_name=bucket_name,
                multipart_upload_id=upload_id,
                chunks=[
                    Chunk(
                        id=part["part_number"],
                        redis_key=f"obj:{upload_id}:part:{part['part_number']}",
                    )
                    for part in parts
                ],
                substrate_url=config.substrate_url,
                ipfs_node=config.ipfs_store_url,
                address=request.state.account.id,
                # use main account instead of subaccount to make encryption
                # be based on main account not only readable by subaccounts
                subaccount=request.state.account.main_account,
                subaccount_seed_phrase=request.state.seed_phrase,
                should_encrypt=should_encrypt,
                object_id=str(object_result["object_id"]),
            ),
            request.app.state.redis_client,
        )

        # Create XML response
        xml_bytes = f"""<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Location>http://{request.headers.get("Host", "")}/{bucket_name}/{object_key}</Location>
  <Bucket>{bucket_name}</Bucket>
  <Key>{object_key}</Key>
  <ETag>"{final_md5_hash}"</ETag>
</CompleteMultipartUploadResult>
""".encode("utf-8")

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
        return s3_error_response(
            "InternalError",
            f"Error completing multipart upload: {str(e)}",
            status_code=500,
        )
