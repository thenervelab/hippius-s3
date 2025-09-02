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
from hippius_s3.queue import Chunk
from hippius_s3.queue import MultipartUploadChainRequest
from hippius_s3.queue import enqueue_upload_request
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
            redis_key=f"multipart:{upload_id}:part:{part['part_number']}",
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
        keys_pattern = f"multipart:{upload_id}:part:*"
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

    # Upload the part data to IPFS
    start_time = time.time()
    logger.info(f"Starting part {part_number} upload for upload {upload_id}")

    try:
        body_start = time.time()
        file_data = await get_request_body(request)
        body_time = time.time() - body_start
        logger.info(f"Part {part_number}: Got request body in {body_time:.3f}s, size: {len(file_data)} bytes")
    except ClientDisconnect:
        logger.warning(f"Client disconnected during part {part_number} upload for upload {upload_id}")

        # Clean up any cached parts for this upload when client disconnects
        keys = await get_all_cached_chunks(
            upload_id,
            request.app.state.redis_client,
        )
        if keys:
            await request.state.redis_client.delete(*keys)
            logger.info(f"Cleaned up {len(keys)} cached parts for disconnected upload {upload_id}")

        return s3_error_response(
            "RequestTimeout",
            "Client disconnected during upload",
            status_code=408,
        )

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
    part_key = f"multipart:{upload_id}:part:{part_number}"

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
        logger.info(f"Part {part_number}: Disconnect check took {disconnect_time:.3f}s")

        # Store in Redis
        redis_start = time.time()
        await redis_client.setex(
            part_key,
            1800,
            file_data,
        )  # 30 minute TTL
        redis_time = time.time() - redis_start
        logger.info(f"Part {part_number}: Redis setex took {redis_time:.3f}s")

        # Create mock part result (no IPFS upload needed for individual parts)
        md5_start = time.time()
        # Move MD5 calculation to thread pool to avoid blocking event loop
        loop = asyncio.get_event_loop()
        md5_hash = await loop.run_in_executor(None, lambda: hashlib.md5(file_data).hexdigest())
        md5_time = time.time() - md5_start
        logger.info(f"Part {part_number}: MD5 calculation took {md5_time:.3f}s (threaded)")

        etag = f"{md5_hash}-{part_number}"

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
        logger.info(f"Part {part_number}: Database insert took {db_time:.3f}s")

        total_time = time.time() - start_time
        logger.info(f"Part {part_number}: TOTAL processing time: {total_time:.3f}s")

        # Return the part's ETag in the response header
        return Response(
            status_code=200,  # etag needs to be double-quoted?
            headers={"ETag": f'"{part_result["etag"]}"'},
        )

    except Exception:
        # If any error occurs, clean up the Redis key
        try:
            await redis_client.delete(part_key)
        except Exception as cleanup_error:
            logger.error(f"Failed to cleanup Redis key after error: {cleanup_error}")

        raise


@router.delete("/{bucket_name}/{object_key:path}/", status_code=204)
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
            redis_keys_to_delete = [f"multipart:{upload_id}:part:{part['part_number']}" for part in parts]
            if redis_keys_to_delete:
                await redis_client.delete(
                    *redis_keys_to_delete,
                )

        # Delete the multipart upload from the database
        await db.fetchrow(
            get_query("abort_multipart_upload"),
            upload_id,
        )
        return Response(status_code=204)

    except Exception as e:
        logger.error(f"Error aborting multipart upload: {e}")
        return s3_error_response(
            "InternalError",
            "We encountered an internal error",
            status_code=500,
        )


# Moving multipart uploads function to main endpoints.py for better routing
# This avoids potential conflicts with other GET handlers


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
            ET.SubElement(upload_elem, "Key").text = upload["object_key"]
            ET.SubElement(upload_elem, "UploadId").text = upload["upload_id"]
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
    combined_etag = hashlib.md5(("".join(etags)).encode()).hexdigest()

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

        # Upload concatenated file to IPFS
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

        # Create the object in database (without CID initially for multipart uploads)
        await db.fetchrow(
            get_query("upsert_object_basic"),
            upload_id,
            bucket_id,
            object_key,
            content_type,
            json.dumps(metadata),
            final_md5_hash,
            total_size,
            file_created_at,
        )

        await enqueue_upload_request(
            MultipartUploadChainRequest(
                object_key=object_key,
                bucket_name=bucket_name,
                upload_id=upload_id,
                chunks=[
                    {
                        "chunk_key": f"multipart:{upload_id}:part:{part['part_number']}",
                        "chunk_index": part["part_number"],
                    }
                    for part in parts
                ],
                substrate_url=config.substrate_url,
                ipfs_node=config.ipfs_store_url,
                address=request.state.main_account,
                subaccount=request.state.account.id,
                subaccount_seed_phrase=request.state.seed_phrase,
                should_encrypt=should_encrypt,
                object_id=upload_id,
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
