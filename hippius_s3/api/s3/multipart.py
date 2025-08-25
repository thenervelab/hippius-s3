"""S3-compatible multipart upload implementation for handling large file uploads."""

import asyncio
import hashlib
import json
import logging
import re
import uuid
from datetime import UTC
from datetime import datetime
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Request
from fastapi import Response
from lxml import etree as ET
from starlette.requests import ClientDisconnect

from hippius_s3 import dependencies
from hippius_s3 import utils
from hippius_s3.api.s3.errors import s3_error_response
from hippius_s3.queue import enqueue_upload_request
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)
router = APIRouter(tags=["s3-multipart"])


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
    seed_phrase = request.state.seed_phrase

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
            return await complete_multipart_upload_handler(
                bucket_name,
                object_key,
                upload_id,
                request,
                db,
                seed_phrase,
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
        content_type = request.headers.get("Content-Type", "application/octet-stream")

        # Extract metadata from headers
        metadata = {}
        for key, value in request.headers.items():
            if key.lower().startswith("x-amz-meta-"):
                meta_key = key[11:]
                metadata[meta_key] = value

        # Create the multipart upload in the database
        await db.fetchrow(
            get_query("create_multipart_upload"),
            upload_id,
            bucket["bucket_id"],
            object_key,
            initiated_at,
            content_type,
            json.dumps(metadata),
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


async def complete_multipart_upload_handler(
    bucket_name: str,
    object_key: str,
    upload_id: str,
    request: Request,
    db: dependencies.DBConnection,
    seed_phrase: Optional[str] = None,
) -> Response:
    """Complete a multipart upload (POST /{bucket_name}/{object_key}?uploadId=xyz)."""
    try:
        # Use the existing internal implementation
        # Get IPFS service from app state
        ipfs_service = request.app.state.ipfs_service

        return await complete_multipart_upload_internal(
            bucket_name,
            object_key,
            upload_id,
            request,
            db,
            ipfs_service,
            seed_phrase=seed_phrase,
        )
    except Exception as e:
        logger.exception(f"Error completing multipart: {e}")
        return s3_error_response(
            "InternalError",
            f"Error completing multipart upload: {str(e)}",
            status_code=500,
        )


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
    bucket_name: str,
    object_key: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
    ipfs_service=Depends(dependencies.get_ipfs_service),
) -> Response:
    """Upload a part for a multipart upload (PUT with partNumber & uploadId)."""
    # These two parameters are required for multipart upload parts
    upload_id = request.query_params.get("uploadId")
    part_number_str = request.query_params.get("partNumber")

    # If this doesn't have both uploadId and partNumber, it's not a multipart upload part
    # so we should return None to allow the main S3 router to handle it
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

    if multipart_upload["is_completed"]:
        return s3_error_response(
            "InvalidRequest",
            "The specified multipart upload has already been completed",
            status_code=400,
        )

    # Upload the part data to IPFS
    try:
        file_data = await get_request_body(request)
    except ClientDisconnect:
        logger.warning(f"Client disconnected during part {part_number} upload for upload {upload_id}")
        return s3_error_response(
            "RequestTimeout",
            "Client disconnected during upload",
            status_code=408,
        )

    file_size = len(file_data)

    # Check Redis memory and wait if insufficient space
    redis_client = request.app.state.redis_client

    # For new uploads (part 1), check if we have enough space for the entire estimated upload
    if part_number == 1:
        # Get total file size from Content-Length or estimate from first part
        estimated_total_size = request.headers.get("content-length")
        if estimated_total_size:  # noqa: SIM108
            estimated_total_size = int(estimated_total_size) * 2500  # Rough estimate: each part ~8MB, total parts
        else:
            # Fallback: estimate based on typical large upload (assume 20GB max)
            estimated_total_size = 20 * 1024 * 1024 * 1024  # 20GB

        # Wait for memory availability with 10% buffer
        memory_needed = int(estimated_total_size * 1.1)  # Add 10% buffer
        max_wait_time = 300  # 5 minutes max wait
        wait_interval = 5  # Check every 5 seconds
        total_waited = 0

        logger.info(
            f"New upload {upload_id}: estimated size {estimated_total_size} bytes, need {memory_needed} bytes with buffer"
        )

        while total_waited < max_wait_time:
            try:
                memory_info = await redis_client.info("memory")
                used_memory = int(memory_info["used_memory"])
                max_memory = int(memory_info.get("maxmemory", 0))

                if max_memory > 0:
                    available_memory = max_memory - used_memory

                    if available_memory >= memory_needed:
                        logger.info(
                            f"Memory available for upload {upload_id}: {available_memory} bytes >= {memory_needed} bytes needed"
                        )
                        break
                    logger.info(
                        f"Waiting for memory: need {memory_needed} bytes, available {available_memory} bytes. Waited {total_waited}s/{max_wait_time}s"
                    )
                    await asyncio.sleep(wait_interval)
                    total_waited += wait_interval
                else:
                    # No memory limit set, proceed
                    break

            except Exception as e:
                logger.error(f"Failed to check Redis memory: {e}")
                break  # Continue with upload if memory check fails

        # If we waited the max time and still don't have space, reject
        if total_waited >= max_wait_time:
            try:
                memory_info = await redis_client.info("memory")
                used_memory = int(memory_info["used_memory"])
                max_memory = int(memory_info.get("maxmemory", 0))
                available_memory = max_memory - used_memory if max_memory > 0 else 0

                logger.error(
                    f"Upload {upload_id} rejected after {max_wait_time}s wait: need {memory_needed} bytes, have {available_memory} bytes"
                )
                return s3_error_response(
                    "ServiceUnavailable",
                    f"Insufficient cache memory for upload after waiting {max_wait_time} seconds. Please try again later.",
                    status_code=503,
                )
            except Exception as e:
                logger.error(f"Failed final memory check: {e}")

    # For all parts, do a quick check for immediate part storage
    try:
        memory_info = await redis_client.info("memory")
        used_memory = int(memory_info["used_memory"])
        max_memory = int(memory_info.get("maxmemory", 0))

        if max_memory > 0:
            available_memory = max_memory - used_memory
            part_buffer = int(file_size * 0.1)  # 10% buffer for this part

            if available_memory < (file_size + part_buffer):
                logger.warning(
                    f"Redis memory insufficient for part {part_number}: need {file_size + part_buffer} bytes, have {available_memory} bytes"
                )
                return s3_error_response(
                    "ServiceUnavailable",
                    "Insufficient cache memory for this part. Please try again later.",
                    status_code=503,
                )
    except Exception as e:
        logger.error(f"Failed to check Redis memory for part: {e}")
        # Continue with upload if memory check fails

    # Check part size limits
    MAX_PART_SIZE = 32 * 1024 * 1024  # 32 MB
    MIN_PART_SIZE = 5 * 1024 * 1024  # 5 MB (AWS S3 minimum)

    if file_size == 0:
        return s3_error_response(
            "InvalidArgument",
            "Zero-length part not allowed",
            status_code=400,
        )

    if file_size > MAX_PART_SIZE:
        return s3_error_response(
            "EntityTooLarge",
            f"Part size {file_size} bytes exceeds maximum {MAX_PART_SIZE} bytes",
            status_code=400,
        )

    # Enforce AWS S3 minimum part size (except for final part)
    # Allow parts smaller than 5MB - AWS S3 allows this for the final part
    # We don't have a way to know if this is the final part until completion,
    # so we'll be permissive here and let completion validation handle it
    if file_size < MIN_PART_SIZE and file_size < 1024:  # Only reject truly tiny parts (< 1KB)
        return s3_error_response(
            "EntityTooSmall",
            f"Part size {file_size} bytes is too small",
            status_code=400,
        )

    # Cache raw part data for concatenation at completion (no IPFS upload for parts)
    redis_client = request.app.state.redis_client
    part_key = f"multipart:{upload_id}:part:{part_number}"
    await redis_client.setex(part_key, 86400, file_data)  # 24 hour TTL

    # Create mock part result (no IPFS upload needed for individual parts)
    md5_hash = hashlib.md5(file_data).hexdigest()
    etag = f"{md5_hash}-{part_number}"

    part_result = {
        "cid": f"temp-part-{part_number}",  # Temporary placeholder
        "size_bytes": file_size,
        "etag": etag,
        "part_number": part_number,
    }

    # Part uploaded successfully (final size computed during completion)

    # Save the part information in the database
    part_id = str(uuid.uuid4())
    await db.fetchrow(
        get_query("upload_part"),
        part_id,
        upload_id,
        part_number,
        part_result["cid"],
        part_result["size_bytes"],
        part_result["etag"],
        datetime.now(UTC),
    )

    # Return the part's ETag in the response header
    return Response(status_code=200, headers={"ETag": f'"{part_result["etag"]}"'})


@router.delete("/{bucket_name}/{object_key:path}/", status_code=204)
@router.delete("/{bucket_name}/{object_key:path}", status_code=204)
async def abort_multipart_upload(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
    ipfs_service=Depends(dependencies.get_ipfs_service),
) -> Response:
    """Abort a multipart upload (DELETE with uploadId)."""
    upload_id = request.query_params.get("uploadId")
    if not upload_id:
        return None

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
        parts = await db.fetch(get_query("list_parts"), upload_id)
        if parts:
            redis_client = request.app.state.redis_client
            redis_keys_to_delete = [f"multipart:{upload_id}:part:{part['part_number']}" for part in parts]
            if redis_keys_to_delete:
                await redis_client.delete(*redis_keys_to_delete)

        # Delete the multipart upload from the database
        await db.fetchrow(get_query("abort_multipart_upload"), upload_id)
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
        xml_content = ET.tostring(root, encoding="UTF-8", xml_declaration=True, pretty_print=True)

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


async def complete_multipart_upload_internal(
    bucket_name: str,
    object_key: str,
    upload_id: str,
    request: Request,
    db: dependencies.DBConnection,
    ipfs_service,
    seed_phrase: Optional[str] = None,
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
        for num, tag in zip(part_numbers, etags, strict=True):
            part_info.append((int(num), tag.replace('"', "").strip()))

        # Sort parts by part number
        part_info.sort(key=lambda x: x[0])

        # Get the parts from the database
        db_parts = await db.fetch(get_query("list_parts"), upload_id)
        logger.info(f"Found {len(db_parts) if db_parts else 0} parts for upload {upload_id}")

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

        # Prepare parts for concatenation in correct order
        parts_for_concat = []
        for part_number, _ in part_info:  # part_info is already sorted by part number
            part_data = db_parts_dict[part_number]
            parts_for_concat.append(
                {
                    "ipfs_cid": part_data["ipfs_cid"],
                    "part_number": part_data["part_number"],
                    "size_bytes": part_data["size_bytes"],
                    "etag": part_data["etag"],
                }
            )

        # Get content type
        content_type = multipart_upload["content_type"] or "application/octet-stream"

        # Get bucket info to check if it's public
        bucket = await db.fetchrow(
            get_query("get_bucket_by_name_and_owner"),
            bucket_name,
            request.state.account.main_account,
        )

        # Ensure parts are in correct order before concatenation
        parts_for_concat.sort(key=lambda x: x["part_number"])
        logger.info(
            f"Processing {len(parts_for_concat)} parts in order: {[p['part_number'] for p in parts_for_concat]}"
        )

        # Compute MD5 hash from concatenated parts data and cleanup Redis in one pass
        redis_client = request.app.state.redis_client
        all_data = b""
        redis_keys_to_delete = []
        processed_parts = set()  # Track processed parts to avoid duplicates

        for part_dict in parts_for_concat:  # Renamed variable for clarity
            part_number = part_dict["part_number"]

            # Check for duplicate part numbers to prevent double processing
            if part_number in processed_parts:
                logger.error(f"Duplicate part number {part_number} detected - skipping")
                continue
            processed_parts.add(part_number)

            part_key = f"multipart:{upload_id}:part:{part_number}"
            part_data = await redis_client.get(part_key)

            if part_data is None:
                return s3_error_response(
                    "InvalidRequest",
                    f"Part {part_number} data not found in cache",
                    status_code=400,
                )

            part_size = len(part_data)
            logger.info(f"Processing part {part_number}: {part_size} bytes")
            all_data += part_data
            redis_keys_to_delete.append(part_key)

        final_md5_hash = hashlib.md5(all_data).hexdigest()
        total_size = len(all_data)  # Use actual computed size instead of DB value
        logger.info(
            f"Multipart concatenation complete: {len(processed_parts)} parts, {total_size} bytes total, MD5: {final_md5_hash}"
        )

        # Batch delete Redis keys
        if redis_keys_to_delete:
            await redis_client.delete(*redis_keys_to_delete)

        # Calculate combined ETag (required for S3 compatibility)
        etags = [str(part_dict["etag"]).split("-")[0] for part_dict in parts_for_concat]
        combined_etag = hashlib.md5(("".join(etags)).encode()).hexdigest()
        final_etag = f"{combined_etag}-{len(parts_for_concat)}"

        # Upload concatenated file to IPFS
        should_encrypt = not bucket["is_public"]

        s3_result = await ipfs_service.client.s3_publish(
            content=all_data,
            encrypt=should_encrypt,
            seed_phrase=request.state.seed_phrase,
            subaccount_id=request.state.account.main_account,
            bucket_name=bucket_name,
            store_node=ipfs_service.config.ipfs_store_url,
            pin_node=ipfs_service.config.ipfs_store_url,
            substrate_url=ipfs_service.config.substrate_url,
            publish=False,
            file_name=object_key,
        )

        # Queue for pinning
        redis_client = request.app.state.redis_client
        await enqueue_upload_request(
            redis_client,
            s3_result,
            request.state.seed_phrase,
            owner=request.state.account.main_account,
        )

        final_cid = s3_result.cid

        # Verification: Check that the file is properly encrypted on IPFS
        logger.info(f"Multipart upload completed. CID: {final_cid}")
        logger.info(f"Verification: File should be encrypted at https://get.hippius.network/ipfs/{final_cid}")
        logger.info(f"Expected size: {total_size} bytes, Should encrypt: {should_encrypt}")

        # No need to delete individual parts from IPFS since they were never uploaded there

        # Prepare metadata
        object_id = str(uuid.uuid4())
        bucket_id = multipart_upload["bucket_id"]
        created_at = datetime.now(UTC)

        metadata = multipart_upload["metadata"] if multipart_upload["metadata"] else {}
        if isinstance(metadata, str):
            metadata = json.loads(metadata)

        # Add metadata for final concatenated file
        metadata.update(
            {
                "multipart": True,
                "parts_count": len(parts_for_concat),
                "original_parts": len(parts_for_concat),  # Track original part count
                "ipfs": {
                    "cid": final_cid,
                    "encrypted": should_encrypt,
                    "multipart": False,
                    # Final file is single IPFS object
                },
            }
        )

        # Mark the upload as completed
        await db.fetchrow(get_query("complete_multipart_upload"), upload_id)

        # Delete any existing object with same key
        await db.execute(
            "DELETE FROM objects WHERE bucket_id = $1 AND object_key = $2",
            bucket_id,
            object_key,
        )

        # Create the object in database
        await db.fetchrow(
            get_query("upsert_object"),
            object_id,
            bucket_id,
            object_key,
            final_cid,
            total_size,
            content_type,
            created_at,
            json.dumps(metadata),
            final_md5_hash,
        )

        # Verify that the object was actually inserted
        verify_result = await db.fetchrow(
            get_query("get_object_by_path"),
            bucket_id,
            object_key,
        )
        if not verify_result:
            logger.error(f"Failed to find object after multipart completion: {bucket_name}/{object_key}")
            raise RuntimeError(f"Failed to insert multipart object: {bucket_name}/{object_key}")

        # Create XML response
        xml_string = f"""<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Location>http://{request.headers.get("Host", "")}/{bucket_name}/{object_key}</Location>
  <Bucket>{bucket_name}</Bucket>
  <Key>{object_key}</Key>
  <ETag>"{final_etag}"</ETag>
</CompleteMultipartUploadResult>
"""
        xml_bytes = xml_string.encode("utf-8")

        # Return with proper headers
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
        logger.exception(f"Error completing multipart upload: {e}")
        return s3_error_response(
            "InternalError",
            f"Error completing multipart upload: {str(e)}",
            status_code=500,
        )
