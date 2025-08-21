"""S3-compatible multipart upload implementation for handling large file uploads."""

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

from hippius_s3 import dependencies
from hippius_s3.api.s3.errors import s3_error_response
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)
router = APIRouter(tags=["s3-multipart"])


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

    try:
        part_number = int(part_number_str)
        if part_number < 1 or part_number > 10000:
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
        file_data = await request.body()
        file_size = len(file_data)
        if file_size == 0:
            return s3_error_response(
                "InvalidArgument",
                "Zero-length part not allowed",
                status_code=400,
            )

        # Upload to IPFS and get part information
        part_result = await ipfs_service.upload_part(
            file_data,
            part_number,
            seed_phrase=request.state.seed_phrase,
        )

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

    except ValueError:
        return s3_error_response(
            "InvalidArgument",
            "Part number must be an integer between 1 and 10000",
            status_code=400,
        )
    except Exception as e:
        logger.exception(f"Error uploading part: {e}")
        return s3_error_response(
            "InternalError",
            f"Error uploading part: {str(e)}",
            status_code=500,
        )


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

        # Get and delete parts from IPFS
        parts = await db.fetch(get_query("list_parts"), upload_id)
        for part in parts:
            await ipfs_service.delete_file(
                part["ipfs_cid"],
                seed_phrase=request.state.seed_phrase,
                unpin=False,
            )

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
            return s3_error_response(
                "InvalidRequest",
                "The specified multipart upload has already been completed",
                status_code=400,
            )

        # Parse the XML request body to get parts list
        body_text = ""
        try:
            body_text = (await request.body()).decode("utf-8")
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

        # Prepare parts for concatenation
        parts_for_concat = []
        for part_number, _ in part_info:
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

        # Concatenate the parts
        concat_result = await ipfs_service.concatenate_parts(
            parts_for_concat,
            content_type,
            object_key,
            subaccount_id=request.state.account.main_account,
            bucket_name=bucket_name,
            is_public_bucket=bucket["is_public"] if bucket else False,
            seed_phrase=seed_phrase,
        )

        # Prepare metadata
        object_id = str(uuid.uuid4())
        bucket_id = multipart_upload["bucket_id"]
        created_at = datetime.now(UTC)
        final_cid = concat_result["cid"]

        metadata = multipart_upload["metadata"] if multipart_upload["metadata"] else {}
        if isinstance(metadata, str):
            metadata = json.loads(metadata)

        # Add metadata for multipart upload
        parts_count = len(parts_for_concat)
        metadata.update(
            {
                "multipart": True,
                "parts_count": parts_count,
                "ipfs": {
                    "cid": final_cid,
                    "encrypted": True,
                    "multipart": True,
                    "part_cids": [part["ipfs_cid"] for part in parts_for_concat],
                },
                "hippius": {
                    "tx_hash": concat_result.get("tx_hash"),
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
            concat_result["size_bytes"],
            content_type,
            created_at,
            json.dumps(metadata),
            concat_result["etag"],
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
  <ETag>"{concat_result.get("etag", final_cid)}"</ETag>
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
