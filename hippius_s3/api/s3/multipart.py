import json
import logging
import uuid
from datetime import UTC
from datetime import datetime

import asyncpg
import dicttoxml
import xmltodict
from fastapi import APIRouter
from fastapi import Depends
from fastapi import Request
from fastapi import Response

from hippius_s3 import dependencies
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)

router = APIRouter(tags=["s3-multipart"])


def xml_error_response(code: str, message: str, status_code: int = 400, **kwargs) -> Response:
    """Generate a standardized XML error response."""
    error_dict = {"Error": {"Code": code, "Message": message, "RequestId": "N/A", "HostId": "hippius-s3", **kwargs}}

    xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
    xml_error += dicttoxml.dicttoxml(error_dict, attr_type=False, root=False)

    return Response(
        content=xml_error,
        media_type="application/xml",
        status_code=status_code,
    )


@router.post("/{bucket_name}/{object_key:path}", status_code=200)
async def create_multipart_upload(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
) -> Response:
    """
    Initiate a multipart upload (POST /{bucket_name}/{object_key}?uploads).

    This endpoint is compatible with the S3 protocol used by MinIO and other S3 clients.
    """
    # Check if this is a multipart upload initiation request
    if "uploads" not in request.query_params:
        return xml_error_response("InvalidRequest", "The specified request is not valid.", status_code=400)

    try:
        # Check if bucket exists
        query = get_query("get_bucket_by_name")
        bucket = await db.fetchrow(query, bucket_name)

        if not bucket:
            return xml_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        bucket_id = bucket["bucket_id"]

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

        query = get_query("create_multipart_upload")
        await db.fetchrow(
            query,
            upload_id,
            bucket_id,
            object_key,
            initiated_at,
            content_type,
            json.dumps(metadata),
        )

        # Return the InitiateMultipartUploadResult XML response
        result_dict = {
            "InitiateMultipartUploadResult": {
                "@xmlns": "http://s3.amazonaws.com/doc/2006-03-01/",
                "Bucket": bucket_name,
                "Key": object_key,
                "UploadId": upload_id,
            }
        }

        xml_response = dicttoxml.dicttoxml(result_dict, attr_type=True, root=False)
        return Response(content=xml_response, media_type="application/xml")

    except Exception as e:
        logger.error(f"Error initiating multipart upload: {e}")
        return xml_error_response(
            "InternalError", "We encountered an internal error. Please try again.", status_code=500
        )


@router.put("/{bucket_name}/{object_key:path}", status_code=200)
async def upload_part(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
    ipfs_service=Depends(dependencies.get_ipfs_service),
) -> Response:
    """
    Upload a part for a multipart upload (PUT /{bucket_name}/{object_key}?partNumber=x&uploadId=y).

    This endpoint is compatible with the S3 protocol used by MinIO and other S3 clients.
    """
    # Check if this is a part upload request
    upload_id = request.query_params.get("uploadId")
    part_number_str = request.query_params.get("partNumber")

    if not upload_id or not part_number_str:
        # Not a multipart upload part, defer to regular PUT handler
        return None

    try:
        part_number = int(part_number_str)
        if part_number < 1 or part_number > 10000:
            return xml_error_response(
                "InvalidArgument", "Part number must be an integer between 1 and 10000, inclusive", status_code=400
            )

        # Check if the multipart upload exists
        query = get_query("get_multipart_upload")
        multipart_upload = await db.fetchrow(query, upload_id)

        if not multipart_upload:
            return xml_error_response(
                "NoSuchUpload",
                "The specified upload does not exist. The upload ID might be invalid, or the upload might have been aborted or completed.",
                status_code=404,
                UploadId=upload_id,
            )

        if multipart_upload["is_completed"]:
            return xml_error_response(
                "InvalidRequest", "The specified multipart upload has already been completed", status_code=400
            )

        bucket_name_from_db = multipart_upload["bucket_name"]
        if bucket_name != bucket_name_from_db:
            return xml_error_response(
                "InvalidRequest",
                f"The bucket specified ({bucket_name}) does not match the bucket in the upload ({bucket_name_from_db})",
                status_code=400,
            )

        object_key_from_db = multipart_upload["object_key"]
        if object_key != object_key_from_db:
            return xml_error_response(
                "InvalidRequest",
                f"The key specified ({object_key}) does not match the key in the upload ({object_key_from_db})",
                status_code=400,
            )

        # Upload the part data to IPFS
        file_data = await request.body()
        file_size = len(file_data)

        if file_size == 0:
            return xml_error_response("InvalidArgument", "Zero-length part not allowed", status_code=400)

        # Upload to IPFS and get part information
        part_result = await ipfs_service.upload_part(file_data, part_number)

        # Save the part information in the database
        part_id = str(uuid.uuid4())
        uploaded_at = datetime.now(UTC)

        query = get_query("upload_part")
        await db.fetchrow(
            query,
            part_id,
            upload_id,
            part_number,
            part_result["cid"],
            part_result["size_bytes"],
            part_result["etag"],
            uploaded_at,
        )

        # Return the part's ETag in the response header
        return Response(status_code=200, headers={"ETag": f'"{part_result["etag"]}"'})

    except ValueError:
        return xml_error_response(
            "InvalidArgument", "Part number must be an integer between 1 and 10000, inclusive", status_code=400
        )
    except asyncpg.UniqueViolationError:
        # If part with this number already exists, overwrite it
        try:
            # First, delete the existing part
            existing_part = await db.fetchrow(
                "DELETE FROM parts WHERE upload_id = $1 AND part_number = $2 RETURNING ipfs_cid", upload_id, part_number
            )

            if existing_part:
                # Delete from IPFS
                await ipfs_service.delete_file(existing_part["ipfs_cid"])

            # Then insert the new part
            part_id = str(uuid.uuid4())
            uploaded_at = datetime.now(UTC)

            query = get_query("upload_part")
            await db.fetchrow(
                query,
                part_id,
                upload_id,
                part_number,
                part_result["cid"],
                part_result["size_bytes"],
                part_result["etag"],
                uploaded_at,
            )

            return Response(status_code=200, headers={"ETag": f'"{part_result["etag"]}"'})

        except Exception as e:
            logger.error(f"Error replacing existing part: {e}")
            return xml_error_response(
                "InternalError", "We encountered an internal error. Please try again.", status_code=500
            )

    except Exception as e:
        logger.error(f"Error uploading part: {e}")
        return xml_error_response(
            "InternalError", "We encountered an internal error. Please try again.", status_code=500
        )


@router.post("/{bucket_name}/{object_key:path}", status_code=200)
async def complete_multipart_upload(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
    ipfs_service=Depends(dependencies.get_ipfs_service),
) -> Response:
    """
    Complete a multipart upload (POST /{bucket_name}/{object_key}?uploadId=y).

    This endpoint is compatible with the S3 protocol used by MinIO and other S3 clients.
    """
    upload_id = request.query_params.get("uploadId")

    if not upload_id or "uploads" in request.query_params:
        # Not a complete multipart upload request
        # If "uploads" is present, it's a create_multipart_upload request
        return None

    try:
        # Get the multipart upload information
        query = get_query("get_multipart_upload")
        multipart_upload = await db.fetchrow(query, upload_id)

        if not multipart_upload:
            return xml_error_response(
                "NoSuchUpload",
                "The specified upload does not exist. The upload ID might be invalid, or the upload might have been aborted or completed.",
                status_code=404,
                UploadId=upload_id,
            )

        if multipart_upload["is_completed"]:
            return xml_error_response(
                "InvalidRequest", "The specified multipart upload has already been completed", status_code=400
            )

        # Parse the XML request body to get the parts list
        try:
            request_body = await request.body()
            parts_data = xmltodict.parse(request_body.decode("utf-8"))

            if not parts_data or "CompleteMultipartUpload" not in parts_data:
                return xml_error_response(
                    "InvalidRequest",
                    "The XML you provided was not well-formed or did not validate against our published schema",
                    status_code=400,
                )

            parts_list = parts_data["CompleteMultipartUpload"].get("Part", [])
            if not parts_list:
                return xml_error_response("InvalidRequest", "You must specify at least one part", status_code=400)

            # Make sure parts_list is a list even if only one part
            if not isinstance(parts_list, list):
                parts_list = [parts_list]

            # Extract part numbers and ETags
            part_info = []
            for part in parts_list:
                part_number = int(part["PartNumber"])
                etag = part["ETag"].strip('"')  # Remove quotes
                part_info.append((part_number, etag))

            # Sort by part number
            part_info.sort(key=lambda x: x[0])

        except Exception as e:
            logger.error(f"Error parsing complete multipart upload request: {e}")
            return xml_error_response(
                "InvalidRequest",
                "The XML you provided was not well-formed or did not validate against our published schema",
                status_code=400,
            )

        # Get the parts from the database
        query = get_query("list_parts")
        db_parts = await db.fetch(query, upload_id)

        # Check that all parts in the request exist in the database
        db_parts_dict = {p["part_number"]: p for p in db_parts}

        missing_parts = []
        for part_number, etag in part_info:
            if part_number not in db_parts_dict:
                missing_parts.append(part_number)
            elif db_parts_dict[part_number]["etag"] != etag:
                return xml_error_response(
                    "InvalidPart",
                    f"One or more of the specified parts has an incorrect ETag. Part number {part_number} has incorrect ETag",
                    status_code=400,
                )

        if missing_parts:
            return xml_error_response(
                "InvalidPart",
                f"One or more of the specified parts could not be found. Part numbers: {', '.join(map(str, missing_parts))}",
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

        # Create the final object by concatenating all parts
        content_type = multipart_upload["content_type"] or "application/octet-stream"
        concat_result = await ipfs_service.concatenate_parts(
            parts_for_concat,
            content_type,
            object_key,
        )

        # Mark the multipart upload as completed
        query = get_query("complete_multipart_upload")
        await db.fetchrow(query, upload_id)

        # Create the final object in the database
        object_id = str(uuid.uuid4())
        bucket_id = multipart_upload["bucket_id"]
        created_at = datetime.now(UTC)

        # Extract metadata from the multipart upload
        metadata = multipart_upload["metadata"] if multipart_upload["metadata"] else {}
        if isinstance(metadata, str):
            metadata = json.loads(metadata)

        # Add IPFS metadata
        metadata["ipfs"] = {
            "cid": concat_result["cid"],
            "encrypted": False,
            "multipart": True,
            "parts_count": len(parts_for_concat),
        }

        # Create the final object
        query = get_query("create_object")
        await db.fetchrow(
            query,
            object_id,
            bucket_id,
            object_key,
            concat_result["cid"],
            concat_result["size_bytes"],
            content_type,
            created_at,
            json.dumps(metadata),
        )

        # Generate the response XML
        location = f"http://{request.headers.get('Host', '')}/{bucket_name}/{object_key}"
        result_dict = {
            "CompleteMultipartUploadResult": {
                "@xmlns": "http://s3.amazonaws.com/doc/2006-03-01/",
                "Location": location,
                "Bucket": bucket_name,
                "Key": object_key,
                "ETag": f'"{concat_result["etag"]}"',
            }
        }

        xml_response = dicttoxml.dicttoxml(result_dict, attr_type=True, root=False)
        return Response(content=xml_response, media_type="application/xml")

    except Exception as e:
        logger.error(f"Error completing multipart upload: {e}")
        return xml_error_response(
            "InternalError", "We encountered an internal error. Please try again.", status_code=500
        )


@router.delete("/{bucket_name}/{object_key:path}", status_code=204)
async def abort_multipart_upload(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
    ipfs_service=Depends(dependencies.get_ipfs_service),
) -> Response:
    """
    Abort a multipart upload (DELETE /{bucket_name}/{object_key}?uploadId=y).

    This endpoint is compatible with the S3 protocol used by MinIO and other S3 clients.
    """
    upload_id = request.query_params.get("uploadId")

    if not upload_id:
        # Not an abort multipart upload request
        return None

    try:
        # Get the multipart upload information
        query = get_query("get_multipart_upload")
        multipart_upload = await db.fetchrow(query, upload_id)

        if not multipart_upload:
            return xml_error_response(
                "NoSuchUpload",
                "The specified upload does not exist. The upload ID might be invalid, or the upload might have been aborted or completed.",
                status_code=404,
                UploadId=upload_id,
            )

        # Get the parts and delete them from IPFS
        query = get_query("list_parts")
        parts = await db.fetch(query, upload_id)

        for part in parts:
            await ipfs_service.delete_file(part["ipfs_cid"])

        # Delete the multipart upload from the database (this will cascade delete parts)
        query = get_query("abort_multipart_upload")
        await db.fetchrow(query, upload_id)

        # Return a success response
        return Response(status_code=204)

    except Exception as e:
        logger.error(f"Error aborting multipart upload: {e}")
        return xml_error_response(
            "InternalError", "We encountered an internal error. Please try again.", status_code=500
        )


@router.get("/{bucket_name}", status_code=200)
async def list_multipart_uploads(
    bucket_name: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
) -> Response:
    """
    List multipart uploads in a bucket (GET /{bucket_name}?uploads).

    This endpoint is compatible with the S3 protocol used by MinIO and other S3 clients.
    """
    # Check if this is a list multipart uploads request
    if "uploads" not in request.query_params:
        return None

    try:
        # Check if bucket exists
        query = get_query("get_bucket_by_name")
        bucket = await db.fetchrow(query, bucket_name)

        if not bucket:
            return xml_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        bucket_id = bucket["bucket_id"]
        prefix = request.query_params.get("prefix")

        # List multipart uploads
        query = get_query("list_multipart_uploads")
        uploads = await db.fetch(query, bucket_id, prefix)

        # Generate the response XML
        uploads_list = []
        for upload in uploads:
            upload_dict = {
                "Key": upload["object_key"],
                "UploadId": upload["upload_id"],
                "Initiated": upload["initiated_at"].isoformat(),
            }
            uploads_list.append(upload_dict)

        result_dict = {
            "ListMultipartUploadsResult": {
                "@xmlns": "http://s3.amazonaws.com/doc/2006-03-01/",
                "Bucket": bucket_name,
                "KeyMarker": "",
                "UploadIdMarker": "",
                "NextKeyMarker": "",
                "NextUploadIdMarker": "",
                "MaxUploads": 1000,
                "IsTruncated": False,
                "Upload": uploads_list or None,  # Use None if empty to omit the tag
            }
        }

        xml_response = dicttoxml.dicttoxml(result_dict, attr_type=True, root=False)
        return Response(content=xml_response, media_type="application/xml")

    except Exception as e:
        logger.error(f"Error listing multipart uploads: {e}")
        return xml_error_response(
            "InternalError", "We encountered an internal error. Please try again.", status_code=500
        )
