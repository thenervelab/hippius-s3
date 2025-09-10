"""S3-compatible API endpoints implementation for bucket and object operations."""

import asyncio
import hashlib
import json
import logging
import os
import re
import uuid
from datetime import UTC
from datetime import datetime
from typing import Dict
from typing import List
from typing import Tuple

import asyncpg
import fastapi
from fastapi import APIRouter
from fastapi import Depends
from fastapi import Request
from fastapi import Response
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPBearer
from hippius_sdk.errors import HippiusIPFSError
from hippius_sdk.errors import HippiusSubstrateError
from lxml import etree as ET

from hippius_s3 import dependencies
from hippius_s3 import utils
from hippius_s3.api.s3 import errors
from hippius_s3.api.s3.multipart import upload_part
from hippius_s3.config import get_config
from hippius_s3.dependencies import DBConnection
from hippius_s3.ipfs_service import IPFSService
from hippius_s3.queue import Chunk
from hippius_s3.queue import ChunkToDownload
from hippius_s3.queue import DownloadChainRequest
from hippius_s3.queue import SimpleUploadChainRequest
from hippius_s3.queue import UnpinChainRequest
from hippius_s3.queue import enqueue_download_request
from hippius_s3.queue import enqueue_unpin_request
from hippius_s3.queue import enqueue_upload_request
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)


def format_s3_timestamp(dt):
    """Format datetime to AWS S3 compatible timestamp: YYYY-MM-DDThh:mm:ss.sssZ"""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


async def get_chunk_from_redis(redis_client, chunk: ChunkToDownload) -> bytes:
    """Get a chunk from Redis with timeout."""
    for _ in range(config.http_redis_get_retries):
        chunk_data = await asyncio.wait_for(
            redis_client.get(chunk.redis_key),
            timeout=config.redis_read_chunk_timeout,
        )
        if chunk_data:
            logger.debug(f"Got chunk {chunk.part_id} from Redis")
            break

        await asyncio.sleep(config.http_download_sleep_loop)

    else:
        raise RuntimeError(f"Chunk {chunk.part_id} not found in Redis")

    return chunk_data


def _handle_range_request(
    file_data: bytes,
    range_header: str,
    result: dict,
    metadata: dict,
    object_key: str,
    ipfs_cid: str,
) -> Response:
    """
    Handle HTTP Range requests for partial content downloads.

    Parses Range header and returns appropriate 206 Partial Content response
    or 416 Range Not Satisfiable if invalid range.
    """
    import re

    file_size = len(file_data)

    # Parse range header - format: "bytes=start-end" or "bytes=start-" or "bytes=-suffix"
    range_match = re.match(r"bytes=(\d*)-(\d*)", range_header.lower())
    if not range_match:
        # Invalid range format
        return Response(
            status_code=416,
            content=f"Invalid range format: {range_header}",
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    start_str, end_str = range_match.groups()

    try:
        # Handle different range formats
        if start_str and end_str:
            start = int(start_str)
            end = int(end_str)
        elif start_str and not end_str:
            # Format: bytes=start- (from start to end of file)
            start = int(start_str)
            end = file_size - 1
        elif not start_str and end_str:
            # Format: bytes=-suffix (last N bytes)
            suffix = int(end_str)
            start = max(0, file_size - suffix)
            end = file_size - 1
        else:
            # Invalid: bytes=-
            raise ValueError("Invalid range")

        # Validate range
        if start < 0 or end < 0 or start > end or start >= file_size:
            return Response(
                status_code=416,
                content=f"Range not satisfiable: {range_header}",
                headers={"Content-Range": f"bytes */{file_size}"},
            )

        # Clamp end to file size
        end = min(end, file_size - 1)
        content_length = end - start + 1

        # Extract the requested byte range
        range_data = file_data[start : end + 1]

        logger.info(f"Range request for {object_key}: bytes {start}-{end}/{file_size} ({content_length} bytes)")

        headers = {
            "Content-Type": result["content_type"],
            "Content-Length": str(content_length),
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "ETag": f'"{result["md5_hash"]}"',
            "Last-Modified": result["created_at"].strftime("%a, %d %b %Y %H:%M:%S GMT"),
            "Content-Disposition": f'inline; filename="{object_key.split("/")[-1]}"',
            "x-amz-ipfs-cid": ipfs_cid,
            "Accept-Ranges": "bytes",
        }

        # Add custom metadata headers
        for key, value in metadata.items():
            if key != "ipfs" and not isinstance(value, dict):
                headers[f"x-amz-meta-{key}"] = str(value)

        return Response(
            content=range_data,
            status_code=206,  # Partial Content
            headers=headers,
        )

    except (ValueError, TypeError) as e:
        logger.warning(f"Invalid range request '{range_header}': {e}")
        return Response(
            status_code=416,
            content=f"Range not satisfiable: {range_header}",
            headers={"Content-Range": f"bytes */{file_size}"},
        )


def parse_range_header(range_header: str, total_size: int) -> Tuple[int, int]:
    """Parse Range header and return start and end bytes."""
    # Support both "bytes=start-end" and "bytes=start-" (open-ended)
    range_match = re.match(r"bytes=(\d+)-(\d*)", range_header.lower())
    if not range_match:
        raise ValueError(f"Invalid range format: {range_header}")

    start = int(range_match.group(1))
    end_str = range_match.group(2)

    # Open-ended range: "bytes=start-" means from start to end of file
    end = int(end_str) if end_str else total_size - 1

    return start, end


def calculate_chunks_for_range(start_byte: int, end_byte: int, parts_info: List[Dict]) -> List[int]:
    """Calculate which parts contain the requested byte range."""
    needed_parts = []
    current_offset = 0

    for part in parts_info:
        part_start = current_offset
        part_end = current_offset + part["size_bytes"] - 1

        # Check if this part overlaps with requested range
        if part_end >= start_byte and part_start <= end_byte:
            needed_parts.append(part["part_number"])

        current_offset += part["size_bytes"]

        # Stop if we've passed the end of requested range
        if part_start > end_byte:
            break

    return needed_parts


def extract_range_from_chunks(
    chunks_data: List[bytes],
    start_byte: int,
    end_byte: int,
    parts_info: List[Dict],
    needed_parts: List[int],
) -> bytes:
    """Extract the specific byte range from downloaded chunks."""
    current_offset = 0
    range_data = b""
    chunk_idx = 0

    logger.debug(f"Starting range extraction: {start_byte}-{end_byte}")

    for part in parts_info:
        part_start = current_offset
        part_end = current_offset + part["size_bytes"] - 1

        logger.debug(
            f"Processing part {part['part_number']}: offset {part_start}-{part_end}, size {part['size_bytes']}"
        )

        if part["part_number"] in needed_parts:
            chunk_data = chunks_data[chunk_idx]

            # Calculate precise slice boundaries within this chunk
            # slice_start: where to start reading within this chunk (0-based)
            slice_start = max(0, start_byte - part_start)
            # slice_end: where to stop reading within this chunk (exclusive, for slicing)
            slice_end = min(part["size_bytes"], end_byte - part_start + 1)

            logger.debug(
                f"Part {part['part_number']}: slice_start={slice_start}, slice_end={slice_end}, chunk_len={len(chunk_data)}"
            )

            # Extract the slice and add to range data
            chunk_slice = chunk_data[slice_start:slice_end]
            logger.debug(f"Part {part['part_number']}: extracted {len(chunk_slice)} bytes")
            range_data += chunk_slice
            chunk_idx += 1

        current_offset += part["size_bytes"]

        # Stop if we've passed the end of requested range
        if current_offset > end_byte:
            break

    # Verify we extracted exactly the right amount
    expected_length = end_byte - start_byte + 1
    if len(range_data) != expected_length:
        logger.error(f"Range extraction mismatch: got {len(range_data)} bytes, expected {expected_length}")
        logger.error(
            f"Range: {start_byte}-{end_byte}, Parts info: {[(p['part_number'], p['size_bytes']) for p in parts_info]}"
        )
        logger.error(f"Needed parts: {needed_parts}, Chunks data lengths: {[len(c) for c in chunks_data]}")

    return range_data


config = get_config()
security = HTTPBearer()
router = APIRouter(tags=["s3"])


async def get_request_body(request: Request) -> bytes:
    """Get request body properly handling chunked encoding from HAProxy."""
    return await utils.get_request_body(request)


def create_xml_error_response(
    code: str,
    message: str,
    status_code: int = 500,
    **kwargs,
) -> Response:
    """Generate a standardized XML error response using lxml."""
    error_root = ET.Element("Error", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")

    code_elem = ET.SubElement(error_root, "Code")
    code_elem.text = code

    message_elem = ET.SubElement(error_root, "Message")
    message_elem.text = message

    # Separate HTTP headers from XML attributes
    http_headers = {}
    xml_attributes = {}

    for key, value in kwargs.items():
        # Headers that should go into HTTP response headers (not XML body)
        if key in ["Retry-After", "x-hippius-retry-count", "x-hippius-max-retries"]:
            http_headers[key] = str(value)
        else:
            # Standard S3 XML attributes
            xml_attributes[key] = value

    # Add XML attributes to the error body
    for key, value in xml_attributes.items():
        elem = ET.SubElement(error_root, key)
        elem.text = str(value)

    # Add RequestId and HostId if not provided
    if "RequestId" not in xml_attributes:
        request_id = ET.SubElement(error_root, "RequestId")
        request_id.text = str(uuid.uuid4())

    if "HostId" not in xml_attributes:
        host_id = ET.SubElement(error_root, "HostId")
        host_id.text = "hippius-s3"

    xml_error = ET.tostring(
        error_root,
        encoding="UTF-8",
        xml_declaration=True,
        pretty_print=True,
    )

    # Merge default headers with custom headers
    response_headers = {
        "Content-Type": "application/xml; charset=utf-8",
        "x-amz-request-id": str(uuid.uuid4()),
    }
    response_headers.update(http_headers)

    return Response(
        content=xml_error,
        media_type="application/xml",
        status_code=status_code,
        headers=response_headers,
    )


@router.get("/", status_code=200)
async def list_buckets(
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
) -> Response:
    """
    List user's buckets using S3 protocol (GET /).

    This endpoint is compatible with the S3 protocol used by MinIO and other S3 clients.
    Returns only buckets owned by the authenticated user.
    """
    try:
        # List only buckets owned by this main account
        results = await db.fetch(get_query("list_user_buckets"), request.state.account.main_account)

        root = ET.Element(
            "ListAllMyBucketsResult",
            xmlns="http://s3.amazonaws.com/doc/2006-03-01/",
        )

        owner = ET.SubElement(root, "Owner")
        owner_id = ET.SubElement(owner, "ID")
        owner_id.text = "hippius-s3-ipfs-gateway"
        display_name = ET.SubElement(owner, "DisplayName")
        display_name.text = "hippius-s3"

        buckets = ET.SubElement(root, "Buckets")

        for row in results:
            bucket = ET.SubElement(buckets, "Bucket")
            name = ET.SubElement(bucket, "Name")
            name.text = row["bucket_name"]

            creation_date = ET.SubElement(bucket, "CreationDate")
            creation_date.text = format_s3_timestamp(row["created_at"])

        xml_content = ET.tostring(
            root,
            encoding="UTF-8",
            xml_declaration=True,
            pretty_print=True,
        )

        return Response(
            content=xml_content,
            media_type="application/xml",
        )

    except Exception:
        logger.exception("Error listing buckets via S3 protocol")
        return create_xml_error_response(
            "InternalError",
            "We encountered an internal error. Please try again.",
            status_code=500,
        )


@router.get("/{bucket_name}/", status_code=200, include_in_schema=True)
@router.get("/{bucket_name}", status_code=200, include_in_schema=True)
async def get_bucket(
    bucket_name: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
) -> Response:
    """
    List objects in a bucket using S3 protocol (GET /{bucket_name}).
    Also handles getting bucket location (GET /{bucket_name}?location).
    And getting bucket tags (GET /{bucket_name}?tagging).
    And getting bucket lifecycle (GET /{bucket_name}?lifecycle).

    This endpoint is compatible with the S3 protocol used by MinIO and other S3 clients.
    """
    # If the location query parameter is present, this is a bucket location request
    if "location" in request.query_params:
        logger.info(f"Handling location request for bucket {bucket_name}")
        return await get_bucket_location(
            bucket_name,
            db,
            request.state.account.main_account,
        )

    # If the tagging query parameter is present, this is a bucket tags request
    if "tagging" in request.query_params:
        logger.info(f"Handling tagging request for bucket {bucket_name}")

        try:
            # Get bucket for this main account
            bucket = await db.fetchrow(
                get_query("get_bucket_by_name_and_owner"), bucket_name, request.state.account.main_account
            )

            if not bucket:
                # For S3 compatibility, return XML error for non-existent buckets
                return errors.s3_error_response(
                    code="NoSuchBucket",
                    message=f"The specified bucket {bucket_name} does not exist",
                    status_code=404,
                    BucketName=bucket_name,
                )

            # Create Tagging XML response
            root = ET.Element("Tagging", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")
            tag_set = ET.SubElement(root, "TagSet")

            # Get tags from the bucket
            tags = bucket.get("tags", {})
            if isinstance(tags, str):
                tags = json.loads(tags)

            # If no tags present, S3 returns
            if not tags:
                return errors.s3_error_response(
                    code="NoSuchTagSet",
                    message="The TagSet does not exist",
                    status_code=404,
                    BucketName=bucket_name,
                )

            # Add tags to the XML response
            for key, value in tags.items():
                tag = ET.SubElement(tag_set, "Tag")
                key_elem = ET.SubElement(tag, "Key")
                key_elem.text = key
                value_elem = ET.SubElement(tag, "Value")
                value_elem.text = str(value)

            # Generate XML with proper declaration
            xml_content = ET.tostring(
                root,
                encoding="UTF-8",
                xml_declaration=True,
                pretty_print=True,
            )

            # Generate request ID for tracing
            request_id = str(uuid.uuid4())

            logger.debug(f"Bucket tags response content length: {len(xml_content)}")

            # Return formatted response
            return Response(
                content=xml_content,
                media_type="application/xml",
                status_code=200,
                headers={
                    "Content-Type": "application/xml; charset=utf-8",
                    "Content-Length": str(len(xml_content)),
                    "x-amz-request-id": request_id,
                },
            )

        except Exception as e:
            logger.exception(f"Error getting bucket tags: {e}")
            return errors.s3_error_response(
                code="InternalError",
                message="We encountered an internal error. Please try again.",
                status_code=500,
            )

    # If the lifecycle query parameter is present, this is a bucket lifecycle request
    if "lifecycle" in request.query_params:
        return await get_bucket_lifecycle(
            bucket_name,
            db,
            request.state.seed_phrase,
            request.state.account.main_account,
        )

    # If the uploads parameter is present, this is a multipart uploads listing request
    if "uploads" in request.query_params:
        logger.info(f"Handling multipart uploads listing request for bucket {bucket_name}")
        # Import locally to avoid circular imports
        from hippius_s3.api.s3.multipart import list_multipart_uploads

        return await list_multipart_uploads(bucket_name, request, db)

    # If the policy query parameter is present, this is a bucket policy request
    if "policy" in request.query_params:
        return await get_bucket_policy(bucket_name, db, request.state.account.main_account)

    try:
        # Get bucket for this main account
        bucket = await db.fetchrow(
            get_query("get_bucket_by_name_and_owner"),
            bucket_name,
            request.state.account.main_account,
        )

        if not bucket:
            return create_xml_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        bucket_id = bucket["bucket_id"]
        prefix = request.query_params.get(
            "prefix",
            None,
        )

        query = get_query("list_objects")
        results = await db.fetch(
            query,
            bucket_id,
            prefix,
        )

        # Create XML using lxml for better compatibility with MinIO client
        root = ET.Element(
            "ListBucketResult",
            xmlns="http://s3.amazonaws.com/doc/2006-03-01/",
        )

        name = ET.SubElement(root, "Name")
        name.text = bucket_name

        prefix_elem = ET.SubElement(root, "Prefix")
        prefix_elem.text = prefix or ""

        marker = ET.SubElement(root, "Marker")
        marker.text = ""

        max_keys = ET.SubElement(root, "MaxKeys")
        max_keys.text = "1000"

        is_truncated = ET.SubElement(root, "IsTruncated")
        is_truncated.text = "false"

        # Add content objects
        for obj in results:
            content = ET.SubElement(root, "Contents")

            key = ET.SubElement(content, "Key")
            key.text = obj["object_key"]

            last_modified = ET.SubElement(content, "LastModified")
            last_modified.text = format_s3_timestamp(obj["created_at"])

            etag = ET.SubElement(content, "ETag")
            # Use MD5 hash as ETag for AWS CLI compatibility, fallback to CID if not available
            etag.text = obj["md5_hash"]

            size = ET.SubElement(content, "Size")
            size.text = str(obj["size_bytes"])

            # Use StorageClass (always STANDARD for AWS CLI compatibility)
            storage_class = ET.SubElement(content, "StorageClass")
            storage_class.text = "STANDARD"

            # Use Owner for IPFS CID and account ID
            owner = ET.SubElement(content, "Owner")
            owner_id = ET.SubElement(owner, "ID")
            owner_display_name = ET.SubElement(owner, "DisplayName")

            ipfs_cid = obj.get("ipfs_cid")
            if ipfs_cid and ipfs_cid.strip():
                owner_id.text = ipfs_cid
            else:
                owner_id.text = "pending"
            owner_display_name.text = request.state.account.main_account  # Account ID of bucket owner

        xml_content = ET.tostring(
            root,
            encoding="UTF-8",
            xml_declaration=True,
            pretty_print=True,
        )

        # Add custom headers with IPFS CID count and status summary
        total_objects = len(results)
        objects_with_cid = sum(1 for obj in results if obj.get("ipfs_cid"))
        status_counts = {}
        for obj in results:
            status = obj.get("status", "unknown")
            status_counts[status] = status_counts.get(status, 0) + 1

        return Response(
            content=xml_content,
            media_type="application/xml",
            headers={
                "x-amz-bucket-objects-total": str(total_objects),
                "x-amz-bucket-objects-with-cid": str(objects_with_cid),
                "x-amz-bucket-objects-pending": str(total_objects - objects_with_cid),
                "x-amz-bucket-status-publishing": str(status_counts.get("publishing", 0)),
                "x-amz-bucket-status-pinning": str(status_counts.get("pinning", 0)),
                "x-amz-bucket-status-uploaded": str(status_counts.get("uploaded", 0)),
            },
        )

    except Exception:
        logger.exception("Error listing bucket objects via S3 protocol")
        return create_xml_error_response(
            "InternalError",
            "We encountered an internal error. Please try again.",
            status_code=500,
        )


async def delete_bucket_tags(
    bucket_name: str,
    db: dependencies.DBConnection,
    main_account_id: str,
) -> Response:
    """
    Delete all tags from a bucket (DELETE /{bucket_name}?tagging).

    This is used by the MinIO client to remove all bucket tags.
    """
    try:
        # Get bucket for this main account
        bucket = await db.fetchrow(
            get_query("get_bucket_by_name_and_owner"),
            bucket_name,
            main_account_id,
        )

        if not bucket:
            # For S3 compatibility, return an XML error response
            return create_xml_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        # Clear bucket tags by setting to empty JSON
        logger.info(f"Deleting all tags for bucket '{bucket_name}' via S3 protocol")

        await db.fetchrow(
            get_query("update_bucket_tags"),
            bucket["bucket_id"],
            json.dumps({}),
        )

        return Response(status_code=204)

    except Exception:
        logger.exception("Error deleting bucket tags")
        return create_xml_error_response(
            "InternalError",
            "We encountered an internal error. Please try again.",
            status_code=500,
        )


async def get_object_tags(
    bucket_name: str,
    object_key: str,
    db: dependencies.DBConnection,
    _: str,
    main_account_id: str,
) -> Response:
    """
    Get the tags of an object (GET /{bucket_name}/{object_key}?tagging).

    This is used by the MinIO client to retrieve object tags.
    """
    try:
        # Get user for user-scoped bucket lookup
        await db.fetchrow(
            get_query("get_or_create_user_by_main_account"),
            main_account_id,
            datetime.now(UTC),
        )

        bucket = await db.fetchrow(
            get_query("get_bucket_by_name_and_owner"),
            bucket_name,
            main_account_id,
        )

        if not bucket:
            # For S3 compatibility, return an XML error response
            return create_xml_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        object_info = await db.fetchrow(
            get_query("get_object_by_path"),
            bucket["bucket_id"],
            object_key,
        )

        if not object_info:
            # For S3 compatibility, return an XML error response
            return create_xml_error_response(
                "NoSuchKey",
                f"The specified key {object_key} does not exist",
                status_code=404,
                Key=object_key,
            )

        # Get object metadata
        metadata = object_info.get("metadata", {})
        if isinstance(metadata, str):
            metadata = json.loads(metadata)

        # Extract tags from metadata
        tags = metadata.get("tags", {})
        if not isinstance(tags, dict):
            tags = {}

        # Create XML response with tags
        root = ET.Element(
            "Tagging",
            xmlns="http://s3.amazonaws.com/doc/2006-03-01/",
        )
        tag_set = ET.SubElement(root, "TagSet")

        for key, value in tags.items():
            tag = ET.SubElement(tag_set, "Tag")
            key_elem = ET.SubElement(tag, "Key")
            key_elem.text = key
            value_elem = ET.SubElement(tag, "Value")
            value_elem.text = str(value)

        xml_content = ET.tostring(
            root,
            encoding="UTF-8",
            xml_declaration=True,
            pretty_print=True,
        )

        return Response(
            content=xml_content,
            media_type="application/xml",
        )

    except Exception:
        logger.exception("Error getting object tags")
        return create_xml_error_response(
            "InternalError",
            "We encountered an internal error. Please try again.",
            status_code=500,
        )


async def set_object_tags(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: dependencies.DBConnection,
    _: str,
    main_account_id: str,
) -> Response:
    """
    Set tags for an object (PUT /{bucket_name}/{object_key}?tagging).

    This is used by the MinIO client to set object tags.
    """
    try:
        await db.fetchrow(
            get_query("get_or_create_user_by_main_account"),
            main_account_id,
            datetime.now(UTC),
        )

        bucket = await db.fetchrow(
            get_query("get_bucket_by_name_and_owner"),
            bucket_name,
            main_account_id,
        )

        if not bucket:
            return create_xml_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        query = get_query("get_object_by_path")
        object_info = await db.fetchrow(
            query,
            bucket["bucket_id"],
            object_key,
        )

        if not object_info:
            return create_xml_error_response(
                "NoSuchKey",
                f"The specified key {object_key} does not exist",
                status_code=404,
                Key=object_key,
            )

        # Parse the XML tag data from the request
        xml_data = await get_request_body(request)
        if not xml_data:
            # Empty tags is valid (clears tags)
            tag_dict = {}
        else:
            try:
                # Parse XML using lxml
                root = ET.fromstring(xml_data)

                # Use S3 namespace for Tagging XML
                ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
                tag_dict = {}
                tag_elements = root.xpath(".//s3:Tag", namespaces=ns)  # type: ignore[attr-defined]

                for tag_elem in tag_elements:
                    key_nodes = tag_elem.xpath("./s3:Key", namespaces=ns)  # type: ignore[attr-defined]
                    value_nodes = tag_elem.xpath("./s3:Value", namespaces=ns)  # type: ignore[attr-defined]
                    if (
                        key_nodes
                        and value_nodes
                        and key_nodes[0] is not None
                        and value_nodes[0] is not None
                        and key_nodes[0].text
                        and value_nodes[0].text
                    ):
                        tag_dict[str(key_nodes[0].text)] = str(value_nodes[0].text)
            except Exception:
                logger.exception("Error parsing XML tags")
                return create_xml_error_response(
                    "MalformedXML",
                    "The XML you provided was not well-formed or did not validate against our published schema.",
                    status_code=400,
                )

        # Update the object's metadata with the new tags
        metadata = object_info.get("metadata", {})
        if isinstance(metadata, str):
            metadata = json.loads(metadata)

        # Add or update the tags in the metadata
        metadata["tags"] = tag_dict

        # Update the object's metadata in the database
        object_id = object_info["object_id"]
        logger.info(f"Setting tags for object '{object_key}' in bucket '{bucket_name}': {tag_dict}")

        # Use the update_object_metadata query
        query = get_query("update_object_metadata")
        await db.fetchrow(
            query,
            json.dumps(metadata),
            object_id,
        )

        return Response(status_code=200)

    except Exception:
        logger.exception("Error setting object tags")
        return create_xml_error_response(
            "InternalError",
            "We encountered an internal error. Please try again.",
            status_code=500,
        )


async def delete_object_tags(
    bucket_name: str,
    object_key: str,
    db: dependencies.DBConnection,
    _: str,
    main_account_id: str,
) -> Response:
    """
    Delete all tags from an object (DELETE /{bucket_name}/{object_key}?tagging).

    This is used by the MinIO client to remove all object tags.
    """
    try:
        # Get user for user-scoped bucket lookup
        user = await db.fetchrow(
            get_query("get_or_create_user_by_main_account"),
            main_account_id,
            datetime.now(UTC),
        )

        bucket = await db.fetchrow(
            get_query("get_bucket_by_name_and_owner"),
            bucket_name,
            user["main_account_id"],
        )

        if not bucket:
            return create_xml_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        query = get_query("get_object_by_path")
        object_info = await db.fetchrow(
            query,
            bucket["bucket_id"],
            object_key,
        )

        if not object_info:
            # For S3 compatibility, return NoSuchKey when deleting tags for a missing object
            return create_xml_error_response(
                "NoSuchKey",
                f"The specified key {object_key} does not exist",
                status_code=404,
                Key=object_key,
            )

        # Update the object's metadata to remove tags
        metadata = object_info.get("metadata", {})
        if isinstance(metadata, str):
            metadata = json.loads(metadata)

        # Remove tags from metadata
        if "tags" in metadata:
            metadata["tags"] = {}

        # Update the object's metadata in the database
        object_id = object_info["object_id"]
        logger.info(f"Deleting all tags for object '{object_key}' in bucket '{bucket_name}'")

        # Use the update_object_metadata query
        query = get_query("update_object_metadata")
        await db.fetchrow(
            query,
            json.dumps(metadata),
            object_id,
        )

        return Response(status_code=204)

    except Exception:
        logger.exception("Error deleting object tags")
        return create_xml_error_response(
            "InternalError",
            "We encountered an internal error. Please try again.",
            status_code=500,
        )


async def get_bucket_tags(
    bucket_name: str,
    db: dependencies.DBConnection,
    _: str,
    main_account_id: str,
) -> Response:
    """
    Get the tags of a bucket (GET /{bucket_name}?tagging).

    This is used by the MinIO client to retrieve bucket tags.
    """
    logger.info(f"Getting tags for bucket {bucket_name}")
    try:
        # Get user for user-scoped bucket lookup
        user = await db.fetchrow(
            get_query("get_or_create_user_by_main_account"),
            main_account_id,
            datetime.now(UTC),
        )

        bucket = await db.fetchrow(
            get_query("get_bucket_by_name_and_owner"),
            bucket_name,
            user["main_account_id"],
        )

        if not bucket:
            # For S3 compatibility, return an XML error response for non-existent buckets
            return errors.s3_error_response(
                code="NoSuchBucket",
                message=f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        # Get tags from the bucket
        tags = bucket.get("tags", {})
        if isinstance(tags, str):
            tags = json.loads(tags)

        # Create XML using lxml for better compatibility with MinIO client
        root = ET.Element("Tagging", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")
        tag_set = ET.SubElement(root, "TagSet")

        # Add tags
        for key, value in tags.items():
            tag = ET.SubElement(tag_set, "Tag")
            key_elem = ET.SubElement(tag, "Key")
            key_elem.text = key
            value_elem = ET.SubElement(tag, "Value")
            value_elem.text = str(value)

        # Generate XML with proper declaration
        xml_content = ET.tostring(root, encoding="UTF-8", xml_declaration=True, pretty_print=True)

        # Generate request ID for tracing
        request_id = str(uuid.uuid4())

        return Response(
            content=xml_content,
            media_type="application/xml",
            headers={
                "Content-Type": "application/xml; charset=utf-8",
                "Content-Length": str(len(xml_content)),
                "x-amz-request-id": request_id,
            },
        )

    except Exception as e:
        logger.exception(f"Error getting bucket tags: {e}")
        # Create an empty tags response for compatibility
        root = ET.Element("Tagging", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")
        ET.SubElement(root, "TagSet")

        # Generate XML with proper declaration
        xml_content = ET.tostring(root, encoding="UTF-8", xml_declaration=True, pretty_print=True)

        # Generate request ID for tracing
        request_id = str(uuid.uuid4())

        return Response(
            content=xml_content,
            media_type="application/xml",
            headers={
                "Content-Type": "application/xml; charset=utf-8",
                "Content-Length": str(len(xml_content)),
                "x-amz-request-id": request_id,
            },
        )


async def get_bucket_lifecycle(
    bucket_name: str,
    db: dependencies.DBConnection,
    _: str,
    main_account_id: str,
) -> Response:
    """
    Get the lifecycle configuration of a bucket (GET /{bucket_name}?lifecycle).

    This is used by the MinIO client to retrieve bucket lifecycle configuration.
    """
    try:
        # Get user for user-scoped bucket lookup
        user = await db.fetchrow(
            get_query("get_or_create_user_by_main_account"),
            main_account_id,
            datetime.now(UTC),
        )

        bucket = await db.fetchrow(
            get_query("get_bucket_by_name_and_owner"),
            bucket_name,
            user["main_account_id"],
        )

        if not bucket:
            # For S3 compatibility, return an XML error response
            return create_xml_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        # Not persisted yet: align with AWS and return NoSuchLifecycleConfiguration when not configured
        return create_xml_error_response(
            "NoSuchLifecycleConfiguration",
            "The lifecycle configuration does not exist",
            status_code=404,
            BucketName=bucket_name,
        )

    except Exception:
        logger.exception("Error getting bucket lifecycle")
        return create_xml_error_response(
            "InternalError",
            "We encountered an internal error. Please try again.",
            status_code=500,
        )


async def get_bucket_location(
    bucket_name: str,
    _: dependencies.DBConnection,
    __: str,
) -> Response:
    """
    Get the region/location of a bucket (GET /{bucket_name}?location).

    This is used by the MinIO client to determine the bucket's region.
    """
    # Always return a valid XML response for bucket location queries
    # Even for buckets that don't exist - this is what Minio client expects
    # For S3 compatibility, we use us-east-1 as the default region
    try:
        # This is what most S3 clients expect for the LocationConstraint response
        # Create a hardcoded response to ensure it's correctly formatted
        xml = '<?xml version="1.0" encoding="UTF-8"?>'
        xml += '<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>'

        logger.info(f"get_bucket_location response for {bucket_name}: {xml}")

        return Response(
            content=xml.encode("utf-8"),
            media_type="application/xml",
            headers={"Content-Type": "application/xml"},
        )
    except Exception as e:
        logger.exception(f"Error getting bucket location: {e}")
        # Even on error, return a valid XML response
        xml = '<?xml version="1.0" encoding="UTF-8"?>'
        xml += '<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>'

        return Response(
            content=xml.encode("utf-8"),
            media_type="application/xml",
            headers={"Content-Type": "application/xml"},
        )


@router.head("/{bucket_name}/", status_code=200)
@router.head("/{bucket_name}", status_code=200)
async def head_bucket(
    bucket_name: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
) -> Response:
    """
    Check if a bucket exists using S3 protocol (HEAD /{bucket_name}).

    This endpoint is compatible with the S3 protocol used by MinIO and other S3 clients.
    """
    try:
        # Get user for user-scoped bucket lookup
        main_account_id = request.state.account.main_account
        await db.fetchrow(
            get_query("get_or_create_user_by_main_account"),
            main_account_id,
            datetime.now(UTC),
        )

        bucket = await db.fetchrow(
            get_query("get_bucket_by_name_and_owner"),
            bucket_name,
            main_account_id,
        )

        if not bucket:
            # For HEAD requests, the Minio client expects a specific format
            # Return a 404 without content for proper bucket_exists handling
            return Response(status_code=404)

        # For existing buckets, return an empty 200 response
        return Response(status_code=200)

    except Exception:
        logger.exception("Error checking bucket via S3 protocol")
        return Response(status_code=500)  # Simple 500 response for HEAD requests


@router.put("/{bucket_name}/", status_code=200)
@router.put("/{bucket_name}", status_code=200)
async def create_bucket(
    bucket_name: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
) -> Response:
    """
    Create a new bucket using S3 protocol (PUT /{bucket_name}).
    Also handles setting bucket tags (PUT /{bucket_name}?tagging=).
    Also handles setting bucket lifecycle (PUT /{bucket_name}?lifecycle=).

    This endpoint is compatible with the S3 protocol used by MinIO and other S3 clients.
    """
    # Check if this is a request to set bucket lifecycle
    if "lifecycle" in request.query_params:
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
                return create_xml_error_response(
                    "NoSuchBucket",
                    f"The specified bucket {bucket_name} does not exist",
                    status_code=404,
                    BucketName=bucket_name,
                )

            # Parse the XML lifecycle configuration
            xml_data = await get_request_body(request)

            # If no XML data provided, return a MalformedXML error
            if not xml_data:
                return create_xml_error_response(
                    "MalformedXML",
                    "The XML you provided was not well-formed or did not validate against our published schema.",
                    status_code=400,
                )

            try:
                parsed_xml = ET.fromstring(xml_data)
                # Accept namespaced and non-namespaced lifecycle XML
                rules = parsed_xml.xpath(".//*[local-name()='Rule']")  # type: ignore[attr-defined]
                rule_ids = []
                for rule in rules:
                    id_nodes = rule.xpath("./*[local-name()='ID']")  # type: ignore[attr-defined]
                    if id_nodes and id_nodes[0] is not None and id_nodes[0].text:
                        rule_ids.append(id_nodes[0].text)

                # todo: For now, just acknowledge receipt
                logger.info(f"Received lifecycle configuration with {len(rules)} rules: {rule_ids}")
                logger.info(f"Setting lifecycle configuration for bucket '{bucket_name}' via S3 protocol")

                # Return success response - no content needed for PUT lifecycle
                return Response(status_code=200)

            except ET.XMLSyntaxError:
                return create_xml_error_response(
                    "MalformedXML",
                    "The XML you provided was not well-formed or did not validate against our published schema.",
                    status_code=400,
                )

        except Exception:
            logger.exception("Error setting bucket lifecycle")
            return create_xml_error_response(
                "InternalError",
                "We encountered an internal error. Please try again.",
                status_code=500,
            )

    # Check if this is a request to set bucket tags
    elif "tagging" in request.query_params:
        try:
            # Get user for user-scoped bucket lookup
            user = await db.fetchrow(
                get_query("get_or_create_user_by_main_account"),
                request.state.account.main_account,
                datetime.now(UTC),
            )

            # First check if the bucket exists
            bucket = await db.fetchrow(
                get_query("get_bucket_by_name_and_owner"),
                bucket_name,
                user["main_account_id"],
            )

            if not bucket:
                return create_xml_error_response(
                    "NoSuchBucket",
                    f"The specified bucket {bucket_name} does not exist",
                    status_code=404,
                    BucketName=bucket_name,
                )

            # Parse the XML tag data from the request
            xml_data = await get_request_body(request)
            if not xml_data:
                # Empty tags is valid (clears tags)
                tag_dict = {}
            else:
                try:
                    # Parse XML using lxml with S3 namespace
                    root = ET.fromstring(xml_data)
                    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

                    # Namespace-qualified selection
                    tag_dict = {}
                    tag_elements = root.xpath(".//s3:Tag", namespaces=ns)  # type: ignore[attr-defined]

                    for tag_elem in tag_elements:
                        key_nodes = tag_elem.xpath("./s3:Key", namespaces=ns)  # type: ignore[attr-defined]
                        value_nodes = tag_elem.xpath("./s3:Value", namespaces=ns)  # type: ignore[attr-defined]
                        if (
                            key_nodes
                            and value_nodes
                            and key_nodes[0] is not None
                            and value_nodes[0] is not None
                            and key_nodes[0].text
                            and value_nodes[0].text
                        ):
                            tag_dict[str(key_nodes[0].text)] = str(value_nodes[0].text)
                except Exception:
                    logger.exception("Error parsing XML tags")
                    return create_xml_error_response(
                        "MalformedXML",
                        "The XML you provided was not well-formed",
                        status_code=400,
                    )

            logger.info(f"Setting tags for bucket '{bucket_name}' via S3 protocol: {tag_dict}")
            await db.fetchrow(
                get_query("update_bucket_tags"),
                bucket["bucket_id"],
                json.dumps(tag_dict),
            )

            return Response(status_code=200)

        except Exception:
            logger.exception("Error setting bucket tags via S3 protocol")
            return create_xml_error_response(
                "InternalError",
                "We encountered an internal error. Please try again.",
                status_code=500,
            )

    # Check if this is a request to set bucket policy
    elif "policy" in request.query_params:
        return await set_bucket_policy(bucket_name, request, db)

    # Handle standard bucket creation if not a tagging, lifecycle, or policy request
    else:
        try:
            # Reject ACLs to match AWS when ObjectOwnership is BucketOwnerEnforced
            acl_header = request.headers.get("x-amz-acl")
            if acl_header:
                return create_xml_error_response(
                    "InvalidBucketAclWithObjectOwnership",
                    "Bucket cannot have ACLs set with ObjectOwnership's BucketOwnerEnforced setting",
                    status_code=400,
                )

            bucket_id = str(uuid.uuid4())
            created_at = datetime.now(UTC)

            # Bucket public/private is managed via policy, not ACL
            is_public = False

            # Get or create user record for the main account
            main_account_id = request.state.account.main_account
            await db.fetchrow(
                get_query("get_or_create_user_by_main_account"),
                main_account_id,
                created_at,
            )

            logger.info(f"Creating bucket '{bucket_name}' via S3 protocol for account {main_account_id}")

            query = get_query("create_bucket")
            await db.fetchrow(
                query,
                bucket_id,
                bucket_name,
                created_at,
                is_public,
                main_account_id,
            )

            return Response(status_code=200)

        except asyncpg.UniqueViolationError:
            return create_xml_error_response(
                "BucketAlreadyExists",
                f"The requested bucket {bucket_name} already exists",
                status_code=409,
                BucketName=bucket_name,
            )
        except Exception:
            logger.exception("Error creating bucket via S3 protocol")
            return create_xml_error_response(
                "InternalError",
                "We encountered an internal error. Please try again.",
                status_code=500,
            )


@router.delete("/{bucket_name}/", status_code=204)
@router.delete("/{bucket_name}", status_code=204)
async def delete_bucket(
    bucket_name: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
    redis_client=Depends(dependencies.get_redis),
) -> Response:
    """
    Delete a bucket using S3 protocol (DELETE /{bucket_name}).
    Also handles removing bucket tags (DELETE /{bucket_name}?tagging).

    This endpoint is compatible with the S3 protocol used by MinIO and other S3 clients.
    """
    # If tagging is in query params, we're just deleting tags
    if "tagging" in request.query_params:
        return await delete_bucket_tags(
            bucket_name,
            db,
            request.state.account.main_account,
        )

    try:
        # Get bucket for this main account
        main_account_id = request.state.account.main_account
        bucket = await db.fetchrow(
            get_query("get_bucket_by_name_and_owner"),
            bucket_name,
            main_account_id,
        )

        if not bucket:
            # For S3 compatibility, return an XML error response
            return create_xml_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        objects = await db.fetch(
            get_query("list_objects"),
            bucket["bucket_id"],
            None,
        )

        # Delete bucket (permission is checked via main_account_id ownership)
        deleted_bucket = await db.fetchrow(
            get_query("delete_bucket"),
            bucket["bucket_id"],
            main_account_id,
        )

        if not deleted_bucket:
            logger.warning(f"Account {main_account_id} tried to delete bucket {bucket_name} without permission")
            return create_xml_error_response(
                "AccessDenied",
                f"You do not have permission to delete bucket {bucket_name}",
                status_code=403,
                BucketName=bucket_name,
            )

        # If we got here, the bucket was successfully deleted, so now enqueue objects for unpinning
        for obj in objects:
            await enqueue_unpin_request(
                payload=UnpinChainRequest(
                    substrate_url=config.substrate_url,
                    ipfs_node=config.ipfs_store_url,
                    address=request.state.account.main_account,
                    subaccount=request.state.account.id,
                    subaccount_seed_phrase=request.state.seed_phrase,
                    bucket_name=bucket_name,
                    object_key=obj["object_key"],
                    cid=obj["ipfs_cid"],
                ),
                redis_client=redis_client,
            )

        return Response(status_code=204)

    except Exception:
        logger.exception("Error deleting bucket via S3 protocol")
        return create_xml_error_response(
            "InternalError",
            "We encountered an internal error. Please try again.",
            status_code=500,
        )


async def _copy_object(
    source_bucket: dict,
    destination_bucket: dict,
    source_object_key: str,
    request: fastapi.Request,
    db: DBConnection,
    ipfs_service: IPFSService,
):
    source_bucket_name = source_bucket["bucket_id"]
    try:
        logger.info(f"Copying {source_bucket}/{source_object_key} to {destination_bucket}")

        # Get the source object
        source_object = await db.fetchrow(
            get_query("get_object_by_path"),
            source_bucket["bucket_id"],
            source_object_key,
        )

        if not source_object:
            return create_xml_error_response(
                "NoSuchKey",
                f"The specified key {source_object_key} does not exist",
                status_code=404,
            )

        # Check if we can do a fast copy (same encryption context) or need decrypt/re-encrypt
        source_is_public = source_bucket["is_public"]
        dest_is_public = destination_bucket["is_public"]
        same_bucket = source_bucket["bucket_id"] == destination_bucket["bucket_id"]
        same_encryption_context = source_is_public == dest_is_public and same_bucket
        source_metadata = json.loads(source_object["metadata"])

        # Check for multipart objects - skip copying them for now
        if source_metadata.get("multipart"):
            return create_xml_error_response(
                "NotImplemented",
                "Copying multipart objects is not currently supported",
                status_code=501,
            )

        object_id = str(uuid.uuid4())
        created_at = datetime.now(UTC)

        if same_encryption_context:
            # Fast path: same encryption context, reuse CID
            logger.info(
                f"Fast copy: same encryption context (same_bucket={same_bucket}, both_public={source_is_public})"
            )
            ipfs_cid = source_object["ipfs_cid"]
            file_size = source_object["size_bytes"]
            content_type = source_object["content_type"]
            md5_hash = source_object["md5_hash"]  # Fallback to CID if no MD5

            # Copy all metadata as-is
            metadata = {
                "ipfs": source_metadata.get("ipfs", {}),
                "hippius": source_metadata.get("hippius", {}),
            }

            # Copy any user metadata (x-amz-meta-*)
            for key, value in source_metadata.items():
                if key not in ["ipfs", "hippius"]:
                    metadata[key] = value  # noqa: PERF403
        else:
            # Slow path: different encryption context, decrypt and re-encrypt
            logger.info(
                f"Slow copy: different encryption (source public={source_is_public}, dest public={dest_is_public})"
            )

            # Download file from source bucket with source encryption
            file_data = await ipfs_service.download_file(
                cid=source_object["ipfs_cid"],
                subaccount_id=request.state.account.main_account,
                bucket_name=source_bucket_name,
                decrypt=not source_is_public,  # Decrypt if source was encrypted
            )

            # Calculate MD5 hash for ETag compatibility
            md5_hash = hashlib.md5(file_data).hexdigest()

            # Re-encrypt for destination bucket
            should_encrypt = not dest_is_public
            s3_result = await ipfs_service.client.s3_publish(
                content=file_data,
                encrypt=should_encrypt,
                seed_phrase=request.state.seed_phrase,
                subaccount_id=request.state.account.main_account,
                bucket_name=source_bucket_name,
                file_name=source_object_key,
                store_node=config.ipfs_store_url,
                pin_node=config.ipfs_store_url,
                substrate_url=config.substrate_url,
                publish=(os.getenv("HIPPIUS_PUBLISH_MODE", "full") != "ipfs_only"),
            )

            ipfs_cid = s3_result.cid
            file_size = len(file_data)
            content_type = source_object["content_type"]

            # Create new metadata for destination
            metadata = {}
            # Copy any user metadata (x-amz-meta-*)
            for key, value in source_metadata.items():
                metadata[key] = value  # noqa: PERF403

        # Create or update the object using upsert with CID table
        cid_id = await utils.upsert_cid_and_get_id(db, ipfs_cid)
        await db.fetchrow(
            get_query("upsert_object_with_cid"),
            object_id,
            destination_bucket["bucket_id"],
            source_object_key,
            cid_id,
            file_size,
            content_type,
            created_at,
            json.dumps(metadata),
            md5_hash,
        )

        # Prepare the XML response
        root = ET.Element("CopyObjectResult")
        etag = ET.SubElement(root, "ETag")
        etag.text = md5_hash
        last_modified = ET.SubElement(root, "LastModified")
        last_modified.text = format_s3_timestamp(created_at)

        xml_response = ET.tostring(
            root,
            encoding="utf-8",
            xml_declaration=True,
        )

        return Response(
            content=xml_response,
            media_type="application/xml",
            status_code=200,
            headers={
                "ETag": f'"{md5_hash}"',
                "x-amz-ipfs-cid": ipfs_cid,
            },
        )

    except Exception as e:
        logger.exception("Error copying object")
        return create_xml_error_response(
            "InternalError",
            f"We encountered an internal error while copying the object: {str(e)}",
            status_code=500,
        )


@router.put("/{bucket_name}/{object_key:path}/", status_code=200)
@router.put("/{bucket_name}/{object_key:path}", status_code=200)
async def put_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
    ipfs_service=Depends(dependencies.get_ipfs_service),
    redis_client=Depends(dependencies.get_redis),
) -> Response:
    """
    Upload an object to a bucket using S3 protocol (PUT /{bucket_name}/{object_key}).
    Also handles setting object tags (PUT /{bucket_name}/{object_key}?tagging).
    Also handles copying objects when x-amz-copy-source header is present.

    Important: For multipart upload parts, this function defers to the multipart.upload_part handler.
    """
    # Check if this is a multipart upload part request (has both uploadId and partNumber)
    upload_id = request.query_params.get("uploadId")
    part_number = request.query_params.get("partNumber")

    if upload_id and part_number:
        # Forward multipart upload requests to the specialized handler
        return await upload_part(
            request,
            db,
        )

    if "tagging" in request.query_params:
        # If tagging is in query params, handle object tagging
        return await set_object_tags(
            bucket_name,
            object_key,
            request,
            db,
            request.state.seed_phrase,
            request.state.account.main_account,
        )

    # Check if this is a copy operation
    if request.headers.get("x-amz-copy-source"):
        # Parse the copy source in format /source-bucket/source-key
        copy_source = request.headers.get("x-amz-copy-source")
        if not copy_source.startswith("/"):
            return create_xml_error_response(
                "InvalidArgument",
                "x-amz-copy-source must start with /",
                status_code=400,
            )

        source_parts = copy_source[1:].split("/", 1)
        if len(source_parts) != 2:
            return create_xml_error_response(
                "InvalidArgument",
                "x-amz-copy-source must be in format /source-bucket/source-key",
                status_code=400,
            )
        source_bucket_name, source_object_key = source_parts

        # Get user for user-scoped bucket lookup
        user = await db.fetchrow(
            get_query("get_or_create_user_by_main_account"),
            request.state.account.main_account,
            datetime.now(UTC),
        )

        # Get the source bucket
        source_bucket = await db.fetchrow(
            get_query("get_bucket_by_name_and_owner"),
            source_bucket_name,
            user["main_account_id"],
        )

        if not source_bucket:
            return create_xml_error_response(
                "NoSuchBucket",
                f"The specified source bucket {source_bucket_name} does not exist",
                status_code=404,
                BucketName=source_bucket_name,
            )

        # Get the destination bucket (using same user as source)
        dest_bucket = await db.fetchrow(
            get_query("get_bucket_by_name_and_owner"),
            bucket_name,
            user["main_account_id"],
        )

        if not dest_bucket:
            return create_xml_error_response(
                "NoSuchBucket",
                f"The specified destination bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )
        return await _copy_object(
            source_bucket=source_bucket,
            destination_bucket=dest_bucket,
            source_object_key=source_object_key,
            request=request,
            db=db,
            ipfs_service=ipfs_service,
        )

    try:
        # Get or create user and bucket for this main account
        main_account_id = request.state.account.main_account
        await db.fetchrow(
            get_query("get_or_create_user_by_main_account"),
            main_account_id,
            datetime.now(UTC),
        )

        bucket = await db.fetchrow(
            get_query("get_bucket_by_name_and_owner"),
            bucket_name,
            main_account_id,
        )

        if not bucket:
            return create_xml_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        bucket_id = bucket["bucket_id"]

        # Read request body properly handling chunked encoding
        file_data = await get_request_body(request)
        file_size = len(file_data)
        content_type = request.headers.get("Content-Type", "application/octet-stream")

        # Calculate MD5 hash for ETag compatibility
        md5_hash = hashlib.md5(file_data).hexdigest()
        logger.info(f"PUT {bucket_name}/{object_key}: size={len(file_data)}, md5={md5_hash}")
        object_id = str(uuid.uuid4())

        # Only encrypt if bucket is private
        should_encrypt = not bucket["is_public"]

        part_index = 0
        redis_key = f"simple:{object_id}:part:{part_index}"

        # Store file data in Redis for the pinner to process
        await redis_client.set(redis_key, file_data)

        await enqueue_upload_request(
            payload=SimpleUploadChainRequest(
                substrate_url=config.substrate_url,
                ipfs_node=config.ipfs_store_url,
                address=request.state.account.main_account,
                subaccount=request.state.account.id,
                subaccount_seed_phrase=request.state.seed_phrase,
                bucket_name=bucket_name,
                object_key=object_key,
                should_encrypt=should_encrypt,
                object_id=object_id,
                chunk=Chunk(
                    id=part_index,
                    redis_key=redis_key,
                ),
            ),
            redis_client=redis_client,
        )

        created_at = datetime.now(UTC)

        # Extract user metadata from headers
        metadata = {}
        for key, value in request.headers.items():
            if key.lower().startswith("x-amz-meta-"):
                meta_key = key[11:]
                metadata[meta_key] = value

        # Begin a transaction to ensure atomic operation
        async with db.transaction():
            await db.fetchrow(
                get_query("upsert_object_basic"),
                object_id,
                bucket_id,
                object_key,
                content_type,
                json.dumps(metadata),
                md5_hash,
                file_size,
                created_at,
            )

        return Response(
            status_code=200,
            headers={
                "ETag": f'"{md5_hash}"',
            },
        )

    except Exception as e:
        logger.exception("Error uploading object")
        if isinstance(e, HippiusIPFSError):
            return create_xml_error_response(
                "InternalError",
                f"IPFS upload failed: {str(e)}",
                status_code=500,
            )
        if isinstance(e, HippiusSubstrateError):
            return create_xml_error_response(
                "InternalError",
                f"Blockchain publishing failed: {str(e)}",
                status_code=500,
            )

        return create_xml_error_response(
            "InternalError",
            "We encountered an internal error. Please try again.",
            status_code=500,
        )


async def _get_object_with_permissions(
    bucket_name: str,
    object_key: str,
    db: dependencies.DBConnection,
    main_account_id: str,
) -> asyncpg.Record:
    """Get object with proper permission checks for both public and private buckets."""
    logger.debug(f"Getting object with permissions {bucket_name}/{object_key}")

    # Get user for user-scoped bucket lookup
    user = await db.fetchrow(
        get_query("get_or_create_user_by_main_account"),
        main_account_id,
        datetime.now(UTC),
    )

    # First check if bucket exists and get its permissions
    bucket = await db.fetchrow(
        get_query("get_bucket_by_name"),
        bucket_name,
    )

    if not bucket:
        logger.info(f"Bucket not found: {bucket_name}")
        raise errors.S3Error(
            code="NoSuchBucket",
            status_code=404,
            message=f"The specified bucket {bucket_name} does not exist",
        )

    # Check if user has access to this bucket (either owner or bucket is public)
    if not bucket["is_public"]:
        # Private bucket - check ownership
        owner_bucket = await db.fetchrow(
            get_query("get_bucket_by_name_and_owner"),
            bucket_name,
            user["main_account_id"],
        )
        if not owner_bucket:
            logger.info(f"Access denied to private bucket: {bucket_name}")
            raise errors.S3Error(
                code="NoSuchBucket",
                status_code=404,
                message=f"The specified bucket {bucket_name} does not exist",
            )

    # Get object by path
    object = await db.fetchrow(
        get_query("get_object_by_path"),
        bucket["bucket_id"],
        object_key,
    )

    if not object:
        logger.info(f"Object not found: {bucket_name}/{object_key}")
        raise errors.S3Error(
            code="NoSuchKey",
            status_code=404,
            message=f"The specified key {object_key} does not exist",
        )

    logger.debug(f"Found object with permissions: {bucket_name}/{object_key}")
    return object


@router.head(
    "/{bucket_name}/{object_key:path}/",
    status_code=200,
)
@router.head(
    "/{bucket_name}/{object_key:path}",
    status_code=200,
)
async def head_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
) -> Response:
    """
    Get object metadata using S3 protocol (HEAD /{bucket_name}/{object_key}).
    Also handles getting object tags (HEAD /{bucket_name}/{object_key}?tagging).

    This endpoint is compatible with the S3 protocol used by MinIO and other S3 clients.
    This endpoint only checks if the object exists in the database and IPFS without
    downloading the entire object content.
    """
    # If tagging is in query params, handle object tags request (HEAD equivalent)
    logger.debug(f"Request query params {request.query_params}")
    if "tagging" in request.query_params:
        try:
            # Just check if the object exists, don't return the tags content for HEAD
            await _get_object_with_permissions(
                bucket_name,
                object_key,
                db,
                request.state.account.main_account,
            )
            return Response(status_code=200)
        except errors.S3Error as e:
            logger.info(f"S3 error in HEAD tagging request: {e.code} - {e.message}")
            return Response(status_code=e.status_code)
        except Exception as e:
            logger.exception(f"Error in HEAD tagging request: {e}")
            return Response(status_code=500)

    # Handle other query parameters similarly to GET
    # Add other query param handlers as needed, matching the GET handler's structure

    # Default HEAD behavior for regular object metadata
    try:
        result = await _get_object_with_permissions(
            bucket_name,
            object_key,
            db,
            main_account_id=request.state.account.main_account,
        )

        metadata = json.loads(result["metadata"])
        ipfs_cid = result["ipfs_cid"]

        # Check object status and return 202 if still processing
        object_status = result.get("status", "unknown")
        if object_status in ["pending", "pinning"]:
            retry_after = "60" if result.get("multipart") else "30"
            headers = {
                "Retry-After": retry_after,
                "x-amz-object-status": object_status,
            }
            return Response(status_code=202, headers=headers)

        headers = {
            "Content-Type": result["content_type"],
            "Content-Length": str(result["size_bytes"]),
            "ETag": f'"{result["md5_hash"]}"',
            "Last-Modified": result["created_at"].strftime("%a, %d %b %Y %H:%M:%S GMT"),
            "x-amz-ipfs-cid": ipfs_cid or "pending",
            "x-amz-object-status": object_status,
        }

        for key, value in metadata.items():
            if key != "ipfs" and not isinstance(value, dict):
                headers[f"x-amz-meta-{key}"] = str(value)

        return Response(status_code=200, headers=headers)

    except errors.S3Error as e:
        # For HEAD requests, we should return just the status code without content
        # Otherwise Minio client gets confused by XML in HEAD responses
        logger.info(f"S3 error in HEAD request: {e.code} - {e.message}")
        return Response(status_code=e.status_code)

    except Exception as e:
        logger.exception(f"Error getting object metadata: {e}")
        # For HEAD requests, just return 500 without XML body
        return Response(status_code=500)


@router.get("/{bucket_name}/{object_key:path}/", status_code=200)
@router.get("/{bucket_name}/{object_key:path}", status_code=200)
async def get_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
    ipfs_service=Depends(dependencies.get_ipfs_service),
    redis_client=Depends(dependencies.get_redis),
) -> Response:
    """
    Get an object using S3 protocol (GET /{bucket_name}/{object_key}).
    Also handles getting object tags (GET /{bucket_name}/{object_key}?tagging).

    This endpoint now uses a queuing system for downloads to handle IPFS retrieval asynchronously.
    """
    # If tagging is in query params, handle object tags request
    if "tagging" in request.query_params:
        return await get_object_tags(
            bucket_name,
            object_key,
            db,
            request.state.seed_phrase,
            request.state.account.main_account,
        )

    # Check for Range header - handle partial content requests
    range_header = request.headers.get("Range") or request.headers.get("range")

    try:
        # Get user for user-scoped bucket lookup (creates user if not exists)
        await db.fetchrow(
            get_query("get_or_create_user_by_main_account"),
            request.state.account.main_account,
            datetime.now(UTC),
        )

        # Get object info for download with permission checks
        # This query handles both public buckets (accessible by anyone) and private buckets (owner only)
        object_info = await db.fetchrow(
            get_query("get_object_for_download_with_permissions"),
            bucket_name,
            object_key,
            request.state.account.main_account,
        )

        if not object_info:
            return create_xml_error_response(
                "NoSuchKey",
                f"The specified key {object_key} does not exist or you don't have permission to access it",
                status_code=404,
                Key=object_key,
            )

        download_chunks = json.loads(object_info["download_chunks"])

        # Check if all chunks have CIDs - if not, object is still being processed
        if not download_chunks:
            # Track retry attempts using request headers
            retry_count = int(request.headers.get("x-hippius-retry-count", "0"))
            max_retries = 5

            if retry_count >= max_retries:
                return create_xml_error_response(
                    "ServiceUnavailable",
                    "Object publish failed after maximum retries. Please check object status.",
                    status_code=503,
                )

            return create_xml_error_response(
                "ServiceUnavailable",
                "Object publish is in progress. Please retry shortly.",
                status_code=503,
                **{
                    "Retry-After": "30",
                    "x-hippius-retry-count": str(retry_count),
                    "x-hippius-max-retries": str(max_retries),
                },
            )

        # Check if any chunk is missing a CID (still being processed)
        missing_cids = [chunk for chunk in download_chunks if not chunk.get("cid")]
        if missing_cids:
            # Track retry attempts using request headers
            retry_count = int(request.headers.get("x-hippius-retry-count", "0"))
            max_retries = 5

            if retry_count >= max_retries:
                return create_xml_error_response(
                    "ServiceUnavailable",
                    "Object publish failed after maximum retries. Please check object status.",
                    status_code=503,
                )

            return create_xml_error_response(
                "ServiceUnavailable",
                "Object publish is in progress. Please retry shortly.",
                status_code=503,
                **{
                    "Retry-After": "30",
                    "x-hippius-retry-count": str(retry_count),
                    "x-hippius-max-retries": str(max_retries),
                },
            )

        # Handle range requests by calculating needed chunks
        if range_header:
            start_byte, end_byte = parse_range_header(range_header, object_info["size_bytes"])
            needed_parts = calculate_chunks_for_range(start_byte, end_byte, download_chunks)
            filtered_chunks = [chunk for chunk in download_chunks if chunk["part_number"] in needed_parts]
        else:
            filtered_chunks = download_chunks
            start_byte = end_byte = None

        # Create ChunkToDownload objects with redis keys
        # Use deterministic request ID so multiple requests for same object reuse same download
        request_uuid = f"{object_info['object_id']}_{request.state.account.main_account}"
        if range_header:
            request_uuid += f"_{start_byte}_{end_byte}"  # Make range requests unique

        chunks = []
        for chunk_info in filtered_chunks:
            redis_key = f"downloaded:{object_key}:{chunk_info['part_number']}:{request_uuid}"
            chunks.append(
                ChunkToDownload(
                    cid=chunk_info["cid"],
                    part_id=chunk_info["part_number"],
                    redis_key=redis_key,
                )
            )

        # Create download request
        config = get_config()
        download_request = DownloadChainRequest(
            request_id=request_uuid,
            object_id=str(object_info["object_id"]),
            object_key=object_info["object_key"],
            bucket_name=object_info["bucket_name"],
            address=request.state.account.main_account,
            subaccount=request.state.account.id,
            subaccount_seed_phrase=request.state.seed_phrase,
            substrate_url=config.substrate_url,
            ipfs_node=config.ipfs_get_url,
            should_decrypt=object_info["should_decrypt"],
            size=object_info["size_bytes"],
            multipart=object_info["multipart"],
            chunks=chunks,
        )

        # Check if download is already in progress using a Redis flag
        download_flag_key = f"download_in_progress:{request_uuid}"

        # Try to set the flag atomically (NX = only set if not exists, EX = expiration in seconds)
        flag_set = await request.app.state.redis_client.set(download_flag_key, "1", nx=True, ex=300)  # 5 min expiry

        if flag_set:
            # We successfully set the flag, so we're the first request - enqueue the download
            await enqueue_download_request(download_request, request.app.state.redis_client)
            logger.info(f"Enqueued download request for {bucket_name}/{object_key}")
        else:
            logger.info(f"Download already in progress for {bucket_name}/{object_key}, reusing existing request")

        # Handle range vs full download
        if range_header and start_byte is not None and end_byte is not None:
            # For range requests, download all chunks first then extract range
            async def generate_range():
                """Download chunks and extract specific byte range."""
                chunks_data = []

                # Download all needed chunks
                for chunk in chunks:
                    chunk_data = await get_chunk_from_redis(
                        request.app.state.redis_client,
                        chunk,
                    )
                    chunks_data.append(chunk_data)

                # Extract the specific range from chunks
                logger.debug(f"Extracting range {start_byte}-{end_byte} from {len(filtered_chunks)} chunks")
                range_data = extract_range_from_chunks(chunks_data, start_byte, end_byte, download_chunks, needed_parts)
                logger.debug(f"Extracted {len(range_data)} bytes for range {start_byte}-{end_byte}")

                # Yield the range data in smaller chunks to avoid streaming issues
                chunk_size = 64 * 1024  # 64KB chunks
                for i in range(0, len(range_data), chunk_size):
                    yield range_data[i : i + chunk_size]

            # Range request headers
            headers = {
                "Content-Type": object_info["content_type"],
                "Content-Range": f"bytes {start_byte}-{end_byte}/{object_info['size_bytes']}",
                "Accept-Ranges": "bytes",
            }

            return StreamingResponse(
                generate_range(),
                status_code=206,
                media_type=object_info["content_type"],
                headers=headers,
            )

        # Full file download
        async def generate_chunks():
            """Stream downloaded chunks as they become available in Redis."""
            for chunk in chunks:
                chunk_data = await get_chunk_from_redis(
                    request.app.state.redis_client,
                    chunk,
                )
                yield chunk_data

        # Full file headers
        headers = {
            "Content-Type": object_info["content_type"],
            "Content-Disposition": f'inline; filename="{object_key.split("/")[-1]}"',
            "Accept-Ranges": "bytes",
            "ETag": f'"{object_info["md5_hash"]}"',
            "Last-Modified": object_info["created_at"].strftime("%a, %d %b %Y %H:%M:%S GMT"),
        }

        # Add custom metadata headers
        metadata = object_info.get("metadata") or {}
        if isinstance(metadata, str):
            metadata = json.loads(metadata)
        for key, value in metadata.items():
            if key != "ipfs" and not isinstance(value, dict):
                headers[f"x-amz-meta-{key}"] = str(value)

        return StreamingResponse(
            generate_chunks(),
            media_type=object_info["content_type"],
            headers=headers,
        )

    except errors.S3Error as e:
        logger.exception(f"S3 Error getting object {bucket_name}/{object_key}: {e.message}")
        return create_xml_error_response(
            e.code,
            e.message,
            status_code=e.status_code,
        )

    except Exception as e:
        logger.exception(f"Error getting object {bucket_name}/{object_key}: {e}")
        return create_xml_error_response(
            "InternalError",
            f"We encountered an internal error: {str(e)}. Please try again.",
            status_code=500,
        )


@router.delete("/{bucket_name}/{object_key:path}/", status_code=204)
@router.delete("/{bucket_name}/{object_key:path}", status_code=204)
async def delete_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
    redis_client=Depends(dependencies.get_redis),
) -> Response:
    """
    Delete an object using S3 protocol (DELETE /{bucket_name}/{object_key}).
    Also handles deleting object tags (DELETE /{bucket_name}/{object_key}?tagging).

    This endpoint is compatible with the S3 protocol used by MinIO and other S3 clients.
    """
    # If tagging is in query params, handle deleting object tags
    if "tagging" in request.query_params:
        return await delete_object_tags(
            bucket_name,
            object_key,
            db,
            request.state.seed_phrase,
            request.state.account.main_account,
        )

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
            # For S3 compatibility, return an XML error response
            return create_xml_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        bucket_id = bucket["bucket_id"]
        result = await db.fetchrow(
            get_query("get_object_by_path"),
            bucket_id,
            object_key,
        )

        if not result:
            # S3 returns 204 even if the object doesn't exist, so no error here
            return Response(status_code=204)

        # Use the user already retrieved above

        if not user:
            logger.warning(f"User not found when trying to delete object {object_key}")
            return create_xml_error_response(
                "AccessDenied",
                f"You do not have permission to delete object {object_key}",
                status_code=403,
                Key=object_key,
            )

        # Delete the object and check if user has permission
        deleted_object = await db.fetchrow(
            get_query("delete_object"),
            bucket_id,
            object_key,
            user["main_account_id"],  # Add user_id parameter for permission check
        )

        if not deleted_object:
            logger.warning(f"User {user['user_id']} tried to delete object {object_key} without permission")
            return create_xml_error_response(
                "AccessDenied",
                f"You do not have permission to delete object {object_key}",
                status_code=403,
                Key=object_key,
            )

        await enqueue_unpin_request(
            payload=UnpinChainRequest(
                substrate_url=config.substrate_url,
                ipfs_node=config.ipfs_store_url,
                address=request.state.account.main_account,
                subaccount=request.state.account.id,
                subaccount_seed_phrase=request.state.seed_phrase,
                bucket_name=bucket_name,
                object_key=object_key,
                should_encrypt=not bucket["is_public"],
                cid=result.get("ipfs_cid") or "",
                object_id=str(deleted_object["object_id"]),
            ),
            redis_client=redis_client,
        )
        logger.info(f"Enqueued unpin request for deleted object {object_key}")

        return Response(
            status_code=204,
        )

    except Exception:
        logger.exception("Error deleting object")
        return create_xml_error_response(
            "InternalError",
            "We encountered an internal error. Please try again.",
            status_code=500,
        )


async def set_bucket_policy(
    bucket_name: str,
    request: Request,
    db: dependencies.DBConnection,
) -> Response:
    """
    Set bucket policy to make bucket public (PUT /{bucket_name}?policy).

    This allows transitioning a private bucket to public (one-way only).
    Validates that bucket is empty and not already public.
    """
    try:
        main_account_id = request.state.account.main_account

        # Get bucket for this main account
        bucket = await db.fetchrow(
            get_query("get_bucket_by_name_and_owner"),
            bucket_name,
            main_account_id,
        )

        if not bucket:
            return create_xml_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        # Check if bucket is already public
        if bucket["is_public"]:
            return create_xml_error_response(
                "PolicyAlreadyExists",
                "The bucket policy already exists and bucket is public",
                status_code=409,
                BucketName=bucket_name,
            )

        # Check if bucket has objects
        if not await bucket_is_empty(bucket["bucket_id"], db):
            return create_xml_error_response(
                "BucketNotEmpty",
                "Cannot make bucket public: bucket contains objects",
                status_code=409,
                BucketName=bucket_name,
            )

        # Parse and validate policy
        policy_data = await get_request_body(request)
        if not policy_data:
            return create_xml_error_response(
                "MalformedPolicy",
                "Policy document is empty",
                status_code=400,
            )

        try:
            policy_json = json.loads(policy_data.decode("utf-8"))
        except json.JSONDecodeError:
            return create_xml_error_response(
                "MalformedPolicy",
                "Policy document is not valid JSON",
                status_code=400,
            )

        # Validate it's a public read policy
        if not await validate_public_policy(policy_json, bucket_name):
            return create_xml_error_response(
                "InvalidPolicyDocument",
                "Policy document is invalid or not a public read policy",
                status_code=400,
            )

        # Update bucket to public
        await db.fetchrow(
            "UPDATE buckets SET is_public = TRUE WHERE bucket_id = $1",
            bucket["bucket_id"],
        )

        logger.info(f"Bucket '{bucket_name}' set to public via policy")
        return Response(status_code=204)

    except Exception as e:
        logger.exception(f"Error setting bucket policy: {e}")
        return create_xml_error_response(
            "InternalError",
            "We encountered an internal error. Please try again.",
            status_code=500,
        )


async def get_bucket_policy(
    bucket_name: str,
    db: dependencies.DBConnection,
    main_account_id: str,
) -> Response:
    """
    Get bucket policy (GET /{bucket_name}?policy).

    Returns the policy for public buckets, or error for private buckets.
    """
    try:
        # Get bucket for this main account
        bucket = await db.fetchrow(
            get_query("get_bucket_by_name_and_owner"),
            bucket_name,
            main_account_id,
        )

        if not bucket:
            return create_xml_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        # If bucket is not public, no policy exists
        if not bucket["is_public"]:
            return create_xml_error_response(
                "NoSuchBucketPolicy",
                "The bucket policy does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        # Return standard public read policy
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{bucket_name}/*"],
                }
            ],
        }

        return Response(
            content=json.dumps(policy, indent=2),
            media_type="application/json",
            status_code=200,
        )

    except Exception as e:
        logger.exception(f"Error getting bucket policy: {e}")
        return create_xml_error_response(
            "InternalError",
            "We encountered an internal error. Please try again.",
            status_code=500,
        )


async def validate_public_policy(policy: dict, bucket_name: str) -> bool:
    """
    Validate that the policy is a valid public read policy.

    Checks for standard S3 public read policy structure.
    """
    try:
        # Check required fields
        if policy.get("Version") != "2012-10-17":
            return False

        statements = policy.get("Statement", [])
        if not statements or not isinstance(statements, list):
            return False

        # Check for at least one public read statement
        for statement in statements:
            if (
                statement.get("Effect") == "Allow"
                and statement.get("Principal") == "*"
                and "s3:GetObject" in statement.get("Action", [])
            ):
                resources = statement.get("Resource", [])
                expected_resource = f"arn:aws:s3:::{bucket_name}/*"

                if expected_resource in resources:
                    return True

        return False

    except Exception:
        logger.exception("Error validating policy")
        return False


async def bucket_is_empty(bucket_id: str, db: dependencies.DBConnection) -> bool:
    """
    Check if bucket contains any objects.
    """
    try:
        result = await db.fetchval(
            "SELECT COUNT(*) FROM objects WHERE bucket_id = $1",
            bucket_id,
        )
        return result == 0

    except Exception:
        logger.exception("Error checking if bucket is empty")
        return False
