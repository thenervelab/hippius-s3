"""S3-compatible API endpoints implementation for bucket and object operations."""

import json
import logging
import uuid
from datetime import UTC
from datetime import datetime

import asyncpg
from fastapi import APIRouter
from fastapi import Depends
from fastapi import Request
from fastapi import Response
from lxml import etree as ET

from hippius_s3 import dependencies
from hippius_s3.api.s3 import errors
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)

router = APIRouter(tags=["s3"])


def create_xml_error_response(
    code: str,
    message: str,
    status_code: int = 500,
    **kwargs,
) -> Response:
    """Generate a standardized XML error response using lxml."""
    error_root = ET.Element("e")

    code_elem = ET.SubElement(error_root, "Code")
    code_elem.text = code

    message_elem = ET.SubElement(error_root, "Message")
    message_elem.text = message

    for key, value in kwargs.items():
        elem = ET.SubElement(error_root, key)
        elem.text = str(value)

    if "RequestId" not in kwargs:
        request_id = ET.SubElement(error_root, "RequestId")
        request_id.text = "N/A"

    if "HostId" not in kwargs:
        host_id = ET.SubElement(error_root, "HostId")
        host_id.text = "hippius-s3"

    xml_error = ET.tostring(
        error_root,
        encoding="UTF-8",
        xml_declaration=True,
        pretty_print=True,
    )

    return Response(
        content=xml_error,
        media_type="application/xml",
        status_code=status_code,
    )


@router.get("/", status_code=200)
async def list_buckets(
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
) -> Response:
    """
    List all buckets using S3 protocol (GET /).

    This endpoint is compatible with the S3 protocol used by MinIO and other S3 clients.
    """
    try:
        results = await db.fetch(get_query("list_buckets"))

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
            timestamp = row["created_at"].strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            creation_date.text = timestamp

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
        return await get_bucket_location(bucket_name, db)

    # If the tagging query parameter is present, this is a bucket tags request
    if "tagging" in request.query_params:
        logger.info(f"Handling tagging request for bucket {bucket_name}")

        try:
            bucket = await db.fetchrow(get_query("get_bucket_by_name"), bucket_name)

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

            # Add tags to the XML response
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
        return await get_bucket_lifecycle(bucket_name, db)

    # If the uploads parameter is present, this is a multipart uploads listing request
    if "uploads" in request.query_params:
        logger.info(f"Handling multipart uploads listing request for bucket {bucket_name}")
        # Import locally to avoid circular imports
        from hippius_s3.api.s3.multipart import list_multipart_uploads

        return await list_multipart_uploads(bucket_name, request, db)

    try:
        bucket = await db.fetchrow(
            get_query("get_bucket_by_name"),
            bucket_name,
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
            # Format timestamp in the exact format MinIO expects: YYYY-MM-DDThh:mm:ssZ
            last_modified.text = obj["created_at"].strftime("%Y-%m-%dT%H:%M:%SZ")

            etag = ET.SubElement(content, "ETag")
            etag.text = f'"{obj["ipfs_cid"]}"'

            size = ET.SubElement(content, "Size")
            size.text = str(obj["size_bytes"])

            storage_class = ET.SubElement(content, "StorageClass")
            storage_class.text = "STANDARD"

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
        logger.exception("Error listing bucket objects via S3 protocol")
        return create_xml_error_response(
            "InternalError",
            "We encountered an internal error. Please try again.",
            status_code=500,
        )


async def delete_bucket_tags(
    bucket_name: str,
    db: dependencies.DBConnection,
) -> Response:
    """
    Delete all tags from a bucket (DELETE /{bucket_name}?tagging).

    This is used by the MinIO client to remove all bucket tags.
    """
    try:
        bucket = await db.fetchrow(
            get_query("get_bucket_by_name"),
            bucket_name,
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
) -> Response:
    """
    Get the tags of an object (GET /{bucket_name}/{object_key}?tagging).

    This is used by the MinIO client to retrieve object tags.
    """
    try:
        bucket = await db.fetchrow(
            get_query("get_bucket_by_name"),
            bucket_name,
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
) -> Response:
    """
    Set tags for an object (PUT /{bucket_name}/{object_key}?tagging).

    This is used by the MinIO client to set object tags.
    """
    try:
        bucket = await db.fetchrow(
            get_query("get_bucket_by_name"),
            bucket_name,
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
        xml_data = await request.body()
        if not xml_data:
            # Empty tags is valid (clears tags)
            tag_dict = {}
        else:
            try:
                # Parse XML using lxml
                root = ET.fromstring(xml_data)

                # Extract tags
                tag_dict = {}
                # Find all Tag elements under TagSet
                tag_elements = root.findall(".//TagSet/Tag")

                for tag_elem in tag_elements:
                    key_elem = tag_elem.find("Key")
                    value_elem = tag_elem.find("Value")

                    if key_elem is not None and key_elem.text and value_elem is not None and value_elem.text:
                        tag_dict[key_elem.text] = value_elem.text
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
) -> Response:
    """
    Delete all tags from an object (DELETE /{bucket_name}/{object_key}?tagging).

    This is used by the MinIO client to remove all object tags.
    """
    try:
        bucket = await db.fetchrow(
            get_query("get_bucket_by_name"),
            bucket_name,
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
            # S3 returns 204 even if the object doesn't exist for DELETE operations
            return Response(status_code=204)

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
) -> Response:
    """
    Get the tags of a bucket (GET /{bucket_name}?tagging).

    This is used by the MinIO client to retrieve bucket tags.
    """
    logger.info(f"Getting tags for bucket {bucket_name}")
    try:
        bucket = await db.fetchrow(
            get_query("get_bucket_by_name"),
            bucket_name,
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
) -> Response:
    """
    Get the lifecycle configuration of a bucket (GET /{bucket_name}?lifecycle).

    This is used by the MinIO client to retrieve bucket lifecycle configuration.
    """
    try:
        bucket = await db.fetchrow(
            get_query("get_bucket_by_name"),
            bucket_name,
        )

        if not bucket:
            # For S3 compatibility, return an XML error response
            return create_xml_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        # todo
        # Create a lifecycle configuration with a minimal default rule
        # This is required for compatibility with the MinIO client
        # In a complete implementation, we would retrieve the actual configuration from the database
        root = ET.Element(
            "LifecycleConfiguration",
            xmlns="http://s3.amazonaws.com/doc/2006-03-01/",
        )

        rule = ET.SubElement(root, "Rule")
        rule_id = ET.SubElement(rule, "ID")
        rule_id.text = "default-rule"
        status = ET.SubElement(rule, "Status")
        status.text = "Enabled"
        filter_elem = ET.SubElement(rule, "Filter")
        prefix = ET.SubElement(filter_elem, "Prefix")
        prefix.text = "tmp/"  # Default prefix
        expiration = ET.SubElement(rule, "Expiration")
        days = ET.SubElement(expiration, "Days")
        days.text = "7"  # Default expiration in days

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
        logger.exception("Error getting bucket lifecycle")
        return create_xml_error_response(
            "InternalError",
            "We encountered an internal error. Please try again.",
            status_code=500,
        )


async def get_bucket_location(
    bucket_name: str,
    db: dependencies.DBConnection,
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


@router.head("/{bucket_name}", status_code=200)
async def head_bucket(
    bucket_name: str,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
) -> Response:
    """
    Check if a bucket exists using S3 protocol (HEAD /{bucket_name}).

    This endpoint is compatible with the S3 protocol used by MinIO and other S3 clients.
    """
    try:
        bucket = await db.fetchrow(
            get_query("get_bucket_by_name"),
            bucket_name,
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
            bucket = await db.fetchrow(
                get_query("get_bucket_by_name"),
                bucket_name,
            )

            if not bucket:
                return create_xml_error_response(
                    "NoSuchBucket",
                    f"The specified bucket {bucket_name} does not exist",
                    status_code=404,
                    BucketName=bucket_name,
                )

            # Parse the XML lifecycle configuration
            xml_data = await request.body()

            # If no XML data provided, return a MalformedXML error
            if not xml_data:
                return create_xml_error_response(
                    "MalformedXML",
                    "The XML you provided was not well-formed or did not validate against our published schema.",
                    status_code=400,
                )

            try:
                parsed_xml = ET.fromstring(xml_data)
                rules = parsed_xml.findall(".//Rule")
                rule_ids = [rule.find("ID").text for rule in rules if rule.find("ID") is not None]

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
            # First check if the bucket exists
            bucket = await db.fetchrow(
                get_query("get_bucket_by_name"),
                bucket_name,
            )

            if not bucket:
                return create_xml_error_response(
                    "NoSuchBucket",
                    f"The specified bucket {bucket_name} does not exist",
                    status_code=404,
                    BucketName=bucket_name,
                )

            # Parse the XML tag data from the request
            xml_data = await request.body()
            if not xml_data:
                # Empty tags is valid (clears tags)
                tag_dict = {}
            else:
                try:
                    # Parse XML using lxml
                    root = ET.fromstring(xml_data)

                    # Extract tags
                    tag_dict = {}
                    # Find all Tag elements under TagSet
                    tag_elements = root.findall(".//TagSet/Tag")

                    for tag_elem in tag_elements:
                        key_elem = tag_elem.find("Key")
                        value_elem = tag_elem.find("Value")

                        if key_elem is not None and key_elem.text and value_elem is not None and value_elem.text:
                            tag_dict[key_elem.text] = value_elem.text
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

    # Handle standard bucket creation if not a tagging or lifecycle request
    else:
        try:
            bucket_id = str(uuid.uuid4())
            created_at = datetime.now(UTC)
            is_public = True

            logger.info(f"Creating bucket '{bucket_name}' via S3 protocol")

            query = get_query("create_bucket")
            await db.fetchrow(
                query,
                bucket_id,
                bucket_name,
                created_at,
                is_public,
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


@router.delete("/{bucket_name}", status_code=204)
async def delete_bucket(
    bucket_name: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
    ipfs_service=Depends(dependencies.get_ipfs_service),
) -> Response:
    """
    Delete a bucket using S3 protocol (DELETE /{bucket_name}).
    Also handles removing bucket tags (DELETE /{bucket_name}?tagging).

    This endpoint is compatible with the S3 protocol used by MinIO and other S3 clients.
    """
    # If tagging is in query params, we're just deleting tags
    if "tagging" in request.query_params:
        return await delete_bucket_tags(bucket_name, db)

    try:
        bucket = await db.fetchrow(
            get_query("get_bucket_by_name"),
            bucket_name,
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

        for obj in objects:
            await ipfs_service.delete_file(obj["ipfs_cid"])

        await db.fetchrow(
            get_query("delete_bucket"),
            bucket["bucket_id"],
        )

        return Response(status_code=204)

    except Exception:
        logger.exception("Error deleting bucket via S3 protocol")
        return create_xml_error_response(
            "InternalError",
            "We encountered an internal error. Please try again.",
            status_code=500,
        )


@router.put("/{bucket_name}/{object_key:path}", status_code=200)
async def put_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
    ipfs_service=Depends(dependencies.get_ipfs_service),
) -> Response:
    """
    Upload an object to a bucket using S3 protocol (PUT /{bucket_name}/{object_key}).

    Important: For multipart upload parts, this function defers to the multipart.upload_part handler.
    """
    # Check if this is a multipart upload part request (has both uploadId and partNumber)
    upload_id = request.query_params.get("uploadId")
    part_number = request.query_params.get("partNumber")

    if upload_id and part_number:
        # Forward multipart upload requests to the specialized handler
        from hippius_s3.api.s3.multipart import upload_part

        return await upload_part(bucket_name, object_key, request, db, ipfs_service)
    """
    Upload an object to a bucket using S3 protocol (PUT /{bucket_name}/{object_key}).
    Also handles setting object tags (PUT /{bucket_name}/{object_key}?tagging).
    Also handles copying objects when x-amz-copy-source header is present.

    This endpoint is compatible with the S3 protocol used by MinIO and other S3 clients.
    """
    # If tagging is in query params, handle object tagging
    if "tagging" in request.query_params:
        return await set_object_tags(
            bucket_name,
            object_key,
            request,
            db,
        )

    # Check if this is a copy operation
    copy_source = request.headers.get("x-amz-copy-source")
    if copy_source:
        try:
            # Parse the copy source in format /source-bucket/source-key
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

            # Get the source bucket
            source_bucket = await db.fetchrow(
                get_query("get_bucket_by_name"),
                source_bucket_name,
            )

            if not source_bucket:
                return create_xml_error_response(
                    "NoSuchBucket",
                    f"The specified source bucket {source_bucket_name} does not exist",
                    status_code=404,
                    BucketName=source_bucket_name,
                )

            # Get the destination bucket
            dest_bucket = await db.fetchrow(
                get_query("get_bucket_by_name"),
                bucket_name,
            )

            if not dest_bucket:
                return create_xml_error_response(
                    "NoSuchBucket",
                    f"The specified destination bucket {bucket_name} does not exist",
                    status_code=404,
                    BucketName=bucket_name,
                )

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

            # Create the object in the destination bucket (reusing the IPFS CID)
            object_id = str(uuid.uuid4())
            created_at = datetime.now(UTC)
            ipfs_cid = source_object["ipfs_cid"]
            file_size = source_object["size_bytes"]
            content_type = source_object["content_type"]

            # Copy any metadata
            source_metadata = json.loads(source_object["metadata"])
            metadata = {"ipfs": source_metadata.get("ipfs", {})}

            # Create or update the object using upsert
            await db.fetchrow(
                get_query("upsert_object"),
                object_id,
                dest_bucket["bucket_id"],
                object_key,
                ipfs_cid,
                file_size,
                content_type,
                created_at,
                json.dumps(metadata),
            )

            # Prepare the XML response
            root = ET.Element("CopyObjectResult")
            etag = ET.SubElement(root, "ETag")
            etag.text = f'"{ipfs_cid}"'
            last_modified = ET.SubElement(root, "LastModified")
            # Format in S3-compatible format: YYYY-MM-DDThh:mm:ssZ
            last_modified.text = created_at.strftime("%Y-%m-%dT%H:%M:%SZ")

            xml_response = ET.tostring(root, encoding="utf-8", xml_declaration=True)

            return Response(
                content=xml_response,
                media_type="application/xml",
                status_code=200,
                headers={"ETag": f'"{ipfs_cid}"'},
            )

        except Exception as e:
            logger.exception("Error copying object")
            return create_xml_error_response(
                "InternalError",
                f"We encountered an internal error while copying the object: {str(e)}",
                status_code=500,
            )

    try:
        bucket = await db.fetchrow(
            get_query("get_bucket_by_name"),
            bucket_name,
        )

        if not bucket:
            return create_xml_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        bucket_id = bucket["bucket_id"]
        file_data = await request.body()
        file_size = len(file_data)
        content_type = request.headers.get("Content-Type", "application/octet-stream")

        # Check if object already exists to clean up IPFS
        existing_object = await db.fetchrow(
            get_query("get_object_by_path"),
            bucket_id,
            object_key,
        )

        # Upload the file to IPFS
        ipfs_result = await ipfs_service.upload_file(
            file_data=file_data,
            file_name=object_key,
            content_type=content_type,
        )

        ipfs_cid = ipfs_result["cid"]
        object_id = str(uuid.uuid4())
        created_at = datetime.now(UTC)

        metadata = {}
        for key, value in request.headers.items():
            if key.lower().startswith("x-amz-meta-"):
                meta_key = key[11:]
                metadata[meta_key] = value

        metadata["ipfs"] = {
            "cid": ipfs_cid,
            "encrypted": ipfs_result.get("encrypted", False),
        }

        # Begin a transaction to ensure atomic operation
        try:
            async with db.transaction():
                # Use upsert to create or update the object
                query = get_query("upsert_object")
                await db.fetchrow(
                    query,
                    object_id,
                    bucket_id,
                    object_key,
                    ipfs_cid,
                    file_size,
                    content_type,
                    created_at,
                    json.dumps(metadata),
                )

                # Execute an immediate verification query within the same transaction
                verify_result = await db.fetchrow(
                    get_query("get_object_by_path"),
                    bucket_id,
                    object_key,
                )
                if not verify_result:
                    raise RuntimeError(f"Failed to insert object: {bucket_name}/{object_key}")
        except Exception as e:
            logger.error(f"Transaction failed for PUT {bucket_name}/{object_key}: {str(e)}")
            raise

        # Clean up old IPFS content if needed
        if existing_object and existing_object["ipfs_cid"] != ipfs_cid:
            try:
                await ipfs_service.delete_file(existing_object["ipfs_cid"])
                logger.info(f"Cleaned up previous IPFS content for {bucket_name}/{object_key}")
            except Exception as e:
                logger.warning(f"Failed to clean up previous IPFS content: {e}")

        return Response(
            status_code=200,
            headers={"ETag": f'"{ipfs_cid}"'},
        )

    except Exception:
        logger.exception("Error uploading object")
        return create_xml_error_response(
            "InternalError",
            "We encountered an internal error. Please try again.",
            status_code=500,
        )


async def _get_object(
    bucket_name: str,
    object_key: str,
    db: dependencies.DBConnection,
) -> asyncpg.Record:
    logger.debug(f"Getting object {bucket_name}/{object_key}")

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

    logger.debug(f"Found object: {bucket_name}/{object_key}")
    return object


@router.head(
    "/{bucket_name}/{object_key:path}",
    status_code=200,
)
async def head_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
    ipfs_service=Depends(dependencies.get_ipfs_service),
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
            await _get_object(bucket_name, object_key, db)
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
        result = await _get_object(
            bucket_name,
            object_key,
            db,
        )

        metadata = json.loads(result["metadata"])
        ipfs_cid = result["ipfs_cid"]

        # Efficiently check if the file exists in IPFS without downloading it
        # This avoids the expensive download operation that occurs with GET requests
        exists_in_ipfs = await ipfs_service.check_file_exists(ipfs_cid)

        if not exists_in_ipfs:
            logger.warning(
                f"Object {bucket_name}/{object_key} exists in database but not in IPFS (CID: {ipfs_cid})"
            )  # Still return 200 for compatibility, but log the inconsistency

        headers = {
            "Content-Type": result["content_type"],
            "Content-Length": str(result["size_bytes"]),
            "ETag": f'"{ipfs_cid}"',
            "Last-Modified": result["created_at"].strftime("%a, %d %b %Y %H:%M:%S GMT"),
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


@router.get("/{bucket_name}/{object_key:path}", status_code=200)
async def get_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
    ipfs_service=Depends(dependencies.get_ipfs_service),
) -> Response:
    """
    Get an object using S3 protocol (GET /{bucket_name}/{object_key}).
    Also handles getting object tags (GET /{bucket_name}/{object_key}?tagging).

    This endpoint is compatible with the S3 protocol used by MinIO and other S3 clients.
    """
    # If tagging is in query params, handle object tags request
    if "tagging" in request.query_params:
        return await get_object_tags(
            bucket_name,
            object_key,
            db,
        )

    try:
        result = await _get_object(
            bucket_name,
            object_key,
            db,
        )

        metadata = json.loads(result["metadata"])

        ipfs_metadata = metadata.get("ipfs", {})
        is_encrypted = ipfs_metadata.get("encrypted", False)
        is_multipart = ipfs_metadata.get("multipart", False)

        logger.debug(f"Getting object {bucket_name}/{object_key} with CID: {result['ipfs_cid']}")

        # Check for multipart flag in either root level or ipfs metadata object
        is_root_multipart = metadata.get("multipart", False)
        if is_multipart or is_root_multipart:
            logger.debug(f"Detected multipart object: {bucket_name}/{object_key}")

            # Double-check metadata CID matches DB CID
            if ipfs_metadata.get("cid") and ipfs_metadata["cid"] != result["ipfs_cid"]:
                logger.error(f"Metadata CID mismatch: {ipfs_metadata['cid']} != {result['ipfs_cid']}")

        # Fetch the ipfs_cid from the result and use it to download the file
        ipfs_cid = result["ipfs_cid"]

        # Detect multipart upload by checking metadata
        part_cids = ipfs_metadata.get("part_cids") or metadata.get("part_cids")

        # Determine if this is a multipart upload based on metadata flags
        found_multipart = is_multipart or is_root_multipart or part_cids

        if found_multipart or (part_cids is not None and ipfs_cid in part_cids):
            # First, try to get the CID from metadata
            metadata_cid = ipfs_metadata.get("cid") or metadata.get("final_cid") or metadata.get("concatenated_cid")
            if metadata_cid and metadata_cid != ipfs_cid:
                logger.warning(f"CID mismatch: DB has {ipfs_cid}, metadata has {metadata_cid}")
                ipfs_cid = metadata_cid

            # When parts are available but concatenated CID is missing, concatenate them
            part_cids = ipfs_metadata.get("part_cids") or metadata.get("part_cids")
            if part_cids:
                try:
                    part_info = []
                    for i, cid in enumerate(part_cids, 1):
                        part_info.append(
                            {
                                "ipfs_cid": cid,
                                "part_number": i,
                                "size_bytes": 0,
                                # Will be filled during download
                            }
                        )

                    # Use the concatenate_parts method to rebuild the file
                    concat_result = await ipfs_service.concatenate_parts(part_info, result["content_type"], object_key)

                    logger.info(f"Successfully concatenated {len(part_cids)} parts for {bucket_name}/{object_key}")
                    ipfs_cid = concat_result["cid"]
                except Exception as e:
                    logger.error(f"Concatenation failed: {e}, using original CID")

        file_data = await ipfs_service.download_file(
            cid=ipfs_cid,
            decrypt=is_encrypted,
        )
        logger.debug(f"Downloaded {len(file_data)} bytes from IPFS CID: {ipfs_cid}")

        headers = {
            "Content-Type": result["content_type"],
            "Content-Length": str(len(file_data)),
            "ETag": f'"{ipfs_cid}"',
            "Last-Modified": result["created_at"].strftime("%a, %d %b %Y %H:%M:%S GMT"),
            "Content-Disposition": f'inline; filename="{object_key.split("/")[-1]}"',
        }

        logger.debug(f"Response headers set for {bucket_name}/{object_key}")

        for key, value in metadata.items():
            if key != "ipfs" and not isinstance(value, dict):
                headers[f"x-amz-meta-{key}"] = str(value)

        return Response(
            content=file_data,
            headers=headers,
        )

    except errors.S3Error as e:
        logger.exception(f"S3 Error getting object {bucket_name}/{object_key}: {e.message}")
        return create_xml_error_response(
            e.message,
            f"The object could not be retrieved: {e.message}",
            status_code=e.status_code,
        )

    except RuntimeError as e:
        # Handle specific RuntimeError from IPFS service
        if "Failed to download file with CID" in str(e):
            logger.exception(f"IPFS download error for {bucket_name}/{object_key}: {e}")
            return create_xml_error_response(
                "ServiceUnavailable",
                f"The IPFS network is currently experiencing issues. Please try again later. ({str(e)})",
                status_code=503,
            )
        logger.exception(f"Runtime error getting object {bucket_name}/{object_key}: {e}")
        return create_xml_error_response(
            "InternalError",
            f"We encountered an internal runtime error: {str(e)}",
            status_code=500,
        )

    except Exception as e:
        logger.exception(f"Error getting object {bucket_name}/{object_key}: {e}")
        return create_xml_error_response(
            "InternalError",
            f"We encountered an internal error: {str(e)}. Please try again.",
            status_code=500,
        )


@router.delete("/{bucket_name}/{object_key:path}", status_code=204)
async def delete_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
    ipfs_service=Depends(dependencies.get_ipfs_service),
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
        )

    try:
        bucket = await db.fetchrow(
            get_query("get_bucket_by_name"),
            bucket_name,
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
        object_info = await db.fetchrow(
            get_query("get_object_by_path"),
            bucket_id,
            object_key,
        )

        if not object_info:
            # S3 returns 204 even if the object doesn't exist, so no error here
            return Response(status_code=204)

        ipfs_cid = object_info["ipfs_cid"]
        await ipfs_service.delete_file(ipfs_cid)
        await db.fetchrow(
            get_query("delete_object"),
            bucket_id,
            object_key,
        )

        return Response(status_code=204)

    except Exception:
        logger.exception("Error deleting object")
        return create_xml_error_response(
            "InternalError",
            "We encountered an internal error. Please try again.",
            status_code=500,
        )
