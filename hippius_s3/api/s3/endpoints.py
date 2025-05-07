"""S3-compatible API endpoints implementation for bucket and object operations."""

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

router = APIRouter(tags=["s3"])


@router.get("/", status_code=200)
async def list_buckets(
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
) -> Response:
    """
    List all buckets using S3 protocol (GET /).

    This endpoint is compatible with the S3 protocol used by MinIO and other S3 clients.
    """
    try:
        query = get_query("list_buckets")
        results = await db.fetch(query)

        xml_content = '<?xml version="1.0" encoding="UTF-8"?>\n'
        xml_content += '<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        xml_content += "<Owner><ID>hippius-s3-ipfs-gateway</ID><DisplayName>hippius-s3</DisplayName></Owner>"
        xml_content += "<Buckets>"

        for row in results:
            xml_content += "<Bucket>"
            xml_content += f"<Name>{row['bucket_name']}</Name>"
            timestamp = row["created_at"].strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            xml_content += f"<CreationDate>{timestamp}</CreationDate>"
            xml_content += "</Bucket>"

        xml_content += "</Buckets>"
        xml_content += "</ListAllMyBucketsResult>"

        return Response(content=xml_content, media_type="application/xml")

    except Exception:
        logger.exception("Error listing buckets via S3 protocol")
        xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
        xml_error += "<Error><Code>InternalError</Code>"
        xml_error += "<Message>We encountered an internal error. Please try again.</Message>"
        xml_error += "<RequestId>N/A</RequestId>"
        xml_error += "<HostId>hippius-s3</HostId></Error>"

        return Response(
            content=xml_error,
            media_type="application/xml",
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

    This endpoint is compatible with the S3 protocol used by MinIO and other S3 clients.
    """
    # If the location query parameter is present, this is a bucket location request
    if "location" in request.query_params:
        return await get_bucket_location(bucket_name, db)

    # If the tagging query parameter is present, this is a bucket tags request
    if "tagging" in request.query_params:
        return await get_bucket_tags(bucket_name, db)

    try:
        query = get_query("get_bucket_by_name")
        bucket = await db.fetchrow(query, bucket_name)

        if not bucket:
            xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
            xml_error += "<Error><Code>NoSuchBucket</Code>"
            xml_error += f"<Message>The specified bucket {bucket_name} does not exist</Message>"
            xml_error += f"<BucketName>{bucket_name}</BucketName>"
            xml_error += "<RequestId>N/A</RequestId>"
            xml_error += "<HostId>hippius-s3</HostId></Error>"

            return Response(
                content=xml_error,
                media_type="application/xml",
                status_code=404,
            )

        bucket_id = bucket["bucket_id"]
        prefix = request.query_params.get("prefix", None)

        query = get_query("list_objects")
        results = await db.fetch(query, bucket_id, prefix)

        contents = [
            {
                "Key": obj["object_key"],
                "LastModified": obj["created_at"].isoformat(),
                "ETag": f'"{obj["ipfs_cid"]}"',
                "Size": obj["size_bytes"],
                "StorageClass": "STANDARD",
            }
            for obj in results
        ]

        list_bucket_result = {
            "ListBucketResult": {
                "@xmlns": "http://s3.amazonaws.com/doc/2006-03-01/",
                "Name": bucket_name,
                "Prefix": prefix or "",
                "Marker": "",
                "MaxKeys": 1000,
                "IsTruncated": False,
                "Contents": contents,
            }
        }

        xml_data = dicttoxml.dicttoxml(list_bucket_result, attr_type=True, root=False)
        return Response(content=xml_data, media_type="application/xml")

    except Exception as e:
        logger.error(f"Error listing bucket objects via S3 protocol: {e}")
        xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
        xml_error += "<Error><Code>InternalError</Code>"
        xml_error += "<Message>We encountered an internal error. Please try again.</Message>"
        xml_error += "<RequestId>N/A</RequestId>"
        xml_error += "<HostId>hippius-s3</HostId></Error>"

        return Response(
            content=xml_error,
            media_type="application/xml",
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
        query = get_query("get_bucket_by_name")
        bucket = await db.fetchrow(query, bucket_name)

        if not bucket:
            # For S3 compatibility, return an XML error response
            xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
            xml_error += "<e><Code>NoSuchBucket</Code>"
            xml_error += f"<Message>The specified bucket {bucket_name} does not exist</Message>"
            xml_error += f"<BucketName>{bucket_name}</BucketName>"
            xml_error += "<RequestId>N/A</RequestId>"
            xml_error += "<HostId>hippius-s3</HostId></e>"

            return Response(
                content=xml_error,
                media_type="application/xml",
                status_code=404,
            )

        # Clear bucket tags by setting to empty JSON
        bucket_id = bucket["bucket_id"]
        logger.info(f"Deleting all tags for bucket '{bucket_name}' via S3 protocol")

        query = get_query("update_bucket_tags")
        await db.fetchrow(
            query,
            bucket_id,
            json.dumps({}),
        )

        return Response(status_code=204)

    except Exception as e:
        logger.error(f"Error deleting bucket tags: {e}")
        xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
        xml_error += "<e><Code>InternalError</Code>"
        xml_error += "<Message>We encountered an internal error. Please try again.</Message>"
        xml_error += "<RequestId>N/A</RequestId>"
        xml_error += "<HostId>hippius-s3</HostId></e>"

        return Response(
            content=xml_error,
            media_type="application/xml",
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
    try:
        query = get_query("get_bucket_by_name")
        bucket = await db.fetchrow(query, bucket_name)

        if not bucket:
            # For S3 compatibility, return an XML error response
            xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
            xml_error += "<e><Code>NoSuchBucket</Code>"
            xml_error += f"<Message>The specified bucket {bucket_name} does not exist</Message>"
            xml_error += f"<BucketName>{bucket_name}</BucketName>"
            xml_error += "<RequestId>N/A</RequestId>"
            xml_error += "<HostId>hippius-s3</HostId></e>"

            return Response(
                content=xml_error,
                media_type="application/xml",
                status_code=404,
            )

        # Get bucket tags
        tags = bucket.get("tags", {})
        if isinstance(tags, str):
            tags = json.loads(tags)

        # Construct the XML response
        xml_content = '<?xml version="1.0" encoding="UTF-8"?>\n'
        xml_content += '<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        xml_content += "<TagSet>"

        for key, value in tags.items():
            xml_content += f"<Tag><Key>{key}</Key><Value>{value}</Value></Tag>"

        xml_content += "</TagSet>"
        xml_content += "</Tagging>"

        return Response(content=xml_content, media_type="application/xml")

    except Exception as e:
        logger.error(f"Error getting bucket tags: {e}")
        xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
        xml_error += "<e><Code>InternalError</Code>"
        xml_error += "<Message>We encountered an internal error. Please try again.</Message>"
        xml_error += "<RequestId>N/A</RequestId>"
        xml_error += "<HostId>hippius-s3</HostId></e>"

        return Response(
            content=xml_error,
            media_type="application/xml",
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
    try:
        query = get_query("get_bucket_by_name")
        bucket = await db.fetchrow(query, bucket_name)

        if not bucket:
            # For bucket_exists, we need to return an XML error
            xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
            xml_error += "<Error><Code>NoSuchBucket</Code>"
            xml_error += f"<Message>The specified bucket {bucket_name} does not exist</Message>"
            xml_error += f"<BucketName>{bucket_name}</BucketName>"
            xml_error += "<RequestId>N/A</RequestId>"
            xml_error += "<HostId>hippius-s3</HostId></Error>"

            return Response(
                content=xml_error,
                media_type="application/xml",
                status_code=404,
            )

        # If bucket exists, return its location (we default to us-east-1)
        xml_content = '<?xml version="1.0" encoding="UTF-8"?>\n'
        xml_content += '<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        xml_content += "us-east-1"  # Default region
        xml_content += "</LocationConstraint>"

        return Response(content=xml_content, media_type="application/xml")

    except Exception as e:
        logger.error(f"Error getting bucket location: {e}")
        xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
        xml_error += "<Error><Code>InternalError</Code>"
        xml_error += "<Message>We encountered an internal error. Please try again.</Message>"
        xml_error += "<RequestId>N/A</RequestId>"
        xml_error += "<HostId>hippius-s3</HostId></Error>"

        return Response(
            content=xml_error,
            media_type="application/xml",
            status_code=500,
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
        query = get_query("get_bucket_by_name")
        bucket = await db.fetchrow(query, bucket_name)

        if not bucket:
            # For HEAD requests, S3 returns 404 with no error body,
            # but we need to return XML for bucket_exists to work correctly
            xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
            xml_error += "<Error><Code>NoSuchBucket</Code>"
            xml_error += f"<Message>The specified bucket {bucket_name} does not exist</Message>"
            xml_error += f"<BucketName>{bucket_name}</BucketName>"
            xml_error += "<RequestId>N/A</RequestId>"
            xml_error += "<HostId>hippius-s3</HostId></Error>"

            return Response(
                content=xml_error,
                media_type="application/xml",
                status_code=404,
            )

        return Response(status_code=200)

    except Exception as e:
        logger.error(f"Error checking bucket via S3 protocol: {e}")
        xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
        xml_error += "<Error><Code>InternalError</Code>"
        xml_error += "<Message>We encountered an internal error. Please try again.</Message>"
        xml_error += "<RequestId>N/A</RequestId>"
        xml_error += "<HostId>hippius-s3</HostId></Error>"

        return Response(
            content=xml_error,
            media_type="application/xml",
            status_code=500,
        )


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
        # Handle bucket lifecycle configuration
        try:
            # First check if the bucket exists
            query = get_query("get_bucket_by_name")
            bucket = await db.fetchrow(query, bucket_name)

            if not bucket:
                xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
                xml_error += "<e><Code>NoSuchBucket</Code>"
                xml_error += f"<Message>The specified bucket {bucket_name} does not exist</Message>"
                xml_error += f"<BucketName>{bucket_name}</BucketName>"
                xml_error += "<RequestId>N/A</RequestId>"
                xml_error += "<HostId>hippius-s3</HostId></e>"

                return Response(
                    content=xml_error,
                    media_type="application/xml",
                    status_code=404,
                )

            # For now we'll just acknowledge the request
            # In a complete implementation, we would parse and store the lifecycle configuration
            logger.info(f"Setting lifecycle configuration for bucket '{bucket_name}' via S3 protocol")

            return Response(status_code=200)

        except Exception as e:
            logger.error(f"Error setting bucket lifecycle: {e}")
            xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
            xml_error += "<e><Code>InternalError</Code>"
            xml_error += "<Message>We encountered an internal error. Please try again.</Message>"
            xml_error += "<RequestId>N/A</RequestId>"
            xml_error += "<HostId>hippius-s3</HostId></e>"

            return Response(
                content=xml_error,
                media_type="application/xml",
                status_code=500,
            )

    # Check if this is a request to set bucket tags
    elif "tagging" in request.query_params:
        # Handle bucket tags
        try:
            # First check if the bucket exists
            query = get_query("get_bucket_by_name")
            bucket = await db.fetchrow(query, bucket_name)

            if not bucket:
                xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
                xml_error += "<e><Code>NoSuchBucket</Code>"
                xml_error += f"<Message>The specified bucket {bucket_name} does not exist</Message>"
                xml_error += f"<BucketName>{bucket_name}</BucketName>"
                xml_error += "<RequestId>N/A</RequestId>"
                xml_error += "<HostId>hippius-s3</HostId></e>"

                return Response(
                    content=xml_error,
                    media_type="application/xml",
                    status_code=404,
                )

            # Parse the XML tag data from the request
            xml_data = await request.body()
            if not xml_data:
                # Empty tags is valid (clears tags)
                tag_dict = {}
            else:
                try:
                    # Parse XML to Python dict
                    parsed_xml = xmltodict.parse(xml_data)

                    # Extract tags
                    tag_dict = {}
                    if "Tagging" in parsed_xml and "TagSet" in parsed_xml["Tagging"]:
                        tag_set = parsed_xml["Tagging"]["TagSet"]
                        if "Tag" in tag_set:
                            # If there's only one tag, it won't be a list
                            tags = tag_set["Tag"]
                            if isinstance(tags, dict):
                                tags = [tags]

                            for tag in tags:
                                if "Key" in tag and "Value" in tag:
                                    tag_dict[tag["Key"]] = tag["Value"]
                except Exception as e:
                    logger.error(f"Error parsing XML tags: {e}")
                    xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
                    xml_error += "<e><Code>MalformedXML</Code>"
                    xml_error += "<Message>The XML you provided was not well-formed or did not validate against our published schema.</Message>"
                    xml_error += "<RequestId>N/A</RequestId>"
                    xml_error += "<HostId>hippius-s3</HostId></e>"

                    return Response(
                        content=xml_error,
                        media_type="application/xml",
                        status_code=400,
                    )

            # Update the bucket tags in the database
            bucket_id = bucket["bucket_id"]
            logger.info(f"Setting tags for bucket '{bucket_name}' via S3 protocol: {tag_dict}")

            query = get_query("update_bucket_tags")
            await db.fetchrow(
                query,
                bucket_id,
                json.dumps(tag_dict),
            )

            return Response(status_code=200)

        except Exception as e:
            logger.error(f"Error setting bucket tags via S3 protocol: {e}")
            xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
            xml_error += "<e><Code>InternalError</Code>"
            xml_error += "<Message>We encountered an internal error. Please try again.</Message>"
            xml_error += "<RequestId>N/A</RequestId>"
            xml_error += "<HostId>hippius-s3</HostId></e>"

            return Response(
                content=xml_error,
                media_type="application/xml",
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
            # For S3 compatibility, return an XML error response
            xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
            xml_error += "<Error><Code>BucketAlreadyExists</Code>"
            xml_error += f"<Message>The requested bucket {bucket_name} already exists</Message>"
            xml_error += f"<BucketName>{bucket_name}</BucketName>"
            xml_error += "<RequestId>N/A</RequestId>"
            xml_error += "<HostId>hippius-s3</HostId></Error>"

            return Response(
                content=xml_error,
                media_type="application/xml",
                status_code=409,
            )
        except Exception as e:
            logger.error(f"Error creating bucket via S3 protocol: {e}")
            xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
            xml_error += "<Error><Code>InternalError</Code>"
            xml_error += "<Message>We encountered an internal error. Please try again.</Message>"
            xml_error += "<RequestId>N/A</RequestId>"
            xml_error += "<HostId>hippius-s3</HostId></Error>"

            return Response(
                content=xml_error,
                media_type="application/xml",
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
        query = get_query("get_bucket_by_name")
        bucket = await db.fetchrow(query, bucket_name)

        if not bucket:
            # For S3 compatibility, return an XML error response
            xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
            xml_error += "<Error><Code>NoSuchBucket</Code>"
            xml_error += f"<Message>The specified bucket {bucket_name} does not exist</Message>"
            xml_error += f"<BucketName>{bucket_name}</BucketName>"
            xml_error += "<RequestId>N/A</RequestId>"
            xml_error += "<HostId>hippius-s3</HostId></Error>"

            return Response(
                content=xml_error,
                media_type="application/xml",
                status_code=404,
            )

        bucket_id = bucket["bucket_id"]

        query = get_query("list_objects")
        objects = await db.fetch(query, bucket_id, None)  # Pass NULL as the prefix

        for obj in objects:
            await ipfs_service.delete_file(obj["ipfs_cid"])

        query = get_query("delete_bucket")
        await db.fetchrow(query, bucket_id)

        return Response(status_code=204)

    except Exception as e:
        logger.error(f"Error deleting bucket via S3 protocol: {e}")
        # For S3 compatibility, return an XML error response
        xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
        xml_error += "<Error><Code>InternalError</Code>"
        xml_error += "<Message>We encountered an internal error. Please try again.</Message>"
        xml_error += "<RequestId>N/A</RequestId>"
        xml_error += "<HostId>hippius-s3</HostId></Error>"

        return Response(
            content=xml_error,
            media_type="application/xml",
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

    This endpoint is compatible with the S3 protocol used by MinIO and other S3 clients.
    """
    try:
        query = get_query("get_bucket_by_name")
        bucket = await db.fetchrow(query, bucket_name)

        if not bucket:
            # For S3 compatibility, return an XML error response
            xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
            xml_error += "<Error><Code>NoSuchBucket</Code>"
            xml_error += f"<Message>The specified bucket {bucket_name} does not exist</Message>"
            xml_error += f"<BucketName>{bucket_name}</BucketName>"
            xml_error += "<RequestId>N/A</RequestId>"
            xml_error += "<HostId>hippius-s3</HostId></Error>"

            return Response(
                content=xml_error,
                media_type="application/xml",
                status_code=404,
            )

        bucket_id = bucket["bucket_id"]

        file_data = await request.body()
        file_size = len(file_data)
        content_type = request.headers.get("Content-Type", "application/octet-stream")

        # Handle possible duplicate object
        try:
            # Try to create the new object first
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

            query = get_query("create_object")
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

            return Response(status_code=200, headers={"ETag": f'"{ipfs_cid}"'})

        except asyncpg.UniqueViolationError:
            # If object already exists, delete it and then try again
            query = get_query("get_object_by_path")
            existing_object = await db.fetchrow(query, bucket_id, object_key)

            if existing_object:
                # Delete the existing object from IPFS
                await ipfs_service.delete_file(existing_object["ipfs_cid"])

                # Delete from database
                query = get_query("delete_object")
                await db.fetchrow(query, bucket_id, object_key)

                # Now create the new object
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

                query = get_query("create_object")
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

                return Response(status_code=200, headers={"ETag": f'"{ipfs_cid}"'})

    except Exception as e:
        logger.error(f"Error uploading object via S3 protocol: {e}")
        # For S3 compatibility, return an XML error response
        xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
        xml_error += "<Error><Code>InternalError</Code>"
        xml_error += "<Message>We encountered an internal error. Please try again.</Message>"
        xml_error += "<RequestId>N/A</RequestId>"
        xml_error += "<HostId>hippius-s3</HostId></Error>"

        return Response(
            content=xml_error,
            media_type="application/xml",
            status_code=500,
        )


@router.head("/{bucket_name}/{object_key:path}", status_code=200)
async def head_object(
    bucket_name: str,
    object_key: str,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
) -> Response:
    """
    Get object metadata using S3 protocol (HEAD /{bucket_name}/{object_key}).

    This endpoint is compatible with the S3 protocol used by MinIO and other S3 clients.
    """
    try:
        query = get_query("get_bucket_by_name")
        bucket = await db.fetchrow(query, bucket_name)

        if not bucket:
            # For S3 compatibility, return an XML error response
            xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
            xml_error += "<Error><Code>NoSuchBucket</Code>"
            xml_error += f"<Message>The specified bucket {bucket_name} does not exist</Message>"
            xml_error += f"<BucketName>{bucket_name}</BucketName>"
            xml_error += "<RequestId>N/A</RequestId>"
            xml_error += "<HostId>hippius-s3</HostId></Error>"

            return Response(
                content=xml_error,
                media_type="application/xml",
                status_code=404,
            )

        bucket_id = bucket["bucket_id"]

        query = get_query("get_object_by_path")
        result = await db.fetchrow(query, bucket_id, object_key)

        if not result:
            # For S3 compatibility, return an XML error response
            xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
            xml_error += "<Error><Code>NoSuchKey</Code>"
            xml_error += f"<Message>The specified key {object_key} does not exist</Message>"
            xml_error += f"<Key>{object_key}</Key>"
            xml_error += "<RequestId>N/A</RequestId>"
            xml_error += "<HostId>hippius-s3</HostId></Error>"

            return Response(
                content=xml_error,
                media_type="application/xml",
                status_code=404,
            )

        metadata = result["metadata"] if result["metadata"] else {}
        if isinstance(metadata, str):
            metadata = json.loads(metadata)

        headers = {
            "Content-Type": result["content_type"],
            "Content-Length": str(result["size_bytes"]),
            "ETag": f'"{result["ipfs_cid"]}"',
            "Last-Modified": result["created_at"].strftime("%a, %d %b %Y %H:%M:%S GMT"),
        }

        for key, value in metadata.items():
            if key != "ipfs" and not isinstance(value, dict):
                headers[f"x-amz-meta-{key}"] = str(value)

        return Response(status_code=200, headers=headers)

    except Exception as e:
        logger.error(f"Error getting object metadata via S3 protocol: {e}")
        # For S3 compatibility, return an XML error response
        xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
        xml_error += "<Error><Code>InternalError</Code>"
        xml_error += "<Message>We encountered an internal error. Please try again.</Message>"
        xml_error += "<RequestId>N/A</RequestId>"
        xml_error += "<HostId>hippius-s3</HostId></Error>"

        return Response(
            content=xml_error,
            media_type="application/xml",
            status_code=500,
        )


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

    This endpoint is compatible with the S3 protocol used by MinIO and other S3 clients.
    """
    try:
        query = get_query("get_bucket_by_name")
        bucket = await db.fetchrow(query, bucket_name)

        if not bucket:
            # For S3 compatibility, return an XML error response
            xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
            xml_error += "<Error><Code>NoSuchBucket</Code>"
            xml_error += f"<Message>The specified bucket {bucket_name} does not exist</Message>"
            xml_error += f"<BucketName>{bucket_name}</BucketName>"
            xml_error += "<RequestId>N/A</RequestId>"
            xml_error += "<HostId>hippius-s3</HostId></Error>"

            return Response(
                content=xml_error,
                media_type="application/xml",
                status_code=404,
            )

        bucket_id = bucket["bucket_id"]

        query = get_query("get_object_by_path")
        result = await db.fetchrow(query, bucket_id, object_key)

        if not result:
            # For S3 compatibility, return an XML error response
            xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
            xml_error += "<Error><Code>NoSuchKey</Code>"
            xml_error += f"<Message>The specified key {object_key} does not exist</Message>"
            xml_error += f"<Key>{object_key}</Key>"
            xml_error += "<RequestId>N/A</RequestId>"
            xml_error += "<HostId>hippius-s3</HostId></Error>"

            return Response(
                content=xml_error,
                media_type="application/xml",
                status_code=404,
            )

        metadata = result["metadata"] if result["metadata"] else {}
        if isinstance(metadata, str):
            metadata = json.loads(metadata)

        ipfs_metadata = metadata.get("ipfs", {})
        is_encrypted = ipfs_metadata.get("encrypted", False)

        file_data = await ipfs_service.download_file(cid=result["ipfs_cid"], decrypt=is_encrypted)

        headers = {
            "Content-Type": result["content_type"],
            "Content-Length": str(len(file_data)),
            "ETag": f'"{result["ipfs_cid"]}"',
            "Last-Modified": result["created_at"].strftime("%a, %d %b %Y %H:%M:%S GMT"),
            "Content-Disposition": f'inline; filename="{object_key.split("/")[-1]}"',
        }

        for key, value in metadata.items():
            if key != "ipfs" and not isinstance(value, dict):
                headers[f"x-amz-meta-{key}"] = str(value)

        return Response(content=file_data, headers=headers)

    except Exception as e:
        logger.error(f"Error getting object via S3 protocol: {e}")
        # For S3 compatibility, return an XML error response
        xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
        xml_error += "<Error><Code>InternalError</Code>"
        xml_error += "<Message>We encountered an internal error. Please try again.</Message>"
        xml_error += "<RequestId>N/A</RequestId>"
        xml_error += "<HostId>hippius-s3</HostId></Error>"

        return Response(
            content=xml_error,
            media_type="application/xml",
            status_code=500,
        )


@router.delete("/{bucket_name}/{object_key:path}", status_code=204)
async def delete_object(
    bucket_name: str,
    object_key: str,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
    ipfs_service=Depends(dependencies.get_ipfs_service),
) -> Response:
    """
    Delete an object using S3 protocol (DELETE /{bucket_name}/{object_key}).

    This endpoint is compatible with the S3 protocol used by MinIO and other S3 clients.
    """
    try:
        query = get_query("get_bucket_by_name")
        bucket = await db.fetchrow(query, bucket_name)

        if not bucket:
            # For S3 compatibility, return an XML error response
            xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
            xml_error += "<Error><Code>NoSuchBucket</Code>"
            xml_error += f"<Message>The specified bucket {bucket_name} does not exist</Message>"
            xml_error += f"<BucketName>{bucket_name}</BucketName>"
            xml_error += "<RequestId>N/A</RequestId>"
            xml_error += "<HostId>hippius-s3</HostId></Error>"

            return Response(
                content=xml_error,
                media_type="application/xml",
                status_code=404,
            )

        bucket_id = bucket["bucket_id"]

        query = get_query("get_object_by_path")
        object_info = await db.fetchrow(query, bucket_id, object_key)

        if not object_info:
            # S3 returns 204 even if the object doesn't exist, so no error here
            return Response(status_code=204)

        ipfs_cid = object_info["ipfs_cid"]
        await ipfs_service.delete_file(ipfs_cid)

        query = get_query("delete_object")
        await db.fetchrow(query, bucket_id, object_key)

        return Response(status_code=204)

    except Exception as e:
        logger.error(f"Error deleting object via S3 protocol: {e}")
        # For S3 compatibility, return an XML error response
        xml_error = '<?xml version="1.0" encoding="UTF-8"?>\n'
        xml_error += "<Error><Code>InternalError</Code>"
        xml_error += "<Message>We encountered an internal error. Please try again.</Message>"
        xml_error += "<RequestId>N/A</RequestId>"
        xml_error += "<HostId>hippius-s3</HostId></Error>"

        return Response(
            content=xml_error,
            media_type="application/xml",
            status_code=500,
        )
