"""S3-compatible API endpoints implementation for bucket and object operations."""

import asyncio
import contextlib
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
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPBearer
from lxml import etree as ET

from hippius_s3 import dependencies
from hippius_s3 import utils
from hippius_s3.api.s3 import errors
from hippius_s3.api.s3.bucket_policy_endpoint import get_bucket_policy as policy_get_bucket_policy
from hippius_s3.api.s3.bucket_policy_endpoint import set_bucket_policy as policy_set_bucket_policy
from hippius_s3.api.s3.bucket_tagging_endpoint import get_bucket_tags as tags_get_bucket_tags
from hippius_s3.api.s3.copy_object_endpoint import handle_copy_object
from hippius_s3.api.s3.delete_object_endpoint import handle_delete_object
from hippius_s3.api.s3.get_object_endpoint import handle_get_object
from hippius_s3.api.s3.head_object_endpoint import handle_head_object
from hippius_s3.api.s3.list_buckets_endpoint import handle_list_buckets
from hippius_s3.api.s3.list_objects_endpoint import handle_list_objects
from hippius_s3.api.s3.multipart import list_parts_internal
from hippius_s3.api.s3.multipart import upload_part
from hippius_s3.api.s3.put_object_endpoint import handle_put_object
from hippius_s3.api.s3.range_utils import calculate_chunks_for_range
from hippius_s3.api.s3.range_utils import extract_range_from_chunks
from hippius_s3.api.s3.range_utils import parse_range_header
from hippius_s3.api.s3.tagging_endpoint import delete_object_tags as tags_delete_object_tags
from hippius_s3.api.s3.tagging_endpoint import get_object_tags as tags_get_object_tags
from hippius_s3.api.s3.tagging_endpoint import set_object_tags as tags_set_object_tags
from hippius_s3.cache import RedisDownloadChunksCache
from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.config import get_config
from hippius_s3.dependencies import RequestContext
from hippius_s3.dependencies import get_object_reader
from hippius_s3.dependencies import get_request_context
from hippius_s3.queue import ChunkToDownload
from hippius_s3.queue import DownloadChainRequest
from hippius_s3.queue import UnpinChainRequest
from hippius_s3.queue import enqueue_download_request
from hippius_s3.queue import enqueue_unpin_request
from hippius_s3.services.manifest_service import ManifestService
from hippius_s3.services.object_reader import ObjectReader
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)
config = get_config()


def format_s3_timestamp(dt):
    """Format datetime to AWS S3 compatible timestamp: YYYY-MM-DDThh:mm:ss.sssZ"""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


async def get_chunk_from_redis(redis_client, object_id: str, request_id: str, chunk: ChunkToDownload) -> bytes:
    """Get a chunk from Redis with timeout."""
    for _ in range(config.http_redis_get_retries):
        repo = RedisDownloadChunksCache(redis_client)
        chunk_data = await asyncio.wait_for(
            repo.get(object_id, request_id, int(chunk.part_id)),
            timeout=config.redis_read_chunk_timeout,
        )
        if chunk_data:
            logger.debug(f"Got chunk {chunk.part_id} from Redis")
            break

        await asyncio.sleep(config.http_download_sleep_loop)

    else:
        raise RuntimeError(f"Chunk {chunk.part_id} not found in Redis")

    return chunk_data


async def load_object_from_cache(
    redis_client,
    object_info: dict,
    range_header: str | None = None,
    start_byte: int | None = None,
    end_byte: int | None = None,
) -> Response:
    """Load object from Redis cache when CID is not available."""
    is_multipart = object_info["multipart"]

    if is_multipart:
        object_id_str = str(object_info["object_id"])

        try:
            download_chunks_raw = json.loads(object_info["download_chunks"])  # type: ignore[index]
        except (json.JSONDecodeError, KeyError, TypeError):
            download_chunks_raw = []

        # Build initial part list strictly from DB-provided manifest
        initial_parts: list[dict] = []
        if isinstance(download_chunks_raw, list):
            for c in download_chunks_raw:
                try:
                    pn_val = c.get("part_number") if isinstance(c, dict) else c
                    if pn_val is None:
                        raise ValueError("missing part_number")
                    pn = int(pn_val)
                except Exception:
                    continue
                entry = {"part_number": pn}
                if isinstance(c, dict) and "size_bytes" in c:
                    with contextlib.suppress(Exception):
                        entry["size_bytes"] = int(c["size_bytes"])  # type: ignore[index]
                initial_parts.append(entry)
        # simplified: no verbose logging of initial_parts
        # Ensure base part(0) is considered if present in cache even when DB CID is pending
        try:
            obj_repo = RedisObjectPartsCache(redis_client)
            has_base0 = await obj_repo.exists(object_id_str, 0)
            if has_base0 and not any(int(p.get("part_number", -1)) == 0 for p in initial_parts):
                initial_parts.insert(0, {"part_number": 0})
            else:
                pass
        except Exception:
            pass
        # If DB manifest is empty, fallback to base-only assumption for simple objects
        if not initial_parts:
            initial_parts = [{"part_number": 0}]

        # Deduplicate and enrich with size_bytes from Redis
        seen_parts: set[int] = set()
        enriched_chunks: list[dict] = []
        for c in initial_parts:
            try:
                pn_val2 = c.get("part_number") if isinstance(c, dict) else None
                if pn_val2 is None:
                    raise ValueError("missing part_number")
                pn = int(pn_val2)
            except Exception:
                continue
            if pn in seen_parts:
                continue
            seen_parts.add(pn)
            size_val = c.get("size_bytes") if isinstance(c, dict) else None
            if not isinstance(size_val, int):
                try:
                    repo = RedisObjectPartsCache(redis_client)
                    object_id_str = str(object_info["object_id"])  # type: ignore[index]
                    size_val = await repo.strlen(object_id_str, pn)
                except Exception:
                    size_val = 0
            enriched_chunks.append({"part_number": pn, "size_bytes": int(size_val)})
        # Sort by part_number
        enriched_chunks = sorted(enriched_chunks, key=lambda x: x["part_number"])
        # simplified: no verbose logging of enriched_chunks

        if range_header and start_byte is not None and end_byte is not None:
            # For range requests, we need to load chunks into memory to extract the range
            needed_parts = calculate_chunks_for_range(start_byte, end_byte, enriched_chunks)
            range_chunks_data: list[bytes] = []
            for chunk_info in enriched_chunks:
                if chunk_info["part_number"] in needed_parts:
                    part_num = int(chunk_info["part_number"])
                    repo = RedisObjectPartsCache(redis_client)
                    chunk_data = await repo.get(object_id_str, part_num)
                    # No legacy fallback in new scheme; part 0 is the base
                    if chunk_data is None:
                        raise RuntimeError(f"Cache key missing: {repo.build_key(object_id_str, part_num)}")
                    range_chunks_data.append(chunk_data)

            range_data = extract_range_from_chunks(
                range_chunks_data, start_byte, end_byte, enriched_chunks, needed_parts
            )

            headers = {
                "Content-Type": object_info["content_type"],
                "Content-Range": f"bytes {start_byte}-{end_byte}/{object_info['size_bytes']}",
                "Accept-Ranges": "bytes",
                "x-hippius-source": "cache",
            }

            return StreamingResponse(
                iter([range_data]),
                status_code=206,
                media_type=object_info["content_type"],
                headers=headers,
            )

        # Assemble full object in-memory to ensure complete body
        chunks_data: list[bytes] = []
        for chunk_info in enriched_chunks:
            part_num = int(chunk_info["part_number"])
            repo = RedisObjectPartsCache(redis_client)
            chunk_data = await repo.get(object_id_str, part_num)
            # No legacy fallback in new scheme; part 0 is the base
            if chunk_data is None:
                raise RuntimeError(f"Cache key missing: {repo.build_key(object_id_str, part_num)}")
            with contextlib.suppress(Exception):
                logger.info(
                    f"GET cache assemble object_id={object_id_str} part={part_num} bytes={len(chunk_data)} key={repo.build_key(object_id_str, part_num)}"
                )
            chunks_data.append(chunk_data)

        headers = {
            "Content-Type": object_info["content_type"],
            "Content-Disposition": f'inline; filename="{object_info["object_key"].split("/")[-1]}"',
            "Accept-Ranges": "bytes",
            "ETag": f'"{object_info["md5_hash"]}"',
            "Last-Modified": object_info["created_at"].strftime("%a, %d %b %Y %H:%M:%S GMT"),
            "x-hippius-source": "cache",
        }

        metadata = object_info.get("metadata") or {}
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = {}
        for key, value in metadata.items():
            if key != "ipfs" and not isinstance(value, dict):
                headers[f"x-amz-meta-{key}"] = str(value)

        return Response(
            content=b"".join(chunks_data),
            media_type=object_info["content_type"],
            headers=headers,
        )

    # Unified cache for simple objects: read part 1
    object_id_str = str(object_info["object_id"])  # type: ignore[index]
    repo = RedisObjectPartsCache(redis_client)
    data = await repo.get(object_id_str, 0)
    if data is None:
        raise RuntimeError(f"Cache key missing: {repo.build_key(object_id_str, 0)}")

    if range_header and start_byte is not None and end_byte is not None:
        range_data = data[start_byte : end_byte + 1]
        headers = {
            "Content-Type": object_info["content_type"],
            "Content-Range": f"bytes {start_byte}-{end_byte}/{len(data)}",
            "Accept-Ranges": "bytes",
            "x-hippius-source": "cache",
        }
        return StreamingResponse(
            iter([range_data]),
            status_code=206,
            media_type=object_info["content_type"],
            headers=headers,
        )

    headers = {
        "Content-Type": object_info["content_type"],
        "Content-Disposition": f'inline; filename="{object_info["object_key"].split("/")[-1]}"',
        "Accept-Ranges": "bytes",
        "ETag": f'"{object_info["md5_hash"]}"',
        "Last-Modified": object_info["created_at"].strftime("%a, %d %b %Y %H:%M:%S GMT"),
        "x-hippius-source": "cache",
    }

    metadata = object_info.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except json.JSONDecodeError:
            metadata = {}
    for key, value in metadata.items():
        if key != "ipfs" and not isinstance(value, dict):
            headers[f"x-amz-meta-{key}"] = str(value)

    return StreamingResponse(
        iter([data]),
        status_code=200,
        media_type=object_info["content_type"],
        headers=headers,
    )


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

    file_size = len(file_data)
    try:
        start, end = parse_range_header(range_header, file_size)
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
            "x-hippius-source": "pipeline",
        }

        # Add custom metadata headers
        for key, value in metadata.items():
            if key != "ipfs" and not isinstance(value, dict):
                headers[f"x-amz-meta-{key}"] = str(value)

        return Response(content=range_data, status_code=206, headers=headers)

    except ValueError:
        return Response(
            status_code=416,
            headers={
                "Content-Range": f"bytes */{file_size}",
                "Accept-Ranges": "bytes",
                "Content-Length": "0",
            },
        )
    except TypeError as e:
        logger.warning(f"Invalid range request '{range_header}': {e}")
        return Response(
            status_code=416,
            headers={
                "Content-Range": f"bytes */{file_size}",
                "Accept-Ranges": "bytes",
                "Content-Length": "0",
            },
        )


"""
Range helpers moved to hippius_s3.api.s3.range_utils.
Keep imports at top; remove local shims to avoid redefinitions.
"""

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
    # S3 error bodies are simple XML without a namespace
    error_root = ET.Element("Error")

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
        # Help SDKs that read error code/message from headers when body parsing is deferred
        "x-amz-error-code": code,
        "x-amz-error-message": message,
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
    ctx: RequestContext = Depends(get_request_context),
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
) -> Response:
    return await handle_list_buckets(ctx, db)


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
        return await tags_get_bucket_tags(bucket_name, db, request.state.account.main_account)

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
        ctx = get_request_context(request)
        return await policy_get_bucket_policy(bucket_name, db, ctx.main_account_id)

    ctx = get_request_context(request)
    return await handle_list_objects(bucket_name, ctx, db, request.query_params.get("prefix"))


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

        # S3 semantics: refuse to delete a non-empty bucket
        if objects:
            return create_xml_error_response(
                "BucketNotEmpty",
                "The bucket you tried to delete is not empty",
                status_code=409,
                BucketName=bucket_name,
            )

        # Also block deletion if there are ongoing multipart uploads (required)
        ongoing_uploads = await db.fetch(get_query("list_multipart_uploads"), bucket["bucket_id"], None)
        if ongoing_uploads:
            return create_xml_error_response(
                "BucketNotEmpty",
                "The bucket has ongoing multipart uploads",
                status_code=409,
                BucketName=bucket_name,
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
                    subaccount=request.state.account.main_account,
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


# (CopyObject implementation moved to copy_object_endpoint.py)


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
            ipfs_service,
        )

    if "tagging" in request.query_params:
        # If tagging is in query params, handle object tagging
        return await tags_set_object_tags(
            bucket_name,
            object_key,
            request,
            db,
            request.state.seed_phrase,
            request.state.account.main_account,
        )

    # CopyObject path delegates to isolated handler
    if request.headers.get("x-amz-copy-source"):
        return await handle_copy_object(
            bucket_name=bucket_name,
            object_key=object_key,
            request=request,
            db=db,
            ipfs_service=ipfs_service,
            redis_client=redis_client,
        )

    return await handle_put_object(
        bucket_name=bucket_name,
        object_key=object_key,
        request=request,
        db=db,
        ipfs_service=ipfs_service,
        redis_client=redis_client,
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
    """Delegates to the isolated HEAD handler."""
    return await handle_head_object(
        bucket_name=bucket_name,
        object_key=object_key,
        request=request,
        db=db,
    )


@router.get("/{bucket_name}/{object_key:path}/", status_code=200)
@router.get("/{bucket_name}/{object_key:path}", status_code=200)
async def get_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
    ipfs_service=Depends(dependencies.get_ipfs_service),
    redis_client=Depends(dependencies.get_redis),
    object_reader: ObjectReader = Depends(get_object_reader),
) -> Response:
    """
    Get an object using S3 protocol (GET /{bucket_name}/{object_key}).
    Also handles getting object tags (GET /{bucket_name}/{object_key}?tagging).

    This endpoint now uses a queuing system for downloads to handle IPFS retrieval asynchronously.
    """
    # Delegated implementation
    return await handle_get_object(
        bucket_name=bucket_name,
        object_key=object_key,
        request=request,
        db=db,
        ipfs_service=ipfs_service,
        redis_client=redis_client,
        object_reader=object_reader,
    )
    # If tagging is in query params, handle object tags request
    if "tagging" in request.query_params:
        return await tags_get_object_tags(
            bucket_name,
            object_key,
            db,
            request.state.seed_phrase,
            request.state.account.main_account,
        )

    # List parts for an ongoing multipart upload
    if "uploadId" in request.query_params:
        return await list_parts_internal(bucket_name, object_key, request, db)

    # Check for Range header - handle partial content requests
    range_header = request.headers.get("Range") or request.headers.get("range")

    # Read-mode control headers
    hdr_mode = (request.headers.get("x-hippius-read-mode") or "").lower()
    force_pipeline = hdr_mode == "pipeline_only"
    # Back-compat x-amz-meta-cache plus explicit cache_only
    force_cache = (request.headers.get("x-amz-meta-cache", "").lower() == "true") or (hdr_mode == "cache_only")
    logger.info(f"GET start {bucket_name}/{object_key} read_mode={hdr_mode or 'auto'} range={bool(range_header)}")

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

        # Defer cache read until we have a reliable download_chunks manifest

        start_byte = end_byte = None
        if range_header:
            try:
                start_byte, end_byte = parse_range_header(range_header, object_info["size_bytes"])
            except ValueError:
                return Response(
                    status_code=416,
                    headers={
                        "Content-Range": f"bytes */{object_info['size_bytes']}",
                        "Accept-Ranges": "bytes",
                        "Content-Length": "0",
                    },
                )
        else:
            start_byte, end_byte = None, None
        # Load chunk metadata after range validation to avoid returning 503 for syntactically invalid ranges
        download_chunks = json.loads(object_info["download_chunks"]) if object_info.get("download_chunks") else []

        # Build manifest purely from DB parts (object_id), 0-based; inject base(0) from object CID if missing
        try:
            built_chunks = await ManifestService.build_initial_download_chunks(
                db, object_info if isinstance(object_info, dict) else dict(object_info)
            )
            if built_chunks:
                download_chunks = built_chunks
        except Exception:
            logger.debug("Failed to build manifest from ManifestService", exc_info=True)

        # Attach manifest to object_info for cache assembly
        try:
            if not isinstance(object_info, dict):
                object_info = dict(object_info)
            object_info["download_chunks"] = json.dumps(download_chunks)
        except Exception:
            pass

        with contextlib.suppress(Exception):
            logger.info(
                f"GET manifest-built multipart={object_info.get('multipart')} parts={[c if isinstance(c, dict) else c for c in download_chunks]}"
            )

        # Remove pipeline_only-specific manifest manipulation. Pipeline_only only disables cache.

        # For simple objects (non-multipart), construct a single-part manifest (independent of cache)
        # Only if there are no part rows resolved yet

        # Check if main CID is missing (still being processed)
        # If simple_cid is None, object hasn't been processed yet and chunks are in cache
        main_cid_missing = object_info.get("simple_cid") is None
        # Quick Redis probe: prefer cache if data is already present for first part and next part
        obj_id_str = str(object_info["object_id"])  # type: ignore[index]
        has_cache = await RedisObjectPartsCache(request.app.state.redis_client).exists(obj_id_str, 0)
        # Decide strategy honoring explicit read-mode header
        if force_pipeline:
            get_from_cache = False
        elif force_cache:
            get_from_cache = True
        else:
            get_from_cache = main_cid_missing or has_cache

        # Range handling: prefer unified cache for any object if present (unless pipeline_only)
        if range_header:
            if force_pipeline:
                get_from_cache = False
            else:
                try:
                    obj_id_str = str(object_info["object_id"])  # type: ignore[index]
                    get_from_cache = await RedisObjectPartsCache(request.app.state.redis_client).exists(obj_id_str, 0)
                except Exception:
                    # Fall back to previous decision
                    pass

        if get_from_cache:
            # Try to load object from cache, fallback to regular download if cache fails
            try:
                logger.info(f"GET serving from cache: {bucket_name}/{object_key}")
                return await load_object_from_cache(
                    redis_client=request.app.state.redis_client,
                    object_info=object_info,
                    range_header=range_header,
                    start_byte=start_byte,
                    end_byte=end_byte,
                )
            except RuntimeError:
                # Cache failed (missing keys), fall back to regular IPFS download
                logger.info(f"GET cache miss for {bucket_name}/{object_key}, falling back to pipeline")
                pass
        else:
            # No scan-based override; rely on initial cache probe only
            pass

        # No cache fallback for pipeline_only

        # Handle range requests by calculating needed chunks
        if range_header and start_byte is not None and end_byte is not None:
            needed_parts = calculate_chunks_for_range(
                start_byte,
                end_byte,
                download_chunks,
            )
            filtered_chunks = [chunk for chunk in download_chunks if chunk["part_number"] in needed_parts]
        else:
            filtered_chunks = download_chunks
            start_byte = end_byte = None

        # Create ChunkToDownload objects
        # Use deterministic request ID so multiple requests for same object reuse same download
        request_uuid = f"{object_info['object_id']}_{request.state.account.main_account}"
        if range_header:
            request_uuid += f"_{start_byte}_{end_byte}"  # Make range requests unique

        # Determine required parts from DB manifest (part numbers), used to enforce readiness
        required_parts: set[int] = set()
        try:
            rows_required = await db.fetch(
                "SELECT part_number FROM parts WHERE object_id = $1",
                object_info["object_id"],
            )
            required_parts = {int(r[0]) for r in rows_required}
        except Exception:
            required_parts = set()

        chunks = []
        available_parts: set[int] = set()
        for chunk_info in filtered_chunks:
            part_number_val = int(chunk_info["part_number"]) if isinstance(chunk_info, dict) else int(chunk_info)
            cid_val = chunk_info.get("cid") if isinstance(chunk_info, dict) else None  # type: ignore[union-attr]
            # Treat placeholder CIDs as missing
            is_placeholder = False
            try:
                is_placeholder = isinstance(cid_val, str) and cid_val.strip().lower() in {"none", "pending", ""}
            except Exception:
                is_placeholder = False
            # Skip chunks with missing/placeholder CIDs; they'll be retried by client
            if not cid_val or is_placeholder:
                continue
            chunks.append(ChunkToDownload(cid=cid_val, part_id=part_number_val))
            available_parts.add(part_number_val)
            try:
                _cid_log = ""
                if isinstance(chunk_info, dict):
                    _cid_val = chunk_info.get("cid")
                    if isinstance(_cid_val, str):
                        _cid_log = _cid_val[:10]
                # simplified: no per-chunk debug log
            except Exception:
                pass

        # If any required parts are missing CIDs, retry briefly before 503; never enqueue without CIDs
        try:
            if required_parts and not required_parts.issubset(available_parts):
                try:
                    waited = await ManifestService.wait_for_cids(
                        db,
                        str(object_info["object_id"]),
                        required_parts,
                        attempts=10,
                        interval_sec=0.5,
                    )
                    if waited:
                        chunks = [ChunkToDownload(cid=str(c["cid"]), part_id=int(c["part_number"])) for c in waited]
                        available_parts = {int(c["part_number"]) for c in waited}
                except Exception:
                    pass
        except Exception:
            # If we couldn't determine required parts, proceed with available ones
            pass

        # Final fallback: if no chunks resolved, try object-level CID as single base(0) chunk
        if not chunks:
            try:
                base_cid_final = await db.fetchval(
                    "SELECT c.cid FROM objects o JOIN cids c ON o.cid_id = c.id WHERE o.object_id = $1",
                    object_info["object_id"],
                )
                if not base_cid_final:
                    base_cid_final = await db.fetchval(
                        "SELECT ipfs_cid FROM objects WHERE object_id = $1",
                        object_info["object_id"],
                    )
                if base_cid_final and str(base_cid_final).strip().lower() not in {"", "none", "pending"}:
                    chunks = [ChunkToDownload(cid=str(base_cid_final), part_id=0)]
            except Exception:
                pass

        # No special part enforcement beyond DB manifest
        # Sort chunks by part_id to ensure correct order
        with contextlib.suppress(Exception):
            chunks = sorted(chunks, key=lambda c: int(c.part_id))

        with contextlib.suppress(Exception):
            logger.info(
                f"GET enqueue initial download chunks object_id={object_info['object_id']} request_id={request_uuid} parts={[c.part_id for c in chunks]}"
            )

        # In pipeline_only mode, never touch unified cache. Only pipeline/IPFS path is allowed.
        # No-op here by design to keep strict persistence validation semantics.

        try:
            # Debug: log expected ETags for parts
            upload_id = object_info["upload_id"] if "upload_id" in object_info and object_info["upload_id"] else None
            if upload_id:
                parts_etags = await db.fetch(get_query("get_parts_etags"), upload_id)
                etag_map = {p["part_number"]: p["etag"] for p in parts_etags}
                try:
                    _chunks_preview = []
                    for c in chunks:
                        _cidp = ""
                        try:
                            _cidp = c.cid[:10] if isinstance(c.cid, str) else ""
                        except Exception:
                            _cidp = ""
                        _chunks_preview.append((c.part_id, _cidp))
                    logger.debug(
                        f"GET manifest: key={object_key} multipart={object_info['multipart']} "
                        f"chunks={_chunks_preview} expected_etags={etag_map}"
                    )
                except Exception:
                    pass
        except Exception:
            logger.debug("Failed to log expected part ETags", exc_info=True)

        # Create download request helper
        def _mk_download_request(_chunks: list[ChunkToDownload]) -> DownloadChainRequest:
            cfg = get_config()
            return DownloadChainRequest(
                request_id=request_uuid,
                object_id=str(object_info["object_id"]),
                object_key=object_info["object_key"],
                bucket_name=object_info["bucket_name"],
                address=request.state.account.main_account,
                subaccount=request.state.account.main_account,
                subaccount_seed_phrase=request.state.seed_phrase,
                substrate_url=cfg.substrate_url,
                ipfs_node=cfg.ipfs_get_url,
                should_decrypt=object_info["should_decrypt"],
                size=object_info["size_bytes"],
                multipart=object_info["multipart"],
                chunks=_chunks,
            )

        # Enqueue download for current chunks
        download_request = _mk_download_request(chunks)
        download_flag_key = f"download_in_progress:{request_uuid}"
        flag_set = await request.app.state.redis_client.set(download_flag_key, "1", nx=True, ex=300)
        if flag_set:
            await enqueue_download_request(download_request, request.app.state.redis_client)
            try:
                logger.info(
                    f"Enqueued download request for {bucket_name}/{object_key} request_id={request_uuid} parts={[c.part_id for c in chunks]}"
                )
            except Exception:
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
                        str(object_info["object_id"]),
                        request_uuid,
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
                "x-hippius-source": "pipeline",
            }

            return StreamingResponse(
                generate_range(),
                status_code=206,
                media_type=object_info["content_type"],
                headers=headers,
            )

        # Precompute expected part ETags (MD5s) if available for integrity checking
        precomputed_etag_map = {}
        try:
            pre_upload_id = (
                object_info["upload_id"] if "upload_id" in object_info and object_info["upload_id"] else None
            )
            if pre_upload_id:
                pre_parts_etags = await db.fetch(get_query("get_parts_etags"), pre_upload_id)
                precomputed_etag_map = {p["part_number"]: p["etag"].split("-")[0] for p in pre_parts_etags}
        except Exception:
            logger.debug("Failed to precompute part ETags for integrity check", exc_info=True)

        # No pipeline_only-specific manifest re-evaluation beyond cache decisions

        # Pre-validate readiness and integrity BEFORE starting the streaming response
        # Readiness: ensure every expected chunk is available
        for chunk in chunks:
            # For pipeline_only, be more patient to allow downloader to fetch all parts
            max_checks = config.http_redis_get_retries
            if force_pipeline:
                try:
                    max_checks = max(max_checks, 50)
                except Exception:
                    max_checks = 50
            for _ in range(max_checks):
                exists = await RedisDownloadChunksCache(request.app.state.redis_client).exists(
                    str(object_info["object_id"]), request_uuid, int(chunk.part_id)
                )
                if exists:
                    break
                # Try preferred caches and hydrate
                if not force_pipeline:
                    try:
                        obj_id_str = str(object_info["object_id"])  # unified cache
                        cached = await RedisObjectPartsCache(request.app.state.redis_client).get(
                            obj_id_str, int(chunk.part_id)
                        )
                        if cached:
                            await RedisDownloadChunksCache(request.app.state.redis_client).set(
                                obj_id_str, request_uuid, int(chunk.part_id), cached, ttl=300
                            )
                            break
                    except Exception:
                        pass
                await asyncio.sleep(config.http_download_sleep_loop)
            else:
                return errors.s3_error_response(
                    "ServiceUnavailable",
                    f"Object not ready: missing chunk {chunk.part_id}",
                    status_code=503,
                )

        # Integrity: verify MD5 of each chunk against expected ETag (if available)
        if precomputed_etag_map:
            import hashlib as _hashlib

            for chunk in chunks:
                data = await RedisDownloadChunksCache(request.app.state.redis_client).get(
                    str(object_info["object_id"]), request_uuid, int(chunk.part_id)
                )
                if data is None:
                    return errors.s3_error_response(
                        "ServiceUnavailable",
                        f"Object not ready: missing chunk {chunk.part_id}",
                        status_code=503,
                    )
                expected = precomputed_etag_map.get(chunk.part_id)
                if expected:
                    md5 = _hashlib.md5(data).hexdigest()
                    if md5 != expected:
                        logger.error(f"Chunk MD5 mismatch part={chunk.part_id} got={md5} expected={expected}")
                        return errors.s3_error_response(
                            "ServiceUnavailable",
                            "Object not ready: chunk integrity mismatch",
                            status_code=503,
                        )

        # Full file download: after validation, stream chunks sequentially
        async def generate_chunks():
            for chunk in chunks:
                try:
                    chunk_data = await get_chunk_from_redis(
                        request.app.state.redis_client,
                        str(object_info["object_id"]),
                        request_uuid,
                        chunk,
                    )
                except RuntimeError:
                    # For pipeline_only, do not fallback to unified cache; enforce pipeline path
                    if force_pipeline:
                        raise
                    # Last-resort: try unified cache directly (non-pipeline_only only)
                    obj_id_str = str(object_info["object_id"])  # unified cache
                    cached = await RedisObjectPartsCache(request.app.state.redis_client).get(
                        obj_id_str, int(chunk.part_id)
                    )
                    if cached is None:
                        raise
                    chunk_data = cached
                yield chunk_data

        # Full file headers
        headers = {
            "Content-Type": object_info["content_type"],
            "Content-Disposition": f'inline; filename="{object_key.split("/")[-1]}"',
            "Accept-Ranges": "bytes",
            "ETag": f'"{object_info["md5_hash"]}"',
            "Last-Modified": object_info["created_at"].strftime("%a, %d %b %Y %H:%M:%S GMT"),
            "x-hippius-source": "pipeline" if force_pipeline else ("cache" if has_cache else "pipeline"),
        }
        logger.debug(
            f"Serving via pipeline for {bucket_name}/{object_key} x-hippius-source={headers['x-hippius-source']}"
        )

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
    # If this is an AbortMultipartUpload (DELETE with uploadId), delegate to multipart handler
    if "uploadId" in request.query_params:
        from hippius_s3.api.s3.multipart import abort_multipart_upload  # local import to avoid circular deps

        return await abort_multipart_upload(
            bucket_name,
            object_key,
            request,
            db,
        )
    # If tagging is in query params, handle deleting object tags
    if "tagging" in request.query_params:
        return await tags_delete_object_tags(
            bucket_name,
            object_key,
            db,
            request.state.seed_phrase,
            request.state.account.main_account,
        )

    return await handle_delete_object(
        bucket_name=bucket_name,
        object_key=object_key,
        request=request,
        db=db,
        redis_client=redis_client,
    )


async def set_bucket_policy(
    bucket_name: str,
    request: Request,
    db: dependencies.DBConnection,
) -> Response:
    return await policy_set_bucket_policy(bucket_name, request, db)


async def get_bucket_policy(
    bucket_name: str,
    db: dependencies.DBConnection,
    main_account_id: str,
) -> Response:
    return await policy_get_bucket_policy(bucket_name, db, main_account_id)


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
