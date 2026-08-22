"""S3-compatible multipart upload implementation for handling large file uploads."""

import contextlib
import hashlib
import json
import logging
import re
import time
import uuid
from collections.abc import AsyncIterator
from datetime import datetime
from datetime import timezone
from typing import Any
from urllib.parse import unquote

import asyncpg
from fastapi import APIRouter
from fastapi import Depends
from fastapi import Request
from fastapi import Response
from opentelemetry import trace
from redis.asyncio import Redis
from starlette.requests import ClientDisconnect

from hippius_s3 import dependencies
from hippius_s3 import utils
from hippius_s3.api.s3.common import format_s3_timestamp
from hippius_s3.api.s3.errors import CLIENT_CLOSED_REQUEST
from hippius_s3.api.s3.errors import s3_error_response
from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.config import get_config
from hippius_s3.db_retry import retry_on_object_version_conflict
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.services.envelope_service import generate_dek
from hippius_s3.services.envelope_service import wrap_dek
from hippius_s3.services.kek_service import get_or_create_active_bucket_kek
from hippius_s3.services.mpu_cleanup import fail_version_replication
from hippius_s3.services.mpu_cleanup import wake_version_replication
from hippius_s3.storage_version import require_supported_storage_version
from hippius_s3.utils import get_query
from hippius_s3.writer.db import set_object_version_address
from hippius_s3.writer.object_writer import ObjectWriter
from hippius_s3.xml_helpers import add_subelement
from hippius_s3.xml_helpers import create_element
from hippius_s3.xml_helpers import parse_untrusted_xml
from hippius_s3.xml_helpers import to_xml_bytes


logger = logging.getLogger(__name__)


def parse_complete_multipart_upload(body: bytes) -> list[tuple[int, str]]:
    """Parse a CompleteMultipartUpload body into (part_number, etag) pairs in document order.

    Matched by local-name so a namespace-prefixed or default-namespaced body parses the same,
    and paired within each ``<Part>`` so a stray element elsewhere in the document cannot shift
    the pairing. Parts are read as direct children of the root, the way S3 specifies the
    document, so a ``<Part>`` buried in some unrelated wrapper is not silently honoured.

    ETag quoting is normalised after parsing, which is what makes every client encoding agree:
    boto3 sends ``"abc"``, Go and Rust send the quotes escaped, and some clients send bare hex
    — all three arrive here as ``abc``.

    Every part must name a non-empty ETag. That is a guarantee callers rely on: the handler
    compares each ETag against the stored part, and an empty string would make that comparison
    vacuous. Enforcing it here means one place has to be right instead of every consumer, and
    the empty string is reachable more ways than it looks — ``<ETag/>``, whitespace, an
    unresolved entity reference, and a literal ``""`` that survives quote-stripping.

    Args:
        body: Raw request body.

    Returns:
        The listed parts, empty if the body carries none.

    Raises:
        ValueError: Not well-formed, or a ``<Part>`` missing/misusing PartNumber or ETag.
    """
    root = parse_untrusted_xml(body)
    parts: list[tuple[int, str]] = []
    for part in root.xpath("./*[local-name()='Part']"):
        numbers = part.xpath("./*[local-name()='PartNumber']")
        etags = part.xpath("./*[local-name()='ETag']")
        if not numbers or not etags:
            raise ValueError("every Part must carry a PartNumber and an ETag")
        try:
            number = int(_plain_text(numbers[0]).strip())
        except ValueError as exc:
            raise ValueError(f"PartNumber {numbers[0].text!r} is not an integer") from exc
        etag = _plain_text(etags[0]).strip().strip('"').strip()
        if not etag:
            raise ValueError(f"part {number} has an empty ETag")
        parts.append((number, etag))
    return parts


def build_complete_result_xml(host: str, bucket_name: str, object_key: str, etag: str) -> bytes:
    """Serialise a CompleteMultipartUploadResult.

    Shared by the success and idempotent-replay paths so both report the same Location; the
    replay path used to hardcode ``http://localhost:8000``, which is wrong for every
    deployment. The Host header is client-controlled, so it is written as element text and
    escaped rather than interpolated into the document.

    Args:
        host: Value of the request's Host header.
        bucket_name: Bucket the upload targeted.
        object_key: Key the upload targeted.
        etag: Final object ETag, unquoted.

    Returns:
        The serialised XML document.
    """
    root = create_element("CompleteMultipartUploadResult", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")
    add_subelement(root, "Location", f"http://{host}/{bucket_name}/{object_key}")
    add_subelement(root, "Bucket", bucket_name)
    add_subelement(root, "Key", object_key)
    add_subelement(root, "ETag", f'"{etag}"')
    return to_xml_bytes(root)


def _plain_text(element: Any) -> str:
    """Text of an element that must contain nothing but text.

    Entity references survive parsing as child nodes because the parser leaves them
    unresolved, and reading ``.text`` would silently return ``None`` for them — so a body
    carrying ``<ETag>&bomb;</ETag>`` would read as an empty ETag. Rejecting it is correct: no
    real client sends entity references here.

    This closes one route to an empty value; the caller rejects the empty string itself, which
    covers the rest.

    Raises:
        ValueError: The element has child nodes.
    """
    if len(element):
        raise ValueError("element must contain text only")
    return element.text or ""


router = APIRouter(tags=["s3-multipart"])

config = get_config()
tracer = trace.get_tracer(__name__)


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
    pool: asyncpg.Pool = Depends(dependencies.get_db_pool),
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
            async with pool.acquire() as conn:
                return await initiate_multipart_upload(
                    bucket_name,
                    object_key,
                    request,
                    conn,
                )

    # Check for uploadId parameter (Complete Multipart Upload)
    if "uploadId" in request.query_params:
        upload_id = request.query_params.get("uploadId")
        if upload_id is not None:
            with tracer.start_as_current_span(
                "multipart.route_complete",
                attributes={"upload_id": upload_id, "has_upload_id": True},
            ):
                async with pool.acquire() as conn:
                    return await complete_multipart_upload(
                        bucket_name,
                        object_key,
                        upload_id,
                        request,
                        conn,
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
        get_query("get_or_create_user_by_main_account"), request.state.main_account_id, datetime.now(timezone.utc)
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
    root = create_element("ListPartsResult", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")
    add_subelement(root, "Bucket", bucket_name)
    add_subelement(root, "Key", object_key)
    add_subelement(root, "UploadId", upload_id)
    add_subelement(root, "PartNumberMarker", str(part_marker))
    add_subelement(root, "NextPartNumberMarker", str(next_part_marker))
    add_subelement(root, "MaxParts", str(max_parts))
    add_subelement(root, "IsTruncated", "true" if is_truncated else "false")

    for part in visible_parts:
        p = add_subelement(root, "Part")
        add_subelement(p, "PartNumber", str(part["part_number"]))
        add_subelement(p, "ETag", f'"{part["etag"]}"')
        add_subelement(p, "Size", str(part["size_bytes"]))
        # Use CreatedAt as LastModified if available
        ts = part.get("created_at")
        if ts:
            add_subelement(p, "LastModified", format_s3_timestamp(ts))

    xml_content = to_xml_bytes(root)
    return Response(
        content=xml_content,
        media_type="application/xml",
        headers={
            "Content-Type": "application/xml; charset=utf-8",
            "x-amz-request-id": str(uuid.uuid4()),
            "Content-Length": str(len(xml_content)),
        },
    )


async def _write_v5_envelope(
    db: Any,
    *,
    bucket_id: str,
    reserve_row: Any,
    kek_id: uuid.UUID,
    kek_bytes: bytes,
    dek: bytes,
) -> None:
    """Write the DEK envelope for a freshly reserved MPU version. MUST run in the reserve's own
    transaction.

    upsert_object_multipart bumps objects.current_object_version, so the new v5 row becomes the LIVE
    version the instant it commits. Deferring the envelope to the first UploadPart (where
    ObjectWriter._ensure_and_get_v5_dek used to create it) left that live row with a NULL
    kek_id/wrapped_dek for the whole initiate→first-part gap — unbounded, and permanent for an MPU
    that is never continued. A GET in that gap 500s on v5_missing_envelope_metadata. Same invariant
    the simple-PUT path enforces in writer/object_writer.py `_reserve_version`.

    _ensure_and_get_v5_dek(rotate=False) finds this envelope on the first UploadPart and unwraps it,
    so parts still encrypt under one DEK per version.
    """
    # AAD binds the DB-returned object_id/version, not the locally generated candidate: a concurrent
    # create can hand back a different object_id, and a DEK wrapped under the wrong AAD never unwraps.
    resolved_object_id = str(reserve_row["object_id"])
    resolved_version = int(reserve_row["current_object_version"])
    aad = f"hippius-dek:{bucket_id}:{resolved_object_id}:{resolved_version}".encode()
    await db.execute(
        get_query("update_object_version_envelope"),
        "hip-enc/aes256gcm",
        int(config.object_chunk_size_bytes),
        kek_id,
        wrap_dek(kek=kek_bytes, dek=dek, aad=aad),
        resolved_object_id,
        resolved_version,
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
            request.state.main_account_id,
            datetime.now(timezone.utc),
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
        initiated_at = datetime.now(timezone.utc)
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

        if file_size and file_size > config.max_object_size:
            return s3_error_response(
                "EntityTooLarge",
                f"Your proposed upload size {file_size} bytes exceeds the maximum "
                f"allowed object size of {config.max_object_size} bytes",
                status_code=400,
            )

        # Create initial objects row for this multipart upload. The version allocation can collide
        # with a concurrent create_migration_version on object_versions_pkey; retry re-reads the
        # committed MAX in a fresh autocommit statement (see writer/db.py).
        # KEK lookup + DEK generation run OUTSIDE the reserve transaction: the KEK lives on a
        # separate keystore pool and may make a KMS round-trip on cache miss, so we must not hold a
        # main-pool connection open across it (mirrors writer/object_writer.py).
        dek = generate_dek()
        kek_id, kek_bytes = await get_or_create_active_bucket_kek(bucket_id=str(bucket["bucket_id"]))

        async def _reserve_version_with_envelope() -> Any:
            async with db.transaction():
                row = await db.fetchrow(
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
                    config.upload_backends,
                )
                if not row:
                    raise RuntimeError("initiate_reserve_missing_row")
                await _write_v5_envelope(
                    db,
                    bucket_id=str(bucket["bucket_id"]),
                    reserve_row=row,
                    kek_id=kek_id,
                    kek_bytes=kek_bytes,
                    dek=dek,
                )
                return row

        upsert_result = await retry_on_object_version_conflict(_reserve_version_with_envelope)

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
            datetime.fromtimestamp(file_mtime, timezone.utc) if file_mtime is not None else None,
            uuid.UUID(object_id),
        )

        root = create_element("InitiateMultipartUploadResult", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")
        add_subelement(root, "Bucket", bucket_name)
        add_subelement(root, "Key", object_key)
        add_subelement(root, "UploadId", upload_id)
        xml_bytes = to_xml_bytes(root)

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
        )
        return s3_error_response(
            "InternalError",
            f"Error initiating multipart upload: {str(e)}",
            status_code=500,
        )


async def get_all_cached_chunks(
    object_id: str,
    redis_client: Redis,
) -> list[Any]:
    """Find all cached part meta keys for an object using non-blocking SCAN."""
    try:
        keys_pattern = f"obj:{object_id}:part:*:meta"
        return [key async for key in redis_client.scan_iter(keys_pattern, count=1000)]
    except Exception as e:
        logger.error(f"Failed to find any cached parts for {object_id=}: {e}")
        return []


async def upload_part(
    request: Request,
    pool: asyncpg.Pool,
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

    if part_number < 1 or part_number > config.max_multipart_part_count:
        logger.error(f"Part number {part_number} out of range 1-{config.max_multipart_part_count}")
        return s3_error_response(
            "InvalidArgument",
            f"Part number must be an integer between 1 and {config.max_multipart_part_count}",
            status_code=400,
        )

    # Check if the multipart upload exists
    ongoing_multipart_upload = await pool.fetchrow(
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
        _ = await pool.fetchrow(
            get_query("get_or_create_user_by_main_account"),
            request.state.main_account_id,
            datetime.now(timezone.utc),
        )
        source_bucket = await pool.fetchrow(get_query("get_bucket_by_name"), source_bucket_name)
        if not source_bucket:
            return s3_error_response("NoSuchBucket", f"Bucket {source_bucket_name} does not exist", status_code=404)

        source_obj = await pool.fetchrow(get_query("get_object_by_path"), source_bucket["bucket_id"], source_object_key)
        if not source_obj:
            return s3_error_response("NoSuchKey", f"Key {source_object_key} not found", status_code=404)

        # Read source via reader pipeline to obtain plaintext when needed
        # Read plaintext via reader pipeline (parts → plan → stream decrypt)
        try:
            from hippius_s3.queue import DownloadChainRequest  # local import
            from hippius_s3.queue import PartChunkSpec  # local import
            from hippius_s3.queue import PartToDownload  # local import
            from hippius_s3.queue import enqueue_download_request  # local import
            from hippius_s3.reader.db_meta import read_parts_list  # local import to avoid cycles
            from hippius_s3.reader.planner import build_chunk_plan  # local import
            from hippius_s3.reader.streamer import stream_plan  # local import
        except Exception:
            return s3_error_response("InternalError", "Reader pipeline unavailable", status_code=500)

        object_id_str = str(source_obj["object_id"])
        src_ver = int(source_obj.get("object_version") or 1)
        parts = await read_parts_list(pool, object_id_str, src_ver)
        rng = None
        source_size = int(source_obj.get("size_bytes") or 0)
        if range_start is not None and range_end is not None:
            if range_start < 0 or range_end < range_start:
                return s3_error_response("InvalidRange", "Copy range invalid", status_code=416)
            if source_size and range_end >= source_size:
                return s3_error_response("InvalidRange", "Copy range invalid", status_code=416)
            from hippius_s3.reader.types import RangeRequest  # local import

            rng = RangeRequest(start=int(range_start), end=int(range_end))
        plan = await build_chunk_plan(pool, object_id_str, parts, rng, object_version=src_ver)

        # Enqueue downloader for any missing chunk indices in cache. CP-2: one batched existence
        # check (off-loop, meta-gated) instead of a serial per-chunk stat, matching the GET path.
        # Lifespan-built cache: it holds the standalone queues client that `stream_plan` below
        # needs for chunk-ready pub/sub. See the note in copy_helpers.handle_streaming_copy.
        obj_cache = request.app.state.obj_cache
        checks = [(int(it.part_number), int(it.chunk_index)) for it in plan]
        exist_flags = await obj_cache.chunks_exist_batch(object_id_str, src_ver, checks)
        indices_by_part: dict[int, list[int]] = {}
        for it, present in zip(plan, exist_flags, strict=True):
            if not present:
                indices_by_part.setdefault(int(it.part_number), []).append(int(it.chunk_index))
        if indices_by_part:
            dl_parts: list[PartToDownload] = []
            for pn, idxs in indices_by_part.items():
                try:
                    rows = await pool.fetch(
                        get_query("get_part_chunks_by_object_and_number"),
                        object_id_str,
                        src_ver,
                        int(pn),
                    )
                    all_entries = [(int(r[0]), str(r[1]), int(r[2]) if r[2] is not None else None) for r in rows or []]
                    chunk_specs: list[PartChunkSpec] = []
                    include = {int(i) for i in idxs}
                    for ci, cid, clen in all_entries:
                        if int(ci) in include:
                            chunk_specs.append(
                                PartChunkSpec(
                                    index=int(ci),
                                    cid=str(cid),
                                    cipher_size_bytes=int(clen) if clen is not None else None,
                                )
                            )
                    if not chunk_specs:
                        continue
                    dl_parts.append(PartToDownload(part_number=int(pn), chunks=chunk_specs))
                except Exception:
                    continue
            req = DownloadChainRequest(
                request_id=f"{object_id_str}::upload_part_copy",
                object_id=object_id_str,
                object_version=src_ver,
                object_storage_version=int(source_obj.get("storage_version") or 0),
                object_key=source_object_key,
                bucket_name=source_bucket_name,
                address=request.state.main_account_id,
                subaccount=request.state.main_account_id,
                substrate_url=config.substrate_url,
                size=int(source_obj.get("size_bytes") or 0),
                multipart=bool((json.loads(source_obj.get("metadata") or "{}") or {}).get("multipart", False)),
                chunks=dl_parts,
            )
            await enqueue_download_request(req)

        # Stream plaintext bytes
        raw_storage_version = source_obj.get("storage_version")
        if raw_storage_version is None:
            return s3_error_response("InternalError", "Missing storage version", status_code=500)
        storage_version = require_supported_storage_version(int(raw_storage_version))
        bucket_id = str(source_obj.get("bucket_id") or "")
        suite_id = str(
            source_obj.get("enc_suite_id") or ("hip-enc/aes256gcm" if storage_version >= 5 else "hip-enc/legacy")
        )
        key_bytes: bytes | None = None
        expected_size = (
            int(range_end - range_start + 1)
            if range_start is not None and range_end is not None
            else int(source_obj.get("size_bytes") or 0)
        )
        if expected_size > int(config.max_multipart_part_size):
            return s3_error_response(
                "EntityTooLarge",
                "UploadPartCopy source is too large to buffer in memory",
                status_code=413,
            )
        if storage_version >= 5:
            from hippius_s3.services.envelope_service import unwrap_dek
            from hippius_s3.services.kek_service import get_bucket_kek_bytes

            kek_id = source_obj.get("kek_id")
            wrapped_dek = source_obj.get("wrapped_dek")
            if not bucket_id or not kek_id or not wrapped_dek:
                return s3_error_response("InternalError", "Missing v5 envelope metadata", status_code=500)
            kek_bytes = await get_bucket_kek_bytes(bucket_id=bucket_id, kek_id=kek_id)
            aad = f"hippius-dek:{bucket_id}:{object_id_str}:{src_ver}".encode("utf-8")
            key_bytes = unwrap_dek(kek=kek_bytes, wrapped_dek=bytes(wrapped_dek), aad=aad)
        else:
            from hippius_s3.services.key_service import get_or_create_encryption_key_bytes

            key_bytes = await get_or_create_encryption_key_bytes(
                main_account_id=request.state.main_account_id,
                bucket_name=source_bucket_name,
            )
        chunks_iter = stream_plan(
            obj_cache=obj_cache,
            object_id=object_id_str,
            object_version=src_ver,
            plan=plan,
            storage_version=storage_version,
            key_bytes=key_bytes,
            suite_id=suite_id,
            bucket_id=bucket_id,
            upload_id="",
            address=request.state.main_account_id,
            bucket_name=source_bucket_name,
        )
        body_iter: AsyncIterator[bytes] = chunks_iter
    else:
        # Stream request body for regular UploadPart
        body_iter = utils.iter_request_body(request)

    # Cache part data in chunked layout via ObjectWriter (no IPFS upload for parts)
    redis_client = request.app.state.redis_client

    try:
        # Store in Redis via chunked cache API (encrypt for private, meta-first for readiness).
        # MPU-2: bucket_name and bucket_id already came from get_multipart_upload above
        # (it returns mu.* + b.bucket_name), so we don't re-run a multipart_uploads⋈buckets JOIN.
        dest_bucket_name = ongoing_multipart_upload.get("bucket_name")
        dest_bucket_id = ongoing_multipart_upload.get("bucket_id")
        if not dest_bucket_name or not dest_bucket_id:
            logger.error(f"Upload row missing bucket for upload_id={upload_id}; refusing to cache part")
            return s3_error_response("NoSuchUpload", "The specified upload does not exist.", status_code=404)

        redis_start = time.time()
        # Route through ObjectWriter for standardized behavior
        writer = ObjectWriter(pool=pool, redis_client=redis_client, fs_store=request.app.state.fs_store)
        try:
            part_res = await writer.mpu_upload_part_stream(
                upload_id=str(upload_id),
                object_id=str(object_id),
                object_version=int(current_object_version),
                bucket_name=str(dest_bucket_name or ""),
                bucket_id=str(dest_bucket_id),
                account_address=request.state.main_account_id,
                part_number=int(part_number),
                body_iter=body_iter,
            )
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

            # Same event as the simple-PUT path and the gateway hop, so it carries the same code.
            # Previously 408 RequestTimeout, which made one abort look like three different
            # failures depending on which hop you were reading. Nothing is delivered either way —
            # the peer is gone — so this only affects how we classify it.
            return Response(status_code=CLIENT_CLOSED_REQUEST)
        except ValueError as exc:
            if "part_size_exceeds_max" in str(exc):
                return s3_error_response(
                    "EntityTooLarge",
                    f"Part size exceeds maximum {config.max_multipart_part_size} bytes",
                    status_code=400,
                )
            if "Zero-length part" in str(exc):
                return s3_error_response(
                    "InvalidArgument",
                    "Zero-length part not allowed",
                    status_code=400,
                )
            raise
        redis_time = time.time() - redis_start
        logger.debug(
            f"Part {part_number}: Cached via RedisObjectPartsCache in {redis_time:.3f}s (object_id={object_id}, encrypted=True)"
        )

        file_size = int(part_res.size_bytes)
        if copy_source:
            with contextlib.suppress(Exception):
                logger.info(
                    f"UploadPartCopy slice: upload_id={upload_id} part={part_number} src={source_bucket_name}/{source_object_key} "
                    f"range={range_start}-{range_end} len={file_size} md5={part_res.etag}"
                )
        part_result = {
            "size_bytes": file_size,
            "etag": part_res.etag,
            "part_number": part_number,
        }

        # Save the part information in the database
        db_start = time.time()
        # (placeholder upsert already handled by writer)
        db_time = time.time() - db_start
        logger.debug(f"Part {part_number}: Database insert took {db_time:.3f}s")

        total_time = time.time() - start_time
        logger.debug(f"Part {part_number}: TOTAL processing time: {total_time:.3f}s")

        get_metrics_collector().record_s3_operation(
            operation="upload_part",
            bucket_name=ongoing_multipart_upload.get("bucket_name", ""),
            success=True,
        )
        get_metrics_collector().record_data_transfer(
            operation="upload_part",
            bytes_transferred=file_size,
            bucket_name=ongoing_multipart_upload.get("bucket_name", ""),
        )

        # Return response
        if copy_source:
            # AWS-style XML body for UploadPartCopy
            root = create_element("CopyPartResult")
            add_subelement(root, "ETag", f'"{part_result["etag"]}"')
            add_subelement(root, "LastModified", format_s3_timestamp(datetime.now(timezone.utc)))
            xml = to_xml_bytes(root)
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


async def abort_multipart_upload(
    _: str,
    __: str,
    request: Request,
    db: Any,
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

        # Resolve THIS upload's version from its own parts, not objects.current_object_version.
        # A simple PUT or another MPU on the same key advances that pointer, so acting on it would
        # fail-replicate + delete an innocent NEWER version's data. No parts for this upload_id =>
        # nothing of ours to clean: skip the version-scoped cleanup entirely (do NOT fall back to
        # the pointer), and just remove the upload row + aborted marker below.
        version_row = await db.fetchrow(get_query("get_multipart_version_by_upload"), upload_id)
        object_version = int(version_row["object_version"]) if version_row else None

        # Clean up Redis keys for cached parts (meta + chunks) — only for our own version.
        if object_version is not None:
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
                    base_key = delegate.build_key(str(object_id), object_version, part_num)
                    # MPU-4: delete the meta + chunk keys by computed name in one pipelined UNLINK
                    # instead of a per-part keyspace SCAN. num_chunks comes from the FS meta; if it's
                    # absent (nothing to compute from), fall back to the SCAN.
                    fs_meta = await request.app.state.fs_store.get_meta(str(object_id), int(object_version), part_num)
                    num_chunks = int(fs_meta.get("num_chunks", 0)) if fs_meta else 0
                    if num_chunks > 0:
                        keys = [meta_key] + [f"{base_key}:chunk:{i}" for i in range(num_chunks)]
                        await redis_client.unlink(*keys)
                    else:
                        await redis_client.delete(meta_key)
                        async for key in redis_client.scan_iter(f"{base_key}:chunk:*"):
                            await redis_client.delete(key)

        # Stop the drain churn BEFORE deleting the upload header. This aborted version's
        # address will never be written, so its parts and the drain's
        # cephor_replication_status rows would otherwise leak: the reconciler keeps
        # re-recording them and the drain keeps re-claiming + re-copying + re-deferring the
        # enqueue forever, on every node. Marking the version's replication rows terminal
        # makes the reconciler skip them and claim_part never re-claim them, fleet-wide.
        # Order matters: mark first so the churn-stop is durable even if we die before the
        # delete below. The backstop for a thrown mark is the cephor-orphan SWEEP
        # (list_orphan_replication_versions) — NOT the abandoned-upload reaper: the reaper
        # keys on multipart_uploads, which the delete below removes, so it could never see
        # this version again. The sweep keys on cephor_replication_status, which survives
        # the delete, so it still finds and terminates the orphan.
        # Skipped when the upload had no parts of its own (object_version is None).
        if object_version is not None:
            with contextlib.suppress(Exception):
                await fail_version_replication(db, object_id=object_id, object_version=object_version)

        # Fully remove the multipart upload (and cascade parts) so it disappears from listings immediately
        async with db.transaction():
            await db.fetchrow(
                get_query("abort_multipart_upload"),
                upload_id,
            )

        # B5: drop the empty reserved object_versions row this aborted upload left behind and
        # repoint current_object_version off it. Reads already fall back to the latest completed
        # version, so this is DB hygiene (no data loss) — best-effort so a hiccup never fails the
        # abort. Skipped when the upload had no version of its own.
        if object_version is not None:
            with contextlib.suppress(Exception):
                await db.fetchrow(
                    get_query("abort_cleanup_orphan_version"),
                    object_id,
                    object_version,
                )

        # Best-effort node-local cleanup: drop THIS node's cached parts (other nodes' copies
        # are left to the orphan GC; the central mark above already stopped their churn).
        if object_version is not None:
            with contextlib.suppress(Exception):
                await request.app.state.fs_store.delete_object(str(object_id), int(object_version))

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
            request.state.main_account_id,
            datetime.now(timezone.utc),
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

        # Filter out any uploads that were very recently aborted (defensive cache for race conditions).
        # MPU-56: one MGET for all recently-aborted flags instead of a serial GET per upload.
        try:
            redis_client: Redis = request.app.state.redis_client
            if uploads:
                flag_keys = [f"aborted_mpu:{str(u['upload_id'])}" for u in uploads]
                aborted_flags = await redis_client.mget(flag_keys)
                uploads = [u for u, flag in zip(uploads, aborted_flags, strict=True) if not flag]
        except Exception as _:
            # If Redis not available, proceed with DB results
            pass

        # Generate the response XML
        root = create_element("ListMultipartUploadsResult", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")
        add_subelement(root, "Bucket", bucket_name)
        add_subelement(root, "KeyMarker", "")
        add_subelement(root, "UploadIdMarker", "")
        add_subelement(root, "NextKeyMarker", "")
        add_subelement(root, "NextUploadIdMarker", "")
        add_subelement(root, "MaxUploads", "1000")
        add_subelement(root, "IsTruncated", "false")

        # Add Upload elements
        for upload in uploads:
            upload_elem = add_subelement(root, "Upload")
            add_subelement(
                upload_elem,
                "Key",
                str(upload["object_key"]) if upload.get("object_key") is not None else "",
            )
            # Ensure UploadId is a string (DB may return uuid.UUID)
            add_subelement(
                upload_elem,
                "UploadId",
                str(upload["upload_id"]) if upload.get("upload_id") is not None else "",
            )
            add_subelement(upload_elem, "Initiated", format_s3_timestamp(upload["initiated_at"]))

        # Generate XML with proper declaration
        xml_content = to_xml_bytes(root)

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
            xml_content = build_complete_result_xml(
                request.headers.get("Host", ""), bucket_name, object_key, final_etag
            )
            return Response(
                content=xml_content,
                media_type="application/xml",
                status_code=200,
                headers={
                    "Content-Type": "application/xml; charset=utf-8",
                    "x-amz-request-id": str(uuid.uuid4()),
                    "Content-Length": str(len(xml_content)),
                    "x-amz-version-id": str(int(multipart_upload.get("current_object_version") or 1)),
                },
            )

        try:
            part_info = parse_complete_multipart_upload(await get_request_body(request))
        except ValueError:
            return s3_error_response(
                "MalformedXML",
                "The XML you provided was not well-formed or did not validate against our published schema",
                status_code=400,
            )

        if not part_info:
            return s3_error_response(
                "MalformedXML",
                "The XML you provided was not well-formed or did not validate against our published schema",
                status_code=400,
            )
        # S3 requires the parts to be listed in strictly ascending part-number order.
        # We used to silently sort here, which masked a malformed part list; reject it
        # (InvalidPartOrder) instead so a client cannot depend on undefined assembly order.
        last_seen_pn = 0
        for pn, _ in part_info:
            if pn <= last_seen_pn:
                return s3_error_response(
                    "InvalidPartOrder",
                    "The list of parts was not in ascending order. Parts must be ordered by part number.",
                    status_code=400,
                )
            last_seen_pn = pn

        # Resolve THIS upload's version from its own parts (keyed by upload_id), not
        # objects.current_object_version — a simple PUT or another MPU on the same key advances that
        # pointer, and completing/writing the address against it would target a different upload's
        # version (and, with no parts of our own, would pull THAT version's parts). No parts for this
        # upload_id => genuinely nothing to complete; do not fall back to the pointer.
        version_row = await db.fetchrow(get_query("get_multipart_version_by_upload"), upload_id)
        if not version_row:
            logger.error(f"No parts found for multipart upload {upload_id}")
            return s3_error_response(
                "InvalidRequest",
                "No parts found for this multipart upload",
                status_code=400,
            )
        object_version = int(version_row["object_version"])
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

        # Validate each part exists AND the client-asserted ETag matches the stored part.
        # A CompleteMultipartUpload names, per part, the ETag the client believes it
        # uploaded; S3 rejects the completion (InvalidPart) if the part is missing OR the
        # ETag does not match. Without the ETag check a client could assemble the object
        # from the wrong part bytes and still receive a 200 — a silent data-integrity hole.
        #
        # Compared unconditionally: this used to skip a falsy client_etag, which meant any
        # body encoding an empty ETag opted itself out of the check the comment above
        # describes. parse_complete_multipart_upload now rejects those bodies outright, so
        # every ETag reaching here is non-empty and there is nothing to guard against.
        missing_parts = []
        mismatched_parts = []
        for pn, client_etag in part_info:
            db_part = db_parts_dict.get(pn)
            if db_part is None:
                missing_parts.append(pn)
                continue
            stored_etag = str(db_part["etag"]).replace('"', "").strip()
            if client_etag.lower() != stored_etag.lower():
                mismatched_parts.append(pn)
        if missing_parts:
            return s3_error_response(
                "InvalidPart",
                f"One or more parts could not be found: {', '.join(map(str, missing_parts))}",
                status_code=400,
            )
        if mismatched_parts:
            return s3_error_response(
                "InvalidPart",
                "The ETag supplied for one or more parts did not match the uploaded part: "
                f"{', '.join(map(str, mismatched_parts))}",
                status_code=400,
            )

        writer = ObjectWriter(
            pool=request.app.state.postgres_pool,
            redis_client=request.app.state.redis_client,
            fs_store=request.app.state.fs_store,
        )
        complete_res = await writer.mpu_complete(
            bucket_name=bucket_name,
            object_id=str(object_id),
            object_key=object_key,
            upload_id=str(upload_id),
            object_version=int(object_version),
            address=request.state.main_account_id,
            # B1: the client's <Part> selection — the final object (bytes + ETag + size) reflects
            # only these; a strict subset is recorded so the reader excludes the unlisted parts.
            selected_parts=[pn for pn, _ in part_info],
            # MPU-3: reuse the parts rows already fetched (and ETag-validated) above so mpu_complete
            # doesn't re-read the parts table for the combined ETag and total size.
            db_parts=db_parts,
        )

        # Drain-direct (s3-2.1 PR-11): the api does NOT enqueue the backend upload. It
        # persists the main-account address (the upload identity); the Rust drain reads
        # it and LPUSHes each part's UploadChainRequest itself, per part, as the part
        # replicates to ceph. The drain is the sole upload producer.
        await set_object_version_address(
            request.app.state.postgres_pool,
            object_id=str(object_id),
            object_version=int(object_version),
            address=request.state.main_account_id,
        )

        # Drain wake: the address write above removes the cause of this version's defer
        # backoff (rationale, incl. the deliberate defer_attempts reset, lives in
        # wake_replication_status_for_version.sql). Best-effort (narrow exception to the
        # no-try/except rule): the complete is already committed, the wake is only an
        # optimization — the backoff self-heals within the cap — and this handler's
        # outer catch-all would otherwise turn a wake failure into a 500 for a success.
        try:
            await wake_version_replication(db, object_id=object_id, object_version=object_version)
        except Exception:
            logger.warning(
                "drain wake failed after CompleteMultipartUpload bucket=%s upload_id=%s object_id=%s version=%s",
                bucket_name,
                upload_id,
                object_id,
                object_version,
                exc_info=True,
            )

        xml_bytes = build_complete_result_xml(
            request.headers.get("Host", ""), bucket_name, object_key, str(complete_res.etag)
        )

        get_metrics_collector().record_s3_operation(
            operation="complete_multipart_upload",
            bucket_name=bucket_name,
            success=True,
        )
        get_metrics_collector().record_data_transfer(
            operation="complete_multipart_upload",
            bytes_transferred=int(complete_res.size_bytes),
            bucket_name=bucket_name,
        )

        # Return with proper headers
        return Response(
            content=xml_bytes,
            media_type="application/xml",
            headers={
                "Content-Type": "application/xml; charset=utf-8",
                "Content-Length": str(len(xml_bytes)),
                "x-amz-version-id": str(object_version),
            },
        )
    except Exception as e:
        logger.exception(f"Error completing multipart upload: {e}")
        get_metrics_collector().record_error(
            error_type="internal_error",
            operation="complete_multipart_upload",
            bucket_name=bucket_name,
        )
        return s3_error_response(
            "InternalError",
            f"Error completing multipart upload: {str(e)}",
            status_code=500,
        )
