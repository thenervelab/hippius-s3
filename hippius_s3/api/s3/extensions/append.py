"""S4 Append extension: version-CAS append for S3-compatible PUT requests.

This module contains the append implementation extracted from the monolithic
endpoints file. It appends new bytes to the existing object atomically using a
DB row lock and updates the object state while keeping S3 client compatibility.

Concurrency control uses version-based CAS via ``x-amz-meta-append-if-version``
and the current value exposed on HEAD as ``x-amz-meta-append-version``. ETag is
maintained for clients, but is not used for CAS.
"""

import contextlib
import json
import logging
import uuid
from typing import Any
from typing import cast

from fastapi import Request
from fastapi import Response

from hippius_s3.api.s3 import errors
from hippius_s3.config import get_config
from hippius_s3.utils import get_query
from hippius_s3.writer.object_writer import ObjectWriter
from hippius_s3.writer.queue import enqueue_upload as writer_enqueue_upload
from hippius_s3.writer.types import AppendPreconditionFailed
from hippius_s3.writer.types import EmptyAppendError
from hippius_s3.writer.types import ObjectNotFound


logger = logging.getLogger(__name__)
config = get_config()


async def handle_append(
    request: Request,
    db: Any,
    redis_client: Any,
    *,
    bucket: dict,
    bucket_id: str,
    bucket_name: str,
    object_key: str,
    incoming_bytes: bytes,
) -> Response:
    """Handle append PUT with ETag CAS and atomic update.

    Args:
        request: FastAPI request (contains headers and state)
        db: asyncpg connection
        redis_client: Redis async client
        redis_client: Redis async client
        bucket: bucket row dict
        bucket_id: bucket ID
        bucket_name: bucket name
        object_key: object key within bucket
    Returns:
        Response compatible with S3 clients
    """

    # Version-based CAS for append
    expected_version_header = request.headers.get("x-amz-meta-append-if-version")
    append_id = request.headers.get("x-amz-meta-append-id")

    # Reject empty append deltas to avoid bloating manifests
    if not incoming_bytes:
        return errors.s3_error_response(
            code="InvalidRequest",
            message="Empty append not allowed",
            status_code=400,
        )

    # Idempotency: if append-id is provided and we've seen it for this object, return stored result
    if append_id and append_id.strip():
        id_key = f"append_id:{bucket_id}:{object_key}:{append_id}"
        try:
            cached = await redis_client.get(id_key)
            if cached:
                try:
                    payload = json.loads(cached)
                    etag = payload.get("etag")
                    if etag:
                        return Response(status_code=200, headers={"ETag": f'"{etag}"'})
                except Exception:
                    pass
        except Exception:
            # On Redis issues, proceed without idempotency
            pass

    # Validate append-if-version header early
    if expected_version_header is None:
        return errors.s3_error_response(
            code="InvalidRequest",
            message="Missing append-if-version",
            status_code=400,
        )
    try:
        expected_version = int(expected_version_header)
    except ValueError:
        return errors.s3_error_response(
            code="InvalidRequest",
            message="append-if-version must be an integer",
            status_code=400,
        )

    # Endpoint delegates CAS and locking to writer.append

    # PHASE 2 (out-of-DB): delegate to ObjectWriter to append, cache, and update version
    writer = ObjectWriter(db=db, redis_client=redis_client)
    try:
        result = await writer.append(
            bucket_id=bucket_id,
            bucket_name=bucket_name,
            object_key=object_key,
            expected_version=int(expected_version),
            account_address=request.state.account.main_account,
            seed_phrase=request.state.seed_phrase,
            incoming_bytes=incoming_bytes,
        )
    except AppendPreconditionFailed as exc:
        return errors.s3_error_response(
            code="PreconditionFailed",
            message="Version precondition failed",
            status_code=412,
            extra_headers={
                "x-amz-meta-append-version": str(exc.current_version),
                "Retry-After": "0.1",
            },
        )
    except ObjectNotFound:
        return errors.s3_error_response(
            code="NoSuchKey",
            message=f"The specified key {object_key} does not exist",
            status_code=404,
            Key=object_key,
        )
    except EmptyAppendError:
        return errors.s3_error_response(
            code="InvalidRequest",
            message="Empty append not allowed",
            status_code=400,
        )
    object_id = result["object_id"]
    next_part = int(result["part_number"])  # type: ignore[index]
    composite_etag = str(result["etag"])  # type: ignore[index]
    object_version = int(result.get("object_version", 1))
    new_append_version = int(result.get("new_append_version", 0))

    # Enqueue background publish of this part via the pinner worker (writer helper)
    try:
        await writer_enqueue_upload(
            redis_client=redis_client,
            address=request.state.account.main_account,
            subaccount_seed_phrase=request.state.seed_phrase,
            subaccount=request.state.account.main_account,
            bucket_name=bucket_name,
            object_key=object_key,
            object_id=object_id,
            object_version=int(object_version),
            upload_id=str(result["upload_id"]),
            chunk_ids=[int(next_part)],
            substrate_url=config.substrate_url,
            ipfs_node=config.ipfs_store_url,
        )
        with contextlib.suppress(Exception):
            logger.info(
                f"APPEND enqueued background publish object_id={object_id} part={int(next_part)} upload_id={str(result.get('upload_id', ''))}"
            )
    except Exception:
        logger.exception("Failed to enqueue append publish request")

    # Successful append: return the new append version so clients can avoid a HEAD
    resp = Response(
        status_code=200,
        headers={
            "ETag": f'"{composite_etag}"',
            "x-amz-meta-append-version": str(new_append_version),
        },
    )
    with contextlib.suppress(Exception):
        logger.info(
            f"APPEND success bucket={bucket_name} key={object_key} new_version={new_append_version} next_part={int(next_part)} size_delta={len(incoming_bytes)}"
        )

    # Record idempotency result for future retries (best-effort)
    if append_id and append_id.strip():
        id_key = f"append_id:{bucket_id}:{object_key}:{append_id}"
        with contextlib.suppress(Exception):
            await redis_client.setex(id_key, 3600, json.dumps({"etag": composite_etag}))

    return resp


async def _upsert_cid(db: Any, cid: str) -> uuid.UUID:
    row = await db.fetchrow(get_query("upsert_cid"), cid)
    return cast(uuid.UUID, row["id"])  # uuid UUID value is acceptable to asyncpg
