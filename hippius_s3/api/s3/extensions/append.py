"""S4 Append extension: version-CAS append for S3-compatible PUT requests.

This module contains the append implementation extracted from the monolithic
endpoints file. It appends new bytes to the existing object atomically using a
DB row lock and updates the object state while keeping S3 client compatibility.

Concurrency control uses version-based CAS via ``x-amz-meta-append-if-version``
and the current value exposed on HEAD as ``x-amz-meta-append-version``. ETag is
maintained for clients, but is not used for CAS.
"""

import asyncio
import contextlib
import hashlib
import json
import logging
import uuid
from datetime import datetime
from datetime import timezone
from typing import Any
from typing import cast

from fastapi import Request
from fastapi import Response

from hippius_s3.api.s3 import errors
from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.config import get_config
from hippius_s3.queue import Chunk
from hippius_s3.queue import UploadChainRequest
from hippius_s3.queue import enqueue_upload_request
from hippius_s3.services.crypto_service import CryptoService
from hippius_s3.services.parts_service import upsert_part_placeholder
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)
config = get_config()


async def handle_append(
    request: Request,
    db: Any,
    ipfs_service: Any,
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
        ipfs_service: IPFS service dependency
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
    content_type = request.headers.get("Content-Type", "application/octet-stream")

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

    # PHASE 0: Preflight CAS check (no lock) to reduce contention
    # Check if object exists and get current version
    current_row = await db.fetchrow(
        "SELECT append_version, object_id FROM objects WHERE bucket_id = $1 AND object_key = $2 LIMIT 1",
        bucket_id,
        object_key,
    )
    if not current_row:
        return errors.s3_error_response(
            code="NoSuchKey",
            message=f"The specified key {object_key} does not exist",
            status_code=404,
            Key=object_key,
        )

    current_version = int(current_row.get("append_version") or 0)
    object_id = str(current_row["object_id"])

    with contextlib.suppress(Exception):
        logger.info(
            f"APPEND preflight bucket={bucket_name} key={object_key} expected={expected_version} current={current_version}"
        )
    if expected_version != current_version:
        with contextlib.suppress(Exception):
            logger.info(
                f"APPEND preflight-mismatch bucket={bucket_name} key={object_key} expected={expected_version} current={current_version} -> 412"
            )
        return errors.s3_error_response(
            code="PreconditionFailed",
            message="Version precondition failed",
            status_code=412,
            extra_headers={
                "x-amz-meta-append-version": str(current_version),
                "Retry-After": "0.1",  # Small jitter to spread retries
            },
        )

    # PHASE 1: Transactional phase (with lock) - only reached if preflight passed
    async with db.transaction():
        # Try to acquire row lock with brief retries to avoid spurious 412s in serial clients
        lock_attempts = 3
        current = None
        for attempt in range(lock_attempts):
            try:
                current = await db.fetchrow(
                    "SELECT * FROM objects WHERE bucket_id = $1 AND object_key = $2 FOR UPDATE NOWAIT",
                    bucket_id,
                    object_key,
                )
                break
            except Exception as e:
                if attempt == lock_attempts - 1:
                    # If still not available, convert to a fast 412 with refreshed version
                    with contextlib.suppress(Exception):
                        logger.info(f"APPEND lock not available for {bucket_name}/{object_key}: {e}")
                    try:
                        fresh = await db.fetchrow(
                            "SELECT append_version FROM objects WHERE bucket_id = $1 AND object_key = $2",
                            bucket_id,
                            object_key,
                        )
                        fresh_version = int((fresh or {}).get("append_version") or current_version)
                    except Exception:
                        fresh_version = current_version
                    return errors.s3_error_response(
                        code="PreconditionFailed",
                        message="Concurrent append in progress",
                        status_code=412,
                        extra_headers={
                            "x-amz-meta-append-version": str(fresh_version),
                            "Retry-After": "0.1",
                        },
                    )
                # brief backoff before retrying lock acquisition
                await asyncio.sleep(0.03 * (attempt + 1))

        if not current:
            # Double-check in case of race (though preflight checked)
            return errors.s3_error_response(
                code="NoSuchKey",
                message=f"The specified key {object_key} does not exist",
                status_code=404,
                Key=object_key,
            )

        # Double-check version (TOCTOU protection)
        current_version_txn = int(current.get("append_version") or 0)
        if expected_version != current_version_txn:
            # This should be extremely rare since we checked preflight
            with contextlib.suppress(Exception):
                logger.warning(
                    f"APPEND txn-version-mismatch bucket={bucket_name} key={object_key} expected={expected_version} current_txn={current_version_txn} -> 412"
                )
            return errors.s3_error_response(
                code="PreconditionFailed",
                message="Version precondition failed",
                status_code=412,
                extra_headers={
                    "x-amz-meta-append-version": str(current_version_txn),
                    "Retry-After": "0.1",
                },
            )

        # Ensure multipart_uploads row exists for parts FK constraint
        upload_row = await db.fetchrow(
            "SELECT upload_id FROM multipart_uploads WHERE object_id = $1 ORDER BY initiated_at DESC LIMIT 1",
            object_id,
        )
        if upload_row:
            upload_id = str(upload_row["upload_id"])  # type: ignore[index]
        else:
            upload_id = str(uuid.uuid4())
            await db.execute(
                """
                INSERT INTO multipart_uploads (upload_id, bucket_id, object_key, initiated_at, is_completed, content_type, metadata, object_id)
                VALUES ($1, $2, $3, $4, TRUE, $5, $6, $7)
                ON CONFLICT (upload_id) DO NOTHING
                """,
                upload_id,
                bucket_id,
                object_key,
                datetime.now(timezone.utc),
                content_type,
                json.dumps(current.get("metadata") or {}),
                object_id,
            )

        # Mark object as multipart (part 0 exists from PUT, now adding more parts)
        await db.execute("UPDATE objects SET multipart = TRUE WHERE object_id = $1", object_id)

        # Determine next part number
        next_part = await db.fetchval(
            "SELECT COALESCE(MAX(part_number), 0) + 1 FROM parts WHERE object_id = $1",
            object_id,
        )
        with contextlib.suppress(Exception):
            logger.info(f"APPEND next_part_decided object_id={object_id} next_part={int(next_part)}")
        # PHASE 1 (in-DB reservation): insert placeholder part and update object metadata atomically
        delta_md5 = hashlib.md5(incoming_bytes).hexdigest()
        delta_size = len(incoming_bytes)
        await upsert_part_placeholder(
            db,
            object_id=object_id,
            upload_id=str(upload_id),
            part_number=int(next_part),
            size_bytes=int(delta_size),
            etag=delta_md5,
            chunk_size_bytes=int(getattr(config, "object_chunk_size_bytes", 4 * 1024 * 1024))
            if not bucket["is_public"]
            else None,
        )

        # Recompute composite ETag from base object MD5 + all appended part etags
        base_md5 = str(current.get("md5_hash") or "").strip()
        if not base_md5 or len(base_md5) != 32:
            # Fallback: compute MD5 from cache if available (1-based base part)
            try:
                obj_cache = RedisObjectPartsCache(redis_client)
                base_bytes = await obj_cache.get(object_id, 1)
                base_md5 = hashlib.md5(base_bytes).hexdigest() if base_bytes else "00000000000000000000000000000000"
            except Exception:
                base_md5 = "00000000000000000000000000000000"  # Placeholder

        # Collect all part MD5s (base + appended parts)
        md5s = [bytes.fromhex(base_md5)]

        parts = await db.fetch(
            "SELECT part_number, etag FROM parts WHERE object_id = $1 ORDER BY part_number",
            object_id,
        )
        for p in parts:
            e = str(p["etag"]).strip('"')  # type: ignore
            e = e.split("-")[0]  # type: ignore
            if len(e) == 32:  # type: ignore
                md5s.append(bytes.fromhex(e))  # type: ignore

        combined_md5 = hashlib.md5(b"".join(md5s)).hexdigest()
        composite_etag = f"{combined_md5}-{len(md5s)}"

        # Update object size, etag, and append_version
        await db.execute(
            """
            UPDATE objects
            SET size_bytes = size_bytes + $2,
                md5_hash = $3,
                append_version = append_version + 1
            WHERE object_id = $1
            """,
            object_id,
            int(delta_size),
            composite_etag,
        )

        # End of transactional phase; publish outside of the lock window

    # PHASE 2 (out-of-DB): enqueue background publish for the reserved part and return immediately
    # Write-through cache first for immediate reads

    # Write-through cache: store appended bytes for immediate reads

    with contextlib.suppress(Exception):
        logger.info(
            f"APPEND cache write object_id={object_id} part={int(next_part)} bytes={len(incoming_bytes)} public={bucket['is_public']}"
        )
        obj_cache = RedisObjectPartsCache(redis_client)
        if not bucket["is_public"]:
            chunk_size = int(getattr(config, "object_chunk_size_bytes", 4 * 1024 * 1024))
            ct_chunks = CryptoService.encrypt_part_to_chunks(
                incoming_bytes,
                object_id=object_id,
                part_number=int(next_part),
                seed_phrase=request.state.seed_phrase,
                chunk_size=chunk_size,
            )
            total_ct = sum(len(ct) for ct in ct_chunks)
            # Write meta first for cheap readiness check
            await obj_cache.set_meta(
                object_id,
                int(next_part),
                chunk_size=chunk_size,
                num_chunks=len(ct_chunks),
                size_bytes=total_ct,
                ttl=1800,
            )
            # Then write chunk data
            for i, ct in enumerate(ct_chunks):
                await obj_cache.set_chunk(object_id, int(next_part), i, ct, ttl=1800)
        else:
            await obj_cache.set(object_id, int(next_part), incoming_bytes, ttl=1800)

    # Enqueue background publish of this part via the pinner worker
    try:
        should_encrypt = not bucket["is_public"]
        logger.debug(
            f"APPEND about to enqueue UploadChainRequest for object_id={object_id}, part={int(next_part)}, upload_id={str(upload_id)}"
        )
        payload = UploadChainRequest(
            substrate_url=config.substrate_url,
            ipfs_node=config.ipfs_store_url,
            address=request.state.account.main_account,
            subaccount=request.state.account.main_account,
            subaccount_seed_phrase=request.state.seed_phrase,
            bucket_name=bucket_name,
            object_key=object_key,
            should_encrypt=should_encrypt,
            object_id=object_id,
            upload_id=str(upload_id),
            chunks=[Chunk(id=int(next_part))],
        )
        logger.debug(f"APPEND UploadChainRequest payload created: {payload}")
        await enqueue_upload_request(payload, redis_client)
        logger.debug("APPEND UploadChainRequest successfully enqueued")
        with contextlib.suppress(Exception):
            logger.info(
                f"APPEND enqueued background publish object_id={object_id} part={int(next_part)} upload_id={str(upload_id)}"
            )
    except Exception:
        # Non-fatal; the object state is already updated, and cache serves reads
        logger.exception("Failed to enqueue append publish request")

    # Successful append: return the new append version so clients can avoid a HEAD
    resp = Response(
        status_code=200,
        headers={
            "ETag": f'"{composite_etag}"',
            "x-amz-meta-append-version": str(current_version + 1),
        },
    )
    with contextlib.suppress(Exception):
        logger.info(
            f"APPEND success bucket={bucket_name} key={object_key} new_version={current_version + 1} next_part={int(next_part)} size_delta={delta_size}"
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
