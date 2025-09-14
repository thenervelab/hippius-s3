"""S4 Append extension: version-CAS append for S3-compatible PUT requests.

This module contains the append implementation extracted from the monolithic
endpoints file. It appends new bytes to the existing object atomically using a
DB row lock and updates the object state while keeping S3 client compatibility.

Concurrency control uses version-based CAS via ``x-amz-meta-append-if-version``
and the current value exposed on HEAD as ``x-amz-meta-append-version``. ETag is
maintained for clients, but is not used for CAS.
"""

import hashlib
import json
import logging
import os
import uuid
from datetime import datetime
from datetime import timezone
from typing import Any
from typing import cast

from fastapi import Request
from fastapi import Response

from hippius_s3.api.s3 import errors
from hippius_s3.config import get_config
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
                    payload = json.loads(cached.decode("utf-8"))
                    etag = payload.get("etag")
                    if etag:
                        return Response(status_code=200, headers={"ETag": f'"{etag}"', "x-amz-append-replayed": "true"})
                except Exception:
                    pass
        except Exception:
            # On Redis issues, proceed without idempotency
            pass

    # Atomic read-modify-write guarded by row lock
    async with db.transaction():
        current = await db.fetchrow(
            "SELECT * FROM objects WHERE bucket_id = $1 AND object_key = $2 FOR UPDATE",
            bucket_id,
            object_key,
        )

        if not current:
            return errors.s3_error_response(
                code="NoSuchKey",
                message=f"The specified key {object_key} does not exist",
                status_code=404,
                Key=object_key,
            )

        # Validate CAS version
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
        current_version = int(current.get("append_version") or 0)
        if expected_version != current_version:
            return errors.s3_error_response(
                code="PreconditionFailed",
                message="Version precondition failed",
                status_code=412,
            )

        # Enforce readiness: allow append once the object has an IPFS CID or is fully uploaded.
        # Actual gating occurs below for non-multipart objects by verifying the presence of a simple CID.

        object_id = str(current["object_id"]) if current.get("object_id") else None
        if object_id is None:
            # Fallback: fetch object_id via get_object_by_path
            row = await db.fetchrow(get_query("get_object_by_path"), bucket_id, object_key)
            if row:
                object_id = str(row["object_id"])  # type: ignore[index]
        if object_id is None:
            return errors.s3_error_response(
                code="InternalError",
                message="Could not resolve object id",
                status_code=500,
            )

        # If object is not yet multipart, create an initial part from existing simple CID
        # Ensure there is a backing multipart_uploads row to satisfy parts.upload_id NOT NULL
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

        if not current.get("multipart"):
            row = await db.fetchrow(get_query("get_object_by_path"), bucket_id, object_key)
            simple_cid = (row.get("ipfs_cid") or "").strip() if row else ""
            if not simple_cid:
                return errors.s3_error_response(
                    code="ServiceUnavailable",
                    message="Object publish is in progress. Please retry shortly.",
                    status_code=503,
                    extra_headers={"Retry-After": "1"},
                )
            # Upsert CID and create part 1 referencing existing content
            cid_id = await _upsert_cid(db, simple_cid)
            await db.execute(
                """
                INSERT INTO parts (part_id, upload_id, part_number, ipfs_cid, size_bytes, etag, uploaded_at, object_id, cid_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (upload_id, part_number) DO NOTHING
                """,
                str(uuid.uuid4()),
                upload_id,
                1,
                simple_cid,
                int(current["size_bytes"]),
                str(current.get("md5_hash") or ""),
                datetime.now(timezone.utc),
                object_id,
                cid_id,
            )
            # Mark object as multipart
            await db.execute(
                "UPDATE objects SET multipart = TRUE WHERE object_id = $1",
                object_id,
            )

        # Determine next part number
        next_part = await db.fetchval(
            "SELECT COALESCE(MAX(part_number), 0) + 1 FROM parts WHERE object_id = $1",
            object_id,
        )

        # Publish delta chunk synchronously and create a part row
        should_encrypt = not bucket["is_public"]
        s3_result = await ipfs_service.client.s3_publish(
            content=incoming_bytes,
            encrypt=should_encrypt,
            seed_phrase=request.state.seed_phrase,
            subaccount_id=request.state.account.main_account,
            bucket_name=bucket_name,
            file_name=object_key,
            store_node=config.ipfs_store_url,
            pin_node=config.ipfs_store_url,
            substrate_url=config.substrate_url,
            publish=(os.getenv("HIPPIUS_PUBLISH_MODE", "full") != "ipfs_only"),
        )
        delta_cid = s3_result.cid
        delta_md5 = hashlib.md5(incoming_bytes).hexdigest()
        delta_size = len(incoming_bytes)

        cid_id = await _upsert_cid(db, delta_cid)
        await db.execute(
            """
            INSERT INTO parts (part_id, upload_id, part_number, ipfs_cid, size_bytes, etag, uploaded_at, object_id, cid_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (upload_id, part_number) DO UPDATE SET
                ipfs_cid = EXCLUDED.ipfs_cid,
                cid_id = EXCLUDED.cid_id,
                size_bytes = EXCLUDED.size_bytes,
                etag = EXCLUDED.etag,
                uploaded_at = EXCLUDED.uploaded_at
            """,
            str(uuid.uuid4()),
            upload_id,
            int(next_part),
            delta_cid,
            int(delta_size),
            delta_md5,
            datetime.now(timezone.utc),
            object_id,
            cid_id,
        )

        # Recompute composite ETag from all part etags
        parts = await db.fetch(
            "SELECT part_number, etag FROM parts WHERE object_id = $1 ORDER BY part_number",
            object_id,
        )
        md5s = []
        for p in parts:
            e = str(p["etag"]).strip('"')
            e = e.split("-")[0]
            if len(e) == 32:
                md5s.append(bytes.fromhex(e))
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

        # Write-through cache: store appended bytes for immediate reads
        try:
            cache_key = f"part_cache:{object_id}:{int(next_part)}"
            # Cache for 30 minutes
            await redis_client.setex(cache_key, 1800, incoming_bytes)
        except Exception:
            # Non-fatal: continue without cache if Redis unavailable
            pass

        resp = Response(
            status_code=200,
            headers={
                "ETag": f'"{composite_etag}"',
            },
        )

        # Record idempotency result for future retries (best-effort)
        if append_id and append_id.strip():
            id_key = f"append_id:{bucket_id}:{object_key}:{append_id}"
            from contextlib import suppress

            with suppress(Exception):
                await redis_client.setex(id_key, 3600, json.dumps({"etag": composite_etag}))

        return resp


async def _upsert_cid(db: Any, cid: str) -> uuid.UUID:
    row = await db.fetchrow(get_query("upsert_cid"), cid)
    return cast(uuid.UUID, row["id"])  # uuid UUID value is acceptable to asyncpg
