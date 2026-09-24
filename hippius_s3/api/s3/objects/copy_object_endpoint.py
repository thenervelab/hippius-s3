from __future__ import annotations

import logging
import uuid
from datetime import datetime
from datetime import timezone
from typing import Any

import asyncpg
from fastapi import Request
from fastapi import Response

from hippius_s3.api.s3 import errors
from hippius_s3.api.s3.copy_helpers import handle_same_bucket_copy
from hippius_s3.api.s3.copy_helpers import handle_streaming_copy
from hippius_s3.api.s3.copy_helpers import is_multipart_object
from hippius_s3.api.s3.copy_helpers import parse_copy_source
from hippius_s3.api.s3.copy_helpers import resolve_copy_resources
from hippius_s3.api.s3.copy_helpers import should_use_v5_fast_path
from hippius_s3.api.s3.objects.object_lock_endpoints import lock_for_new_version
from hippius_s3.api.s3.objects.object_lock_endpoints import validate_lock_intent
from hippius_s3.config import get_config
from hippius_s3.repositories.objects import ObjectRepository
from hippius_s3.services.copy_service_v5 import execute_v5_fast_path_copy
from hippius_s3.storage_version import require_supported_storage_version


logger = logging.getLogger(__name__)
config = get_config()


async def handle_copy_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    pool: asyncpg.Pool,
    redis_client: Any,
) -> Response:
    try:
        # Refuse bad lock intent before any bytes move, for the same reason PutObject does: a 4xx
        # returned after the destination is written leaves an unlocked copy behind, and an
        # overwriting copy has already destroyed what was there.
        lock_rejection = validate_lock_intent(request)
        if lock_rejection is not None:
            return lock_rejection

        # A copy is a write to the destination key, so an If-None-Match on it means the same
        # create-only intent PutObject honours — and silently ignoring it is exactly how a
        # write-once client ends up with an overwritten object. The copy paths (alias, v5 fast
        # path, streaming) each land the destination their own way, none of them under the
        # conditional reserve/finalize the PUT writer uses, so the create-only semantics are not
        # implemented here yet. Refuse rather than ignore, for EVERY value including "*".
        if request.headers.get("if-none-match") is not None:
            return errors.conditional_write_not_implemented_response()

        lock_intent = lock_for_new_version(request)
        assert not isinstance(lock_intent, Response)  # validate_lock_intent already returned it

        source_bucket_name, source_object_key, source_version_id = parse_copy_source(
            request.headers.get("x-amz-copy-source")
        )

        user, source_bucket, dest_bucket, source_object = await resolve_copy_resources(
            db=pool,
            main_account=request.state.main_account_id,
            source_bucket_name=source_bucket_name,
            source_object_key=source_object_key,
            dest_bucket_name=bucket_name,
            source_version_id=source_version_id,
        )

        existing_dest = await ObjectRepository(pool).get_by_path(dest_bucket["bucket_id"], object_key)
        object_id = str(existing_dest["object_id"]) if existing_dest else str(uuid.uuid4())
        copy_created_at = datetime.now(timezone.utc)

        src_obj_row = source_object
        src_multipart = is_multipart_object(src_obj_row)

        # A copy pinned to a source version can never be an alias. An alias is a second name on the
        # object_id, so it shows whatever that object's CURRENT version is — for a versioned copy
        # that is the wrong content immediately (the caller asked for an older version), and it
        # would keep tracking the source afterwards, which no copy should do. Fall through to the
        # real byte copy, which snapshots the version that was asked for.
        #
        # A lock also disqualifies the alias. An alias is a second NAME on one object_id, not a new
        # version, so there is no separate row to carry the copy's retention — writing one would
        # apply it to the source as well, locking an object the caller never named. Falling through
        # to a real byte copy gives the destination its own version, which is the only thing a
        # per-version lock can attach to.
        if (
            lock_intent is None
            and source_version_id is None
            and str(source_bucket["bucket_id"]) == str(dest_bucket["bucket_id"])
        ):
            aliased = await handle_same_bucket_copy(
                pool,
                dest_bucket_id=str(dest_bucket["bucket_id"]),
                dest_key=object_key,
                src_obj_row=src_obj_row,
                copy_created_at=copy_created_at,
            )
            if aliased is not None:
                return aliased

        raw_storage_version = src_obj_row.get("storage_version")
        if raw_storage_version is None:
            return errors.s3_error_response(
                "InternalError",
                "Missing storage version",
                status_code=500,
            )
        src_storage_version = require_supported_storage_version(int(raw_storage_version))

        # Multipart objects are supported via the streaming copy fallback (copy-by-bytes).
        # The v5 fast-path reuses chunk CIDs, which is currently incompatible with v5 chunk
        # crypto binding (bucket/object identifiers).
        if src_multipart:
            logger.info("CopyObject multipart source: forcing streaming fallback")
            return await handle_streaming_copy(
                pool=pool,
                redis_client=redis_client,
                request=request,
                source_bucket=source_bucket,
                dest_bucket=dest_bucket,
                source_object=source_object,
                src_obj_row=src_obj_row,
                object_id=object_id,
                object_key=object_key,
                copy_created_at=copy_created_at,
                config=config,
                lock=lock_intent,
            )

        eligible, chunk_rows, reason = await should_use_v5_fast_path(
            db=pool,
            src_obj_row=src_obj_row,
            existing_dest=existing_dest,
            src_storage_version=src_storage_version,
            src_multipart=src_multipart,
        )

        # A lock also disqualifies the v5 fast path. Its version is serveable from its first
        # autocommit statement, so a lock could only be written afterwards — a window in which a
        # key allowed to DELETE ?versionId= can destroy the copy before it is retained. The
        # streaming writer stores the lock in the transaction that makes the version serveable.
        if eligible and lock_intent is None:
            assert chunk_rows is not None
            logger.info("CopyObject using v5 fast path (envelope rewrap + CID reuse)")
            return await execute_v5_fast_path_copy(
                db=pool,
                source_bucket=source_bucket,
                dest_bucket=dest_bucket,
                source_object=source_object,
                src_obj_row=src_obj_row,
                object_id=object_id,
                object_key=object_key,
                chunk_rows=chunk_rows,
                copy_created_at=copy_created_at,
                config=config,
            )

        logger.info(f"CopyObject using streaming fallback: {reason if lock_intent is None else 'object lock'}")
        return await handle_streaming_copy(
            pool=pool,
            redis_client=redis_client,
            request=request,
            source_bucket=source_bucket,
            dest_bucket=dest_bucket,
            source_object=source_object,
            src_obj_row=src_obj_row,
            object_id=object_id,
            object_key=object_key,
            copy_created_at=copy_created_at,
            config=config,
            lock=lock_intent,
        )
    except errors.S3Error as e:
        return errors.s3_error_response(
            code=e.code,
            message=e.message,
            status_code=e.status_code,
        )
