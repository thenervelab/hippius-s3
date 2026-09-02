from __future__ import annotations

import logging
from typing import Any

from fastapi import Request
from fastapi import Response
from opentelemetry import trace

from hippius_s3.api.middlewares.tracing import set_span_attributes
from hippius_s3.api.s3 import errors
from hippius_s3.api.s3.common import parse_version_id
from hippius_s3.api.s3.object_lock_enforcement import deletion_refusal_reason
from hippius_s3.api.s3.object_lock_enforcement import is_version_locked
from hippius_s3.api.s3.object_lock_enforcement import request_is_bucket_owner
from hippius_s3.api.s3.object_names import drop_s3_name
from hippius_s3.backend_routing import resolve_object_backends
from hippius_s3.config import get_config
from hippius_s3.db_retry import retry_on_object_version_conflict
from hippius_s3.queue import UnpinChainRequest
from hippius_s3.queue import enqueue_unpin_request
from hippius_s3.repositories.buckets import BucketRepository
from hippius_s3.repositories.users import UserRepository
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)
config = get_config()
tracer = trace.get_tracer(__name__)


def parse_version_id_or_error(raw: str | None) -> tuple[int | None, Response | None]:
    """Returns (version_id, error). A None version_id means "no version specified"."""
    try:
        return parse_version_id(raw), None
    except (ValueError, TypeError):
        return None, errors.s3_error_response(
            "InvalidArgument",
            f"Invalid version ID: {raw}",
            status_code=400,
        )


async def enqueue_object_unpin(
    db: Any,
    *,
    object_id: str,
    object_version: int | None,
    address: str,
    ray_id: str | None,
) -> None:
    """Enqueue ONE unpin. `object_version=None` means "every version of this object".

    Fanning out one request per version is not viable on the request path: prod holds an object
    with 646,993 versions (S4 append churn mints a version per append), so a single DELETE would
    run a 647k-row join and LPUSH 647k queue entries before responding — a replay of the 1.29M
    `arion_unpin_requests` overrun that made redis slow enough to break prod GETs.

    A NULL version defers the resolution to the unpinner, which already handles it
    (`get_chunk_backend_identifiers` takes a nullable version) under its own batching and pacing.
    The revive race is covered there too: that query's guard refuses to return the current version
    of a live object, so a re-PUT between the soft delete and the unpin cannot destroy live data.
    """
    db_backends = await resolve_object_backends(db, object_id, object_version)
    await enqueue_unpin_request(
        payload=UnpinChainRequest(
            address=address,
            object_id=object_id,
            object_version=object_version,
            ray_id=ray_id,
            delete_backends=db_backends if db_backends else None,
        )
    )


async def delete_object_version(
    bucket_id: Any,
    object_key: str,
    version_id: int,
    request: Request,
    db: Any,
) -> Response:
    """Permanently delete a single version, AWS `DELETE ?versionId=` semantics.

    This used to ignore the version id entirely and soft-delete the whole object, taking every
    version with it — a silent data loss for any client pruning old versions.
    """
    # One transaction with the objects row locked up front: soft-deleting the version and moving
    # current_object_version off it must be atomic against a concurrent versioned DELETE of a
    # DIFFERENT version, which could otherwise leave the pointer on a soft-deleted row.
    async with db.transaction():
        row = await db.fetchrow(get_query("lock_object_and_get_version"), bucket_id, object_key, version_id)
        if not row or row["object_version"] is None:
            # Key or version absent, or the version is already deleted — AWS treats a versioned
            # DELETE as idempotent.
            return Response(status_code=204)

        object_id = str(row["object_id"])
        is_delete_marker = bool(row["is_delete_marker"])
        was_current = int(row["current_object_version"]) == version_id

        # OBJECT LOCK: a permanent delete of a locked version is refused with 403, per AWS. The
        # check sits inside the FOR UPDATE taken above, so a hold committed concurrently cannot
        # slip between the read and the delete. Only the ?versionId form is refused — a
        # versionId-less DELETE writes a delete marker and is always allowed (handled by the
        # caller), which is the half implementations usually get backwards.
        refusal = deletion_refusal_reason(
            row,
            is_bucket_owner=request_is_bucket_owner(request),
            headers=request.headers,
        )
        if refusal is not None:
            logger.info("Refusing versioned DELETE of locked object %s v%s: %s", object_id, version_id, refusal)
            return errors.s3_error_response(
                "AccessDenied",
                "Access Denied",
                status_code=403,
                Key=object_key,
                VersionId=str(version_id),
            )

        # A same-bucket CopyObject attaches a second S3 key to ONE object_id — the v5 AAD binds the
        # id, so a copy cannot mint a new one. Versions therefore belong to the object, not to the
        # name, and destroying one would remove content still published under the other key. S3
        # semantics say those keys are independent objects, so there is no correct version to
        # delete here; splitting them means a real re-encrypting copy, which is exactly what the
        # alias exists to avoid.
        #
        # Refusing is the only non-destructive answer. Checked inside the row lock taken above, so
        # an alias cannot appear between here and the delete.
        if int(row["alias_count"] or 0) > 0:
            return errors.s3_error_response(
                "NotImplemented",
                "This object is published under more than one key; deleting a single version of it "
                "is not supported. Delete the other key first, or delete the object without a versionId.",
                status_code=501,
                Key=object_key,
                VersionId=str(version_id),
            )

        # Reaching here while still locked means exactly one thing: a GOVERNANCE retention the
        # bucket owner overrode with x-amz-bypass-governance-retention. A legal hold and a
        # COMPLIANCE retention are both refused above, bypass or not.
        bypassed_lock = is_version_locked(row)

        deleted = await db.fetchrow(get_query("soft_delete_object_version"), object_id, version_id)
        if not deleted:
            return Response(status_code=204)

        if bypassed_lock:
            # The delete is authorised, so the retention must come off the row as well. Otherwise
            # the version stays "locked" to every SQL gate — which has no concept of a bypass — and
            # the unpinner, reaper and hard-delete ring all withhold the bytes forever, silently.
            await db.execute(get_query("clear_version_lock_after_bypass"), object_id, version_id)
            logger.info(
                "Cleared GOVERNANCE retention on %s v%s after an authorised bypass delete",
                object_id,
                version_id,
            )

        whole_object_deleted = False
        if was_current:
            # Removing the current version exposes the next-newest one. With nothing left to point
            # at, the object itself goes — the pre-existing whole-object soft-delete path.
            repointed = await db.fetchrow(get_query("repoint_current_version_after_delete"), object_id, version_id)
            if not repointed:
                await db.fetchrow(get_query("soft_delete_object"), bucket_id, object_key)
                whole_object_deleted = True

    # A delete marker holds no data, so there is nothing to unpin. Must run AFTER the pointer
    # moves: get_chunk_backend_identifiers refuses to unpin the current version of a live object.
    if not is_delete_marker:
        await enqueue_object_unpin(
            db,
            object_id=object_id,
            # Falling back to the whole-object delete means every remaining version is now
            # unreachable, so scope the unpin the same way that path does. Leaving it version-scoped
            # re-opens the wedge this PR exists to fix: hard_delete_object's readiness gate waits on
            # ALL versions, so any sibling still holding live chunk_backend rows — an out-of-band
            # migration row above current, or one whose earlier unpin went to the DLQ — would keep
            # the object un-hard-deletable forever.
            object_version=None if whole_object_deleted else version_id,
            address=request.state.main_account_id,
            ray_id=getattr(request.state, "ray_id", None),
        )

    headers = {"x-amz-version-id": str(version_id)}
    if is_delete_marker:
        headers["x-amz-delete-marker"] = "true"
    return Response(status_code=204, headers=headers)


async def insert_delete_marker(bucket_id: Any, object_key: str, db: Any) -> Response:
    """Versioning-enabled simple DELETE: hide the key behind a marker, destroy nothing."""

    async def _reserve() -> Any:
        return await db.fetchrow(get_query("insert_delete_marker"), bucket_id, object_key)

    marker = await retry_on_object_version_conflict(_reserve)
    if not marker:
        # No live object under that key; nothing to mark.
        return Response(status_code=204)

    return Response(
        status_code=204,
        headers={
            "x-amz-delete-marker": "true",
            "x-amz-version-id": str(marker["object_version"]),
        },
    )


async def handle_delete_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: Any,
    redis_client: Any,
) -> Response:
    # Abort multipart upload path is handled in the router before delegating to us
    version_id, invalid = parse_version_id_or_error(request.query_params.get("versionId"))
    if invalid is not None:
        return invalid

    try:
        with tracer.start_as_current_span("delete_object.ensure_user") as span:
            user = await UserRepository(db).ensure_by_main_account(request.state.main_account_id)
            set_span_attributes(span, {"hippius.account.main": user["main_account_id"]})

        with tracer.start_as_current_span("delete_object.get_bucket") as span:
            bucket = await BucketRepository(db).get_by_name_and_owner(bucket_name, user["main_account_id"])
            if not bucket:
                return errors.s3_error_response(
                    "NoSuchBucket",
                    f"The specified bucket {bucket_name} does not exist",
                    status_code=404,
                    BucketName=bucket_name,
                )
            bucket_id = bucket["bucket_id"]
            set_span_attributes(span, {"bucket_id": str(bucket_id)})

        if version_id is not None:
            # Version ids are only addressable on a versioning-enabled bucket. An unversioned
            # bucket still RETAINS superseded versions (~25 TB of them in prod), and deleting one
            # by id would repoint current_object_version back onto the row the user overwrote —
            # resurrecting content they believe they replaced. Refuse instead: destroying nothing
            # is the only safe answer, and AWS has no addressable version here either.
            if bucket["versioning_status"] != "Enabled":
                return errors.s3_error_response(
                    "NoSuchVersion",
                    "The specified version does not exist; versioning is not enabled on this bucket",
                    status_code=404,
                    BucketName=bucket_name,
                    Key=object_key,
                    VersionId=str(version_id),
                )
            # Deliberately BEFORE drop_s3_name, which mutates: a versioned DELETE names one
            # version of the object, not one of the S3 names pointing at it, so it must not drop
            # or promote a name as a side effect.
            with tracer.start_as_current_span(
                "delete_object.delete_version",
                attributes={"object_version": version_id},
            ):
                return await delete_object_version(bucket_id, object_key, version_id, request, db)

        # Same-bucket CopyObject makes extra names for one object_id. "alias"/"promoted" mean the
        # ciphertext is still reachable under another name, so there is nothing to tombstone or
        # unpin — and on a versioning-enabled bucket a delete marker would wrongly hide every name.
        # Only "last" falls through to the real delete below.
        kind = await drop_s3_name(db, str(bucket_id), object_key)
        if kind in {"alias", "promoted"}:
            return Response(status_code=204)

        if bucket["versioning_status"] == "Enabled":
            with tracer.start_as_current_span("delete_object.insert_delete_marker"):
                return await insert_delete_marker(bucket_id, object_key, db)

        # OBJECT LOCK: this branch soft-deletes the WHOLE object, taking every version with it, so
        # a locked version would become unreachable even though its bytes survive (the unpin gate
        # keeps those). Refuse instead.
        #
        # Only reachable on an UNVERSIONED bucket — the versioning-enabled branch above writes a
        # delete marker, which is additive and always allowed. AWS requires versioning for Object
        # Lock, so a lock here means something already went wrong upstream; failing closed is the
        # answer that cannot lose access to retained data.
        # fetchrow, not fetchval: the repo's db doubles and pooled connections all implement
        # fetchrow, and this runs on the ordinary delete path.
        locked_row = await db.fetchrow(get_query("count_locked_versions"), bucket_id, object_key)
        locked_here = int(locked_row["locked_count"] or 0) if locked_row is not None else 0
        if locked_here > 0:
            logger.info(
                "Refusing whole-object DELETE of %s/%s: %s live version(s) under Object Lock",
                bucket_name,
                object_key,
                locked_here,
            )
            return errors.s3_error_response(
                "AccessDenied",
                "Access Denied",
                status_code=403,
                BucketName=bucket_name,
                Key=object_key,
            )

        # HD-78: no existence pre-check — soft_delete_object's RETURNING already distinguishes
        # absent/already-deleted (both → idempotent 204 below), so the pre-check was a wasted read.
        # Soft-delete the object (sets deleted_at, does NOT cascade-delete rows)
        with tracer.start_as_current_span("delete_object.soft_delete") as span:
            deleted = await db.fetchrow(get_query("soft_delete_object"), bucket_id, object_key)
            if not deleted:
                # Already deleted or permission issue — idempotent 204
                return Response(status_code=204)
            object_id = str(deleted["object_id"])
            object_version = int(deleted["current_object_version"])
            set_span_attributes(
                span,
                {
                    "object_id": object_id,
                    "has_object_id": True,
                    "object_version": object_version,
                },
            )

        # Cleanup provisional multipart uploads
        with tracer.start_as_current_span("delete_object.cleanup_multipart_uploads"):
            try:
                await db.execute(
                    "DELETE FROM multipart_uploads WHERE bucket_id = $1 AND object_key = $2 AND is_completed = FALSE",
                    bucket_id,
                    object_key,
                )
            except Exception:
                logger.debug("Failed to cleanup provisional multipart uploads on object delete", exc_info=True)

        with tracer.start_as_current_span(
            "delete_object.enqueue_unpin",
            attributes={
                "object_id": object_id,
                "has_object_id": True,
                "object_version": object_version,
            },
        ):
            # NULL version = every version of this object. Unpinning only current_object_version
            # left superseded versions pinned forever, which also wedged the object's hard-delete
            # (its readiness gate waits on ALL versions). Resolving the version list here instead
            # would be O(versions) work on the request path — prod holds an object with 646,993 of
            # them — so the unpinner resolves it under its own batching.
            await enqueue_object_unpin(
                db,
                object_id=object_id,
                object_version=None,
                address=request.state.main_account_id,
                ray_id=getattr(request.state, "ray_id", None),
            )

        return Response(status_code=204)

    except Exception:
        logger.exception("Error deleting object")
        return errors.s3_error_response(
            "InternalError", "We encountered an internal error. Please try again.", status_code=500
        )
