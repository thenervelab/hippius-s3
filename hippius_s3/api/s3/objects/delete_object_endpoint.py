from __future__ import annotations

import logging
from typing import Any

from fastapi import Request
from fastapi import Response
from opentelemetry import trace

from hippius_s3.api.middlewares.tracing import set_span_attributes
from hippius_s3.api.s3 import errors
from hippius_s3.api.s3.common import parse_version_id
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

        deleted = await db.fetchrow(get_query("soft_delete_object_version"), object_id, version_id)
        if not deleted:
            return Response(status_code=204)

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
            with tracer.start_as_current_span(
                "delete_object.delete_version",
                attributes={"object_version": version_id},
            ):
                return await delete_object_version(bucket_id, object_key, version_id, request, db)

        if bucket["versioning_status"] == "Enabled":
            with tracer.start_as_current_span("delete_object.insert_delete_marker"):
                return await insert_delete_marker(bucket_id, object_key, db)

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
