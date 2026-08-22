from __future__ import annotations

import logging
from typing import Any

from fastapi import Request
from fastapi import Response
from opentelemetry import trace

from hippius_s3.api.middlewares.tracing import set_span_attributes
from hippius_s3.api.s3 import errors
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

# AWS clients send the literal "null" as the version id of an object that predates versioning.
# We never mint that id, so it means "whatever the current version is" — i.e. a plain DELETE.
NULL_VERSION_ID = "null"


def parse_version_id(raw: str | None) -> tuple[int | None, Response | None]:
    """Returns (version_id, error). A None version_id means "no version specified"."""
    if raw is None or raw == "" or raw == NULL_VERSION_ID:
        return None, None
    try:
        version_id = int(raw)
    except ValueError:
        version_id = 0
    if version_id <= 0:
        return None, errors.s3_error_response(
            "InvalidArgument",
            f"Invalid version ID: {raw}",
            status_code=400,
        )
    return version_id, None


async def enqueue_unpins_for_versions(
    db: Any,
    *,
    object_id: str,
    versions: list[int],
    address: str,
    ray_id: str | None,
) -> None:
    """Enqueue one unpin per version.

    Deliberately one request per version rather than a single NULL-version request meaning "all
    versions, resolved later": a re-PUT between the soft delete and the unpin revives the object
    and changes what NULL would resolve to. Resolving the list here is race-free, and
    `get_chunk_backend_identifiers` still refuses to hand back the current version of a live
    object. Backends are resolved once across all versions rather than per version.
    """
    if not versions:
        return

    db_backends = await resolve_object_backends(db, object_id, None)
    for object_version in versions:
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
    row = await db.fetchrow(get_query("get_object_version_for_delete"), bucket_id, object_key, version_id)
    if not row:
        # Absent or already deleted — AWS treats a versioned DELETE as idempotent.
        return Response(status_code=204)

    object_id = str(row["object_id"])
    is_delete_marker = bool(row["is_delete_marker"])
    was_current = int(row["current_object_version"]) == version_id

    deleted = await db.fetchrow(get_query("soft_delete_object_version"), object_id, version_id)
    if not deleted:
        return Response(status_code=204)

    if was_current:
        # Removing the current version exposes the next-newest one. With nothing left to point at,
        # the object itself goes, which is the pre-existing whole-object soft-delete path.
        successor = await db.fetchrow(get_query("next_live_object_version"), object_id, version_id)
        if successor:
            await db.fetchrow(
                get_query("swap_current_version_cas"),
                object_id,
                version_id,
                int(successor["object_version"]),
            )
        else:
            await db.fetchrow(get_query("soft_delete_object"), bucket_id, object_key)

    # A delete marker holds no data, so there is nothing to unpin. Must run AFTER the pointer
    # moves: get_chunk_backend_identifiers refuses to unpin the current version of a live object.
    if not is_delete_marker:
        await enqueue_unpins_for_versions(
            db,
            object_id=object_id,
            versions=[version_id],
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
    version_id, invalid = parse_version_id(request.query_params.get("versionId"))
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

        # Every version that still holds a backend copy, not just the current one. Unpinning only
        # current_object_version left superseded versions pinned forever, which also wedged the
        # object's hard-delete: its readiness gate waits on ALL versions being unpinned.
        rows = await db.fetch(get_query("list_object_versions_for_unpin"), object_id)
        versions = [int(r["object_version"]) for r in rows] or [object_version]

        with tracer.start_as_current_span(
            "delete_object.enqueue_unpin",
            attributes={
                "object_id": object_id,
                "has_object_id": True,
                "object_version": object_version,
                "unpinned_versions": len(versions),
            },
        ):
            await enqueue_unpins_for_versions(
                db,
                object_id=object_id,
                versions=versions,
                address=request.state.main_account_id,
                ray_id=getattr(request.state, "ray_id", None),
            )

        return Response(status_code=204)

    except Exception:
        logger.exception("Error deleting object")
        return errors.s3_error_response(
            "InternalError", "We encountered an internal error. Please try again.", status_code=500
        )
