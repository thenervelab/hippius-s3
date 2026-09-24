from __future__ import annotations

import logging
from typing import Any

from fastapi import Request
from fastapi import Response

from hippius_s3.api.s3 import errors
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)


async def handle_delete_bucket(bucket_name: str, request: Request, db: Any, redis_client: Any) -> Response:
    """
    Delete a bucket using S3 protocol (DELETE /{bucket_name}).
    Also handles removing bucket tags (DELETE /{bucket_name}?tagging).

    Soft-deletes the bucket by setting buckets.deleted_at. Returns 204 in O(1).
    The Phase 2 bucket_reaper worker discovers soft-deleted buckets by polling
    `buckets WHERE deleted_at IS NOT NULL` (via idx_buckets_deleted_at_pending)
    and drains child rows leaves-up. There is intentionally no Redis queue —
    the DB partial index is the single source of truth.

    The previous synchronous DELETE FROM buckets cascade hit the API's
    statement_timeout for buckets with significant child-row residue.
    """
    # If tagging is in query params, we're just deleting tags
    if "tagging" in request.query_params:
        try:
            bucket = await db.fetchrow(
                get_query("get_bucket_by_name"),
                bucket_name,
            )

            if not bucket:
                return errors.s3_error_response(
                    "NoSuchBucket",
                    f"The specified bucket {bucket_name} does not exist",
                    status_code=404,
                    BucketName=bucket_name,
                )

            logger.info(f"Deleting all tags for bucket '{bucket_name}' via S3 protocol")

            await db.fetchrow(
                get_query("update_bucket_tags"),
                bucket["bucket_id"],
                "{}",
            )

            return Response(status_code=204)

        except Exception:
            logger.exception("Error deleting bucket tags")
            return errors.s3_error_response(
                "InternalError",
                "We encountered an internal error. Please try again.",
                status_code=500,
            )

    bucket = await db.fetchrow(
        get_query("get_bucket_by_name"),
        bucket_name,
    )

    if not bucket:
        return errors.s3_error_response(
            "NoSuchBucket",
            f"The specified bucket {bucket_name} does not exist",
            status_code=404,
            BucketName=bucket_name,
        )

    # One transaction around the check and the soft-delete, with the bucket row locked first, so
    # an object or upload being created concurrently is either committed before the check sees it
    # or held off until the bucket is gone. See lock_bucket_for_delete.sql.
    #
    # Residual: a write that resolved the bucket BEFORE this commits and inserts AFTER it still
    # lands in the soft-deleted bucket — the write path does not re-check buckets.deleted_at. That
    # includes a simple PUT still streaming, whose reserved version is not counted as data yet.
    async with db.transaction():
        if not await db.fetchrow(get_query("lock_bucket_for_delete"), bucket["bucket_id"]):
            return errors.s3_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        # S3 semantics: refuse while any version or delete marker remains — not merely a listable
        # key — and while an upload is open. See the query for why ListObjects' view of "empty"
        # was not enough.
        emptiness = await db.fetchrow(get_query("bucket_emptiness"), bucket["bucket_id"])
        if emptiness["has_versions"]:
            return errors.s3_error_response(
                "BucketNotEmpty",
                "The bucket you tried to delete is not empty",
                status_code=409,
                BucketName=bucket_name,
            )
        if emptiness["has_open_uploads"]:
            return errors.s3_error_response(
                "BucketNotEmpty",
                "The bucket has ongoing multipart uploads",
                status_code=409,
                BucketName=bucket_name,
            )

        # Soft-delete: set deleted_at. Idempotent — the row lock above makes a concurrent
        # DeleteBucket wait and then find nothing to lock, so an empty result here is unexpected
        # but still means the bucket is gone (404 NoSuchBucket per S3 spec, NOT 403 — auth is
        # enforced upstream).
        soft_deleted = await db.fetchrow(get_query("soft_delete_bucket"), bucket["bucket_id"])
        if not soft_deleted:
            return errors.s3_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

    return Response(status_code=204)
