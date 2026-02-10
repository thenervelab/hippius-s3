from __future__ import annotations

import logging
from typing import Any

from fastapi import Request
from fastapi import Response

from hippius_s3.api.s3 import errors
from hippius_s3.config import get_config
from hippius_s3.queue import UnpinChainRequest
from hippius_s3.queue import enqueue_unpin_request
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)
config = get_config()


async def handle_delete_bucket(bucket_name: str, request: Request, db: Any) -> Response:
    """
    Delete a bucket using S3 protocol (DELETE /{bucket_name}).
    Also handles removing bucket tags (DELETE /{bucket_name}?tagging).

    This endpoint is compatible with the S3 protocol used by MinIO and other S3 clients.
    """
    # If tagging is in query params, we're just deleting tags
    if "tagging" in request.query_params:
        try:
            # Get user for user-scoped bucket lookup
            main_account_id = request.state.account.main_account

            # Get bucket for this main account
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

            # Clear bucket tags by setting to empty JSON
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

    try:
        # Get bucket for this main account
        main_account_id = request.state.account.main_account
        bucket = await db.fetchrow(
            get_query("get_bucket_by_name"),
            bucket_name,
        )

        if not bucket:
            # For S3 compatibility, return an XML error response
            return errors.s3_error_response(
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
            return errors.s3_error_response(
                "BucketNotEmpty",
                "The bucket you tried to delete is not empty",
                status_code=409,
                BucketName=bucket_name,
            )

        # Also block deletion if there are ongoing multipart uploads (required)
        ongoing_uploads = await db.fetch(get_query("list_multipart_uploads"), bucket["bucket_id"], None)
        if ongoing_uploads:
            return errors.s3_error_response(
                "BucketNotEmpty",
                "The bucket has ongoing multipart uploads",
                status_code=409,
                BucketName=bucket_name,
            )

        # Delete bucket (permission is checked via main_account_id ownership)
        deleted_bucket = await db.fetchrow(get_query("delete_bucket"), bucket["bucket_id"])

        if not deleted_bucket:
            logger.warning(f"Account {main_account_id} tried to delete bucket {bucket_name} without permission")
            return errors.s3_error_response(
                "AccessDenied",
                f"You do not have permission to delete bucket {bucket_name}",
                status_code=403,
                BucketName=bucket_name,
            )

        # If we got here, the bucket was successfully deleted, so now enqueue objects for unpinning
        ray_id = getattr(request.state, "ray_id", None)
        for obj in objects:
            try:
                await enqueue_unpin_request(
                    payload=UnpinChainRequest(
                        address=request.state.account.main_account,
                        object_id=str(obj["object_id"]),
                        object_version=obj.get("current_object_version"),
                        cid=obj["ipfs_cid"],
                        ray_id=ray_id,
                    ),
                )
            except Exception:
                logger.debug("Failed to enqueue unpin for object during bucket delete", exc_info=True)

        return Response(status_code=204)

    except Exception:
        logger.exception("Error deleting bucket via S3 protocol")
        return errors.s3_error_response(
            "InternalError",
            "We encountered an internal error. Please try again.",
            status_code=500,
        )
