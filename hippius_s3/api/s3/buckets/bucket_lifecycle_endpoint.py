from __future__ import annotations

import logging
from datetime import datetime
from datetime import timezone
from typing import Any

from fastapi import Response

from hippius_s3.api.s3 import errors
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)


async def handle_get_bucket_lifecycle(bucket_name: str, db: Any, main_account_id: str) -> Response:
    """
    Get the lifecycle configuration of a bucket (GET /{bucket_name}?lifecycle).

    This is used by the MinIO client to retrieve bucket lifecycle configuration.
    """
    try:
        # Get user for user-scoped bucket lookup
        user = await db.fetchrow(
            get_query("get_or_create_user_by_main_account"),
            main_account_id,
            datetime.now(timezone.utc),
        )

        bucket = await db.fetchrow(
            get_query("get_bucket_by_name_and_owner"),
            bucket_name,
            user["main_account_id"],
        )

        if not bucket:
            # For S3 compatibility, return an XML error response
            return errors.s3_error_response(
                code="NoSuchBucket",
                message=f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        # Not persisted yet: align with AWS and return NoSuchLifecycleConfiguration when not configured
        return errors.s3_error_response(
            code="NoSuchLifecycleConfiguration",
            message="The lifecycle configuration does not exist",
            status_code=404,
            BucketName=bucket_name,
        )

    except Exception:
        logger.exception("Error getting bucket lifecycle")
        return errors.s3_error_response(
            code="InternalError",
            message="We encountered an internal error. Please try again.",
            status_code=500,
        )
