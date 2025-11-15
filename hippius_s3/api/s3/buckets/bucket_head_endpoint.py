from __future__ import annotations

import logging
from datetime import datetime
from datetime import timezone
from typing import Any

from fastapi import Request
from fastapi import Response

from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)


async def handle_head_bucket(bucket_name: str, request: Request, db: Any) -> Response:
    """Check if a bucket exists using S3 protocol (HEAD /{bucket_name})."""
    try:
        main_account_id = request.state.account.main_account
        await db.fetchrow(
            get_query("get_or_create_user_by_main_account"),
            main_account_id,
            datetime.now(timezone.utc),
        )

        bucket = await db.fetchrow(
            get_query("get_bucket_by_name"),
            bucket_name,
        )

        if not bucket:
            # For HEAD requests, the Minio client expects a specific format
            # Return a 404 without content for proper bucket_exists handling
            return Response(status_code=404)

        # For existing buckets, return an empty 200 response
        return Response(status_code=200)

    except Exception:
        logger.exception("Error checking bucket via S3 protocol")
        return Response(status_code=500)
