from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Request
from fastapi import Response

from hippius_s3 import dependencies
from hippius_s3.api.s3 import errors as s3_errors
from hippius_s3.api.s3.objects.get_object_endpoint import handle_get_object
from hippius_s3.api.s3.objects.head_object_endpoint import handle_head_object


logger = logging.getLogger(__name__)

router = APIRouter(tags=["public-s3"])


@router.get("/public/{bucket_name}/{object_key:path}", status_code=200)
async def get_public_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
    ipfs_service: Any = Depends(dependencies.get_ipfs_service),
    redis_client: Any = Depends(dependencies.get_redis),
    object_reader: Any = Depends(dependencies.get_object_reader),
) -> Response:
    """Anonymous GET object endpoint for public buckets."""
    # Whitelist: only allow GET with no special query parameters
    if request.query_params:
        # Public GET only supports normal object fetch without special query params
        return s3_errors.s3_error_response(
            code="SignatureDoesNotMatch",
            message="The request signature we calculated does not match the signature you provided",
            status_code=403,
        )
    # Call the regular get_object handler with anonymous account
    response = await handle_get_object(bucket_name, object_key, request, db, ipfs_service, redis_client, object_reader)

    # Add anonymous access header
    response.headers["x-hippius-access-mode"] = "anon"
    return response


@router.head("/public/{bucket_name}/{object_key:path}", status_code=200)
async def head_public_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
) -> Response:
    """Anonymous HEAD object endpoint for public buckets."""
    # Whitelist: only allow HEAD with no special query parameters
    if request.query_params:
        return s3_errors.s3_error_response(
            code="SignatureDoesNotMatch",
            message="The request signature we calculated does not match the signature you provided",
            status_code=403,
        )
    # Call the regular head_object handler with anonymous account
    response = await handle_head_object(bucket_name, object_key, request, db)

    # Add anonymous access header
    response.headers["x-hippius-access-mode"] = "anon"
    return response
