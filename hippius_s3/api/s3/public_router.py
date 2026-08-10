from __future__ import annotations

import logging
from typing import Any

import asyncpg
from fastapi import APIRouter
from fastapi import Depends
from fastapi import Request
from fastapi import Response

from hippius_s3 import dependencies
from hippius_s3.api.s3 import errors as s3_errors
from hippius_s3.api.s3.objects.get_object_endpoint import handle_get_object
from hippius_s3.api.s3.objects.head_object_endpoint import handle_head_object
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)

router = APIRouter(tags=["public-s3"])


async def _bucket_is_public(pool: asyncpg.Pool, bucket_name: str) -> bool:
    """Whether this bucket is marked public. Missing or deleted counts as not public.

    These two routes are the only api entry point the gateway does not authorize. It parses
    `/public/<bucket>/<key>` as bucket `public` with key `<bucket>/<key>`, and `public` is a
    reserved segment so no such bucket can exist — which puts the ACL middleware on its
    "bucket not found, let the backend return the proper S3 error" path, where it performs no
    permission check. `handle_get_object` in turn documents that the gateway has already checked
    permissions, and its query is keyed on bucket name and object key alone. So the publicness
    the route's own name asserts has to be established right here, before delegating.
    """
    row = await pool.fetchrow(get_query("get_bucket_is_public"), bucket_name)
    return bool(row and row["is_public"])


@router.get("/public/{bucket_name}/{object_key:path}", status_code=200)
async def get_public_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    pool: asyncpg.Pool = Depends(dependencies.get_db_pool),
    redis_client: Any = Depends(dependencies.get_redis),
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
    if not await _bucket_is_public(pool, bucket_name):
        # NoSuchKey, and deliberately not AccessDenied: a private bucket must be indistinguishable
        # from an absent one on an unauthenticated route, or the 403 itself confirms the object.
        return s3_errors.s3_error_response(
            code="NoSuchKey",
            message="The specified key does not exist.",
            status_code=404,
        )

    # Call the regular get_object handler with anonymous account
    response = await handle_get_object(bucket_name, object_key, request, pool, redis_client)

    # Add anonymous access header
    response.headers["x-hippius-access-mode"] = "anon"
    return response


@router.head("/public/{bucket_name}/{object_key:path}", status_code=200)
async def head_public_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    pool: asyncpg.Pool = Depends(dependencies.get_db_pool),
) -> Response:
    """Anonymous HEAD object endpoint for public buckets."""
    # Whitelist: only allow HEAD with no special query parameters
    if request.query_params:
        return s3_errors.s3_error_response(
            code="SignatureDoesNotMatch",
            message="The request signature we calculated does not match the signature you provided",
            status_code=403,
        )
    if not await _bucket_is_public(pool, bucket_name):
        return s3_errors.s3_error_response(
            code="NoSuchKey",
            message="The specified key does not exist.",
            status_code=404,
        )

    # Call the regular head_object handler with anonymous account
    response = await handle_head_object(bucket_name, object_key, request, pool)

    # Add anonymous access header
    response.headers["x-hippius-access-mode"] = "anon"
    return response
