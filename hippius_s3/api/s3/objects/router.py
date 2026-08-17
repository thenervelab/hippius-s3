from __future__ import annotations

from typing import Any

import asyncpg
from fastapi import APIRouter
from fastapi import Depends
from fastapi import Request
from fastapi import Response

from hippius_s3 import dependencies
from hippius_s3.api.s3.acl_endpoints import get_object_acl
from hippius_s3.api.s3.acl_endpoints import invalid_canned_acl_response
from hippius_s3.api.s3.acl_endpoints import materialize_canned_object_acl
from hippius_s3.api.s3.acl_endpoints import put_object_acl
from hippius_s3.api.s3.multipart import abort_multipart_upload
from hippius_s3.api.s3.multipart import list_parts_internal
from hippius_s3.api.s3.multipart import upload_part
from hippius_s3.api.s3.objects.copy_object_endpoint import handle_copy_object
from hippius_s3.api.s3.objects.delete_object_endpoint import handle_delete_object
from hippius_s3.api.s3.objects.get_object_endpoint import handle_get_object
from hippius_s3.api.s3.objects.head_object_endpoint import handle_head_object
from hippius_s3.api.s3.objects.put_object_endpoint import handle_put_object
from hippius_s3.api.s3.objects.tagging_endpoint import delete_object_tags as tags_delete_object_tags
from hippius_s3.api.s3.objects.tagging_endpoint import get_object_tags as tags_get_object_tags
from hippius_s3.api.s3.objects.tagging_endpoint import set_object_tags as tags_set_object_tags
from hippius_s3.config import get_config
from hippius_s3.db_pool import acquire_with_timeout


router = APIRouter()
config = get_config()


@router.head("/{bucket_name}/{object_key:path}", status_code=200)
async def head_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    pool: asyncpg.Pool = Depends(dependencies.get_db_pool),
) -> Response:
    return await handle_head_object(bucket_name, object_key, request, pool)


@router.get("/{bucket_name}/{object_key:path}", status_code=200)
async def get_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    pool: asyncpg.Pool = Depends(dependencies.get_db_pool),
    redis_client: Any = Depends(dependencies.get_redis),
) -> Response:
    # Handle query variants by delegation
    if "acl" in request.query_params:
        return await get_object_acl(bucket_name, object_key, request)
    if "tagging" in request.query_params:
        async with pool.acquire() as conn:
            return await tags_get_object_tags(bucket_name, object_key, conn, request.state.main_account_id)
    if "uploadId" in request.query_params:
        async with pool.acquire() as conn:
            return await list_parts_internal(bucket_name, object_key, request, conn)
    return await handle_get_object(bucket_name, object_key, request, pool, redis_client)


@router.put("/{bucket_name}/{object_key:path}/", status_code=200, include_in_schema=False)
@router.put("/{bucket_name}/{object_key:path}", status_code=200)
async def put_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    pool: asyncpg.Pool = Depends(dependencies.get_db_pool),
    redis_client: Any = Depends(dependencies.get_redis),
) -> Response:
    if "acl" in request.query_params:
        return await put_object_acl(bucket_name, object_key, request)
    # Write-path ACL extras (formerly wrapped around every object PUT by the ?acl
    # dispatcher): reject an unknown canned ACL before the write, materialize the
    # object ACL after a successful write that carried x-amz-acl.
    x_amz_acl = request.headers.get("x-amz-acl")
    if (invalid := invalid_canned_acl_response(x_amz_acl)) is not None:
        return invalid

    upload_id = request.query_params.get("uploadId")
    part_number = request.query_params.get("partNumber")
    if upload_id and part_number:
        response = await upload_part(request, pool)
    elif "tagging" in request.query_params:
        async with pool.acquire() as conn:
            response = await tags_set_object_tags(bucket_name, object_key, request, conn, request.state.main_account_id)
    elif request.headers.get("x-amz-copy-source"):
        response = await handle_copy_object(bucket_name, object_key, request, pool, redis_client)
    else:
        response = await handle_put_object(bucket_name, object_key, request, pool, redis_client)

    if x_amz_acl and response.status_code == 200:
        await materialize_canned_object_acl(request, bucket_name, object_key, x_amz_acl)
    return response


@router.delete("/{bucket_name}/{object_key:path}", status_code=204)
async def delete_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    pool: asyncpg.Pool = Depends(dependencies.get_db_pool),
    redis_client: Any = Depends(dependencies.get_redis),
) -> Response:
    if "uploadId" in request.query_params:
        async with pool.acquire() as conn:
            return await abort_multipart_upload(bucket_name, object_key, request, conn)
    if "tagging" in request.query_params:
        async with pool.acquire() as conn:
            return await tags_delete_object_tags(bucket_name, object_key, conn, request.state.main_account_id)
    # Bound the acquire: the delete handler holds this connection across the
    # multipart_uploads cleanup, which can be a multi-second scan on large buckets.
    # A bare pool.acquire() would block indefinitely and pin a pool slot under load;
    # a timeout surfaces PoolAcquireTimeout -> 503 SlowDown via the global handler.
    async with acquire_with_timeout(pool, config.db_pool_acquire_timeout) as conn:
        return await handle_delete_object(bucket_name, object_key, request, conn, redis_client)
