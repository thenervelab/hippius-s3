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
from hippius_s3.api.s3.errors import s3_error_response
from hippius_s3.api.s3.multipart import abort_multipart_upload
from hippius_s3.api.s3.multipart import list_parts_internal
from hippius_s3.api.s3.multipart import upload_part
from hippius_s3.api.s3.object_lock_guard import maybe_object_lock_not_implemented_response
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


def _reject_version_id(request: Request, object_key: str, subresource: str) -> Response | None:
    """501 when ?versionId is combined with ?acl or ?tagging, instead of silently ignoring it.

    In AWS both subresources are per-version. Here they are not: tags live in
    object_versions.metadata and the handlers resolve the CURRENT version, so a versionId in the
    query string had no effect on which row was read or written. A read returned the current
    version's tags labelled as an old version's, and — the reason this is a 501 rather than a
    documentation note — `PUT ?tagging&versionId=N` wrote the tags onto the LIVE version and
    answered 200. That is the same silent-write-to-the-wrong-version shape as the
    `DELETE ?versionId` data-loss bug this branch exists to fix.

    Rejecting is the honest answer until tags and ACLs are stored per version. It is
    unconditional: whether the bucket has versioning enabled makes no difference to the fact that
    we cannot honour the parameter.
    """
    if "versionId" not in request.query_params:
        return None
    return s3_error_response(
        "NotImplemented",
        f"versionId is not supported on ?{subresource}: tags and ACLs are only addressable on the current version",
        status_code=501,
        Key=object_key,
        VersionId=request.query_params["versionId"],
    )


@router.head("/{bucket_name}/{object_key:path}", status_code=200)
async def head_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    pool: asyncpg.Pool = Depends(dependencies.get_db_pool),
) -> Response:
    object_lock_response = maybe_object_lock_not_implemented_response(request)
    if object_lock_response is not None:
        return object_lock_response
    return await handle_head_object(bucket_name, object_key, request, pool)


@router.get("/{bucket_name}/{object_key:path}", status_code=200)
async def get_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    pool: asyncpg.Pool = Depends(dependencies.get_db_pool),
    redis_client: Any = Depends(dependencies.get_redis),
) -> Response:
    object_lock_response = maybe_object_lock_not_implemented_response(request)
    if object_lock_response is not None:
        return object_lock_response
    # Handle query variants by delegation
    if "acl" in request.query_params:
        if (rejected := _reject_version_id(request, object_key, "acl")) is not None:
            return rejected
        return await get_object_acl(bucket_name, object_key, request)
    if "tagging" in request.query_params:
        if (rejected := _reject_version_id(request, object_key, "tagging")) is not None:
            return rejected
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
    object_lock_response = maybe_object_lock_not_implemented_response(request)
    if object_lock_response is not None:
        return object_lock_response
    if "acl" in request.query_params:
        if (rejected := _reject_version_id(request, object_key, "acl")) is not None:
            return rejected
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
        if (rejected := _reject_version_id(request, object_key, "tagging")) is not None:
            return rejected
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
    object_lock_response = maybe_object_lock_not_implemented_response(request)
    if object_lock_response is not None:
        return object_lock_response
    if "uploadId" in request.query_params:
        async with pool.acquire() as conn:
            return await abort_multipart_upload(bucket_name, object_key, request, conn)
    if "tagging" in request.query_params:
        if (rejected := _reject_version_id(request, object_key, "tagging")) is not None:
            return rejected
        async with pool.acquire() as conn:
            return await tags_delete_object_tags(bucket_name, object_key, conn, request.state.main_account_id)
    # Bound the acquire: the delete handler holds this connection across the
    # multipart_uploads cleanup, which can be a multi-second scan on large buckets.
    # A bare pool.acquire() would block indefinitely and pin a pool slot under load;
    # a timeout surfaces PoolAcquireTimeout -> 503 SlowDown via the global handler.
    async with acquire_with_timeout(pool, config.db_pool_acquire_timeout) as conn:
        return await handle_delete_object(bucket_name, object_key, request, conn, redis_client)
