from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Request
from fastapi import Response

from hippius_s3 import dependencies
from hippius_s3.api.s3.buckets.bucket_create_endpoint import handle_create_bucket
from hippius_s3.api.s3.buckets.bucket_delete_endpoint import handle_delete_bucket
from hippius_s3.api.s3.buckets.bucket_head_endpoint import handle_head_bucket
from hippius_s3.api.s3.buckets.bucket_lifecycle_endpoint import handle_get_bucket_lifecycle
from hippius_s3.api.s3.buckets.bucket_location_endpoint import handle_get_bucket_location
from hippius_s3.api.s3.buckets.bucket_policy_endpoint import get_bucket_policy as policy_get_bucket_policy
from hippius_s3.api.s3.buckets.bucket_tagging_endpoint import delete_bucket_tags as tags_delete_bucket_tags
from hippius_s3.api.s3.buckets.bucket_tagging_endpoint import get_bucket_tags as tags_get_bucket_tags
from hippius_s3.api.s3.buckets.list_buckets_endpoint import handle_list_buckets
from hippius_s3.api.s3.buckets.list_objects_endpoint import handle_list_objects
from hippius_s3.dependencies import RequestContext
from hippius_s3.dependencies import get_request_context


router = APIRouter()


@router.get("/", status_code=200)
async def list_buckets(
    ctx: RequestContext = Depends(get_request_context),
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
) -> Response:
    return await handle_list_buckets(ctx, db)


@router.get("/{bucket_name}", status_code=200)
async def get_bucket(
    bucket_name: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
) -> Response:
    if "location" in request.query_params:
        return await handle_get_bucket_location(bucket_name)
    if "tagging" in request.query_params:
        return await tags_get_bucket_tags(bucket_name, db, request.state.account.main_account)
    if "lifecycle" in request.query_params:
        return await handle_get_bucket_lifecycle(bucket_name, db, request.state.account.main_account)
    if "uploads" in request.query_params:
        from hippius_s3.api.s3.multipart import list_multipart_uploads

        return await list_multipart_uploads(bucket_name, request, db)
    if "policy" in request.query_params:
        return await policy_get_bucket_policy(bucket_name, db, request.state.account.main_account)
    # list objects
    ctx = get_request_context(request)
    return await handle_list_objects(bucket_name, ctx, db, request.query_params.get("prefix"))


@router.put("/{bucket_name}")
async def create_or_modify_bucket(
    bucket_name: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
) -> Response:
    # Delegate to the new comprehensive handler (supports create/tagging/lifecycle/policy)
    return await handle_create_bucket(bucket_name, request, db)


@router.delete("/{bucket_name}")
async def delete_bucket_tags_route(
    bucket_name: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
    redis_client: Any = Depends(dependencies.get_redis),
) -> Response:
    if "tagging" in request.query_params:
        return await tags_delete_bucket_tags(bucket_name, db, request.state.account.main_account)
    return await handle_delete_bucket(bucket_name, request, db, redis_client)


@router.head("/{bucket_name}", status_code=200)
async def head_bucket(
    bucket_name: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
) -> Response:
    return await handle_head_bucket(bucket_name, request, db)
