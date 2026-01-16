from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Request
from fastapi import Response

from hippius_s3 import dependencies
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


router = APIRouter()


@router.head("/{bucket_name}/{object_key:path}", status_code=200)
async def head_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
) -> Response:
    return await handle_head_object(bucket_name, object_key, request, db)


@router.get("/{bucket_name}/{object_key:path}", status_code=200)
async def get_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
    redis_client: Any = Depends(dependencies.get_redis),
) -> Response:
    # Handle query variants by delegation
    if "tagging" in request.query_params:
        return await tags_get_object_tags(
            bucket_name, object_key, db, request.state.seed_phrase, request.state.account.main_account
        )
    if "uploadId" in request.query_params:
        return await list_parts_internal(bucket_name, object_key, request, db)
    return await handle_get_object(bucket_name, object_key, request, db, redis_client)


@router.put("/{bucket_name}/{object_key:path}/", status_code=200, include_in_schema=False)
@router.put("/{bucket_name}/{object_key:path}", status_code=200)
async def put_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
    redis_client: Any = Depends(dependencies.get_redis),
) -> Response:
    upload_id = request.query_params.get("uploadId")
    part_number = request.query_params.get("partNumber")
    if upload_id and part_number:
        return await upload_part(request, db)
    if "tagging" in request.query_params:
        return await tags_set_object_tags(
            bucket_name, object_key, request, db, request.state.seed_phrase, request.state.account.main_account
        )
    if request.headers.get("x-amz-copy-source"):
        return await handle_copy_object(bucket_name, object_key, request, db, redis_client)
    return await handle_put_object(bucket_name, object_key, request, db, redis_client)


@router.delete("/{bucket_name}/{object_key:path}", status_code=204)
async def delete_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: dependencies.DBConnection = Depends(dependencies.get_postgres),
    redis_client: Any = Depends(dependencies.get_redis),
) -> Response:
    if "uploadId" in request.query_params:
        return await abort_multipart_upload(bucket_name, object_key, request, db)
    if "tagging" in request.query_params:
        return await tags_delete_object_tags(
            bucket_name, object_key, db, request.state.seed_phrase, request.state.account.main_account
        )
    return await handle_delete_object(bucket_name, object_key, request, db, redis_client)
