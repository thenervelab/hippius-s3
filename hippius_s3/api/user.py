"""User API endpoints for frontend JSON responses."""

import base64
import logging
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Query
from fastapi.responses import JSONResponse
from hippius_sdk.substrate import SubstrateClient
from starlette import status

from hippius_s3.config import get_config
from hippius_s3.dependencies import get_postgres
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)
router = APIRouter(tags=["user"])
config = get_config()


@router.get("/list_buckets")
async def list_buckets(
    main_account_id: str = Query(..., description="Main account ID to list buckets for"),
    db=Depends(get_postgres),
) -> JSONResponse:
    """
    List all buckets owned by a specific main account.

    Returns JSON response with bucket information.
    """
    try:
        buckets = await db.fetch(
            get_query("list_user_buckets"),
            main_account_id,
        )

        bucket_list = [
            {
                "bucket_id": str(bucket["bucket_id"]),
                "bucket_name": bucket["bucket_name"],
                "created_at": bucket["created_at"].isoformat(),
                "is_public": bucket["is_public"],
                "tags": bucket["tags"] or {},
            }
            for bucket in buckets
        ]

        return JSONResponse(
            {
                "buckets": bucket_list,
                "count": len(bucket_list),
                "main_account_id": main_account_id,
            }
        )

    except Exception as e:
        logger.exception(f"Error listing buckets for {main_account_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list buckets",
        ) from e


@router.get("/get_bucket_location")
async def get_bucket_location(
    bucket_name: str = Query(..., description="Bucket name to get location for"),
    main_account_id: str = Query(..., description="Main account ID that owns the bucket"),
    db=Depends(get_postgres),
) -> JSONResponse:
    """
    Get bucket location information for a specific bucket.

    Returns JSON response with bucket details including location/region.
    """
    try:
        bucket = await db.fetchrow(
            get_query("get_bucket_by_name_and_owner"),
            bucket_name,
            main_account_id,
        )

        if not bucket:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Bucket '{bucket_name}' not found for account '{main_account_id}'",
            )

        return JSONResponse(
            {
                "bucket_id": str(bucket["bucket_id"]),
                "bucket_name": bucket["bucket_name"],
                "created_at": bucket["created_at"].isoformat(),
                "is_public": bucket["is_public"],
                "tags": bucket["tags"] or {},
                "main_account_id": bucket["main_account_id"],
                "location": "decentralized",  # Default S3 region
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error getting bucket location for {bucket_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get bucket location",
        ) from e


@router.get("/credits")
async def credits(
    b64_subaccount_seed_phrase: str = Query(..., description="Subaccount seed phrase in base64"),
) -> JSONResponse:
    try:
        subaccount_seed = base64.b64decode(b64_subaccount_seed_phrase).decode()

        substrate_client = SubstrateClient(
            url=config.substrate_url,
        )
        substrate_client.connect(seed_phrase=subaccount_seed)

        main_account = substrate_client.query_sub_account(
            substrate_client._account_address,
            seed_phrase=subaccount_seed,
        )

        if not main_account:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="The seed phrase you passed in belongs to a main account, please pass in a subaccount seed phrase instead",
            )

        remaining_credits = await substrate_client.get_free_credits(
            account_address=main_account,
        )

        return JSONResponse(
            {
                "credits": remaining_credits,
            }
        )
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Provided value is not a valid seed phrase",
        ) from None


@router.get("/list_objects")
async def list_objects(
    bucket_name: str = Query(..., description="Bucket name to list objects from"),
    main_account_id: str = Query(..., description="Main account ID that owns the bucket"),
    prefix: Optional[str] = Query(None, description="Object key prefix filter"),
    max_keys: int = Query(1000, description="Maximum number of objects to return"),
    db=Depends(get_postgres),
) -> JSONResponse:
    """
    List objects in a bucket owned by a specific main account.

    Returns JSON response with object information.
    """
    try:
        # First verify the bucket exists and is owned by the main account
        bucket = await db.fetchrow(
            get_query("get_bucket_by_name_and_owner"),
            bucket_name,
            main_account_id,
        )

        if not bucket:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Bucket '{bucket_name}' not found for account '{main_account_id}'",
            )

        # List objects in the bucket
        objects = await db.fetch(
            get_query("list_objects"),
            bucket["bucket_id"],
            prefix,
        )

        # Limit results to max_keys
        if len(objects) > max_keys:
            objects = objects[:max_keys]
            truncated = True
        else:
            truncated = False

        object_list = [
            {
                "object_id": str(obj["object_id"]),
                "object_key": obj["object_key"],
                "ipfs_cid": obj["ipfs_cid"],
                "size_bytes": obj["size_bytes"],
                "content_type": obj["content_type"],
                "created_at": obj["created_at"].isoformat(),
            }
            for obj in objects
        ]

        return JSONResponse(
            {
                "bucket_name": bucket_name,
                "bucket_id": str(bucket["bucket_id"]),
                "main_account_id": main_account_id,
                "objects": object_list,
                "count": len(object_list),
                "truncated": truncated,
                "prefix": prefix,
                "max_keys": max_keys,
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error listing objects for bucket {bucket_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list objects",
        ) from e
