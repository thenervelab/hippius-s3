"""User API endpoints for frontend JSON responses."""

import base64
import hashlib
import hmac
import logging
from datetime import datetime
from datetime import timezone
from typing import Optional
from urllib.parse import quote
from urllib.parse import urlencode

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
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


@router.get("/presign_url")
async def presign_url(
    request: Request,
    method: str = Query("GET", description="HTTP method to presign: GET|PUT|HEAD|DELETE"),
    bucket_name: str = Query(..., description="Bucket name"),
    object_key: str = Query(..., description="Object key (path)"),
    expires: int = Query(900, description="Expiry in seconds, max 7 days"),
) -> JSONResponse:
    """Generate an AWS SigV4 presigned URL for S3-compatible access.

    This endpoint requires SigV4 on the request so the server derives the seed
    from the Authorization header; no seed phrase is accepted via parameters.
    """
    try:
        # Validate method
        method = method.upper()
        if method not in {"GET", "PUT", "HEAD", "DELETE"}:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unsupported method")

        # Determine seed phrase to sign with
        seed_phrase = getattr(request.state, "seed_phrase", "")
        if not seed_phrase:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Authentication required")

        # Cap expiry to AWS maximum (7 days)
        if expires <= 0:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="expires must be > 0")
        max_expires = 7 * 24 * 3600
        if expires > max_expires:
            expires = max_expires

        # Prepare SigV4 elements
        host = (
            request.headers.get("x-forwarded-host")
            or request.headers.get("x-original-host")
            or request.headers.get("host", "localhost:8000")
        )
        scheme = "https" if request.headers.get("x-forwarded-proto", "http") == "https" else "http"
        region = config.validator_region
        service = "s3"
        now = datetime.now(timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_scope = now.strftime("%Y%m%d")

        # Access key is base64-encoded seed phrase, like our SigV4 header flow
        access_key = base64.b64encode(seed_phrase.encode()).decode()
        credential = f"{access_key}/{date_scope}/{region}/{service}/aws4_request"

        # Signed headers for presign: host only is sufficient for GET/PUT/HEAD/DELETE
        signed_headers = "host"

        # Build canonical request pieces
        # Encode per S3 SigV4 rules (RFC3986). Keep '/', don't encode -_.~
        path = f"/{bucket_name}/{quote(object_key, safe='/-_.~')}"
        q_params = {
            "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
            "X-Amz-Credential": credential,
            "X-Amz-Date": amz_date,
            "X-Amz-Expires": str(expires),
            "X-Amz-SignedHeaders": signed_headers,
        }
        canonical_qs = urlencode(sorted(q_params.items()), quote_via=quote, safe="-_.~")
        canonical_headers = f"host:{host}\n"
        payload_hash = "UNSIGNED-PAYLOAD"
        canonical_request = "\n".join([method, path, canonical_qs, canonical_headers, "", signed_headers, payload_hash])

        # String to sign
        credential_scope = f"{date_scope}/{region}/{service}/aws4_request"
        hashed_canonical_request = hashlib.sha256(canonical_request.encode()).hexdigest()
        string_to_sign = f"AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{hashed_canonical_request}"

        # Derive signing key from seed phrase
        def _sign(key: bytes, msg: str) -> bytes:
            return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

        k_secret = ("AWS4" + seed_phrase).encode("utf-8")
        k_date = _sign(k_secret, date_scope)
        k_region = _sign(k_date, region)
        k_service = _sign(k_region, service)
        k_signing = _sign(k_service, "aws4_request")
        signature = hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

        # Final presigned URL
        q_params["X-Amz-Signature"] = signature
        # Align final presign URL encoding with canonicalization for parity
        final_qs = urlencode(sorted(q_params.items()), quote_via=quote, safe="-_.~")
        url = f"{scheme}://{host}{path}?{final_qs}"

        return JSONResponse(
            {
                "url": url,
                "method": method,
                "expires_in": expires,
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error generating presigned URL for {bucket_name}/{object_key}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to generate presigned URL"
        ) from None


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
