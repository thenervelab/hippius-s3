"""Credit checking middleware for all S3 operations."""

import base64
import json
import logging
import re
from typing import Callable
from typing import Optional

from fastapi import Request
from fastapi import Response
from hippius_sdk.substrate import SubstrateClient
from pydantic import BaseModel
from starlette import status

from hippius_s3.api.s3.errors import s3_error_response
from hippius_s3.config import get_config
from hippius_s3.dependencies import check_account_has_credit
from hippius_s3.dependencies import get_redis


config = get_config()

logger = logging.getLogger(__name__)


class HippiusAccount(BaseModel):
    seed: str
    id: str
    main_account: str
    upload: bool
    delete: bool
    has_credits: bool

    @property
    def as_b64(self):
        return base64.b64decode(self.seed.encode("utf-8")).decode("utf-8")


class BadAccount(Exception):
    pass


async def fetch_account_from_cache(
    seed_phrase: str,
    request: Request,
) -> Optional[HippiusAccount]:
    """
    Try to fetch account data from Redis cache first.

    Returns:
        HippiusAccount if found in cache, None if not found or cache miss
    """
    redis_client = get_redis(request)

    # Cache seed phrase to subaccount_id mapping to avoid substrate connection
    seed_cache_key = f"hippius_seed_to_subaccount:{hash(seed_phrase)}"
    cached_subaccount_id = await redis_client.get(seed_cache_key)

    if cached_subaccount_id:
        subaccount_id = cached_subaccount_id.decode()
        logger.debug(f"Cache hit for seed phrase hash -> {subaccount_id}")
    else:
        # Only connect to substrate if we don't have the subaccount_id cached
        client = SubstrateClient(
            seed_phrase=seed_phrase,
            url=config.substrate_url,
        )
        client.connect(seed_phrase=seed_phrase)
        subaccount_id = client._account_address

        # Cache the seed phrase to subaccount_id mapping for 2 hour
        await redis_client.setex(seed_cache_key, 7200, subaccount_id)
        logger.debug(f"Cached seed phrase hash -> {subaccount_id}")

    # Try to get cached subaccount data
    cache_key = f"hippius_subaccount_cache:{subaccount_id}"
    cached_data = await redis_client.get(cache_key)

    if cached_data:
        data = json.loads(cached_data)
        logger.debug(f"Cache hit for subaccount {subaccount_id}")

        # Extract role information
        role = data.get("role", "Unknown")
        upload = role in ["Upload", "UploadDelete"]
        delete = role == "UploadDelete"
        has_credits = data.get("has_credits", False)
        main_account_id = data.get("main_account_id")

        return HippiusAccount(
            seed=seed_phrase,
            id=subaccount_id,
            main_account=main_account_id,
            has_credits=has_credits,
            upload=upload,
            delete=delete,
        )

    logger.debug(f"Cache miss for subaccount {subaccount_id}")
    return None


async def fetch_account(
    seed_phrase: str,
    substrate_url: str,
    request: Request,
) -> HippiusAccount:
    # First try to get from cache
    cached_account = await fetch_account_from_cache(seed_phrase, request)
    if cached_account:
        return cached_account

    # Get or cache the subaccount_id from seed phrase
    redis_client = get_redis(request)
    seed_cache_key = f"hippius_seed_to_subaccount:{hash(seed_phrase)}"
    cached_subaccount_id = await redis_client.get(seed_cache_key)

    if cached_subaccount_id:
        subaccount_id = cached_subaccount_id.decode()
        logger.debug(f"Using cached subaccount_id: {subaccount_id}")

        # Create client for substrate queries but we already know the address
        client = SubstrateClient(
            seed_phrase=seed_phrase,
            url=substrate_url,
        )
        client.connect(seed_phrase=seed_phrase)
        # Override the address to use our cached one
        client._account_address = subaccount_id
    else:
        client = SubstrateClient(
            seed_phrase=seed_phrase,
            url=substrate_url,
        )
        client.connect(seed_phrase=seed_phrase)
        subaccount_id = client._account_address

        # Cache for future use
        await redis_client.setex(seed_cache_key, 3600, subaccount_id)
        logger.debug(f"Cached new subaccount_id: {subaccount_id}")

    # If we reach here, it means we have a cache miss for the full subaccount data
    # This should be rare since the cacher runs every 5 minutes
    # As a fallback, we need to query substrate for the missing data

    main_account = client.query_sub_account(
        subaccount_id,
        seed_phrase=seed_phrase,
    )
    if not main_account:
        raise BadAccount(f"{subaccount_id} is a main account, please use subaccounts for s3 interactions")

    roles = client.get_account_roles(
        subaccount_id,
        seed_phrase=seed_phrase,
    )

    # Check if we have main account credits cached
    main_cache_key = f"hippius_main_account_credits:{main_account}"
    cached_credits = await redis_client.get(main_cache_key)

    if cached_credits:
        credits_data = json.loads(cached_credits)
        has_credits = credits_data.get("has_credits", False)
        logger.debug(f"Using cached credits for main account {main_account}: {has_credits}")
    else:
        # Fall back to substrate query for credits
        has_credits = await check_account_has_credit(
            subaccount=subaccount_id,
            main_account=main_account,
            seed_phrase=seed_phrase,
            substrate_url=config.substrate_url,
        )
        logger.debug(f"Fetched credits from substrate for {main_account}: {has_credits}")

    logger.warning(f"Cache miss fallback used for subaccount={subaccount_id} {main_account=} {has_credits=}")

    return HippiusAccount(
        seed=seed_phrase,
        id=subaccount_id,
        main_account=main_account,
        has_credits=has_credits,
        upload="Upload" in roles,
        delete="Delete" in roles,
    )


async def check_credit_for_all_operations(request: Request, call_next: Callable) -> Response:
    """
    Middleware to check if the account has enough credit for any S3 operation.

    This middleware intercepts all requests and creates the account object.
    For operations that modify state (PUT, POST, DELETE), it also checks if
    the account has enough credit. If not, it returns a 402 Payment Required response.

    Read operations (GET, HEAD) get the account object but skip credit checks.
    """
    path = request.url.path

    # Skip credit checks for frontend user endpoints and docs
    if path.startswith("/user/") or path in ["/docs", "/openapi.json", "/redoc"] or path.startswith("/docs/"):
        return await call_next(request)

    # Check if we have a seed phrase in request.state (set by HMAC middleware)
    if hasattr(request.state, "seed_phrase"):
        seed_phrase = request.state.seed_phrase

        try:
            request.state.account = await fetch_account(
                seed_phrase,
                substrate_url=config.substrate_url,
                request=request,
            )

            # Only check permissions and credits for operations that modify state
            if request.method in ["PUT", "POST", "DELETE"]:
                logger.info(f"Checking credit for {request.method} operation: {path}")

                if not request.state.account.delete and request.method == "DELETE":
                    raise BadAccount("This account does not have DELETE permissions")

                if not request.state.account.has_credits:
                    logger.warning(f"Account does not have credit for {request.method} operation: {path}")

                    # Extract bucket name for better error response
                    bucket_name = None
                    bucket_match = re.match(r"^/([^/]+)", path)
                    if bucket_match:
                        bucket_name = bucket_match.group(1)

                    return s3_error_response(
                        code="InsufficientAccountCredit",
                        message="The account does not have sufficient credit to perform this operation",
                        status_code=status.HTTP_402_PAYMENT_REQUIRED,
                        BucketName=bucket_name if bucket_name else "",
                    )
        except BadAccount as e:
            return s3_error_response(
                code="AccountVerificationError",
                message=str(e),
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        except Exception as e:
            logger.exception(f"Error in account verification for {request.method} {path}: {e}")

            # Extract bucket name for better error response
            bucket_name = None
            bucket_match = re.match(r"^/([^/]+)", path)
            if bucket_match:
                bucket_name = bucket_match.group(1)

            return s3_error_response(
                code="AccountVerificationError",
                message="Something went wrong when verifying your account. Please try again later.",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                BucketName=bucket_name if bucket_name else "",
            )

    # Continue with the request
    response: Response = await call_next(request)
    return response
