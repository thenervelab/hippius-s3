"""Credit checking middleware for all S3 operations."""

import asyncio
import base64
import json
import logging
import os
import re
from typing import Callable

from fastapi import Request
from fastapi import Response
from hippius_sdk.substrate import SubstrateClient
from mnemonic import Mnemonic
from pydantic import BaseModel
from starlette import status

from hippius_s3.api.s3.endpoints import create_xml_error_response
from hippius_s3.api.s3.errors import s3_error_response
from hippius_s3.config import get_config
from hippius_s3.dependencies import get_redis_accounts


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


async def get_subaccount_id_from_seed(seed_phrase: str, substrate_url: str) -> str | None:
    """Get subaccount ID from seed phrase using Substrate client in a separate thread.

    Returns:
        str: The subaccount ID if this is a valid subaccount seed phrase
        None: If this is a main account seed phrase (not allowed for S3)
    """

    def _connect_substrate():
        client = SubstrateClient(
            seed_phrase=seed_phrase,
            url=substrate_url,
        )
        client.connect(seed_phrase=seed_phrase)
        if client.is_main_account(
            account_id=client._account_address,
            seed_phrase=seed_phrase,
        ):
            return None
        return client._account_address

    # Run the blocking Substrate connection in a thread pool
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _connect_substrate)


class MainAccountError(Exception):
    pass


class InvalidSeedPhraseError(Exception):
    pass


async def fetch_account(
    seed_phrase: str,
    request: Request,
) -> HippiusAccount:
    """Fetch account information with full caching logic handled internally."""
    # Validate seed phrase format before any processing
    m = Mnemonic("english")
    if not m.check(seed_phrase):
        raise InvalidSeedPhraseError("Invalid seed phrase format")

    redis_accounts_client = get_redis_accounts(request)

    # Cache seed phrase to subaccount_id mapping to avoid substrate connection
    seed_cache_key = f"hippius_seed_to_subaccount:{hash(seed_phrase)}"
    cached_subaccount_id = await redis_accounts_client.get(seed_cache_key)

    if cached_subaccount_id:
        cached_value = cached_subaccount_id.decode()
        if cached_value == "False":
            # This seed phrase is for a main account, not allowed for S3
            raise MainAccountError("Please use a Hippius subaccount seed phrase for S3 operations")
        subaccount_id = cached_value
    else:
        # Only connect to substrate if we don't have the subaccount_id cached
        # Run substrate connection in a separate thread to avoid blocking
        subaccount_id = await get_subaccount_id_from_seed(
            seed_phrase,
            config.substrate_url,
        )

        if subaccount_id is None:
            # This is a main account seed phrase, cache False to avoid future lookups
            await redis_accounts_client.set(seed_cache_key, "False")
            logger.error("Cached main account detection (False)")
            raise MainAccountError("Please use a Hippius subaccount seed phrase for S3 operations")

        # Cache the seed phrase to subaccount_id mapping for 2 hour
        await redis_accounts_client.set(seed_cache_key, subaccount_id)

    # Try to get cached subaccount data
    cache_key = f"hippius_subaccount_cache:{subaccount_id}"
    cached_data = await redis_accounts_client.get(cache_key)

    if cached_data:
        data = json.loads(cached_data)

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

    # If we reach here, account is not in cache - raise exception
    raise BadAccount("Your account does not exist")


async def check_credit_for_all_operations(request: Request, call_next: Callable) -> Response:
    """
    Middleware to check if the account has enough credit for any S3 operation.

    This middleware intercepts all requests and creates the account object.
    For operations that modify state (PUT, POST, DELETE), it also checks if
    the account has enough credit. If not, it returns a 402 Payment Required response.

    Read operations (GET, HEAD) get the account object but skip credit checks.
    """
    path = request.url.path

    # Test bypass: short-circuit credit and substrate/redis access entirely
    if os.getenv("HIPPIUS_BYPASS_CREDIT_CHECK", "false").lower() == "true":
        # Ensure downstream code has an account object
        from contextlib import suppress

        with suppress(Exception):
            request.state.account = HippiusAccount(
                seed=getattr(request.state, "seed_phrase", ""),
                id="bypass",
                main_account="bypass",
                has_credits=True,
                upload=True,
                delete=True,
            )
        return await call_next(request)

    # Skip credit checks for frontend user endpoints and docs
    if path.startswith("/user/") or path in ["/docs", "/openapi.json", "/redoc"] or path.startswith("/docs/"):
        return await call_next(request)

    # Check if we have a seed phrase in request.state (set by HMAC middleware)
    if hasattr(request.state, "seed_phrase"):
        seed_phrase = request.state.seed_phrase

        try:
            request.state.account = await fetch_account(
                seed_phrase,
                request=request,
            )

            # Only check permissions and credits for operations that modify state
            if request.method in ["PUT", "POST", "DELETE"]:
                logger.debug(f"Checking credit for {request.method} operation: {path}")

                if not request.state.account.delete and request.method == "DELETE":
                    raise BadAccount("This account does not have DELETE permissions")

                # Bypass for testing if enabled
                if os.getenv("HIPPIUS_BYPASS_CREDIT_CHECK", "false").lower() == "true":
                    logger.info("Credit check bypassed for testing")
                elif not request.state.account.has_credits:
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
        except MainAccountError as e:
            return s3_error_response(
                code="InvalidAccessKeyId",
                message=str(e),
                status_code=status.HTTP_403_FORBIDDEN,
            )
        except InvalidSeedPhraseError:
            return create_xml_error_response(
                code="InvalidAccessKeyId",
                message="The AWS Access Key Id you provided does not exist in our records.",
                status_code=403,
            )
        except BadAccount as e:
            return s3_error_response(
                code="AccountVerificationError",
                message=str(e),
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        except Exception as e:
            logger.exception(f"Error in account verification for {request.method} {path}: {e}")

            return s3_error_response(
                code="AccountVerificationError",
                message="Something went wrong when verifying your account. Please try again later.",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
    # Continue with the request
    response: Response = await call_next(request)
    return response
