"""Account verification and credit checking middleware for the gateway."""

import hashlib
import logging
import re
from typing import Callable

from fastapi import Request
from fastapi import Response
from starlette import status

from gateway.config import get_config
from gateway.services.account_service import BadAccount
from gateway.services.account_service import InvalidSeedPhraseError
from gateway.services.account_service import MainAccountError
from gateway.services.account_service import fetch_account_by_main_address
from gateway.services.auth_cache import cached_seed_auth
from gateway.utils.errors import s3_error_response
from hippius_s3.models.account import HippiusAccount
from hippius_s3.services.arion_service import CanUploadResponse
from hippius_s3.services.ray_id_service import get_logger_with_ray_id


config = get_config()


async def _check_can_upload(request: Request, logger: logging.Logger | logging.LoggerAdapter) -> Response | None:  # type: ignore[type-arg]
    """
    Call Arion's can_upload endpoint for PUT/POST requests.

    Returns an error Response if the upload is not allowed, or None if it should proceed.
    """
    if request.method not in ("PUT", "POST"):
        return None

    # AWS CLI v2+ uses chunked transfer encoding and sends the actual file size
    # in x-amz-decoded-content-length instead of Content-Length
    content_length = int(
        request.headers.get("x-amz-decoded-content-length")
        or request.headers.get("content-length")
        or "0"
    )
    main_account = request.state.account.main_account
    arion_client = request.app.state.arion_client

    response: CanUploadResponse = await arion_client.can_upload(main_account, content_length)
    if not response.result:
        error_message = response.error or "Upload not permitted by billing service"
        logger.warning(f"can_upload denied for {main_account}: {error_message}")
        return s3_error_response(
            code="UploadNotPermitted",
            message=error_message,
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
        )

    return None


async def account_middleware(request: Request, call_next: Callable) -> Response:
    """
    Middleware to check if the account has enough credit for any S3 operation.

    This middleware intercepts all requests and creates the account object.
    For operations that modify state (PUT, POST, DELETE), it also checks if
    the account has enough credit. If not, it returns a 402 Payment Required response.

    Read operations (GET, HEAD) get the account object but skip credit checks.
    """
    ray_id = getattr(request.state, "ray_id", "no-ray-id")
    logger = get_logger_with_ray_id(__name__, ray_id)

    path = request.url.path

    # Test bypass: short-circuit credit and substrate/redis access entirely
    if config.bypass_credit_check:
        auth_method = getattr(request.state, "auth_method", None)

        if auth_method == "access_key":
            account_address = getattr(request.state, "account_address", "anonymous")
            request.state.account_id = account_address
            request.state.account = HippiusAccount(
                id=account_address,
                main_account=account_address,
                has_credits=True,
                upload=True,
                delete=True,
            )
        elif hasattr(request.state, "seed_phrase"):
            seed_phrase = request.state.seed_phrase
            seed_hash = hashlib.sha256(seed_phrase.encode()).digest()
            account_id = seed_hash.hex()
            request.state.account_id = account_id
            request.state.account = HippiusAccount(
                id=account_id,
                main_account=account_id,
                has_credits=True,
                upload=True,
                delete=True,
            )
        else:
            account_id = "anonymous"
            request.state.account_id = account_id
            request.state.account = HippiusAccount(
                id=account_id,
                main_account=account_id,
                has_credits=True,
                upload=True,
                delete=True,
            )

        response: Response = await call_next(request)
        return response

    # Skip credit checks for frontend user endpoints, docs, and metrics
    if (
        path.startswith("/user/")
        or path in ["/docs", "/openapi.json", "/redoc", "/metrics"]
        or path.startswith("/docs/")
    ):
        resp: Response = await call_next(request)
        return resp

    auth_method = getattr(request.state, "auth_method", None)

    if auth_method == "access_key":
        account_address = request.state.account_address

        try:
            redis_accounts_client = request.app.state.redis_accounts
            request.state.account = await fetch_account_by_main_address(
                account_address,
                redis_accounts_client,
                config.substrate_url,
            )
            request.state.account_id = account_address

            if request.method in ["PUT", "POST", "DELETE"]:
                logger.debug(f"Checking credit for {request.method} operation: {path}")

                if not request.state.account.has_credits:
                    logger.warning(f"Access key account lacks credits: {account_address}")
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

                can_upload_error = await _check_can_upload(request, logger)
                if can_upload_error is not None:
                    return can_upload_error
        except Exception as e:
            logger.exception(f"Error in access key account verification: {e}")
            return s3_error_response(
                code="AccountVerificationError",
                message="Something went wrong when verifying your account. Please try again later.",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

    elif hasattr(request.state, "seed_phrase"):
        seed_phrase = request.state.seed_phrase

        try:
            redis_accounts_client = request.app.state.redis_accounts
            redis_client = request.app.state.redis_client
            request.state.account = await cached_seed_auth(
                seed_phrase,
                redis_client,
                redis_accounts_client,
                config.substrate_url,
            )

            request.state.account_id = request.state.account.main_account

            # Only check permissions and credits for operations that modify state
            if request.method in ["PUT", "POST", "DELETE"]:
                logger.debug(f"Checking credit for {request.method} operation: {path}")

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

                can_upload_error = await _check_can_upload(request, logger)
                if can_upload_error is not None:
                    return can_upload_error
        except MainAccountError as e:
            return s3_error_response(
                code="InvalidAccessKeyId",
                message=str(e),
                status_code=status.HTTP_403_FORBIDDEN,
            )
        except InvalidSeedPhraseError:
            return s3_error_response(
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
    else:
        account_id = "anonymous"
        request.state.account_id = account_id
        request.state.account = HippiusAccount(
            id=account_id,
            main_account=account_id,
            has_credits=True,
            upload=False,
            delete=False,
        )
    # Continue with the request
    return await call_next(request)
