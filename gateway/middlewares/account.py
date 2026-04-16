"""Account verification and credit checking middleware for the gateway."""

import logging
import re
from typing import Callable

from fastapi import Request
from fastapi import Response
from starlette import status

from gateway.config import get_config
from gateway.utils.errors import s3_error_response
from hippius_s3.models.account import HippiusAccount
from hippius_s3.services.arion_service import CanUploadResponse
from hippius_s3.services.ray_id_service import get_logger_with_ray_id


config = get_config()


async def _check_can_upload(
    request: Request,
    logger: logging.Logger | logging.LoggerAdapter,
) -> Response | None:
    """
    Call Arion's can_upload endpoint for PUT/POST requests.

    Returns an error Response if the upload is not allowed, or None if it should proceed.
    Uses Redis cache to avoid redundant Arion calls during multipart uploads.
    """
    if request.method not in ("PUT", "POST"):
        return None

    # AWS CLI v2+ uses chunked transfer encoding and sends the actual file size
    # in x-amz-decoded-content-length instead of Content-Length
    content_length = int(
        request.headers.get("x-amz-decoded-content-length") or request.headers.get("content-length") or "0"
    )
    main_account = request.state.account.main_account
    arion_client = request.app.state.arion_client
    redis_accounts = request.app.state.redis_accounts

    # Check cache before calling Arion (avoids rate limiting on multipart uploads)
    cache_key = f"can_upload:{main_account}"
    cached = await redis_accounts.get(cache_key)
    if cached == b"1":
        logger.debug(f"can_upload cache hit for {main_account}, skipping Arion call")
        return None

    response: CanUploadResponse = await arion_client.can_upload(main_account, content_length)
    logger.info(
        f"can_upload billing check for {main_account}: size_bytes={content_length}, result={response.result}, error={response.error}"
    )
    if not response.result:
        error_message = response.error or "Upload not permitted by billing service"
        logger.warning(f"can_upload denied for {main_account}: {error_message}")
        return s3_error_response(
            code="UploadNotPermitted",
            message=error_message,
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
        )

    # Cache successful result for 60s — denials are NOT cached so users can retry after topping up
    await redis_accounts.set(cache_key, b"1", ex=60)

    return None


async def account_middleware(
    request: Request,
    call_next: Callable,
) -> Response:
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

    # Test bypass: short-circuit credit checks entirely
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

    if auth_method in ("access_key", "bearer_access_key"):
        account_address = request.state.account_address
        has_credits = getattr(request.state, "has_credits", True)

        request.state.account_id = account_address
        request.state.account = HippiusAccount(
            id=account_address,
            main_account=account_address,
            has_credits=has_credits,
            upload=True,
            delete=True,
        )

        if request.method in ["PUT", "POST", "DELETE"]:
            logger.debug(f"Checking credit for {request.method} operation: {path}")

            if not has_credits:
                logger.warning(f"Account lacks credits: {account_address}")
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

            try:
                can_upload_error = await _check_can_upload(request, logger)
            except Exception as e:
                logger.exception(f"can_upload check failed for {account_address}: {e}")
                return s3_error_response(
                    code="AccountVerificationError",
                    message="Something went wrong when verifying your account. Please try again later.",
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                )
            if can_upload_error is not None:
                return can_upload_error

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
