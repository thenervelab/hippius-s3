"""Account verification and credit checking middleware for the gateway."""

import asyncio
import logging
import re
from typing import Callable

import httpx
from fastapi import Request
from fastapi import Response
from starlette import status

from gateway.config import get_config
from gateway.services.account_service import fetch_account_by_main_address
from gateway.utils.errors import s3_error_response
from hippius_s3.models.account import HippiusAccount
from hippius_s3.services.arion_service import ArionClient
from hippius_s3.services.arion_service import CanUploadResponse
from hippius_s3.services.ray_id_service import get_logger_with_ray_id


config = get_config()

# Substrings that mark a can_upload denial as a TRANSIENT billing-service failure (the upstream
# balance lookup could not complete) rather than a genuine "account is out of credit" denial. The
# former is retryable and must never surface as a hard 402 — a client reads that as "insufficient
# funds" and gives up, when in reality the billing backend just blipped.
#
# These are matched as substrings, so they MUST be phrases that describe an infra/lookup FAILURE and
# can never appear in a genuine out-of-credit verdict. In particular do NOT add a bare "billing
# balance" — a real denial like "insufficient billing balance" contains it and would be wrongly
# retried into a 503. Keep this list anchored to Arion's known fetch-failure wording; the durable
# fix is a structured `transient` flag on CanUploadResponse instead of string-sniffing another
# service's free text.
_TRANSIENT_BILLING_ERROR_MARKERS = (
    "failed to fetch billing balance",
    "could not fetch billing",
    "error fetching billing",
    "timeout",
    "timed out",
    "temporarily unavailable",
    "service unavailable",
)


def _is_transient_billing_error(error: str | None) -> bool:
    if not error:
        return False
    lowered = error.lower()
    return any(marker in lowered for marker in _TRANSIENT_BILLING_ERROR_MARKERS)


# Transport failures that mean "we could not reach Arion". Deliberately NOT httpx.RequestError,
# which also covers UnsupportedProtocol (a malformed HIPPIUS_ARION_BASE_URL), LocalProtocolError
# (we built an invalid request), DecodingError and TooManyRedirects. Those are our bugs; folding
# them in would turn a total upload outage into a fleet-wide "please retry" that never resolves
# and carries no stack trace. They stay on the blanket handler where they stay loud.
_UNREACHABLE_ERRORS = (httpx.TimeoutException, httpx.NetworkError, httpx.ProxyError, httpx.RemoteProtocolError)


async def _can_upload(
    arion_client: ArionClient,
    main_account: str,
    content_length: int,
) -> tuple[CanUploadResponse, bool]:
    """Call Arion can_upload, mapping an unreachable billing backend onto a transient verdict.

    ArionClient.can_upload ends in `raise_for_status()`, so an upstream 5xx — in practice
    `502 Next Hop Connection Failed` from the ATS edge in front of Arion — leaves as an
    exception rather than a CanUploadResponse. That skips the transient handling below
    entirely and lands in account_middleware's blanket `except Exception`, which answers
    `AccountVerificationError`, a non-standard code. The retry ladder and SlowDown response
    written for exactly this case never got a chance to run.

    Returns (verdict, worth_retrying). The flag exists so a backend that told us to slow down
    is not immediately hammered again — see the 429 branch.
    """
    try:
        return await arion_client.can_upload(main_account, content_length), True
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code
        # 429 IS transient, but re-driving it is the one response guaranteed to be wrong: Arion
        # just said "too many requests". Surface the SlowDown without running the ladder.
        if status_code == 429:
            return CanUploadResponse(result=False, error="billing service unavailable (upstream 429)"), False
        # 507 means Arion is full; retry_on_error re-raises it precisely because retrying cannot
        # help, so don't undo that here. Other 4xx means we sent something it rejected — our bug,
        # and retrying it into a SlowDown would hide it. Both fall through to the blanket handler.
        if status_code == 507 or status_code < 500:
            raise
        return CanUploadResponse(result=False, error=f"billing service unavailable (upstream {status_code})"), True
    except _UNREACHABLE_ERRORS as exc:
        return CanUploadResponse(result=False, error=f"billing service unavailable ({type(exc).__name__})"), True


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

    response, worth_retrying = await _can_upload(arion_client, main_account, content_length)
    logger.info(
        f"can_upload billing check for {main_account}: size_bytes={content_length}, result={response.result}, error={response.error}"
    )

    # A transient billing-service failure is not a real denial — retry a few times before giving up.
    attempts = 0
    while (
        not response.result
        and worth_retrying
        and _is_transient_billing_error(response.error)
        and attempts < config.can_upload_transient_retries
    ):
        attempts += 1
        logger.warning(
            f"can_upload transient billing failure for {main_account} (attempt {attempts}/{config.can_upload_transient_retries}): {response.error}"
        )
        await asyncio.sleep(config.can_upload_transient_retry_delay_seconds)
        response, worth_retrying = await _can_upload(arion_client, main_account, content_length)

    if not response.result:
        # Still failing on a transient billing error after retries: surface a retryable 503
        # SlowDown, NOT a hard 402 — the account may well have credit; the billing lookup is down.
        if _is_transient_billing_error(response.error):
            logger.warning(f"can_upload billing service unavailable for {main_account}: {response.error}")
            return s3_error_response(
                code="SlowDown",
                message="Billing service is temporarily unavailable. Please retry.",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        error_message = response.error or "Upload not permitted by billing service"
        logger.warning(f"can_upload denied for {main_account}: {error_message}")
        return s3_error_response(
            code="UploadNotPermitted",
            message=error_message,
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
        )

    # Cache successful result briefly (can_upload_cache_ttl_seconds) — denials are NOT cached so users
    # can retry after topping up. Short TTL bounds how long an exhausted account keeps slipping uploads
    # through after its balance is gone.
    await redis_accounts.set(cache_key, b"1", ex=config.can_upload_cache_ttl_seconds)

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

    Only access key authentication is supported. Seed phrase authentication has been removed.
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

    # Skip credit checks for frontend user endpoints, admin endpoints, docs, and metrics
    if (
        path.startswith("/user/")
        or path.startswith("/admin/")
        or path in ["/docs", "/openapi.json", "/redoc", "/metrics"]
        or path.startswith("/docs/")
    ):
        resp: Response = await call_next(request)
        return resp

    auth_method = getattr(request.state, "auth_method", None)

    if auth_method == "access_key":
        account_address = request.state.account_address

        try:
            request.state.account_id = account_address

            # GW-4: reads (GET/HEAD) carry no credit gate and the internal API never reads the credit
            # fields, so skip the redis-accounts fetch on the hottest path and stamp a lightweight
            # account (main_account is still needed for audit/header stamping). Only mutating methods
            # fetch the real account and run the credit/can_upload checks.
            if request.method not in ["PUT", "POST", "DELETE"]:
                request.state.account = HippiusAccount(
                    id=account_address,
                    main_account=account_address,
                    has_credits=False,
                    upload=False,
                    delete=False,
                )
            else:
                redis_accounts_client = request.app.state.redis_accounts
                request.state.account = await fetch_account_by_main_address(
                    account_address,
                    redis_accounts_client,
                    config.substrate_url,
                )
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
    else:
        # Anonymous access (no auth method)
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
