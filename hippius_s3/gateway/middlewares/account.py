"""Account verification and credit checking middleware for the gateway."""

import asyncio
import logging
from typing import Callable

import httpx
from fastapi import Request
from fastapi import Response
from starlette import status

from hippius_s3.config import get_config
from hippius_s3.gateway.middlewares.acl import parse_s3_path
from hippius_s3.gateway.services import plan_gate
from hippius_s3.gateway.services.account_service import fetch_account_by_main_address
from hippius_s3.gateway.services.sub_token_scope import required_op
from hippius_s3.gateway.utils.errors import s3_error_response
from hippius_s3.gateway.utils.paths import first_path_segment
from hippius_s3.gateway.utils.paths import routing_path
from hippius_s3.models.account import HippiusAccount
from hippius_s3.models.sub_token import Op
from hippius_s3.monitoring import PlanGateOutcome
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.peer_auth import is_authorized_peer_fetch
from hippius_s3.services.arion_service import ArionClient
from hippius_s3.services.arion_service import CanUploadResponse
from hippius_s3.services.plans_cache import PlanQuota
from hippius_s3.services.ray_id_service import get_logger_with_ray_id
from hippius_s3.services.service_accounts import is_service_account


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


# Shadow verdicts get their own label so an enforced denial and a shadow one are never summed
# together on plan_gate_total. Enforcing mode needs no map: its outcomes already ARE the labels,
# and `would_deny` is unreachable there.
_SHADOW_LABEL: dict[plan_gate.Outcome, PlanGateOutcome] = {
    "allow": "shadow_allow",
    "would_deny": "shadow_would_deny",
    "catalog_miss": "shadow_catalog_miss",
}


def _permissive_account(account_address: str) -> HippiusAccount:
    """The account object for a caller whose write is not credit-gated.

    Used by the test bypass, the service-account bypass and the plan branch alike: each has already
    decided the request may proceed, and none of them consults the credit fields afterwards.
    """
    return HippiusAccount(
        id=account_address,
        main_account=account_address,
        has_credits=True,
        upload=True,
        delete=True,
    )


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


def _declared_content_length(request: Request) -> int:
    """Bytes this request claims it will write.

    AWS CLI v2+ uses chunked transfer encoding and sends the real size in
    x-amz-decoded-content-length instead of Content-Length.

    This is a CLAIM, not a measurement: it is whatever the caller declared, and it is 0 when neither
    header is present. Treat it as a lower bound on the write, never as its true size. Both billing
    gates have always been built on it — the plan gate inherits exactly the same precision as
    can_upload, no better and no worse. Anything that needs the real figure must be enforced after
    the write, where it is known. See todo.md.
    """
    return int(request.headers.get("x-amz-decoded-content-length") or request.headers.get("content-length") or "0")


def _adds_storage(request: Request) -> bool:
    """Whether this operation can ADD stored bytes, and so is subject to the plan quota.

    Delegates to the repo's existing verb+query -> operation mapping rather than re-deriving one.
    Judging this on the HTTP verb alone is wrong in both directions, and both mistakes are
    user-hostile:

      * `POST /{bucket}?delete` is the bulk DeleteObjects that `aws s3 rm --recursive` issues. It
        frees space, so gating it would answer an over-quota customer's bulk delete with a 402
        telling them to delete things.
      * `PUT /{bucket}?acl|?tagging|?versioning|?lifecycle|...` and `CreateBucket` store no object
        bytes. Refusing "set a tag on my bucket" with "this upload would exceed your storage quota"
        is the same class of error, one query param over.

    `required_op` already encodes both, and is the same mapping sub-token authorisation reads — so
    there is one place that knows what an S3 request is, not two.

    CompleteMultipartUpload is excluded on top of that: its parts have already been written and
    already passed this gate individually, so refusing the commit reclaims nothing and strands them.
    That is `POST ?uploadId` WITHOUT `partNumber` — matching on `uploadId` alone would also exempt
    every `PUT ?partNumber&uploadId` part upload, i.e. exactly the requests that carry the bytes,
    which is how large uploads happen.

    Residual, tracked in todo.md: `required_op` grades `PUT /{bucket}/{key}?acl|?tagging` as
    write_object, so those stay gated. Fixing that means splitting object subresources in the shared
    mapping, which also changes sub-token authorisation — deliberately not done here.
    """
    params = dict(request.query_params)
    _, key = parse_s3_path(routing_path(request))
    if required_op(request.method, key is not None, params) is not Op.write_object:
        return False

    is_complete_mpu = request.method == "POST" and "uploadId" in params and "partNumber" not in params
    return not is_complete_mpu


def _log_plan_shadow(
    request: Request,
    logger: logging.Logger | logging.LoggerAdapter,
    account_address: str,
    quota: PlanQuota,
) -> None:
    """Record what the plan gate WOULD have done, while HIPPIUS_ENABLE_BILLING_PLANS is off.

    The request itself is untouched: it goes on to the pay-as-you-go path and is billed exactly as
    it is today. This exists so the whole chain -- plans-cacher -> redis maps -> usage rollup ->
    quota arithmetic -- is observably working in prod logs before the flag is flipped and it can
    cost anyone an upload.

    Only accounts that actually HAVE a plan are logged. Emitting a line for every pay-as-you-go
    write would bury the signal in the volume it is meant to be found in.

    Grep in Loki:
        {namespace="hippius-s3-prod",app="api"} |= "BILLING_PLAN_SHADOW"
        {namespace="hippius-s3-prod",app="api"} |= "BILLING_PLAN_SHADOW" |= "would=would_deny"

    Deliberately synchronous and side-effect-free apart from the log line and the counter: shadow
    mode runs on the pay-as-you-go path of every write while the feature is OFF, so anything that
    can block, fail or wait does not belong here. There is no try/except because there is nothing
    here that can raise -- if you add a fallible call (a Redis write, a DB read), it will surface as
    a 503 on a live upload, so wrap it then.
    """
    if not _adds_storage(request):
        return

    incoming = _declared_content_length(request)
    decision = plan_gate.evaluate_quota(quota, incoming, enforcing=False)

    get_metrics_collector().record_plan_gate(outcome=_SHADOW_LABEL[decision.outcome])
    logger.info(
        f"BILLING_PLAN_SHADOW enforcement=disabled account={account_address} plan={quota.plan_id} "
        f"method={request.method} used_bytes={decision.used_bytes} limit_bytes={decision.quota_bytes} "
        f"incoming_bytes={incoming} would={decision.outcome} "
        f"note=charged via pay-as-you-go instead; set HIPPIUS_ENABLE_BILLING_PLANS=true to enforce"
    )


async def _check_plan_quota(
    request: Request,
    logger: logging.Logger | logging.LoggerAdapter,
    account_address: str,
) -> tuple[bool, Response | None]:
    """Storage-quota gate for accounts on a billing plan.

    Returns (handled, error_response). `handled` False means the caller must run the normal
    pay-as-you-go path (credits + can_upload) -- because the account has no plan, because the caches
    could not be consulted, or because HIPPIUS_ENABLE_BILLING_PLANS is off.
    """
    redis_accounts = request.app.state.redis_accounts_client

    # The plan lookup consults our own state (one redis-accounts HGET; there is no database work on
    # this path). Failing to reach a verdict is not worth failing a customer's upload over, so it
    # falls back to the pay-as-you-go path — exactly what the account would have got before this
    # feature existed, so nothing here can be a NEW failure mode.
    try:
        resolved = await plan_gate.resolve_plan(redis_accounts, account_address)
        if resolved is None:
            return False, None

        quota = resolved
        request.state.plan_id = quota.plan_id

        if not config.enable_billing_plans:
            _log_plan_shadow(request, logger, account_address, quota)
            return False, None

        if not _adds_storage(request):
            get_metrics_collector().record_plan_gate(outcome="allow")
            return True, None

        decision = plan_gate.evaluate_quota(quota, _declared_content_length(request))
    except plan_gate.PlanLookupUnavailable as e:
        logger.warning(f"PLAN_LOOKUP unavailable account={account_address}: {e}; falling back to pay-as-you-go")
        get_metrics_collector().record_plan_gate(outcome="unavailable")
        return False, None
    # `would_deny` cannot occur when enforcing; the remaining outcomes are already valid labels.
    enforced: PlanGateOutcome = "allow" if decision.outcome == "would_deny" else decision.outcome
    get_metrics_collector().record_plan_gate(outcome=enforced)

    if decision.outcome == "catalog_miss":
        logger.warning(
            f"PLAN_QUOTA catalog miss account={account_address} plan={quota.plan_id}; allowing. "
            f"The plans-cacher may be cold or this plan id is unknown to the catalog."
        )
        return True, None

    if decision.outcome == "deny":
        logger.warning(
            f"PLAN_QUOTA denied account={account_address} plan={quota.plan_id} "
            f"used={decision.used_bytes} limit={decision.quota_bytes}"
        )
        return True, s3_error_response(
            code="QuotaExceeded",
            message=plan_gate.quota_exceeded_message(decision),
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            BucketName=first_path_segment(request),
        )

    return True, None


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

    content_length = _declared_content_length(request)
    main_account = request.state.account.main_account
    arion_client = request.app.state.arion_client
    redis_accounts = request.app.state.redis_accounts_client

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

    # The routing view, for the same reason as acl.py: judged as sent, `/docs/../anybucket/key`
    # matched the `/docs/` skip below and took an S3 write past the credit gate.
    path = routing_path(request)

    # Test bypass: short-circuit credit and substrate/redis access entirely
    if config.enable_bypass_credit_check:
        auth_method = getattr(request.state, "auth_method", None)

        if auth_method == "access_key":
            account_address = getattr(request.state, "account_address", "anonymous")
            request.state.account_id = account_address
            request.state.account = _permissive_account(account_address)
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

    # Secret-authenticated peer chunk fetches (see input_validation for the rationale).
    if is_authorized_peer_fetch(request):
        return await call_next(request)

    auth_method = getattr(request.state, "auth_method", None)

    if auth_method == "access_key":
        account_address = request.state.account_address

        # Keyed on account_address, which auth_router derived from a VERIFIED signature/token —
        # never from a client-supplied header — so only the holder of the service account's own
        # credentials can land here. Recorded for reads too, where nothing is bypassed, so the
        # audit log tells the whole story of what an internal account did.
        service_account = is_service_account(account_address, config.service_account_ids)
        request.state.service_account = service_account

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
            elif service_account:
                # Skips the redis-accounts fetch as well as the gates: an internal account has no
                # meaningful credit row to consult, and consulting one would make our own writes
                # fail whenever the account-cacher lags.
                request.state.account = _permissive_account(account_address)
                logger.info(
                    f"BILLING_BYPASS surface=gateway account={account_address} method={request.method} path={path}"
                )
                get_metrics_collector().record_billing_bypass(surface="gateway")
            else:
                # Billing plans run in PARALLEL with pay-as-you-go. An account on a plan buys a
                # storage allowance rather than credits, so it skips BOTH gates below: the substrate
                # has_credits check (a plan customer holds no substrate credits and would be 402'd
                # by it) and Arion can_upload. An account with no plan falls straight through to the
                # unchanged pay-as-you-go path.
                plan_handled, plan_error = await _check_plan_quota(request, logger, account_address)
                if plan_handled:
                    request.state.account = _permissive_account(account_address)
                    if plan_error is not None:
                        return plan_error
                    # Deliberately no `return await call_next(request)` here: falling out of the try
                    # reaches the shared call_next at the end of the middleware. Calling it inside
                    # this block would put the whole downstream request under the `except Exception`
                    # below, turning any handler error into a 503 AccountVerificationError.
                else:
                    redis_accounts_client = request.app.state.redis_accounts_client
                    request.state.account = await fetch_account_by_main_address(
                        account_address,
                        redis_accounts_client,
                        config.substrate_url,
                    )
                    logger.debug(f"Checking credit for {request.method} operation: {path}")

                    if not request.state.account.has_credits:
                        logger.warning(f"Access key account lacks credits: {account_address}")
                        return s3_error_response(
                            code="InsufficientAccountCredit",
                            message="The account does not have sufficient credit to perform this operation",
                            status_code=status.HTTP_402_PAYMENT_REQUIRED,
                            BucketName=first_path_segment(request),
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
        request.state.service_account = False
        request.state.account = HippiusAccount(
            id=account_id,
            main_account=account_id,
            has_credits=True,
            upload=False,
            delete=False,
        )
    # Continue with the request
    return await call_next(request)
