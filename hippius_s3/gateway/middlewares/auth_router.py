from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from hippius_s3.gateway.middlewares.auth_probe import is_valid_auth_probe
from hippius_s3.gateway.services.auth_orchestrator import authenticate_request
from hippius_s3.gateway.utils.paths import routing_path
from hippius_s3.peer_auth import is_authorized_peer_fetch
from hippius_s3.services.ray_id_service import get_logger_with_ray_id


# Matched on the EXACT first path segment. A bare startswith() also matched bucket names like
# /docs2, which then skipped SigV4 verification entirely and landed as anonymous-owned buckets
# (prod incident 2026-08-03).
#
# Exempt at the segment itself and at any depth below it.
EXEMPT_SEGMENTS = frozenset({"docs", "openapi.json", "robots.txt", "metrics", "health"})

# Exempt only WITH a subpath (`/user/...`, `/admin/...`), which is what the frontend and
# admin endpoints actually use. A bare `/user` is not a route — it is bucket-shaped — and
# acl.py / account.py both special-case `/user/` with the slash, so exempting it here would
# make auth_router the only layer that treats it as non-S3. `/admin/...` is HMAC-gated by
# admin_hmac_middleware instead of SigV4.
EXEMPT_SUBPATH_ONLY_SEGMENTS = frozenset({"admin", "user"})

# Every segment auth_router skips authentication for must be unusable as a bucket name, or a
# bucket created under it lands with no owner. Enforced by
# test_every_auth_exempt_segment_is_a_reserved_bucket_name.
ALL_EXEMPT_SEGMENTS = EXEMPT_SEGMENTS | EXEMPT_SUBPATH_ONLY_SEGMENTS


def _is_exempt(request: Request) -> bool:
    # Judged on the path the api will RECEIVE (`routing_path`), not the one the client sent. httpx
    # collapses dot segments on the way out, so `/docs/../anybucket/key` reads as the exempt segment
    # `docs` when judged as sent — skipping authentication entirely and processing the request as
    # anonymous — while the api is handed `/anybucket/key`. The subpath test uses the same view, so
    # `/user#/x` cannot borrow a subpath that is never forwarded either.
    path = routing_path(request)
    segment = path.strip("/").split("/", 1)[0]
    if segment in EXEMPT_SEGMENTS:
        return True
    if segment in EXEMPT_SUBPATH_ONLY_SEGMENTS and "/" in path.strip("/"):
        return True
    # Secret-authenticated peer chunk fetches (see input_validation for the rationale).
    return is_authorized_peer_fetch(request)


async def auth_router_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """
    Route authentication to appropriate handler based on credential format.

    Delegates logic to `authenticate_request` service.
    """
    ray_id = getattr(request.state, "ray_id", "no-ray-id")
    logger = get_logger_with_ray_id(__name__, ray_id)

    if request.method == "OPTIONS":
        return await call_next(request)

    if _is_exempt(request):
        return await call_next(request)

    # PURGE from the gateway → ATS bounces back here via authproxy. There is
    # no Authorization header on internal PURGE traffic; the probe secret is
    # the trust boundary (same defense-in-depth as auth_probe_middleware uses
    # for cached reads). Without this bypass, auth_router would 403 every
    # PURGE and ats_purge_middleware's invalidation-on-write would break.
    if request.method == "PURGE" and is_valid_auth_probe(request):
        return await call_next(request)

    auth_result = await authenticate_request(request)

    if auth_result.error_response:
        return auth_result.error_response

    # Populate request state
    request.state.auth_method = auth_result.auth_method

    if auth_result.auth_method == "access_key":
        request.state.access_key = auth_result.access_key
        request.state.account_address = auth_result.account_address
        request.state.account_id = auth_result.account_address
        request.state.token_type = auth_result.token_type
        if auth_result.access_key:
            logger.debug(f"Authenticated with access key: {auth_result.access_key[:8]}***")
    elif auth_result.auth_method == "bearer_access_key":
        request.state.access_key = auth_result.access_key
        request.state.account_address = auth_result.account_address
        request.state.token_type = auth_result.token_type
        request.state.account_id = auth_result.account_id
        if auth_result.access_key:
            logger.debug(f"Authenticated with Bearer access key: {auth_result.access_key[:8]}***")
    elif auth_result.auth_method == "anonymous":
        logger.debug("Anonymous request")

    return await call_next(request)
