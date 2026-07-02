from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from gateway.middlewares.auth_probe import is_valid_auth_probe
from gateway.services.auth_orchestrator import authenticate_request
from hippius_s3.services.ray_id_service import get_logger_with_ray_id


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

    exempt_paths = ["/docs", "/openapi.json", "/user/", "/robots.txt", "/metrics", "/health"]

    if request.method == "OPTIONS":
        return await call_next(request)

    path = request.url.path
    if any(path.startswith(exempt_path) or path == exempt_path for exempt_path in exempt_paths):
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
