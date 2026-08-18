import hashlib
import hmac
import logging
from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response
from fastapi.responses import JSONResponse
from starlette import status

from gateway.config import get_config


config = get_config()
logger = logging.getLogger(__name__)


async def verify_admin_hmac_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """HMAC gate for the /admin/* account-management endpoints (issue #421).

    Same scheme as the frontend HMAC layer — X-HMAC-Signature =
    HMAC-SHA256(secret, METHOD + PATH[+?QUERY]) — but with a dedicated secret
    (HIPPIUS_ADMIN_HMAC_SECRET): this surface can suspend and destroy whole accounts,
    so it does not share the frontend credential's blast radius. The target account
    always travels in the signed path, never the unsigned body.

    Fail-closed: an empty secret disables the admin API entirely (unlike
    FRONTEND_HMAC_SECRET, which would silently verify against ""), matching the
    auth_probe_secret precedent.
    """
    if not request.url.path.startswith("/admin/"):
        return await call_next(request)

    if request.method == "OPTIONS":
        return await call_next(request)

    if not config.admin_hmac_secret:
        logger.warning(f"Admin API request rejected — HIPPIUS_ADMIN_HMAC_SECRET is not configured: {request.url.path}")
        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content={"detail": "Admin API is not enabled"},
        )

    hmac_signature = request.headers.get("x-hmac-signature")
    if not hmac_signature:
        logger.warning(f"Missing X-HMAC-Signature header for {request.method} {request.url.path}")
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={"detail": "Missing X-HMAC-Signature header"},
        )

    message = request.method + request.url.path
    if request.url.query:
        message += "?" + request.url.query

    expected_signature = hmac.new(
        config.admin_hmac_secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(expected_signature, hmac_signature):
        logger.warning(f"Admin HMAC verification failed for {request.method} {request.url.path}")
        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content={"detail": "Invalid HMAC signature"},
        )

    return await call_next(request)
