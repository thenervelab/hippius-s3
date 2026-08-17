import hashlib
import hmac
import logging
from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response
from fastapi.responses import JSONResponse
from starlette import status

from gateway.utils.paths import routing_path
from hippius_s3.config import get_config


config = get_config()
logger = logging.getLogger(__name__)


def _error(status_code: int, detail: str) -> Response:
    # Starlette http-level middlewares can't raise HTTPException — FastAPI's
    # exception handler only catches those from route handlers. Return the
    # response directly.
    return JSONResponse(status_code=status_code, content={"detail": detail})


async def verify_frontend_hmac_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """HMAC middleware for frontend user endpoints.

    Expects an X-HMAC-Signature header containing the HMAC signature of the request.
    The signature is calculated as HMAC-SHA256(secret, method + path + query_string).
    """
    # Gated on the path the api will RECEIVE. Judged as sent, `/x/../user/foo` does not start with
    # `/user/`, so HMAC was skipped — and the api was still handed `/user/foo`. acl.py and
    # account.py skip their own checks for `/user/` on that same view, so this is the only layer
    # left in front of the frontend endpoints and it has to agree with them.
    #
    # The SIGNED message below deliberately stays `request.url.path`: it must remain byte-identical
    # to what the frontend signs. The two are equal for every real `/user/...` request (no dot
    # segments, no literal `#`), and for a crafted one they differ, which fails the comparison —
    # the safe direction.
    if not routing_path(request).startswith("/user/"):
        return await call_next(request)

    if request.method == "OPTIONS":
        return await call_next(request)

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
        config.frontend_hmac_secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(expected_signature, hmac_signature):
        logger.warning(
            f"Frontend HMAC verification failed for {request.method} {request.url.path}, raw message={message}"
        )
        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content={"detail": "Invalid HMAC signature"},
        )

    logger.debug(f"Frontend HMAC verification successful for {request.method} {request.url.path}")
    return await call_next(request)
