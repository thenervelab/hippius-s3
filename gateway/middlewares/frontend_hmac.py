import hashlib
import hmac
import logging
from typing import Awaitable
from typing import Callable

from fastapi import HTTPException
from fastapi import Request
from fastapi import Response
from starlette import status

from gateway.config import get_config


config = get_config()
logger = logging.getLogger(__name__)


async def verify_frontend_hmac_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """
    HMAC middleware for frontend user endpoints.

    Expects an X-HMAC-Signature header containing the HMAC signature of the request.
    The signature is calculated as HMAC-SHA256(secret, method + path + query_string).
    """
    if not request.url.path.startswith("/user/"):
        return await call_next(request)

    # Skip HMAC verification for OPTIONS requests (CORS preflight)
    if request.method == "OPTIONS":
        return await call_next(request)

    hmac_signature = request.headers.get("x-hmac-signature")
    if not hmac_signature:
        logger.warning(f"Missing X-HMAC-Signature header for {request.method} {request.url.path}")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing X-HMAC-Signature header")

    # Create the message to sign: method + path + query_string
    message = request.method + request.url.path
    if request.url.query:
        message += "?" + request.url.query

    # Calculate the expected signature
    expected_signature = hmac.new(
        config.frontend_hmac_secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    logger.info(f"{message=} {expected_signature=} {hmac_signature=}")

    # Compare signatures
    if not hmac.compare_digest(expected_signature, hmac_signature):
        logger.warning(
            f"Frontend HMAC verification failed for {request.method} {request.url.path}, raw message={message}"
        )
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid HMAC signature")

    logger.debug(
        f"Frontend HMAC verification successful for {request.method} {request.url.path} {message=} {expected_signature=}"
    )
    return await call_next(request)
