import logging
from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from gateway.utils.errors import s3_error_response


logger = logging.getLogger(__name__)

WRITE_METHODS = {"PUT", "POST", "DELETE", "PATCH"}
ALWAYS_ALLOWED_PATHS = {"/health"}


async def read_only_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    if request.method in WRITE_METHODS and request.url.path not in ALWAYS_ALLOWED_PATHS:
        logger.warning("Write request rejected in read-only mode: %s %s", request.method, request.url.path)
        return s3_error_response(
            code="MethodNotAllowed",
            message="This endpoint is read-only",
            status_code=405,
        )
    return await call_next(request)
