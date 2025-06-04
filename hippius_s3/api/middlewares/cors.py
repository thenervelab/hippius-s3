"""Custom CORS middleware that preserves exceptions from inner middlewares."""

import logging
from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response


logger = logging.getLogger(__name__)


async def cors_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """
    Custom CORS middleware that adds CORS headers without intercepting exceptions.

    This middleware adds CORS headers to all responses but allows exceptions
    from inner middlewares to bubble up properly for debugging.
    """
    # Handle preflight OPTIONS requests
    if request.method == "OPTIONS":
        response = Response(status_code=200)
    else:
        response = await call_next(request)

    # Add CORS headers to the response
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, HEAD, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "*"
    response.headers["Access-Control-Allow-Credentials"] = "true"
    response.headers["Access-Control-Expose-Headers"] = "*"

    # Handle preflight request headers
    if request.method == "OPTIONS":
        # Echo back any requested headers
        if "Access-Control-Request-Headers" in request.headers:
            response.headers["Access-Control-Allow-Headers"] = request.headers["Access-Control-Request-Headers"]

        # Echo back any requested methods
        if "Access-Control-Request-Method" in request.headers:
            response.headers["Access-Control-Allow-Methods"] = request.headers["Access-Control-Request-Method"]

    return response
