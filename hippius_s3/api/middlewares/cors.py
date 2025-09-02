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
    """
    logger.info(f"CORS middleware: {request.method} {request.url.path}")

    # Handle preflight OPTIONS requests
    if request.method == "OPTIONS":
        logger.info("Handling OPTIONS request")
        response = Response(status_code=204)
    else:
        response = await call_next(request)

    # Add CORS headers to ALL responses (including OPTIONS)
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, HEAD, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "*"
    response.headers["Access-Control-Allow-Credentials"] = "true"
    response.headers["Access-Control-Expose-Headers"] = "*"
    response.headers["Access-Control-Max-Age"] = "86400"

    # Echo back any requested headers for OPTIONS requests
    if request.method == "OPTIONS":
        if "Access-Control-Request-Headers" in request.headers:
            response.headers["Access-Control-Allow-Headers"] = request.headers["Access-Control-Request-Headers"]

        if "Access-Control-Request-Method" in request.headers:
            response.headers["Access-Control-Allow-Methods"] = request.headers["Access-Control-Request-Method"]

    # Add your other security headers...
    response.headers["X-Robots-Tag"] = "noindex, nofollow, nosnippet, noarchive"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"

    # CSP and other headers...
    path = request.url.path
    if path in ["/docs", "/redoc"] or path.startswith("/docs/"):
        response.headers[
            "Content-Security-Policy"
        ] = "default-src 'self' 'unsafe-inline' 'unsafe-eval' data: https:; frame-ancestors 'none';"
    else:
        response.headers["Content-Security-Policy"] = "default-src 'none'; frame-ancestors 'none';"

    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"

    return response
