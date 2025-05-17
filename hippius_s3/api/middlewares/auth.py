"""Authentication middleware for the application."""

import logging
from typing import Callable

from fastapi import Request
from fastapi import Response
from starlette import status

from hippius_s3.api.s3.errors import s3_error_response
from hippius_s3.dependencies import get_seed_phrase_from_auth_header


logger = logging.getLogger(__name__)


async def extract_seed_phrase_middleware(request: Request, call_next: Callable) -> Response:
    """
    Middleware that extracts the seed phrase from the authorization header
    and adds it to request.state for reuse by endpoints and other middleware.

    This middleware requires an authorization header for all endpoints.
    If the header is missing or invalid, it returns a 401 Unauthorized response.
    """
    # Define paths that are exempt from authentication
    # Currently we're not exempting any paths, but this list can be expanded
    # if needed in the future (e.g., health checks, documentation, etc.)
    exempt_paths = [
        # "/health",
        # "/status",
        "/docs",
        "/openapi.json",
    ]

    # Allow OPTIONS requests to pass through for CORS preflight
    if request.method == "OPTIONS":
        options_response: Response = await call_next(request)
        return options_response

    # Allow exempt paths to pass through without authentication
    path = request.url.path
    if any(path.startswith(exempt_path) for exempt_path in exempt_paths):
        logger.debug(f"Skipping authentication for exempt path: {path}")
        exempt_response: Response = await call_next(request)
        return exempt_response

    # Get authorization header
    auth_header = request.headers.get("authorization")

    if not auth_header:
        logger.warning(f"Authorization header missing: {request.method} {path}")
        return s3_error_response(
            code="AccessDenied",
            message="Authorization header is required for all requests",
            status_code=status.HTTP_401_UNAUTHORIZED,
        )

    try:
        # Extract seed phrase and store in request.state
        seed_phrase = get_seed_phrase_from_auth_header(auth_header)
        request.state.seed_phrase = seed_phrase
        logger.debug(f"Successfully extracted seed phrase for: {request.method} {path}")
    except ValueError as e:
        logger.warning(f"Invalid authorization header format: {str(e)}")
        return s3_error_response(
            code="InvalidAuthorizationHeader",
            message="The authorization header format is invalid",
            status_code=status.HTTP_401_UNAUTHORIZED,
        )

    # Continue with the request
    final_response: Response = await call_next(request)
    return final_response
