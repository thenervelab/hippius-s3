from __future__ import annotations

from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response


async def trailing_slash_normalizer(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """Normalize trailing slashes without redirecting.

    Removes a trailing slash from the URL path (except for "/") so both
    with/without trailing slash resolve to the same route.
    """
    path = request.url.path
    if path != "/" and path.endswith("/"):
        normalized = path.rstrip("/")
        request.scope["path"] = normalized
        request.scope["raw_path"] = normalized.encode()

    return await call_next(request)
