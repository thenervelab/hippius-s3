from __future__ import annotations

from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from gateway.config import get_config
from gateway.middlewares.acl import parse_s3_path


REVALIDATE_ALWAYS = "public, max-age=0, must-revalidate"
STANDARD_PUBLIC = "public, max-age=300, stale-while-revalidate=60"
PRIVATE_CACHE_CONTROL = "private, no-store"


async def cache_control_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    response = await call_next(request)

    if request.method not in ("GET", "HEAD"):
        return response
    if response.status_code not in (200, 206, 304):
        return response

    bucket = getattr(request.state, "s3_bucket", None)
    key = getattr(request.state, "s3_key", None)
    if bucket is None:
        bucket, key = parse_s3_path(request.url.path)
    if not bucket or not key:
        response.headers["Cache-Control"] = PRIVATE_CACHE_CONTROL
        return response

    anon_readable = bool(getattr(request.state, "anonymous_read_allowed", False))
    if not anon_readable:
        response.headers["Cache-Control"] = PRIVATE_CACHE_CONTROL
        return response

    offload = get_config().ats_cache_offload_buckets
    if bucket in offload:
        response.headers["Cache-Control"] = STANDARD_PUBLIC
    else:
        response.headers["Cache-Control"] = REVALIDATE_ALWAYS
    return response
