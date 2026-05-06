from __future__ import annotations

from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from gateway.middlewares.acl import parse_s3_path


PUBLIC_CACHE_CONTROL = "public, max-age=300, stale-while-revalidate=60"
# 30d fresh + 1d stale-while-revalidate. Effectively indefinite — used for buckets
# the cache-control service has flagged as is_cache_warm. Combined with PURGE on
# write, ATS holds bodies until either the next write or LRU eviction.
WARM_PUBLIC_CACHE_CONTROL = "public, max-age=2592000, stale-while-revalidate=86400"
PRIVATE_CACHE_CONTROL = "private, no-store"


async def cache_control_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    response = await call_next(request)

    if request.method not in ("GET", "HEAD"):
        return response

    # Explicit no-store on non-cacheable statuses (4xx/5xx) prevents ATS from
    # falling back to its heuristic negative-caching defaults and serving a
    # stale 404 for minutes after the object has been uploaded.
    if response.status_code not in (200, 206, 304):
        response.headers["Cache-Control"] = PRIVATE_CACHE_CONTROL
        return response

    bucket = getattr(request.state, "s3_bucket", None)
    key = getattr(request.state, "s3_key", None)
    if bucket is None:
        bucket, key = parse_s3_path(request.url.path)
    if not bucket or not key:
        response.headers["Cache-Control"] = PRIVATE_CACHE_CONTROL
        return response

    if not getattr(request.state, "anonymous_read_allowed", False):
        response.headers["Cache-Control"] = PRIVATE_CACHE_CONTROL
        return response

    if getattr(request.state, "bucket_is_cache_warm", False):
        response.headers["Cache-Control"] = WARM_PUBLIC_CACHE_CONTROL
        return response

    response.headers["Cache-Control"] = PUBLIC_CACHE_CONTROL
    return response
