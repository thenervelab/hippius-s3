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
#
# Emitted in two scenarios:
#  - warm bucket, anonymous-readable (browsers + ATS may cache).
#  - warm bucket, NOT anonymous-readable. In this case ATS stores under this
#    cache-friendly directive but header_rewrite on the ATS side rewrites the
#    response to "private, no-store" before egress to the client (based on the
#    X-Hippius-Visibility: private sentinel set below). Per-request access is
#    gated by ATS authproxy → gateway /__authcheck probe.
WARM_PUBLIC_CACHE_CONTROL = "public, max-age=2592000, stale-while-revalidate=86400"
PRIVATE_CACHE_CONTROL = "private, no-store"
VISIBILITY_HEADER = "X-Hippius-Visibility"


async def cache_control_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    response = await call_next(request)

    # ATS authproxy subrequests are short-circuited by auth_probe_middleware
    # (innermost) which marks the request before returning. Their response
    # isn't stored in the ATS cache and never reaches the real client, so
    # leave it untouched.
    if getattr(request.state, "is_auth_probe", False):
        return response

    if request.method not in ("GET", "HEAD"):
        return response

    # Explicit no-store on non-cacheable statuses (4xx/5xx) prevents ATS from
    # falling back to its heuristic negative-caching defaults and serving a
    # stale 404 for minutes after the object has been uploaded.
    if response.status_code not in (200, 206, 304):
        response.headers["Cache-Control"] = PRIVATE_CACHE_CONTROL
        return response

    # Honor the S3 response-cache-control override when a signed client opted in.
    # Anonymous callers can't set this — parse_response_overrides on the API side
    # ignores it for them, so any cache-control they smuggle won't be on the
    # response anyway. We additionally guard here so the gateway can't be tricked
    # by an unsigned URL that happens to carry the query param.
    if "response-cache-control" in request.query_params and getattr(request.state, "auth_method", None) != "anonymous":
        return response

    bucket = getattr(request.state, "s3_bucket", None)
    key = getattr(request.state, "s3_key", None)
    if bucket is None:
        bucket, key = parse_s3_path(request.url.path)
    if not bucket or not key:
        response.headers["Cache-Control"] = PRIVATE_CACHE_CONTROL
        return response

    bucket_is_cache_warm = getattr(request.state, "bucket_is_cache_warm", False)
    anonymous_read_allowed = getattr(request.state, "anonymous_read_allowed", False)

    if not anonymous_read_allowed:
        if bucket_is_cache_warm:
            # Warm-private: emit cache-friendly directives so ATS stores the
            # response for 30d, plus a sentinel that triggers ATS header_rewrite
            # to demote Cache-Control to "private, no-store" on egress to the
            # real client. Per-request authorization is enforced by ATS
            # authproxy subrequests carrying X-Hippius-Auth-Probe.
            response.headers["Cache-Control"] = WARM_PUBLIC_CACHE_CONTROL
            response.headers[VISIBILITY_HEADER] = "private"
            return response
        response.headers["Cache-Control"] = PRIVATE_CACHE_CONTROL
        return response

    if bucket_is_cache_warm:
        response.headers["Cache-Control"] = WARM_PUBLIC_CACHE_CONTROL
        return response

    response.headers["Cache-Control"] = PUBLIC_CACHE_CONTROL
    return response
