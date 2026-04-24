from __future__ import annotations

from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from gateway.config import get_config
from gateway.middlewares.acl import parse_s3_path
from gateway.services.ats_cache_client import schedule_purge


DEFAULT_HOST = "s3.hippius.com"


async def ats_purge_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    response = await call_next(request)

    if not get_config().ats_cache_endpoints:
        return response
    if response.status_code >= 300:
        return response
    method = request.method
    if method not in ("PUT", "POST", "DELETE"):
        return response

    bucket = getattr(request.state, "s3_bucket", None)
    key = getattr(request.state, "s3_key", None)
    if bucket is None:
        bucket, key = parse_s3_path(request.url.path)
    if not bucket or not key:
        # Bucket-level invalidation (ACL flip, bucket delete) isn't supported:
        # stock ATS HTTP PURGE takes a literal cache key, not a glob. Objects
        # age out naturally within the 5-min TTL. Revisit via regex_revalidate
        # plugin if that turns out to be too long a window.
        return response

    qs = request.query_params

    if method == "PUT" and "partNumber" in qs:
        # MPU part upload — not visible until CompleteMultipartUpload, skip.
        return response

    host = request.headers.get("host", DEFAULT_HOST)
    is_complete_mpu = method == "POST" and "uploadId" in qs and "partNumber" not in qs
    if method in ("PUT", "DELETE") or is_complete_mpu:
        schedule_purge(host, f"{bucket}/{key}")
        # NOTE: x-amz-copy-source is deliberately NOT purged. COPY reads the
        # source; its contents haven't changed. Purging would needlessly cold
        # the cache for what could be a hot source object.

    return response
