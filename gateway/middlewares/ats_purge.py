from __future__ import annotations

from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from gateway.config import get_config
from gateway.middlewares.acl import parse_s3_path
from gateway.services.ats_cache_client import schedule_purge


DEFAULT_HOST = "s3.hippius.com"


def _public_host(request: Request) -> str:
    # The cache key in ATS is built from the inbound URL the GET hit, so the
    # PURGE must carry the same public Host. By the time the request reaches
    # the gateway pod, the on-wire `Host` has been rewritten by ATS to the
    # upstream NodePort (or by in-cluster callers to the k8s service DNS),
    # which would produce a different cache key. The original public host is
    # preserved on `x-forwarded-host` / `x-original-host` — same fallback
    # chain SigV4 uses (gateway/middlewares/sigv4.py).
    host = (
        request.headers.get("x-forwarded-host")
        or request.headers.get("x-original-host")
        or request.headers.get("host")
        or DEFAULT_HOST
    )
    # cachekey.so includes scheme://host[:port]/path; default ports are
    # implicit so '<host>:80' and '<host>:443' must collapse to '<host>'.
    if host.endswith(":80") or host.endswith(":443"):
        host = host.rsplit(":", 1)[0]
    return host


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

    host = _public_host(request)
    is_complete_mpu = method == "POST" and "uploadId" in qs and "partNumber" not in qs
    if method in ("PUT", "DELETE") or is_complete_mpu:
        schedule_purge(host, f"{bucket}/{key}")
        # NOTE: x-amz-copy-source is deliberately NOT purged. COPY reads the
        # source; its contents haven't changed. Purging would needlessly cold
        # the cache for what could be a hot source object.

    return response
