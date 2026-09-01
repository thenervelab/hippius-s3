from __future__ import annotations

from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from hippius_s3.config import get_config
from hippius_s3.gateway.middlewares.acl import parse_s3_path
from hippius_s3.gateway.services.ats_cache_client import schedule_purge


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

    # A brand-new key has nothing cached to purge (edges never store 404s — negative caching is
    # off), yet purges fired for every write and creations dominate: measured on the EU edge
    # 2026-08-31, PURGE was 24% of ALL requests through ATS, effectively every one answering 404,
    # and their stripe write locks are the standing suspect for the pre-cache-lookup GET stall
    # (ttfb-report.md).
    #
    # The handler sets the flag only when the write allocated object version 1; absent flag ->
    # purge, so every unaudited path (overwrite, delete, CompleteMultipartUpload, copy) keeps
    # today's behaviour. Two deliberate limits: PUT-scoped, so a stray flag can never suppress a
    # DELETE's purge; and warm buckets never skip — they cache for 30 days on a purge-on-write
    # contract (cache_control.py), and version 1 recurs when the objects row was removed wholesale
    # (bucket name reused after delete, janitor hard-delete), where a dropped delete-time purge
    # could have left a stale entry. For normal buckets that corner is bounded by max-age=300, the
    # staleness any dropped purge already costs.
    if (
        method == "PUT"
        and getattr(request.state, "ats_object_created", False)
        and not getattr(request.state, "bucket_is_cache_warm", False)
    ):
        return response

    is_complete_mpu = method == "POST" and "uploadId" in qs and "partNumber" not in qs
    if method in ("PUT", "DELETE") or is_complete_mpu:
        # TODO: CompleteMultipartUpload resolves its version too; wire the same created-flag there.
        schedule_purge(get_config().ats_purge_host, f"{bucket}/{key}")
        # NOTE: x-amz-copy-source is deliberately NOT purged. COPY reads the
        # source; its contents haven't changed. Purging would needlessly cold
        # the cache for what could be a hot source object.

    return response
