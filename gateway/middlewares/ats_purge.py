from __future__ import annotations

from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from gateway.middlewares.acl import parse_s3_path
from gateway.services.ats_cache_client import schedule_purge


DEFAULT_HOST = "s3.hippius.com"


async def ats_purge_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    response = await call_next(request)
    if response.status_code >= 300:
        return response

    method = request.method
    if method not in ("PUT", "POST", "DELETE"):
        return response

    bucket, key = parse_s3_path(request.url.path)
    if not bucket:
        return response

    qs = request.query_params
    host = request.headers.get("host", DEFAULT_HOST)

    if not key:
        if method == "PUT" and "acl" in qs:  # noqa: SIM114 — kept distinct for readability
            schedule_purge(host, f"{bucket}/*")
        elif method == "DELETE":
            schedule_purge(host, f"{bucket}/*")
        return response

    if method == "PUT":
        if "partNumber" in qs:
            # MPU part upload — not visible until CompleteMultipartUpload, skip.
            return response
        schedule_purge(host, f"{bucket}/{key}")
        copy_source = request.headers.get("x-amz-copy-source")
        if copy_source:
            schedule_purge(host, copy_source.lstrip("/"))
    elif method == "DELETE":  # noqa: SIM114 — kept distinct for readability
        schedule_purge(host, f"{bucket}/{key}")
    elif method == "POST" and "uploadId" in qs and "partNumber" not in qs:
        schedule_purge(host, f"{bucket}/{key}")

    return response
