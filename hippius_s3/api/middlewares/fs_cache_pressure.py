from __future__ import annotations

import logging
from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from hippius_s3.api.s3 import errors as s3_errors
from hippius_s3.fs_pressure import should_reject_fs_cache_write
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.pressure_signal import get_published_pressure_mode


logger = logging.getLogger(__name__)


def _should_gate_request(request: Request) -> bool:
    """Return True if this request is likely to write to the FS cache."""
    method = request.method.upper()
    if method != "PUT":
        return False

    # S3 object paths are "/{bucket}/{key...}".
    path = request.url.path.strip("/")
    if not path:
        return False
    bucket_and_key = path.split("/", 1)
    if len(bucket_and_key) < 2:
        return False

    # PUT /bucket/key is either PutObject or UploadPart; both write to FS cache.
    headers = request.headers
    if "chunked" in headers.get("transfer-encoding", "").lower():
        return True

    for key in ("x-amz-decoded-content-length", "content-length"):
        raw = headers.get(key)
        if raw is None:
            continue
        try:
            return int(raw) > 0
        except (TypeError, ValueError):
            return True

    return False


async def fs_cache_pressure_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    if not _should_gate_request(request):
        return await call_next(request)

    config = getattr(request.app.state, "config", None)
    if config is None:
        return await call_next(request)

    # Janitor-published pool signal (memoized ~5s; None = unavailable → the
    # local statvfs check alone governs, which is the pre-signal behavior).
    published_mode = await get_published_pressure_mode(getattr(request.app.state, "redis_client", None))
    reject, retry_after, pressure, reason = should_reject_fs_cache_write(config=config, published_mode=published_mode)
    if not reject:
        return await call_next(request)

    # This middleware is registered LAST, i.e. it is the OUTERMOST — it has to answer before the
    # body is read. metrics_middleware is registered first, i.e. innermost, so it never runs on a
    # shed request: the 503 lands in no request counter and no error counter on either side (the
    # gateway just proxies it through). Record it here or a pressure event is invisible outside
    # the log line below.
    # Both labels are bounded: reason is threshold|pool, published_mode is 0|1|2|None.
    get_metrics_collector().record_fs_cache_shed(reason=reason, pressure_mode=str(published_mode))

    # IMPORTANT: return BEFORE reading request body to avoid moving pressure to RAM.
    logger.warning(
        "FS cache pressure: rejecting request method=%s path=%s free_bytes=%s free_ratio=%.4f reason=%s",
        request.method,
        request.url.path,
        int(pressure.free_bytes),
        float(pressure.free_ratio),
        reason,
    )
    return s3_errors.s3_error_response(
        code="SlowDown",
        message="Upload temporarily throttled due to filesystem cache pressure. Please retry.",
        status_code=503,
        extra_headers={"Retry-After": str(max(1, round(retry_after)))},
    )
