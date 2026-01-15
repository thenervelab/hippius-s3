from __future__ import annotations

import logging
from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from hippius_s3.api.s3 import errors as s3_errors
from hippius_s3.fs_pressure import should_reject_fs_cache_write


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
    try:
        if not _should_gate_request(request):
            return await call_next(request)

        config = getattr(request.app.state, "config", None)
        if config is None:
            # Shouldn't happen; fail open.
            return await call_next(request)

        reject, retry_after, pressure, reason = should_reject_fs_cache_write(config=config)
        if not reject:
            return await call_next(request)

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
            extra_headers={"Retry-After": str(retry_after)},
        )
    except Exception:
        # Fail open to avoid taking down the API on middleware bug.
        logger.exception("fs_cache_pressure_middleware error (failing open)")
        return await call_next(request)
