import time
from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from hippius_s3.services.audit_service import AuditLogger
from hippius_s3.services.ray_id_service import get_logger_with_ray_id


base_audit_logger = AuditLogger("audit")


async def audit_log_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    path = str(request.url.path)
    client_ip = request.client.host if request.client else "unknown"

    if base_audit_logger.should_skip(path, client_ip):
        return await call_next(request)

    start_time = time.time()

    user_agent = request.headers.get("user-agent", "unknown")
    method = request.method
    query_params = dict(request.query_params)

    response = await call_next(request)

    # The audit log attributes operations to the CALLER. state.account carries caller
    # semantics only (request_context never rebinds it — bucket-owner attribution lives
    # under the separate state.main_account_id), so reading it after the inner stack ran
    # is safe: anonymous public reads and cross-account ops book to the caller, not the
    # bucket owner.
    account = getattr(request.state, "account", None)
    # request_context binds an EMPTY stand-in for anonymous callers; "" must still log
    # as "unknown", matching what a missing account always logged.
    account_id = (getattr(account, "main_account", "") if account else "") or "unknown"

    processing_time = time.time() - start_time

    ray_id = getattr(request.state, "ray_id", "no-ray-id")

    ray_id_logger = get_logger_with_ray_id("audit", ray_id)
    audit_logger = AuditLogger("audit", logger=ray_id_logger)
    audit_logger.log_request(
        client_ip=client_ip,
        user_agent=user_agent,
        account_id=account_id,
        method=method,
        path=path,
        query_params=query_params,
        status_code=response.status_code,
        processing_time_ms=processing_time * 1000,
        content_length=response.headers.get("content-length", 0),
        timestamp=time.time(),
        ray_id=ray_id,
    )

    return response
