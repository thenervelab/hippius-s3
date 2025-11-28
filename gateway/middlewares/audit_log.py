import time
from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from hippius_s3.services.audit_service import AuditLogger


audit_logger = AuditLogger("audit")


async def audit_log_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    path = str(request.url.path)
    client_ip = request.client.host if request.client else "unknown"

    if audit_logger.should_skip(path, client_ip):
        return await call_next(request)

    start_time = time.time()

    user_agent = request.headers.get("user-agent", "unknown")
    method = request.method
    query_params = dict(request.query_params)

    account = getattr(request.state, "account", None)
    account_id = getattr(account, "main_account", "unknown") if account else "unknown"
    ray_id = getattr(request.state, "ray_id", "no-ray-id")

    response = await call_next(request)

    processing_time = time.time() - start_time

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
