"""Audit logging middleware for S3 operations."""

import json
import logging
import time
from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response


logger = logging.getLogger("audit")


async def audit_log_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """Log all S3 operations for security and compliance."""
    start_time = time.time()

    # Extract key information before processing
    client_ip = request.client.host if request.client else "unknown"
    user_agent = request.headers.get("user-agent", "unknown")
    method = request.method
    path = str(request.url.path)
    query_params = dict(request.query_params)

    # Extract account info if available (after HMAC middleware)
    account = getattr(request.state, "account", None)
    account_id = getattr(account, "main_account", "unknown") if account else "unknown"

    # Process request
    response = await call_next(request)

    # Calculate processing time
    processing_time = time.time() - start_time

    # Log the operation
    audit_data = {
        "timestamp": time.time(),
        "client_ip": client_ip,
        "user_agent": user_agent,
        "account_id": account_id,
        "method": method,
        "path": path,
        "query_params": query_params,
        "status_code": response.status_code,
        "processing_time_ms": round(processing_time * 1000, 2),
        "content_length": response.headers.get("content-length", 0),
    }

    # Log different levels based on status code
    if response.status_code >= 500:
        logger.error(f"S3_OPERATION_ERROR: {json.dumps(audit_data)}")
    elif response.status_code >= 400:
        logger.warning(f"S3_OPERATION_CLIENT_ERROR: {json.dumps(audit_data)}")
    else:
        logger.info(f"S3_OPERATION_SUCCESS: {json.dumps(audit_data)}")

    return response
