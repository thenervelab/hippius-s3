import logging
from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from gateway.utils.errors import s3_error_response
from hippius_s3.models.acl import Permission


logger = logging.getLogger(__name__)


def parse_s3_path(path: str) -> tuple[str | None, str | None]:
    """
    Parse S3 path into bucket and key components.

    Returns:
        (bucket, key) tuple where:
        - (None, None) for root path /
        - (bucket, None) for bucket-only paths
        - (bucket, key) for object paths
    """
    if path == "/" or path == "":
        return None, None

    path_stripped = path.lstrip("/")
    if not path_stripped:
        return None, None

    parts = path_stripped.split("/", 1)
    bucket = parts[0] if parts else None
    key = parts[1] if len(parts) > 1 else None

    return bucket, key


def get_required_permission(
    method: str,
    query_params: dict,
    has_key: bool,
) -> Permission:
    """
    Determine required permission from HTTP method and query parameters.

    Args:
        method: HTTP method (GET, PUT, POST, DELETE, HEAD)
        query_params: Query parameters dict
        has_key: Whether request has an object key

    Returns:
        Required Permission enum value
    """
    if "acl" in query_params:
        return Permission.READ_ACP if method == "GET" else Permission.WRITE_ACP

    if method in ["GET", "HEAD"]:
        return Permission.READ

    if method in ["PUT", "POST", "DELETE"]:
        return Permission.WRITE

    raise ValueError(f"Unknown HTTP method: {method}")


async def acl_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """
    ACL enforcement middleware that checks S3 permissions before forwarding requests.

    Blocks unauthorized requests with 403 AccessDenied.
    Allows requests that pass ACL checks to continue to backend.
    """
    path = request.url.path

    if path == "/health" or path.startswith("/user/"):
        return await call_next(request)

    if request.method == "OPTIONS":
        return await call_next(request)

    bucket, key = parse_s3_path(path)

    if bucket is None:
        return await call_next(request)

    query_params = dict(request.query_params)

    is_create_bucket = request.method == "PUT" and key is None and len(query_params) == 0
    if is_create_bucket:
        logger.info(f"Bypassing ACL check for CreateBucket: {bucket}")
        return await call_next(request)

    permission = get_required_permission(
        method=request.method,
        query_params=query_params,
        has_key=key is not None,
    )

    account_id = getattr(request.state, "account_id", None)

    acl_service = request.app.state.acl_service

    check_key = key if permission in [Permission.READ, Permission.READ_ACP] else None

    try:
        has_permission = await acl_service.check_permission(
            account_id=account_id,
            bucket=bucket,
            key=check_key,
            permission=permission,
        )
    except ValueError as e:
        if "Bucket not found" in str(e):
            logger.info(f"Bucket not found in ACL check: {bucket}, passing through to backend for proper S3 error")
            return await call_next(request)
        raise

    if not has_permission:
        logger.info(f"Access denied: account={account_id}, bucket={bucket}, key={key}, permission={permission.value}")
        return s3_error_response(
            code="AccessDenied",
            message="Access Denied",
            status_code=403,
        )

    bucket_owner_id = await acl_service.get_bucket_owner(bucket)
    if bucket_owner_id is None:
        logger.info(f"Bucket not found in ACL check: {bucket}, passing through to backend for proper S3 error")
        return await call_next(request)

    request.state.bucket_owner_id = bucket_owner_id

    return await call_next(request)
