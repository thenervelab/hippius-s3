from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from gateway.services.sub_token_scope import OP_LIST_BUCKETS
from gateway.services.sub_token_scope import evaluate as evaluate_sub_token_scope
from gateway.services.sub_token_scope import permission_allows
from gateway.services.sub_token_scope_cache import get_cached_sub_token_scope
from gateway.utils.errors import s3_error_response
from hippius_s3.models.acl import Permission
from hippius_s3.services.ray_id_service import get_logger_with_ray_id


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

    if "tagging" in query_params:
        return Permission.READ_ACP if method in ["GET", "HEAD"] else Permission.WRITE_ACP

    if "uploads" in query_params or "uploadId" in query_params:
        return Permission.WRITE

    if method in ["GET", "HEAD"]:
        return Permission.READ

    if method in ["PUT", "POST", "DELETE"]:
        return Permission.WRITE

    raise ValueError(f"Unknown HTTP method: {method}")


def _access_denied() -> Response:
    return s3_error_response(code="AccessDenied", message="Access Denied", status_code=403)


async def acl_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """
    ACL enforcement middleware that checks S3 permissions before forwarding requests.

    Blocks unauthorized requests with 403 AccessDenied.
    Allows requests that pass ACL checks to continue to backend.
    """
    ray_id = getattr(request.state, "ray_id", "no-ray-id")
    logger = get_logger_with_ray_id(__name__, ray_id)

    path = request.url.path

    if path == "/health" or path.startswith("/user/"):
        return await call_next(request)

    if request.method == "OPTIONS":
        return await call_next(request)

    bucket, key = parse_s3_path(path)
    query_params = dict(request.query_params)

    auth_method = getattr(request.state, "auth_method", None)
    token_type = getattr(request.state, "token_type", None)
    account_id = getattr(request.state, "account_id", None)
    access_key = (
        getattr(request.state, "access_key", None) if auth_method in ("access_key", "bearer_access_key") else None
    )

    acl_service = request.app.state.acl_service

    # Resolve bucket ownership / id once (if a bucket is in play).
    bucket_owner_id: str | None = None
    bucket_id: str | None = None
    if bucket is not None:
        bucket_owner_id = await acl_service.get_bucket_owner(bucket)
        if bucket_owner_id is not None:
            bucket_id = await acl_service.get_bucket_id(bucket)
            request.state.bucket_owner_id = bucket_owner_id

    # -------------------------------------------------------------------------
    # Sub-token branch (R2-style): authoritative for intra-account requests,
    # falls through to bucket ACL grants for cross-account (contractor) access.
    # -------------------------------------------------------------------------
    if auth_method in ("access_key", "bearer_access_key") and token_type == "sub" and access_key:
        is_cross_account = bucket_owner_id is not None and bucket_owner_id != account_id
        if not is_cross_account:
            repo = request.app.state.sub_token_scope_repo
            redis_client = request.app.state.redis_client
            scope = await get_cached_sub_token_scope(access_key, repo, redis_client)

            # ListBuckets: no bucket in play.
            if bucket is None:
                if scope is None or not permission_allows(scope.permission, OP_LIST_BUCKETS):
                    logger.info(
                        f"Sub-token ListBuckets denied: account={account_id}, "
                        f"permission={scope.permission if scope else 'none'}"
                    )
                    return _access_denied()
                logger.info(f"Sub-token ListBuckets allowed: account={account_id}, permission={scope.permission}")
                return await call_next(request)

            # Bucket does not exist yet AND this is CreateBucket (PUT /bucket, no key, no query).
            # evaluate_sub_token_scope handles the OP_CREATE_BUCKET check including scope='all'.
            if bucket_owner_id is None:
                is_create_bucket = request.method == "PUT" and key is None and len(query_params) == 0
                if not is_create_bucket:
                    # Non-create op on a nonexistent bucket: pass through so backend returns NoSuchBucket.
                    return await call_next(request)

            allowed, reason = evaluate_sub_token_scope(
                scope=scope,
                bucket_id=bucket_id,
                method=request.method,
                has_key=key is not None,
                query_params=query_params,
            )
            logger.info(
                f"Sub-token scope check: account={account_id}, bucket={bucket}, key={key or 'None'}, "
                f"method={request.method}, result={'GRANTED' if allowed else 'DENIED'}"
                f"{'' if allowed else f' ({reason})'}"
            )
            if not allowed:
                return _access_denied()

            request.state.is_anonymous_access = False
            return await call_next(request)
        # cross-account sub-token falls through to check_permission below.

    # -------------------------------------------------------------------------
    # Master token + all other auth paths below here.
    # -------------------------------------------------------------------------
    if bucket is None:
        return await call_next(request)

    is_create_bucket = request.method == "PUT" and key is None and len(query_params) == 0
    if is_create_bucket:
        # AWS S3 default: BucketOwnerEnforced enabled, ACLs disabled (since April 2023)
        x_amz_acl = request.headers.get("x-amz-acl")
        if x_amz_acl:
            logger.info(f"Rejecting CreateBucket with ACL header for bucket: {bucket}")
            return s3_error_response(
                code="InvalidBucketAclWithObjectOwnership",
                message="Bucket cannot be created with ACLs. Object Ownership is set to BucketOwnerEnforced.",
                status_code=400,
            )
        logger.info(f"Bypassing ACL check for CreateBucket: {bucket}")
        return await call_next(request)

    if bucket_owner_id is None:
        logger.info(f"Bucket not found in ACL check: {bucket}, passing through to backend for proper S3 error")
        return await call_next(request)

    if auth_method == "access_key" and token_type == "master" and bucket_owner_id == account_id:
        logger.info(f"Master token bypass for account {account_id} on bucket {bucket}")
        request.state.is_anonymous_access = False
        return await call_next(request)

    permission = get_required_permission(
        method=request.method,
        query_params=query_params,
        has_key=key is not None,
    )

    try:
        has_permission = await acl_service.check_permission(
            account_id=account_id,
            bucket=bucket,
            key=key,
            permission=permission,
            access_key=access_key,
            bucket_owner_id=bucket_owner_id,
        )
    except ValueError as e:
        if "Bucket not found" in str(e):
            logger.info(f"Bucket not found in ACL check: {bucket}, passing through to backend for proper S3 error")
            return await call_next(request)
        raise

    if not has_permission:
        logger.info(f"Access denied: account={account_id}, bucket={bucket}, key={key}, permission={permission.value}")
        return _access_denied()

    is_anonymous = account_id is None or account_id == "anonymous"
    request.state.is_anonymous_access = is_anonymous

    response = await call_next(request)

    if is_anonymous:
        response.headers["x-hippius-access-mode"] = "anon"

    return response
