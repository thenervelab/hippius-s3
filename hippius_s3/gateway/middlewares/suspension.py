from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from hippius_s3.gateway.middlewares.acl import parse_s3_path
from hippius_s3.gateway.services.suspension import MODE_FULL
from hippius_s3.gateway.services.suspension import get_account_suspension
from hippius_s3.gateway.services.suspension import suspension_blocks
from hippius_s3.gateway.utils.errors import s3_error_response
from hippius_s3.services.ray_id_service import get_logger_with_ray_id


async def suspension_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """Account-level suspension gate (issue #421).

    Runs immediately inner to auth_router so identity is resolved but nothing has been
    clobbered: keyed on request.state.account_address (the main-account SS58, identical
    for master tokens, sub-tokens, bearer and presigned auth — account_id is NOT usable
    here, account_middleware rewrites it to "anonymous" for bearer). Must stay OUTER to
    acl_middleware: master tokens bypass ACL on their own buckets, so a check inside ACL
    would never see them.

    Anonymous requests carry no identity and pass through — reads of a suspended owner's
    public buckets are blocked by the bucket-owner check inside acl_middleware instead.
    """
    if request.method == "OPTIONS":
        return await call_next(request)

    path = request.url.path
    if path == "/health" or path.startswith("/user/") or path.startswith("/admin/"):
        return await call_next(request)

    account_address = getattr(request.state, "account_address", None)
    if not account_address:
        return await call_next(request)

    mode = await get_account_suspension(
        account_address,
        request.app.state.postgres_pool,
        request.app.state.redis_client,
    )
    if mode is None:
        return await call_next(request)

    _bucket, key = parse_s3_path(path)
    if suspension_blocks(mode, method=request.method, query_params=dict(request.query_params), has_key=key is not None):
        ray_id = getattr(request.state, "ray_id", "no-ray-id")
        logger = get_logger_with_ray_id(__name__, ray_id)
        logger.info(f"Blocked request from suspended account: account={account_address}, mode={mode}")
        if mode == MODE_FULL:
            return s3_error_response(
                code="AccessDenied",
                message="Account suspended. Contact support.",
                status_code=403,
            )
        return s3_error_response(
            code="AccessDenied",
            message="Account is suspended for writes (read-only). Contact support.",
            status_code=403,
        )

    return await call_next(request)
