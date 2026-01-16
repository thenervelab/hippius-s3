"""Parse internal headers set by the gateway into request.state."""

import logging
import time
from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from hippius_s3.models.account import HippiusAccount
from hippius_s3.services.ray_id_service import get_logger_with_ray_id
from hippius_s3.services.ray_id_service import ray_id_context


logger = logging.getLogger(__name__)


async def parse_internal_headers_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """
    Parse X-Hippius-* headers injected by the gateway into request.state.

    The gateway sets these headers after authenticating the request:
    - X-Hippius-Ray-ID: Request tracing ID
    - X-Hippius-Account-Id: Subaccount ID from seed phrase
    - X-Hippius-Seed: Seed phrase
    - X-Hippius-Main-Account: Main account ID
    - X-Hippius-Has-Credits: Boolean credit status
    - X-Hippius-Can-Upload: Boolean upload permission
    - X-Hippius-Can-Delete: Boolean delete permission
    """
    ray_id = request.headers.get("X-Hippius-Ray-ID", "no-ray-id")
    ray_id_context.set(ray_id)
    request.state.ray_id = ray_id
    request.state.logger = get_logger_with_ray_id(__name__, ray_id)

    gateway_time_ms = request.headers.get("x-hippius-gateway-time-ms")
    if gateway_time_ms:
        request.state.gateway_time_ms = float(gateway_time_ms)

    request.state.api_start_time = time.time()

    request.state.request_user_id = request.headers.get("X-Hippius-Request-User", "")
    request.state.bucket_owner_id = request.headers.get("X-Hippius-Bucket-Owner", "")
    request.state.account_id = request.state.request_user_id
    request.state.seed_phrase = request.headers.get("X-Hippius-Seed", "")

    main_account = request.state.bucket_owner_id or request.headers.get("X-Hippius-Main-Account", "")

    request.state.account = HippiusAccount(
        id=request.state.account_id,
        main_account=main_account,
        has_credits=request.headers.get("X-Hippius-Has-Credits") == "True",
        upload=request.headers.get("X-Hippius-Can-Upload") == "True",
        delete=request.headers.get("X-Hippius-Can-Delete") == "True",
    )

    logger.info(
        f"Headers: request_user={request.state.request_user_id[:16]}..., "
        f"bucket_owner={request.state.bucket_owner_id[:16] if request.state.bucket_owner_id else 'NONE'}..., "
        f"main_account={main_account[:16]}..."
    )

    return await call_next(request)
