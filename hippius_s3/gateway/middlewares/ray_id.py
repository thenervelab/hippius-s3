import os
import time
from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from hippius_s3.services.ray_id_service import generate_ray_id
from hippius_s3.services.ray_id_service import get_logger_with_ray_id
from hippius_s3.services.ray_id_service import ray_id_context


async def ray_id_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """Ray ID middleware that generates a unique ID for each request.

    This middleware:
    1. Generates a unique 16-char hex ray ID
    2. Sets ray_id in contextvar for automatic logging across all loggers
    3. Sets request.state.ray_id for use by downstream middlewares
    4. Creates a logger adapter with ray_id for request-scoped logging
    5. Adds X-Hippius-Ray-ID response header for client visibility
    6. Adds X-Hippius-Node (the k8s node this pod runs on) when NODE_NAME is set, so a
       slow or failed GET can be tied to the pod that served it without a log search

    IMPORTANT: This middleware must be registered second-outermost in gateway/main.py
    (just inside cors_middleware) so it executes near-first — stamping ray_id and
    gateway_start_time before auth/acl/account run — while CORS stays outermost to wrap
    error responses. See the registration block in gateway/main.py.
    """
    ray_id = generate_ray_id()
    ray_id_context.set(ray_id)
    request.state.ray_id = ray_id
    request.state.logger = get_logger_with_ray_id(__name__, ray_id)
    request.state.gateway_start_time = time.time()

    response = await call_next(request)

    response.headers["X-Hippius-Ray-ID"] = ray_id
    node_name = os.environ.get("NODE_NAME", "")
    if node_name:
        response.headers["X-Hippius-Node"] = node_name

    return response
