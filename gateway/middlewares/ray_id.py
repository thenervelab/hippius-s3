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

    IMPORTANT: This middleware should be registered LAST in gateway/main.py
    so it executes FIRST, ensuring ray_id is available to all other middlewares.
    """
    ray_id = generate_ray_id()
    ray_id_context.set(ray_id)
    request.state.ray_id = ray_id
    request.state.logger = get_logger_with_ray_id(__name__, ray_id)

    response = await call_next(request)

    response.headers["X-Hippius-Ray-ID"] = ray_id

    return response
