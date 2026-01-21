"""Performance profiling middleware for debugging."""

import logging
import pathlib
from datetime import datetime
from datetime import timezone

import aiofiles
from fastapi import Request
from pyinstrument import Profiler
from pyinstrument.renderers.speedscope import SpeedscopeRenderer
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.base import RequestResponseEndpoint
from starlette.responses import Response
from starlette.types import ASGIApp

from hippius_s3.config import get_config


config = get_config()
logger = logging.getLogger(__name__)


class SpeedscopeProfilerMiddleware(BaseHTTPMiddleware):
    """Profile requests when ENABLE_REQUEST_PROFILING environment variable is set to true."""

    def __init__(self, app: ASGIApp) -> None:
        super().__init__(app)
        # Create profiler_runs directory if it doesn't exist
        self.profile_dir = pathlib.Path("profiler_runs")
        self.profile_dir.mkdir(exist_ok=True)

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        """Profile the current request if environment variable is set.

        Args:
            request: FastAPI request
            call_next: Function to call at the end of the request

        Returns:
            Response from the next middleware/handler
        """
        if not config.enable_request_profiling:
            return await call_next(request)

        # Only profile S3 requests (exclude /user paths)
        if str(request.url.path).startswith("/user"):
            return await call_next(request)

        # Generate filename components before profiling
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")[:-3]  # microseconds to milliseconds
        method = request.method
        # Clean URL path for filename (replace / with _)
        url_path = str(request.url.path).replace("/", "_").replace("?", "_")
        if url_path.startswith("_"):
            url_path = url_path[1:]  # Remove leading underscore

        logger.info(f"Starting profiling for: {method} {request.url.path}")

        # Profile the request by interrupting every 1ms and recording the stack
        with Profiler(interval=0.001, async_mode="enabled") as profiler:
            response = await call_next(request)

        # Extract account info AFTER all middlewares have run
        account = getattr(request.state, "account", None)
        account_id = getattr(account, "main_account", "unknown") if account else "unknown"

        # Generate final filename with captured account info
        filename = f"{timestamp}_{account_id}_{method}_{url_path}.speedscope.json"
        profile_path = self.profile_dir / filename

        # Write profiling data to file
        renderer = SpeedscopeRenderer()
        async with aiofiles.open(profile_path, mode="w") as out:
            await out.write(profiler.output(renderer=renderer))

        logger.info(f"Profile saved to {profile_path} (account: {account_id})")

        return response
