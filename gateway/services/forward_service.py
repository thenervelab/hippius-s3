import logging

import httpx
from fastapi import Request
from fastapi.responses import StreamingResponse


logger = logging.getLogger(__name__)


class ForwardService:
    def __init__(self, backend_url: str):
        self.backend_url = backend_url
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(300.0, connect=10.0),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
            follow_redirects=False,
        )
        logger.info(f"ForwardService initialized with backend: {backend_url}")

    async def forward_request(self, request: Request) -> StreamingResponse:
        headers = dict(request.headers)

        if hasattr(request.state, "account_id"):
            headers["X-Hippius-Account-Id"] = request.state.account_id
        if hasattr(request.state, "seed_phrase"):
            headers["X-Hippius-Seed"] = request.state.seed_phrase
        if hasattr(request.state, "account"):
            headers["X-Hippius-Main-Account"] = request.state.account.main_account
            headers["X-Hippius-Has-Credits"] = str(request.state.account.has_credits)
            headers["X-Hippius-Can-Upload"] = str(request.state.account.upload)
            headers["X-Hippius-Can-Delete"] = str(request.state.account.delete)

        for key in list(headers.keys()):
            if key.lower() in ["host", "x-forwarded-for", "x-forwarded-host"]:
                del headers[key]

        target_url = f"{self.backend_url}{request.url.path}"
        if request.url.query:
            target_url += f"?{request.url.query}"

        logger.debug(
            f"Forwarding {request.method} {target_url} (original: {request.url.path}?{request.url.query})"
        )

        if request.method in ["GET", "HEAD", "DELETE"]:
            response = await self.client.request(
                method=request.method,
                url=target_url,
                headers=headers,
            )
        else:
            async def generate():
                async for chunk in request.stream():
                    yield chunk

            response = await self.client.request(
                method=request.method,
                url=target_url,
                headers=headers,
                content=generate(),
            )

        response_headers = dict(response.headers)
        if "transfer-encoding" in response_headers:
            del response_headers["transfer-encoding"]

        return StreamingResponse(
            content=response.aiter_bytes(),
            status_code=response.status_code,
            headers=response_headers,
            media_type=response.headers.get("content-type"),
        )

    async def close(self):
        await self.client.aclose()
        logger.info("ForwardService client closed")
