import asyncio
import logging
import time
import typing

import httpx
from fastapi import Request
from fastapi.responses import StreamingResponse


logger = logging.getLogger(__name__)

_HOP_BY_HOP_HEADERS = {
    # RFC 7230 hop-by-hop headers that proxies should not forward end-to-end
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}


def _filter_hop_by_hop_raw_headers(raw_headers: list[tuple[bytes, bytes]]) -> list[tuple[bytes, bytes]]:
    """
    Filter hop-by-hop headers from raw (multi-value preserving) header list.

    RFC 7230: In addition to the standard hop-by-hop headers, any header field-name
    listed in the `Connection` header is also hop-by-hop and must not be forwarded.
    """
    connection_tokens: set[str] = set()
    for k, v in raw_headers:
        if k.lower() == b"connection":
            try:
                toks = v.decode("latin-1").split(",")
            except Exception:
                toks = []
            connection_tokens |= {t.strip().lower() for t in toks if t.strip()}

    hop = _HOP_BY_HOP_HEADERS | connection_tokens
    out: list[tuple[bytes, bytes]] = []
    for k, v in raw_headers:
        if k.decode("latin-1").lower() in hop:
            continue
        out.append((k, v))
    return out


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

        # SECURITY: Strip any client-provided X-Hippius-* headers to prevent header injection attacks
        for key in list(headers.keys()):
            if key.lower().startswith("x-hippius-"):
                del headers[key]
                logger.warning(f"Stripped client-provided header: {key}")

        # Add authenticated context headers from gateway
        if hasattr(request.state, "ray_id"):
            headers["X-Hippius-Ray-ID"] = request.state.ray_id

        if hasattr(request.state, "account_id"):
            headers["X-Hippius-Request-User"] = request.state.account_id

        bucket_owner = getattr(request.state, "bucket_owner_id", None) or getattr(request.state, "account_id", "")
        if bucket_owner:
            headers["X-Hippius-Bucket-Owner"] = bucket_owner
            headers["X-Hippius-Main-Account"] = bucket_owner

        if hasattr(request.state, "seed_phrase"):
            headers["X-Hippius-Seed"] = request.state.seed_phrase
        if hasattr(request.state, "account"):
            headers["X-Hippius-Has-Credits"] = str(request.state.account.has_credits)
            headers["X-Hippius-Can-Upload"] = str(request.state.account.upload)
            headers["X-Hippius-Can-Delete"] = str(request.state.account.delete)

        # Remove proxy-related headers that shouldn't be forwarded
        for key in list(headers.keys()):
            if key.lower() in ["host", "x-forwarded-for", "x-forwarded-host"]:
                del headers[key]

        if hasattr(request.state, "gateway_start_time"):
            gateway_time_ms = (time.time() - request.state.gateway_start_time) * 1000
            headers["X-Hippius-Gateway-Time-Ms"] = str(round(gateway_time_ms, 2))

        target_url = f"{self.backend_url}{request.scope['path']}"
        if request.url.query:
            target_url += f"?{request.url.query}"

        logger.debug(f"Forwarding {request.method} {target_url} (original: {request.url.path}?{request.url.query})")

        async def request_body_iter() -> typing.AsyncGenerator[bytes, None]:
            async for chunk in request.stream():
                yield chunk

        # Stream backend response for ALL methods (prevents buffering large downloads in gateway memory).
        # Important: We must keep the upstream httpx stream open until the client finishes consuming.
        stream_cm = self.client.stream(
            method=request.method,
            url=target_url,
            headers=headers,
            content=None if request.method == "HEAD" else request_body_iter(),
        )
        upstream_response = await stream_cm.__aenter__()

        # Preserve multi-value headers (e.g. Set-Cookie) by forwarding raw headers.
        filtered_raw = _filter_hop_by_hop_raw_headers(list(upstream_response.headers.raw))

        async def iter_upstream() -> typing.AsyncGenerator[bytes, None]:
            try:
                async for chunk in upstream_response.aiter_bytes():
                    yield chunk
            finally:
                # Always close the upstream stream to avoid leaking connections.
                # Shield cleanup so the connection returns to the pool even if cancelled.
                try:
                    await asyncio.shield(stream_cm.__aexit__(None, None, None))
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Failed to close upstream stream")

        # NOTE: For HEAD, StreamingResponse will just send headers; our iterator yields nothing.
        resp = StreamingResponse(
            content=iter_upstream(),
            status_code=upstream_response.status_code,
            media_type=None,  # we forward backend Content-Type (and all other headers) via raw_headers
        )
        resp.raw_headers = filtered_raw
        return resp

    async def close(self) -> None:
        await self.client.aclose()
        logger.info("ForwardService client closed")
