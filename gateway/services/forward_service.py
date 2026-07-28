import asyncio
import logging
import time
import typing

import httpx
from fastapi import Request
from fastapi import Response
from fastapi.responses import StreamingResponse
from starlette.requests import ClientDisconnect

from hippius_s3.monitoring import get_metrics_collector


logger = logging.getLogger(__name__)

# Non-standard status used by nginx for "client closed the request before we answered".
# Nothing is written to the socket — uvicorn's send() short-circuits on a disconnected peer, so
# this never reaches the access log either. It exists purely so the gateway's own audit/metrics
# middlewares see a classified response instead of an exception tearing through the stack.
_CLIENT_CLOSED_REQUEST = 499

# Methods that are safe to re-send when the upstream connection dies before any response
# byte exists. Deliberately excludes DELETE: it is idempotent by S3 semantics, but a retry
# after an ambiguous failure can turn one caller's delete into a second version-deleting
# call, so it stays single-shot.
_RETRIABLE_METHODS = frozenset({"GET", "HEAD"})

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


def _trusted_hippius_headers(request: Request) -> dict[str, str]:
    """Build the trusted X-Hippius-* context headers injected into forwarded requests.

    The gateway strips any client-supplied X-Hippius-* headers and replaces them with these,
    derived from authenticated request.state. X-Hippius-Gateway-Time-Ms is added separately by
    forward_request since it depends on elapsed time.
    """
    headers: dict[str, str] = {}

    if hasattr(request.state, "ray_id"):
        headers["X-Hippius-Ray-ID"] = request.state.ray_id

    if hasattr(request.state, "account_id"):
        headers["X-Hippius-Request-User"] = request.state.account_id

    bucket_owner = getattr(request.state, "bucket_owner_id", None) or getattr(request.state, "account_id", "")
    if bucket_owner:
        headers["X-Hippius-Bucket-Owner"] = bucket_owner
        headers["X-Hippius-Main-Account"] = bucket_owner

    # Forwarded so the API can skip its own get_bucket_by_name lookup (resolved by the ACL middleware).
    bucket_id = getattr(request.state, "bucket_id", None)
    if bucket_id:
        headers["X-Hippius-Bucket-Id"] = str(bucket_id)

    if hasattr(request.state, "account"):
        headers["X-Hippius-Has-Credits"] = str(request.state.account.has_credits)
        headers["X-Hippius-Can-Upload"] = str(request.state.account.upload)
        headers["X-Hippius-Can-Delete"] = str(request.state.account.delete)

    return headers


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
    # Strip headers that the gateway's own server layer will add,
    # preventing duplicates (e.g. "server: uvicorn, uvicorn").
    single_value_headers = {"date", "server"}
    out: list[tuple[bytes, bytes]] = []
    for k, v in raw_headers:
        name = k.decode("latin-1").lower()
        if name in hop or name in single_value_headers:
            continue
        out.append((k, v))
    return out


class ForwardService:
    def __init__(self, backend_url: str):
        self.backend_url = backend_url
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(300.0, connect=10.0),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=100, keepalive_expiry=30),
            follow_redirects=False,
        )
        logger.info(f"ForwardService initialized with backend: {backend_url}")

    async def forward_request(self, request: Request) -> Response:
        headers = dict(request.headers)

        # SECURITY: Strip any client-provided X-Hippius-* headers to prevent header injection attacks
        for key in list(headers.keys()):
            if key.lower().startswith("x-hippius-"):
                del headers[key]
                logger.warning(f"Stripped client-provided header: {key}")

        # Add authenticated context headers from gateway
        headers.update(_trusted_hippius_headers(request))

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

        bytes_received = 0
        bytes_sent = 0

        async def request_body_iter() -> typing.AsyncGenerator[bytes, None]:
            nonlocal bytes_received
            async for chunk in request.stream():
                bytes_received += len(chunk)
                yield chunk

        # A pooled keep-alive connection to the API can be closed under us at any time — most
        # visibly when an api pod is rolled, which shows up here as RemoteProtocolError before
        # a single response byte exists. Retrying is only sound when we can reproduce the
        # request exactly, and `request.stream()` is one-shot: it cannot be replayed. So the
        # retry is limited to bodyless idempotent requests, where there is nothing to replay.
        has_body = request.headers.get("content-length", "").strip() not in ("", "0") or (
            "chunked" in request.headers.get("transfer-encoding", "").lower()
        )
        can_retry = request.method in _RETRIABLE_METHODS and not has_body
        send_body = request.method != "HEAD" and has_body

        # Stream backend response for ALL methods (prevents buffering large downloads in gateway memory).
        # Important: We must keep the upstream httpx stream open until the client finishes consuming.
        for attempt in range(2 if can_retry else 1):
            stream_cm = self.client.stream(
                method=request.method,
                url=target_url,
                headers=headers,
                content=request_body_iter() if send_body else None,
            )
            try:
                upstream_response = await stream_cm.__aenter__()
                break
            except ClientDisconnect:
                # The client hung up while we were still pumping its body upstream. This is normal
                # client behaviour (SDK timeout, ^C, reset), not a server fault, and it is not
                # retryable — request.stream() is one-shot. Letting it escape makes uvicorn log a
                # full middleware-deep ASGI traceback and ship it to Sentry, which drowns the real
                # 5xx. Measured on prod 2026-07-26..28: 13650 of these in 48h, ~71% of all gateway
                # ASGI exceptions, and ~9% of one customer's part uploads.
                #
                # WARNING rather than INFO on purpose: an abort is not always the client's fault.
                # The API can answer early (fs_cache_pressure sheds PUTs with a 503 before reading
                # the body), but httpx will not surface that response until the whole body is sent,
                # so our own backpressure reaches this same branch looking like a client abort.
                # Losing the ERROR that used to fire here must not mean losing the signal entirely.
                logger.warning(
                    "Client disconnected while sending request body: %s %s (%d bytes received, no upstream response)",
                    request.method,
                    request.url.path,
                    bytes_received,
                )
                # Ingress already pulled off the client still counts. This is the only place it can
                # be recorded — iter_upstream()'s finally, which normally does it, never runs here.
                get_metrics_collector().record_gateway_bandwidth(
                    bytes_received=bytes_received,
                    bytes_sent=0,
                    method=request.method,
                    status_code=_CLIENT_CLOSED_REQUEST,
                )
                return Response(status_code=_CLIENT_CLOSED_REQUEST)
            except (httpx.RemoteProtocolError, httpx.ConnectError) as exc:
                if not can_retry or attempt == 1:
                    raise
                logger.warning(
                    "Upstream connection died before a response (%s); retrying %s %s once",
                    type(exc).__name__,
                    request.method,
                    request.url.path,
                )

        # Preserve multi-value headers (e.g. Set-Cookie) by forwarding raw headers.
        filtered_raw = _filter_hop_by_hop_raw_headers(list(upstream_response.headers.raw))

        async def iter_upstream() -> typing.AsyncGenerator[bytes, None]:
            nonlocal bytes_sent
            try:
                async for chunk in upstream_response.aiter_bytes():
                    bytes_sent += len(chunk)
                    yield chunk
            finally:
                # Record bandwidth metrics after streaming completes
                collector = get_metrics_collector()
                collector.record_gateway_bandwidth(
                    bytes_received=bytes_received,
                    bytes_sent=bytes_sent,
                    method=request.method,
                    status_code=upstream_response.status_code,
                )
                # Detect truncated responses before uvicorn raises RuntimeError
                expected_raw = upstream_response.headers.get("content-length")
                if expected_raw and bytes_sent < int(expected_raw):
                    logger.warning(
                        "Incomplete upstream stream: sent=%d expected=%s method=%s path=%s status=%d",
                        bytes_sent,
                        expected_raw,
                        request.method,
                        request.url.path,
                        upstream_response.status_code,
                    )
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
