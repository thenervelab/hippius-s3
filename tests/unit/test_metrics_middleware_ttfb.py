"""`http_request_ttfb_seconds` must measure time-to-first-byte, not what the other clocks measure.

`http_request_duration_seconds` is useless as a TTFB signal on the write path: the handler drains
the whole body before responding, so on a multi-GB PUT the histogram measures the client's
bandwidth. The TTFB metric instead stamps the FIRST body byte the app accepts (via the
`request._receive` wrap in metrics_middleware) and, for requests whose body is never read, the
moment the response starts. Its baseline is `gateway_start_time` — the outermost clock, stamped by
ray_id just inside CORS — so the auth/ACL chain above the metrics middleware is included.

These tests pin those three semantics, because each fails silently: a broken receive wrap degrades
TTFB into total duration, a lost gateway stamp quietly shrinks every sample by the outer chain, and
neither produces an error — just a metric that stops meaning what the dashboard says it means.
"""

from __future__ import annotations

import asyncio
import time
import types
from typing import Any
from unittest.mock import patch

from starlette.requests import Request
from starlette.responses import Response

from hippius_s3.api.middlewares.metrics import metrics_middleware


def get_object() -> None:  # route endpoint stand-in; only its __name__ matters
    pass


def _request(body: bytes, *, gateway_start_time: float | None) -> Request:
    scope: dict[str, Any] = {
        "type": "http",
        "method": "PUT" if body else "GET",
        "path": "/bucket/key",
        "raw_path": b"/bucket/key",
        "query_string": b"",
        "headers": [],
        "path_params": {},
        "route": types.SimpleNamespace(endpoint=get_object),
    }
    sent = False

    async def receive() -> dict[str, Any]:
        nonlocal sent
        if not sent:
            sent = True
            return {"type": "http.request", "body": body, "more_body": False}
        return {"type": "http.disconnect"}

    request = Request(scope, receive)
    if gateway_start_time is not None:
        request.state.gateway_start_time = gateway_start_time
    return request


def _run(request: Request, *, pre_body_seconds: float, post_body_seconds: float) -> dict[str, Any]:
    """Drive the middleware around a handler that works, drains the body, then works again."""

    async def call_next(req: Request) -> Response:
        await asyncio.sleep(pre_body_seconds)
        async for _ in req.stream():
            pass
        await asyncio.sleep(post_body_seconds)
        return Response(status_code=200)

    recorded: dict[str, Any] = {}

    class _Collector:
        def record_http_request(self, **kwargs: Any) -> None:
            recorded.update(kwargs)

        def record_error(self, **kwargs: Any) -> None:
            pass

    with patch("hippius_s3.api.middlewares.metrics.get_metrics_collector", lambda: _Collector()):
        asyncio.run(metrics_middleware(request, call_next))

    assert recorded, "the collector must be fed"
    return recorded


def test_upload_ttfb_is_time_to_first_accepted_byte_not_total_duration() -> None:
    """The whole point: 100 ms of pre-body work, 100 ms of drain-and-finish work.

    TTFB must report the first figure. If the receive wrap breaks, TTFB silently collapses into
    total duration and the metric stops isolating our latency from the client's bandwidth.
    """
    request = _request(b"x" * 64, gateway_start_time=time.time())
    recorded = _run(request, pre_body_seconds=0.100, post_body_seconds=0.100)

    assert 0.080 <= recorded["ttfb"] <= 0.170, "ttfb should be ~100 ms of pre-body work"
    assert recorded["duration"] >= 0.180, "duration should cover both halves"
    assert recorded["ttfb"] < recorded["duration"] - 0.050, "ttfb must not absorb the body drain"


def test_bodyless_request_measures_from_the_gateway_clock() -> None:
    """A GET's TTFB is response start — measured from `gateway_start_time`, not this middleware.

    The 200 ms offset simulates the auth/ACL chain above metrics_middleware; losing it would
    under-report TTFB by exactly the half of the request PR #469 proved is not small.
    """
    request = _request(b"", gateway_start_time=time.time() - 0.200)
    recorded = _run(request, pre_body_seconds=0.050, post_body_seconds=0.0)

    assert recorded["duration"] <= 0.150, "duration still measures only below this middleware"
    assert 0.230 <= recorded["ttfb"] <= 0.320, "ttfb must include the 200 ms outer chain"
    assert recorded["handler"] == "get_object", "ttfb shares the handler label of the other series"


def test_falls_back_to_the_local_clock_when_no_gateway_stamp_exists() -> None:
    """Without `gateway_start_time` the middleware's own clock is the honest baseline —
    a shrunken-but-real sample, never a crash and never a negative."""
    request = _request(b"", gateway_start_time=None)
    recorded = _run(request, pre_body_seconds=0.050, post_body_seconds=0.0)

    assert recorded["ttfb"] >= 0.0
    assert abs(recorded["ttfb"] - recorded["duration"]) <= 0.020, "with one clock the two must agree"
