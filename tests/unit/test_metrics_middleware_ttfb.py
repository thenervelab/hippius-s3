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

import pytest
from starlette.requests import Request
from starlette.responses import Response

from hippius_s3.api.middlewares import metrics as metrics_module
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


def test_zero_length_body_still_stamps_the_first_byte(monkeypatch: Any) -> None:
    """A `Content-Length: 0` PUT arrives as one EMPTY `http.request` message.

    Testing the body for truthiness excluded exactly that case, so a zero-byte PUT silently fell
    back to response-start and measured something different from every other PUT on the same panel.

    Driven by a FAKE CLOCK rather than sleeps: the assertion is about which timestamp is chosen,
    and a wall-clock version of it can only be expressed as millisecond margins that a loaded CI
    runner overshoots (an earlier draft was reproducibly flaky under CPU contention).
    """
    ticks = iter([100.0, 100.5, 103.0])  # start_time, first-body-byte, response start
    monkeypatch.setattr(metrics_module.time, "time", lambda: next(ticks))

    request = _request(b"", gateway_start_time=None)
    request.scope["method"] = "PUT"
    request.state.gateway_start_time = 100.0
    recorded = _run(request, pre_body_seconds=0.0, post_body_seconds=0.0)

    # first-body-byte is at 100.5, response start at 103.0. Stamping the body gives 0.5;
    # falling back to response start would give 3.0.
    assert recorded["ttfb"] == pytest.approx(0.5), "a zero-byte PUT must stamp at body read, not response start"
    assert recorded["duration"] == pytest.approx(3.0)


# ---------------------------------------------------------------------------------------------
# The wrap has to survive the REAL stack, not just a direct call.
#
# In production `metrics_middleware` is one of ~20 BaseHTTPMiddleware layers, and each one wraps
# the request in its own `_CachedRequest`, delegating `receive` down the chain. The tests above
# call the middleware directly with a hand-rolled receive, so they would keep passing if that
# delegation ever broke — from a reordering, or a Starlette upgrade changing `_CachedRequest`.
# The result would be a TTFB metric that silently degrades into total duration.
# ---------------------------------------------------------------------------------------------


def _stack_app(*, layers: int, post_body_seconds: float) -> tuple[Any, dict[str, Any]]:
    """A real FastAPI app with `layers` passthrough middlewares on each side of metrics_middleware.

    Annotations here MUST resolve against this module's globals: the file uses
    `from __future__ import annotations`, so FastAPI reads the handler's parameter types as strings
    and evaluates them in module scope. A `Request` imported inside this function is invisible
    there, and FastAPI silently degrades the parameter to a query field (422) instead of injecting
    the request.
    """
    from fastapi import FastAPI

    app = FastAPI()

    async def passthrough(request: Request, call_next: Any) -> Response:
        return await call_next(request)

    # Registered on BOTH sides so metrics_middleware sits mid-stack exactly as it does in
    # production: the handler's body read then travels down through every layer beneath it.
    for _ in range(layers):
        app.middleware("http")(passthrough)
    app.middleware("http")(metrics_middleware)
    for _ in range(layers):
        app.middleware("http")(passthrough)

    @app.put("/{bucket_name}/{object_key:path}")
    async def put_object(bucket_name: str, object_key: str, request: Request) -> Response:
        body = await request.body()
        await asyncio.sleep(post_body_seconds)
        return Response(status_code=200, content=str(len(body)))

    return app, {}


def _run_through_stack_with_clock(body: bytes, *, layers: int = 6) -> dict[str, Any]:
    """Drive the real stack against a FAKE clock.

    Wall-clock margins are what made an earlier draft of these flaky: `asyncio.sleep` can only
    overshoot, and under CPU contention the two sleeps bunch together so `ttfb` drifts toward
    `duration` — a reproducible spurious failure on a loaded runner. The question these tests ask
    is "which timestamp did the middleware choose", which a monotonically-stepping clock answers
    exactly, with no timing at all.

    Steps of 1.0 make the two candidates unmistakable: the first-body-byte stamp is the SECOND
    tick, response start the third, so a working wrap yields 1.0 and a broken one 2.0.
    """
    from httpx import ASGITransport
    from httpx import AsyncClient

    app, recorded = _stack_app(layers=layers, post_body_seconds=0.0)

    clock = iter(float(i) for i in range(100))

    class _Collector:
        def record_http_request(self, **kwargs: Any) -> None:
            recorded.update(kwargs)

        def record_error(self, **kwargs: Any) -> None:
            pass

    async def _go() -> None:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
            r = await client.put("/bucket/key.bin", content=body)
        assert r.status_code == 200, f"handler did not run: {r.status_code} {r.text[:200]}"

    with (
        patch("hippius_s3.api.middlewares.metrics.get_metrics_collector", lambda: _Collector()),
        patch.object(metrics_module.time, "time", lambda: next(clock)),
    ):
        asyncio.run(_go())

    assert recorded, "the collector must be fed through the real stack"
    return recorded


def test_receive_wrap_survives_the_nested_middleware_stack() -> None:
    """TTFB must still stamp at the body read when the request passes through many
    BaseHTTPMiddleware layers, each re-wrapping receive.

    With the wrap intact the stamp is the tick taken inside `receive` (start + 1). If the
    delegation breaks, no body tick is ever taken and TTFB falls back to response start, a
    strictly later tick — silently, since nothing errors. This is the only test that catches it.
    """
    recorded = _run_through_stack_with_clock(b"x" * 4096)
    assert recorded["ttfb"] == pytest.approx(1.0), (
        f"expected the body-read tick, got {recorded['ttfb']} — the receive wrap did not survive the stack"
    )
    assert recorded["duration"] == pytest.approx(2.0)


def test_zero_length_body_stamps_through_the_nested_stack() -> None:
    """The zero-byte case, end to end through the same nesting: an empty `http.request` is still
    a body read, so it must take the body tick rather than falling through to response start."""
    recorded = _run_through_stack_with_clock(b"")
    assert recorded["ttfb"] == pytest.approx(1.0), "zero-byte PUT lost its first-byte stamp"
    assert recorded["duration"] == pytest.approx(2.0)
