"""The audit middleware must report the time spent ABOVE it, not just below it.

`audit_log_middleware` is registered 18 layers deep, so its own `processing_time_ms` covers only
what runs inside it. The 17 layers above — SigV4 verification, account resolution, ACL, input
validation, and the BaseHTTPMiddleware wrapping around each — were unmeasured, and on prod that
blind spot was the same order as everything it wrapped: a small PUT spent ~247 ms inside the window
and ~261 ms outside it, while a request rejected by auth (no handler, no DB, no disk) still cost
176 ms.

These tests pin the arithmetic, because the failure mode is silent. If `gateway_start_time` stops
being stamped, or a refactor moves audit outward so the two clocks converge, the numbers keep being
emitted and merely stop meaning anything — no error, no alert, just a metric that quietly reads zero.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import types
from typing import Any
from unittest.mock import patch

import pytest

from hippius_s3.gateway.middlewares.audit_log import audit_log_middleware


class _Response:
    status_code = 200
    headers = {"content-length": "42"}


def _request(*, gateway_start_time: float | None) -> Any:
    request = types.SimpleNamespace(
        method="PUT",
        headers={"user-agent": "pytest"},
        query_params={},
        client=types.SimpleNamespace(host="10.0.0.1"),
        url=types.SimpleNamespace(path="/bucket/key.json"),
        state=types.SimpleNamespace(ray_id="ray-test", account=None),
    )
    if gateway_start_time is not None:
        request.state.gateway_start_time = gateway_start_time
    return request


def _run(request: Any, inner_seconds: float) -> tuple[dict[str, Any], list[tuple]]:
    """Drive the middleware and return (parsed audit line, recorded metric calls)."""

    async def call_next(_: Any) -> _Response:
        await asyncio.sleep(inner_seconds)
        return _Response()

    recorded: list[tuple] = []

    class _Collector:
        def record_pre_handler_duration(self, method: str, status: int, duration: float) -> None:
            recorded.append((method, status, duration))

    lines: list[str] = []

    class _Handler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            lines.append(record.getMessage())

    audit_logger = logging.getLogger("audit")
    previous = audit_logger.handlers
    audit_logger.handlers = [_Handler()]
    audit_logger.setLevel(logging.INFO)
    try:
        with patch("hippius_s3.gateway.middlewares.audit_log.get_metrics_collector", lambda: _Collector()):
            asyncio.run(audit_log_middleware(request, call_next))
    finally:
        audit_logger.handlers = previous

    payload = [line for line in lines if "S3_OPERATION" in line][0]
    return json.loads(payload.split(": ", 1)[1]), recorded


def test_reports_the_time_spent_above_this_middleware() -> None:
    """The whole point: the outer chain is 250 ms here and must not be invisible."""
    request = _request(gateway_start_time=time.time() - 0.250)
    audit, recorded = _run(request, inner_seconds=0.050)

    assert 40 <= audit["processing_time_ms"] <= 100, "inner window should be ~50 ms"
    assert 230 <= audit["pre_handler_ms"] <= 290, "outer chain should be ~250 ms"
    assert recorded, "the histogram must be fed, not just the log line"
    assert recorded[0][0] == "PUT" and recorded[0][1] == 200


def test_the_three_figures_are_internally_consistent() -> None:
    request = _request(gateway_start_time=time.time() - 0.200)
    audit, recorded = _run(request, inner_seconds=0.030)

    assert audit["total_time_ms"] == pytest.approx(
        audit["processing_time_ms"] + audit["pre_handler_ms"], abs=1.0
    ), "total must be the two halves, or the split is describing nothing"
    assert recorded[0][2] * 1000 == pytest.approx(audit["pre_handler_ms"], abs=1.0), (
        "the metric and the log line must agree; if they drift, one of them is lying"
    )


def test_processing_time_still_measures_only_the_inner_window() -> None:
    """Guards the compatibility promise.

    Dashboards, alerts and the /s3-prod-health collector all parse `processing_time_ms`. Widening
    it to include the outer chain would move every historical baseline at once, with nothing to
    show that the definition had changed — so the new figures are added ALONGSIDE it, never folded in.
    """
    request = _request(gateway_start_time=time.time() - 0.400)
    audit, _ = _run(request, inner_seconds=0.020)

    assert audit["processing_time_ms"] < 100, (
        "processing_time_ms absorbed the outer chain — this silently rebaselines every dashboard"
    )


def test_degrades_cleanly_when_no_start_time_was_stamped() -> None:
    """No `gateway_start_time` means no honest way to compute the gap.

    Emitting a zero or a bogus figure would be worse than emitting nothing: it would enter the
    histogram and drag its quantiles toward a number nobody measured.
    """
    audit, recorded = _run(_request(gateway_start_time=None), inner_seconds=0.010)

    assert "total_time_ms" not in audit
    assert "pre_handler_ms" not in audit
    assert recorded == [], "must not record a fabricated sample"
    assert "processing_time_ms" in audit, "the pre-existing field must survive regardless"
