"""Unit tests for fs_cache_pressure_middleware — the pool-signal → PUT-throttle wiring."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from starlette.requests import Request
from starlette.responses import Response

import hippius_s3.pressure_signal as ps
from hippius_s3.api.middlewares.fs_cache_pressure import fs_cache_pressure_middleware
from hippius_s3.pressure_signal import PRESSURE_KEY


class FakeRedis:
    def __init__(self, value=None):
        self.value = value

    async def get(self, key):
        return self.value


def _config(tmp_path):
    # Local statvfs on tmp_path has ample headroom, so any rejection here is
    # driven purely by the published pool signal (the wiring under test).
    return SimpleNamespace(
        object_cache_dir=str(tmp_path),
        fs_cache_min_free_bytes=0,
        fs_cache_min_free_ratio=0.0,
        fs_cache_retry_after_seconds=30,
    )


def _make_put_request(config, redis_client):
    app = SimpleNamespace(state=SimpleNamespace(config=config, redis_client=redis_client))
    scope = {
        "type": "http",
        "method": "PUT",
        "path": "/bucket/key",
        "scheme": "http",
        "server": ("testserver", 80),
        "query_string": b"",
        "headers": [(b"content-length", b"1024")],
        "app": app,
    }
    return Request(scope)


@pytest.fixture(autouse=True)
def _reset_consumer_memo():
    ps._published_cache = (None, 0.0)
    ps._last_good = (None, 0.0)
    yield
    ps._published_cache = (None, 0.0)
    ps._last_good = (None, 0.0)


@pytest.mark.asyncio
async def test_put_rejected_when_published_mode_is_critical(tmp_path):
    """Published pool mode 2 must throttle the PUT even with local headroom —
    severing this wiring (published_mode := None) is the 2026-07-24 regression."""
    redis = FakeRedis(value=json.dumps({"mode": 2, "ratio": 0.96, "source": "janitor", "ts": 1.0}))
    request = _make_put_request(_config(tmp_path), redis)

    called = []

    async def call_next(req):
        called.append(req)
        return Response("passed", status_code=200)

    resp = await fs_cache_pressure_middleware(request, call_next)

    assert called == []  # body must never be read once the gate fires
    assert resp.status_code == 503
    assert b"SlowDown" in resp.body
    assert int(resp.headers["Retry-After"]) >= 1


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [None, json.dumps({"mode": 1, "ratio": 0.9, "source": "janitor", "ts": 1.0})])
async def test_put_passes_when_signal_absent_or_below_critical(tmp_path, value):
    """No signal (None) or a sub-critical mode with local headroom must pass through."""
    request = _make_put_request(_config(tmp_path), FakeRedis(value=value))

    sentinel = Response("passed", status_code=200)

    async def call_next(req):
        return sentinel

    resp = await fs_cache_pressure_middleware(request, call_next)

    assert resp is sentinel


@pytest.mark.asyncio
async def test_non_put_bypasses_the_gate_entirely(tmp_path):
    """A GET is not an FS-cache write; the middleware must not even consult the signal."""
    app = SimpleNamespace(state=SimpleNamespace(config=_config(tmp_path), redis_client=FakeRedis(value=None)))
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/bucket/key",
        "scheme": "http",
        "server": ("testserver", 80),
        "query_string": b"",
        "headers": [],
        "app": app,
    }
    request = Request(scope)

    ps._published_cache = (2, -100.0)  # a stale critical memo must be irrelevant to a GET
    sentinel = Response("passed", status_code=200)

    async def call_next(req):
        return sentinel

    assert await fs_cache_pressure_middleware(request, call_next) is sentinel
    assert await FakeRedis(value=None).get(PRESSURE_KEY) is None  # signal untouched


# ---------------------------------------------------------------------------
# A shed request must leave a metric behind, not just a log line
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_shed_request_is_recorded(tmp_path, monkeypatch):
    """This gate is the OUTERMOST middleware — it has to answer before the body is read — while
    metrics_middleware is the innermost. So metrics never runs on a shed request and the 503
    lands in no request or error counter on either side. Without this call a pressure event is
    invisible outside the log.
    """
    recorded: list[tuple[str, str]] = []

    class _Collector:
        def record_fs_cache_shed(self, reason: str, pressure_mode: str) -> None:
            recorded.append((reason, pressure_mode))

    monkeypatch.setattr(
        "hippius_s3.api.middlewares.fs_cache_pressure.get_metrics_collector",
        lambda: _Collector(),
    )
    monkeypatch.setattr(ps, "_CACHE", None, raising=False)

    config = _config(tmp_path)
    redis_client = FakeRedis(json.dumps({"mode": 2}).encode())
    request = _make_put_request(config, redis_client)

    async def _call_next(_req):  # pragma: no cover - must not be reached
        raise AssertionError("a shed request must not reach the handler")

    response = await fs_cache_pressure_middleware(request, _call_next)

    assert response.status_code == 503
    assert recorded == [("pool", "2")], "the shed must be attributed to the pool signal"


@pytest.mark.asyncio
async def test_passthrough_records_nothing(tmp_path, monkeypatch):
    """Blast-radius guard: only an actual rejection increments the counter."""
    recorded: list[tuple[str, str]] = []

    class _Collector:
        def record_fs_cache_shed(self, reason: str, pressure_mode: str) -> None:
            recorded.append((reason, pressure_mode))

    monkeypatch.setattr(
        "hippius_s3.api.middlewares.fs_cache_pressure.get_metrics_collector",
        lambda: _Collector(),
    )
    monkeypatch.setattr(ps, "_CACHE", None, raising=False)

    config = _config(tmp_path)
    redis_client = FakeRedis(json.dumps({"mode": 0}).encode())
    request = _make_put_request(config, redis_client)

    async def _call_next(_req):
        return Response(status_code=200)

    response = await fs_cache_pressure_middleware(request, _call_next)

    assert response.status_code == 200
    assert recorded == []
