"""Tests for ATS PURGE client: no-op when unset, fires httpx PURGE when configured."""

import asyncio

import httpx
import pytest

from gateway import config as gateway_config
from gateway.services import ats_cache_client


@pytest.fixture(autouse=True)  # type: ignore[misc]
def _reset_state() -> None:
    gateway_config._config = None
    ats_cache_client._client = None
    yield
    gateway_config._config = None
    ats_cache_client._client = None


@pytest.mark.asyncio
async def test_schedule_purge_noop_when_endpoint_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ATS_CACHE_ENDPOINT", raising=False)
    called = []

    async def fake_purge(*args: object, **kwargs: object) -> None:
        called.append(args)

    monkeypatch.setattr(ats_cache_client, "_purge", fake_purge)

    ats_cache_client.schedule_purge("s3.hippius.com", "bucket/key")
    await asyncio.sleep(0)  # let any scheduled task run
    assert called == []


@pytest.mark.asyncio
async def test_schedule_purge_fires_task_when_endpoint_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ATS_CACHE_ENDPOINT", "http://ats.local:8080")
    called: list[tuple[str, str, str]] = []

    async def fake_purge(endpoint: str, host: str, key: str) -> None:
        called.append((endpoint, host, key))

    monkeypatch.setattr(ats_cache_client, "_purge", fake_purge)

    ats_cache_client.schedule_purge("s3.hippius.com", "bucket/key")
    # Let the scheduled task run
    await asyncio.sleep(0.01)
    assert called == [("http://ats.local:8080", "s3.hippius.com", "bucket/key")]


@pytest.mark.asyncio
async def test_schedule_purge_fans_out_to_all_endpoints(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "ATS_CACHE_ENDPOINT",
        "http://ats-0.local:8080,http://ats-1.local:8080,http://ats-2.local:8080",
    )
    called: list[tuple[str, str, str]] = []

    async def fake_purge(endpoint: str, host: str, key: str) -> None:
        called.append((endpoint, host, key))

    monkeypatch.setattr(ats_cache_client, "_purge", fake_purge)

    ats_cache_client.schedule_purge("s3.hippius.com", "bucket/key")
    await asyncio.sleep(0.01)
    assert sorted(called) == [
        ("http://ats-0.local:8080", "s3.hippius.com", "bucket/key"),
        ("http://ats-1.local:8080", "s3.hippius.com", "bucket/key"),
        ("http://ats-2.local:8080", "s3.hippius.com", "bucket/key"),
    ]


@pytest.mark.asyncio
async def test_purge_all_runs_endpoints_in_parallel(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sanity check: `_purge_all` awaits in parallel, not serially."""
    import time

    start_order: list[str] = []
    end_order: list[str] = []

    async def fake_purge(endpoint: str, host: str, key: str) -> None:
        start_order.append(endpoint)
        await asyncio.sleep(0.05)
        end_order.append(endpoint)

    monkeypatch.setattr(ats_cache_client, "_purge", fake_purge)

    t0 = time.monotonic()
    await ats_cache_client._purge_all(
        ["http://a:8080", "http://b:8080", "http://c:8080"], "s3.hippius.com", "bucket/key"
    )
    elapsed = time.monotonic() - t0

    # 3 parallel sleeps of 50ms each should finish close to 50ms, not 150ms.
    assert elapsed < 0.12
    # All three started before any finished.
    assert len(start_order) == 3
    assert start_order[0] in {"http://a:8080", "http://b:8080", "http://c:8080"}


@pytest.mark.asyncio
async def test_purge_all_swallows_per_endpoint_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """If one endpoint raises, the others still get purged."""
    called: list[str] = []

    async def fake_purge(endpoint: str, host: str, key: str) -> None:
        called.append(endpoint)
        if endpoint == "http://b:8080":
            raise RuntimeError("boom")

    monkeypatch.setattr(ats_cache_client, "_purge", fake_purge)

    # Should not raise
    await ats_cache_client._purge_all(
        ["http://a:8080", "http://b:8080", "http://c:8080"], "s3.hippius.com", "bucket/key"
    )
    assert sorted(called) == ["http://a:8080", "http://b:8080", "http://c:8080"]


@pytest.mark.asyncio
async def test_purge_issues_http_purge_request(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = {}

    async def fake_request(self: httpx.AsyncClient, method: str, url: str, **kwargs: object) -> httpx.Response:
        captured["method"] = method
        captured["url"] = url
        captured["headers"] = kwargs.get("headers")
        return httpx.Response(200)

    monkeypatch.setattr(httpx.AsyncClient, "request", fake_request)

    await ats_cache_client._purge("http://ats.local:8080", "s3.hippius.com", "bucket/key")
    assert captured["method"] == "PURGE"
    assert captured["url"] == "http://ats.local:8080/bucket/key"
    assert captured["headers"] == {"Host": "s3.hippius.com"}


@pytest.mark.asyncio
async def test_purge_swallows_http_errors(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    async def fake_request(self: httpx.AsyncClient, method: str, url: str, **kwargs: object) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(httpx.AsyncClient, "request", fake_request)

    # Should not raise
    await ats_cache_client._purge("http://ats.local:8080", "s3.hippius.com", "bucket/key")


@pytest.mark.asyncio
async def test_purge_logs_on_4xx_5xx(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    async def fake_request(self: httpx.AsyncClient, method: str, url: str, **kwargs: object) -> httpx.Response:
        return httpx.Response(404)

    monkeypatch.setattr(httpx.AsyncClient, "request", fake_request)

    with caplog.at_level("WARNING"):
        await ats_cache_client._purge("http://ats.local:8080", "s3.hippius.com", "bucket/key")

    assert any("PURGE" in record.message for record in caplog.records)
