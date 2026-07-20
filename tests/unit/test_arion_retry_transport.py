import asyncio
from typing import Any

import httpx
import pytest

from hippius_s3.services.arion_service import HippiusAPIError
from hippius_s3.services.arion_service import HippiusAuthenticationError
from hippius_s3.services.arion_service import retry_on_error


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Record backoff sleeps instead of serving them, so the retry budget is asserted without waiting."""
    slept: list[float] = []

    async def fake_sleep(delay: float, *args: Any, **kwargs: Any) -> None:
        slept.append(delay)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    return slept


def _always_raises(exc: Exception) -> tuple[Any, list[int]]:
    calls: list[int] = []

    @retry_on_error(retries=3, backoff=5.0)
    async def failing() -> None:
        calls.append(1)
        raise exc

    return failing, calls


def _http_status_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "https://arion.test/upload")
    response = httpx.Response(status_code, request=request, text="boom")
    return httpx.HTTPStatusError("boom", request=request, response=response)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exc",
    [
        httpx.ConnectError("connection refused"),
        httpx.ConnectTimeout("timed out connecting"),
        httpx.ReadError("peer reset"),
        httpx.ReadTimeout("read timed out"),
    ],
    ids=["connect_error", "connect_timeout", "read_error", "read_timeout"],
)
async def test_transport_errors_are_not_retried_in_this_layer(exc: Exception, no_sleep: list[float]) -> None:
    """Layering guard: the worker's Redis retry ZSET owns transport retries, not this decorator.

    `classify_upload_error` already marks these `transient`, so the uploader loop re-drives them with
    exponential backoff + jitter, durably across pod restarts, without holding `_put_semaphore`. Retrying
    here as well multiplies the two budgets into ~24 requests at a backend that is already failing. If a
    future change adds these to the caught tuple, this test fails and that is the intended signal.
    """
    failing, calls = _always_raises(exc)

    with pytest.raises(type(exc)):
        await failing()

    assert len(calls) == 1
    assert no_sleep == []


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [401, 403])
async def test_auth_status_short_circuits(status_code: int, no_sleep: list[float]) -> None:
    failing, calls = _always_raises(_http_status_error(status_code))

    with pytest.raises(HippiusAuthenticationError):
        await failing()

    assert len(calls) == 1
    assert no_sleep == []


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [404, 507])
async def test_not_found_and_insufficient_storage_short_circuit(status_code: int, no_sleep: list[float]) -> None:
    failing, calls = _always_raises(_http_status_error(status_code))

    with pytest.raises(httpx.HTTPStatusError):
        await failing()

    assert len(calls) == 1
    assert no_sleep == []


@pytest.mark.asyncio
async def test_server_error_status_still_retried(no_sleep: list[float]) -> None:
    failing, calls = _always_raises(_http_status_error(500))

    with pytest.raises(httpx.HTTPStatusError):
        await failing()

    assert len(calls) == 4
    assert no_sleep == [5.0, 5.0, 5.0]


@pytest.mark.asyncio
async def test_hippius_api_error_without_response_still_retried(no_sleep: list[float]) -> None:
    """HippiusAPIError has no .response — the status-code branches must be skipped, not crash."""
    failing, calls = _always_raises(HippiusAPIError("backend said no"))

    with pytest.raises(HippiusAPIError):
        await failing()

    assert len(calls) == 4
    assert no_sleep == [5.0, 5.0, 5.0]
