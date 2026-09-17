"""The read path's backend tier: fetch a ciphertext chunk into memory, in backend order, with bounded
retries and one concurrency budget per process. Nothing here writes to any cache — that is the point.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from hippius_s3.reader.backend_fetch import BackendChunkFetcher
from hippius_s3.reader.backend_fetch import ChunkUnavailableError


def _fetcher(fetchers: dict, *, attempts: int = 3, concurrency: int = 4) -> BackendChunkFetcher:
    return BackendChunkFetcher(fetchers, concurrency=concurrency, attempts=attempts, base_sleep=0.0, jitter=0.0)


@pytest.mark.asyncio
async def test_the_first_location_that_serves_wins_and_nothing_else_is_asked() -> None:
    calls: list[tuple[str, str]] = []

    async def arion(identifier: str, address: str) -> bytes:
        calls.append(("arion", identifier))
        return b"cipher"

    async def other(identifier: str, address: str) -> bytes:
        calls.append(("other", identifier))
        return b"never"

    out = await _fetcher({"arion": arion, "other": other}).fetch([("arion", "id-a"), ("other", "id-o")], "addr")

    assert out == b"cipher"
    assert calls == [("arion", "id-a")]


@pytest.mark.asyncio
async def test_a_transient_failure_is_retried_on_the_same_location() -> None:
    attempts: list[int] = []

    async def flaky(identifier: str, address: str) -> bytes:
        attempts.append(1)
        if len(attempts) < 3:
            raise ConnectionError("blip")
        return b"cipher"

    assert await _fetcher({"arion": flaky}, attempts=3).fetch([("arion", "id")], "addr") == b"cipher"
    assert len(attempts) == 3


@pytest.mark.asyncio
async def test_a_permanent_failure_moves_to_the_next_location_without_retrying() -> None:
    # A 404 means the identifier is stale on that backend (a re-pin under a new id, an unpin);
    # retrying it is pointless, the next location may still serve.
    from hippius_s3.services.hippius_api_service import HippiusAPIError

    tried: list[str] = []

    async def gone(identifier: str, address: str) -> bytes:
        tried.append("arion")
        raise HippiusAPIError("404 not found")

    async def ok(identifier: str, address: str) -> bytes:
        tried.append("ovh")
        return b"cipher"

    out = await _fetcher({"arion": gone, "ovh": ok}, attempts=3).fetch([("arion", "a"), ("ovh", "o")], "addr")
    assert out == b"cipher"
    assert tried == ["arion", "ovh"], "one attempt on the permanent failure, then the next location"


@pytest.mark.asyncio
async def test_exhausting_every_location_is_chunk_unavailable() -> None:
    async def down(identifier: str, address: str) -> bytes:
        raise ConnectionError("down")

    with pytest.raises(ChunkUnavailableError):
        await _fetcher({"arion": down}, attempts=2).fetch([("arion", "a")], "addr")


@pytest.mark.asyncio
async def test_a_location_on_a_backend_this_pod_cannot_reach_is_skipped() -> None:
    # A chunk whose only live row is on a backend with no client here (a legacy backend) cannot be
    # served by this pod: that is an unavailable chunk, not a crash.
    with pytest.raises(ChunkUnavailableError):
        await _fetcher({"arion": None}).fetch([("ipfs", "Qm...")], "addr")  # type: ignore[dict-item]


@pytest.mark.asyncio
async def test_the_concurrency_budget_is_shared_across_fetches() -> None:
    in_flight = 0
    peak = 0

    async def slow(identifier: str, address: str) -> bytes:
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.01)
        in_flight -= 1
        return b"c"

    fetcher = _fetcher({"arion": slow}, concurrency=2)
    await asyncio.gather(*(fetcher.fetch([("arion", str(i))], "addr") for i in range(8)))
    assert peak == 2, f"the semaphore bounds backend concurrency (peak {peak})"


@pytest.mark.asyncio
async def test_a_saturated_budget_fails_fast_instead_of_queueing_until_the_first_chunk_bound() -> None:
    # One slot, held by a fetch that never returns. The next fetch must not wait on the
    # semaphore indefinitely (every queued read would then 503 together at the 25 s
    # first-chunk bound): it gives up after the queue timeout with the retryable error.
    release = asyncio.Event()

    async def stuck(identifier: str, address: str) -> bytes:
        await release.wait()
        return b"late"

    fetcher = BackendChunkFetcher(
        {"arion": stuck}, concurrency=1, attempts=1, base_sleep=0.0, jitter=0.0, queue_timeout=0.05
    )
    holder = asyncio.create_task(fetcher.fetch([("arion", "id-1")], "addr"))
    await asyncio.sleep(0)
    with pytest.raises(ChunkUnavailableError, match="budget saturated"):
        await fetcher.fetch([("arion", "id-2")], "addr")
    release.set()
    assert await holder == b"late", "the holder keeps its slot and completes"


@pytest.mark.asyncio
async def test_the_slot_is_released_when_the_fetch_is_cancelled() -> None:
    # A client that disconnects cancels the fetch mid-flight; the slot it held must come back,
    # or a pod's budget leaks one slot per abandoned cold read until nothing can be served.
    started = asyncio.Event()

    async def slow(identifier: str, address: str) -> bytes:
        started.set()
        await asyncio.sleep(60)
        return b"never"

    fetcher = BackendChunkFetcher(
        {"arion": slow}, concurrency=1, attempts=1, base_sleep=0.0, jitter=0.0, queue_timeout=0.05
    )
    task = asyncio.create_task(fetcher.fetch([("arion", "id")], "addr"))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    async def quick(identifier: str, address: str) -> bytes:
        return b"served"

    fetcher._fetchers["arion"] = quick
    assert await fetcher.fetch([("arion", "id")], "addr") == b"served"


@pytest.mark.asyncio
async def test_transient_retries_back_off_exponentially() -> None:
    sleeps: list[float] = []

    async def flaky(identifier: str, address: str) -> bytes:
        raise ConnectionError("429")

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    fetcher = BackendChunkFetcher({"arion": flaky}, concurrency=1, attempts=3, base_sleep=1.0, jitter=0.0)
    with patch("hippius_s3.reader.backend_fetch.asyncio.sleep", fake_sleep), pytest.raises(ChunkUnavailableError):
        await fetcher.fetch([("arion", "id")], "addr")
    assert sleeps == [1.0, 2.0], "base × 2^(attempt-1), and no sleep after the last attempt"


@pytest.mark.asyncio
async def test_transient_backoff_does_not_hold_the_concurrency_slot() -> None:
    calls = 0
    sleeping = asyncio.Event()

    async def flaky(identifier: str, address: str) -> bytes:
        nonlocal calls
        calls += 1
        if identifier == "id-1":
            raise ConnectionError("429")
        return b"ok"

    async def fake_sleep(seconds: float) -> None:
        sleeping.set()
        await asyncio.Event().wait()

    fetcher = BackendChunkFetcher(
        {"arion": flaky}, concurrency=1, attempts=3, base_sleep=1.0, jitter=0.0, queue_timeout=0.5
    )
    with patch("hippius_s3.reader.backend_fetch.asyncio.sleep", fake_sleep):
        first = asyncio.create_task(fetcher.fetch([("arion", "id-1")], "addr"))
        await sleeping.wait()
        assert await fetcher.fetch([("arion", "id-2")], "addr") == b"ok"
        first.cancel()
