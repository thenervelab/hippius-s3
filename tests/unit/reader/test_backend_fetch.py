"""The read path's backend tier: fetch a ciphertext chunk into memory, in backend order, with bounded
retries and one concurrency budget per process. Nothing here writes to any cache — that is the point.
"""

from __future__ import annotations

import asyncio

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
