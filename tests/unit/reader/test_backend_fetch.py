"""The read path's backend tier: fetch a ciphertext chunk into memory, in backend order, with bounded
retries and one concurrency budget per process. Nothing here writes to any cache — that is the point.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Coroutine
from typing import Any
from unittest.mock import patch

import httpx
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


@pytest.mark.asyncio
async def test_a_pool_timeout_is_not_retried_and_does_not_try_the_next_location() -> None:
    # A PoolTimeout is THIS process failing to hand out a connection. Retrying, or moving to the
    # next location on the same client, only re-queues behind the same stuck pool.
    import httpx

    calls: list[str] = []

    async def stuck(identifier: str, address: str) -> bytes:
        calls.append("arion")
        raise httpx.PoolTimeout("pool")

    async def never(identifier: str, address: str) -> bytes:
        calls.append("ovh")
        return b"x"

    with pytest.raises(ChunkUnavailableError, match="pool saturated"):
        await _fetcher({"arion": stuck, "ovh": never}, attempts=3).fetch([("arion", "a"), ("ovh", "o")], "addr")
    assert calls == ["arion"]


@pytest.mark.asyncio
async def test_a_pool_timeout_that_rebuilds_the_client_retries_on_the_new_client() -> None:
    # Retrying the same wedged pool is useless; retrying the replacement is the point of the swap.
    # Mid-stream that is what keeps a committed 200 from becoming IncompleteRead after part 1.
    import httpx

    calls: list[str] = []
    resets: list[int] = []

    async def flaky(identifier: str, address: str) -> bytes:
        calls.append("arion")
        if len(calls) == 1:
            raise httpx.PoolTimeout("pool")
        return b"cipher"

    def reset() -> Awaitable[None]:
        resets.append(1)
        return asyncio.sleep(0)

    fetcher = BackendChunkFetcher(
        {"arion": flaky},
        concurrency=4,
        attempts=1,
        base_sleep=0.0,
        jitter=0.0,
        reset_fn=reset,
        reset_after_pool_timeouts=1,
        attempt_budget_seconds=24.0,
    )
    deadline = asyncio.get_running_loop().time() + 300.0
    assert await fetcher.fetch([("arion", "a")], "addr", deadline=deadline) == b"cipher"
    assert calls == ["arion", "arion"]
    assert resets == [1]


def _read_timeout_then_success() -> tuple[Callable[[str, str], Awaitable[bytes]], list[str]]:
    calls: list[str] = []

    async def flaky(identifier: str, address: str) -> bytes:
        calls.append("arion")
        if len(calls) == 1:
            raise httpx.ReadTimeout("slow")
        return b"cipher"

    return flaky, calls


@pytest.mark.asyncio
async def test_a_read_timeout_is_retried_when_the_deadline_allows(monkeypatch: pytest.MonkeyPatch) -> None:
    # A mid-stream chunk has stream_chunk_timeout_seconds (300 s) of budget: one 10 s read stall
    # must not truncate a committed 200 body when a second attempt fits comfortably.
    from hippius_s3.reader import backend_fetch

    seen: list[str] = []
    monkeypatch.setattr(backend_fetch, "_record_backend_fetch_outcome", seen.append)
    flaky, calls = _read_timeout_then_success()

    fetcher = BackendChunkFetcher(
        {"arion": flaky}, concurrency=4, attempts=3, base_sleep=0.0, jitter=0.0, attempt_budget_seconds=24.0
    )
    deadline = asyncio.get_running_loop().time() + 300.0
    assert await fetcher.fetch([("arion", "a")], "addr", deadline=deadline) == b"cipher"
    assert calls == ["arion", "arion"]
    assert seen == ["read_timeout", "ok"]


@pytest.mark.asyncio
async def test_a_read_timeout_is_not_retried_when_no_attempt_fits_the_deadline(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # The first chunk carries the reader's 25 s bound. A whole attempt is 24 s (slot wait + pool
    # wait + connect + read), so once anything has failed another attempt cannot finish before it:
    # retrying would only reproduce the silent cancel the bounds exist to remove, so the fetch
    # fails now, with a line.
    import logging

    from hippius_s3.reader import backend_fetch

    seen: list[str] = []
    monkeypatch.setattr(backend_fetch, "_record_backend_fetch_outcome", seen.append)
    flaky, calls = _read_timeout_then_success()

    fetcher = BackendChunkFetcher(
        {"arion": flaky}, concurrency=4, attempts=3, base_sleep=0.0, jitter=0.0, attempt_budget_seconds=24.0
    )
    deadline = asyncio.get_running_loop().time() + 20.0
    with caplog.at_level(logging.WARNING, logger="hippius_s3.reader.backend_fetch"):
        with pytest.raises(ChunkUnavailableError, match="out of time"):
            await fetcher.fetch([("arion", "a")], "addr", deadline=deadline)
    assert calls == ["arion"], "no second attempt was started"
    assert seen == ["read_timeout"]
    deadline_lines = [r.getMessage() for r in caplog.records if "kind=deadline" in r.getMessage()]
    assert len(deadline_lines) == 1
    assert "retry=False" in deadline_lines[0] and "attempt needs 24.0s" in deadline_lines[0]


@pytest.mark.asyncio
async def test_production_first_chunk_deadline_admits_exactly_one_attempt() -> None:
    # Production: 25 s first-chunk bound, 24 s attempt budget, 1 s base backoff, no jitter.
    # An instant failure leaves ~25 s; needed is 24 + 1 = 25. Strict `left > needed` refuses
    # the equality so a retry cannot start and then be cancelled silently by the reader's wait_for.
    calls: list[str] = []

    async def boom(identifier: str, address: str) -> bytes:
        calls.append("arion")
        raise ConnectionError("blip")

    fetcher = BackendChunkFetcher(
        {"arion": boom},
        concurrency=4,
        attempts=3,
        base_sleep=1.0,
        jitter=0.0,
        attempt_budget_seconds=24.0,
    )
    deadline = asyncio.get_running_loop().time() + 25.0
    with pytest.raises(ChunkUnavailableError, match="out of time"):
        await fetcher.fetch([("arion", "a")], "addr", deadline=deadline)
    assert calls == ["arion"]


@pytest.mark.asyncio
async def test_a_retry_is_refused_when_the_slot_wait_alone_could_overrun_the_deadline(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The budget counts the slot wait, not only the wire: a retry that could sit 8 s in the queue
    # before its 16 s of httpx bounds even start does not fit in 20 s, whether or not the queue
    # is busy right now.
    import logging

    calls: list[str] = []

    async def flaky(identifier: str, address: str) -> bytes:
        calls.append("arion")
        if len(calls) == 1:
            raise ConnectionError("blip")
        return b"cipher"

    fetcher = BackendChunkFetcher(
        {"arion": flaky},
        concurrency=1,
        attempts=3,
        base_sleep=0.0,
        jitter=0.0,
        queue_timeout=8.0,
        attempt_budget_seconds=24.0,
    )
    deadline = asyncio.get_running_loop().time() + 20.0
    with caplog.at_level(logging.WARNING, logger="hippius_s3.reader.backend_fetch"):
        with pytest.raises(ChunkUnavailableError, match="out of time"):
            await fetcher.fetch([("arion", "a")], "addr", deadline=deadline)
    assert calls == ["arion"], "the transient failure was not retried"
    assert any("kind=deadline" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_no_deadline_means_the_old_retry_behaviour() -> None:
    flaky, calls = _read_timeout_then_success()

    fetcher = BackendChunkFetcher(
        {"arion": flaky}, concurrency=4, attempts=3, base_sleep=0.0, jitter=0.0, attempt_budget_seconds=24.0
    )
    assert await fetcher.fetch([("arion", "a")], "addr") == b"cipher"
    assert calls == ["arion", "arion"]


@pytest.mark.asyncio
async def test_a_second_location_is_not_tried_when_no_attempt_fits_the_deadline() -> None:
    # Moving to the next location is an attempt too; without budget for it the request fails
    # with the same "out of time" reason rather than starting a fetch the caller will cancel.
    from hippius_s3.services.hippius_api_service import HippiusAPIError

    tried: list[str] = []

    async def gone(identifier: str, address: str) -> bytes:
        tried.append("arion")
        raise HippiusAPIError("404 not found")

    async def ok(identifier: str, address: str) -> bytes:
        tried.append("ovh")
        return b"cipher"

    fetcher = BackendChunkFetcher(
        {"arion": gone, "ovh": ok}, concurrency=4, attempts=3, base_sleep=0.0, jitter=0.0, attempt_budget_seconds=24.0
    )
    deadline = asyncio.get_running_loop().time() + 20.0
    with pytest.raises(ChunkUnavailableError, match="out of time for ovh"):
        await fetcher.fetch([("arion", "a"), ("ovh", "o")], "addr", deadline=deadline)
    assert tried == ["arion"]


@pytest.mark.asyncio
async def test_every_failed_attempt_leaves_a_warning(caplog: pytest.LogCaptureFixture) -> None:
    import logging

    attempts: list[int] = []

    async def flaky(identifier: str, address: str) -> bytes:
        attempts.append(1)
        if len(attempts) < 3:
            raise ConnectionError("blip")
        return b"cipher"

    with caplog.at_level(logging.WARNING, logger="hippius_s3.reader.backend_fetch"):
        assert await _fetcher({"arion": flaky}, attempts=3).fetch([("arion", "id")], "addr") == b"cipher"
    warnings = [r for r in caplog.records if "backend chunk fetch failed" in r.getMessage()]
    assert len(warnings) == 2, "both retried attempts are logged, not only a final one"


@pytest.mark.asyncio
async def test_consecutive_pool_timeouts_reset_the_client_once_at_the_threshold() -> None:
    import httpx

    resets: list[int] = []

    def reset() -> Awaitable[None]:
        # The swap happens here, synchronously; the returned awaitable is the deferred close.
        resets.append(1)
        return asyncio.sleep(0)

    async def stuck(identifier: str, address: str) -> bytes:
        raise httpx.PoolTimeout("pool")

    fetcher = BackendChunkFetcher(
        {"arion": stuck},
        concurrency=4,
        attempts=1,
        base_sleep=0.0,
        jitter=0.0,
        reset_fn=reset,
        reset_after_pool_timeouts=3,
    )
    for _ in range(3):
        with pytest.raises(ChunkUnavailableError):
            await fetcher.fetch([("arion", "a")], "addr")
    assert resets == [1], "the third consecutive PoolTimeout triggers exactly one reset"

    with pytest.raises(ChunkUnavailableError):
        await fetcher.fetch([("arion", "a")], "addr")
    assert resets == [1], "the streak restarts after a reset; one more failure is not a second reset"


@pytest.mark.asyncio
async def test_a_successful_fetch_ends_the_pool_timeout_streak() -> None:
    import httpx

    resets: list[int] = []

    def reset() -> Awaitable[None]:
        # The swap happens here, synchronously; the returned awaitable is the deferred close.
        resets.append(1)
        return asyncio.sleep(0)

    outcomes = iter(["timeout", "timeout", "ok", "timeout", "timeout"])

    async def flaky(identifier: str, address: str) -> bytes:
        if next(outcomes) == "ok":
            return b"cipher"
        raise httpx.PoolTimeout("pool")

    fetcher = BackendChunkFetcher(
        {"arion": flaky},
        concurrency=4,
        attempts=1,
        base_sleep=0.0,
        jitter=0.0,
        reset_fn=reset,
        reset_after_pool_timeouts=3,
    )
    for expect_ok in (False, False, True, False, False):
        if expect_ok:
            assert await fetcher.fetch([("arion", "a")], "addr") == b"cipher"
        else:
            with pytest.raises(ChunkUnavailableError):
                await fetcher.fetch([("arion", "a")], "addr")
    assert resets == [], "two-then-success-then-two never reaches three consecutive"


@pytest.mark.asyncio
async def test_a_success_from_the_old_client_does_not_clear_the_new_streak() -> None:
    # A fetch that started on the old client and finishes after the swap says nothing about the
    # replacement; letting it zero the counter would hide a still-wedged new client.
    import httpx

    resets: list[int] = []

    def reset() -> Awaitable[None]:
        resets.append(1)
        return asyncio.sleep(0)

    gate = asyncio.Event()

    async def arion(identifier: str, address: str) -> bytes:
        if identifier == "slow":
            await gate.wait()
            return b"cipher"
        raise httpx.PoolTimeout("pool")

    fetcher = BackendChunkFetcher(
        {"arion": arion},
        concurrency=4,
        attempts=1,
        base_sleep=0.0,
        jitter=0.0,
        reset_fn=reset,
        reset_after_pool_timeouts=2,
    )
    slow = asyncio.create_task(fetcher.fetch([("arion", "slow")], "addr"))
    await asyncio.sleep(0)  # `slow` holds its slot and captured generation 0

    for _ in range(2):
        with pytest.raises(ChunkUnavailableError):
            await fetcher.fetch([("arion", "stuck")], "addr")
    assert resets == [1]
    with pytest.raises(ChunkUnavailableError):
        await fetcher.fetch([("arion", "stuck")], "addr")  # first of the new streak

    gate.set()
    assert await slow == b"cipher"

    with pytest.raises(ChunkUnavailableError):
        await fetcher.fetch([("arion", "stuck")], "addr")  # second of the new streak
    assert resets == [1, 1], "the old-generation success did not restart the new client's streak"


@pytest.mark.asyncio
async def test_reset_swaps_in_a_fresh_client_before_closing_the_old_one() -> None:
    from hippius_s3.reader.backend_fetch import _ReplaceableArionClient

    events: list[str] = []

    class FakeClient:
        def __init__(self, n: int) -> None:
            self.n = n

        async def close(self) -> None:
            # The swap happens before the close: a fetch arriving during a slow close must already
            # be handed the replacement, never the client being torn down.
            assert holder.client is not self
            events.append(f"close-{self.n}")

        def pool_snapshot(self) -> str:
            return f"snapshot-{self.n}"

    counter = iter(range(1, 10))
    holder = _ReplaceableArionClient(lambda: FakeClient(next(counter)), drain_seconds=0.0, name="Arion client")
    first = holder.client
    await holder.reset()
    assert holder.client is not first
    assert holder.client.n == 2
    assert events == ["close-1"]


@pytest.mark.asyncio
async def test_reset_logs_the_old_pool_before_the_swap(caplog: pytest.LogCaptureFixture) -> None:
    # The snapshot is the only evidence of what was holding the connections; it must be taken
    # from the client being replaced, not the fresh one.
    import logging

    from hippius_s3.reader.backend_fetch import _ReplaceableArionClient

    class FakeClient:
        def __init__(self, n: int) -> None:
            self.n = n

        async def close(self) -> None:
            pass

        def pool_snapshot(self) -> str:
            return f"connections={self.n}"

    counter = iter(range(1, 10))
    holder = _ReplaceableArionClient(lambda: FakeClient(next(counter)), drain_seconds=0.0, name="Arion client")
    with caplog.at_level(logging.ERROR, logger="hippius_s3.http_client"):
        await holder.reset()
    lines = [r.getMessage() for r in caplog.records if "replacing the Arion client" in r.getMessage()]
    assert lines == ["replacing the Arion client; old pool connections=1"]


@pytest.mark.asyncio
async def test_the_old_client_drains_before_it_is_closed() -> None:
    # Only the pool is wedged, not every connection: the old client's healthy in-flight fetches
    # get one attempt's worst case to finish before the close kills them.
    from hippius_s3.reader.backend_fetch import _ReplaceableArionClient

    closed = asyncio.Event()

    class FakeClient:
        async def close(self) -> None:
            closed.set()

        def pool_snapshot(self) -> str:
            return "fake"

    holder = _ReplaceableArionClient(FakeClient, drain_seconds=0.2, name="Arion client")
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    pending = asyncio.ensure_future(holder.reset())
    await asyncio.sleep(0.05)
    assert not closed.is_set(), "closed before the drain elapsed"
    await pending
    assert closed.is_set()
    assert loop.time() - t0 >= 0.2


@pytest.mark.asyncio
async def test_a_failed_rebuild_keeps_the_previous_client_and_raises() -> None:
    from hippius_s3.reader.backend_fetch import _ReplaceableArionClient

    class Fine:
        async def close(self) -> None:
            raise AssertionError("the old client must not be closed when its replacement failed to build")

        def pool_snapshot(self) -> str:
            return "fine"

    def make() -> Fine:
        if made:
            raise RuntimeError("cannot build")
        made.append(1)
        return Fine()

    made: list[int] = []
    holder = _ReplaceableArionClient(make, drain_seconds=0.0, name="Arion client")
    before = holder.client
    # The swap is synchronous, so a failed build raises from the call itself, not from awaiting.
    with pytest.raises(RuntimeError, match="cannot build"):
        holder.reset()
    assert holder.client is before


@pytest.mark.asyncio
async def test_a_failing_reset_fn_does_not_escape_fetch_and_the_next_streak_retries_it(
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    import httpx

    rebuilds: list[int] = []

    def reset() -> Awaitable[None]:
        rebuilds.append(1)
        raise RuntimeError("cannot build")

    async def stuck(identifier: str, address: str) -> bytes:
        raise httpx.PoolTimeout("pool")

    fetcher = BackendChunkFetcher(
        {"arion": stuck},
        concurrency=4,
        attempts=1,
        base_sleep=0.0,
        jitter=0.0,
        reset_fn=reset,
        reset_after_pool_timeouts=1,
    )
    with caplog.at_level(logging.ERROR, logger="hippius_s3.reader.backend_fetch"):
        # The caller still sees the fetch failure, not the rebuild's.
        with pytest.raises(ChunkUnavailableError, match="pool saturated"):
            await fetcher.fetch([("arion", "a")], "addr")
    assert rebuilds == [1]
    assert any("rebuild failed" in r.getMessage() for r in caplog.records)
    # The generation is bumped even though the swap failed: the rest of the same burst must not
    # hammer a rebuild that just failed. The still-wedged client produces a fresh streak, which
    # retries it.
    assert fetcher._generation == 1
    assert fetcher._reset_tasks == set(), "nothing to defer when the swap never happened"

    with pytest.raises(ChunkUnavailableError):
        await fetcher.fetch([("arion", "a")], "addr")
    assert rebuilds == [1, 1], "the next streak retries the rebuild"
    assert fetcher._generation == 2


@pytest.mark.asyncio
async def test_a_burst_of_pool_timeouts_rebuilds_the_client_once() -> None:
    # A wedged pool fails its waiters in a burst. Every attempt started under the same client, so
    # the whole burst is evidence about that one client: one rebuild, not one per threshold-worth.
    import httpx

    resets: list[int] = []

    def reset() -> Awaitable[None]:
        # The swap happens here, synchronously; the returned awaitable is the deferred close.
        resets.append(1)
        return asyncio.sleep(0)

    async def stuck(identifier: str, address: str) -> bytes:
        # Let every attempt start (and capture the generation) before any of them fails, the way
        # a real pool_timeout expiry lands seconds after the attempts began.
        await asyncio.sleep(0)
        raise httpx.PoolTimeout("pool")

    fetcher = BackendChunkFetcher(
        {"arion": stuck},
        concurrency=32,
        attempts=1,
        base_sleep=0.0,
        jitter=0.0,
        reset_fn=reset,
        reset_after_pool_timeouts=3,
    )
    results = await asyncio.gather(
        *(fetcher.fetch([("arion", str(i))], "addr") for i in range(32)), return_exceptions=True
    )
    assert all(isinstance(r, ChunkUnavailableError) for r in results)
    assert resets == [1], f"32 PoolTimeouts from one client generation are one rebuild, got {len(resets)}"
    # Every slot came back; the terminal paths release explicitly rather than via finally.
    assert fetcher._inflight == 0


@pytest.mark.asyncio
async def test_the_tripping_requester_does_not_wait_for_the_old_client_to_close() -> None:
    # The requester is a GET whose 503 must not wait on a wedged pool's close: the swap is
    # synchronous and the close is its own task, which outlives the requester.
    import httpx

    release = asyncio.Event()
    events: list[str] = []

    async def close_old() -> None:
        await release.wait()
        events.append("closed")

    def reset() -> Awaitable[None]:
        events.append("swapped")
        return close_old()

    async def stuck(identifier: str, address: str) -> bytes:
        raise httpx.PoolTimeout("pool")

    fetcher = BackendChunkFetcher(
        {"arion": stuck},
        concurrency=1,
        attempts=1,
        base_sleep=0.0,
        jitter=0.0,
        reset_fn=reset,
        reset_after_pool_timeouts=1,
    )
    with pytest.raises(ChunkUnavailableError, match="pool saturated"):
        await fetcher.fetch([("arion", "a")], "addr")
    assert events == ["swapped"], "the requester returned while the old client was still open"
    assert fetcher._inflight == 0, "the failed fetch gave its slot back"
    assert len(fetcher._reset_tasks) == 1, "the deferred close is held while it runs"

    release.set()
    await asyncio.gather(*fetcher._reset_tasks)
    assert events == ["swapped", "closed"], "the deferred close completed on its own"
    assert fetcher._reset_tasks == set(), "a finished close is dropped from the store"


@pytest.mark.asyncio
async def test_reset_does_not_wait_forever_on_a_client_that_will_not_close() -> None:
    from hippius_s3.reader.backend_fetch import _ReplaceableArionClient

    class Hanging:
        async def close(self) -> None:
            await asyncio.sleep(3600)

        def pool_snapshot(self) -> str:
            return "hanging"

    class Fresh:
        async def close(self) -> None:
            pass

        def pool_snapshot(self) -> str:
            return "fresh"

    async def gave_up(awaitable: Coroutine[Any, Any, None], **_bound: float) -> None:
        # Stand in for the bound expiring; close the coroutine so the loop does not warn that
        # the hanging close() was never awaited.
        awaitable.close()
        raise asyncio.TimeoutError

    made = iter([Hanging(), Fresh()])
    holder = _ReplaceableArionClient(lambda: next(made), drain_seconds=0.0, name="Arion client")
    with patch("hippius_s3.http_client.asyncio.wait_for", gave_up):
        await holder.reset()  # must return, not raise
    assert isinstance(holder.client, Fresh)


@pytest.mark.asyncio
async def test_each_attempt_records_its_outcome(monkeypatch: pytest.MonkeyPatch) -> None:
    # Attempts, not fetches: a wedged client shows up as attempts that do not end in `ok`, which
    # is the shape a per-success counter cannot show.
    import httpx

    from hippius_s3.reader import backend_fetch

    seen: list[str] = []
    monkeypatch.setattr(backend_fetch, "_record_backend_fetch_outcome", seen.append)

    tries = iter([ConnectionError("blip"), httpx.ConnectError("no route"), None])

    async def eventually(identifier: str, address: str) -> bytes:
        exc = next(tries)
        if exc is not None:
            raise exc
        return b"cipher"

    assert await _fetcher({"arion": eventually}, attempts=3).fetch([("arion", "a")], "addr") == b"cipher"
    assert seen == ["error", "error", "ok"]


@pytest.mark.asyncio
async def test_timeouts_record_their_outcome_per_attempt(monkeypatch: pytest.MonkeyPatch) -> None:
    # Connect and read timeouts are retried (no deadline here), so each attempt counts under its
    # own label; a PoolTimeout is terminal, so it counts once.
    import httpx

    from hippius_s3.reader import backend_fetch

    seen: list[str] = []
    monkeypatch.setattr(backend_fetch, "_record_backend_fetch_outcome", seen.append)

    async def slow(identifier: str, address: str) -> bytes:
        raise httpx.ReadTimeout("slow")

    async def unreachable(identifier: str, address: str) -> bytes:
        raise httpx.ConnectTimeout("no")

    async def stuck(identifier: str, address: str) -> bytes:
        raise httpx.PoolTimeout("pool")

    for fetch_one in (slow, unreachable, stuck):
        with pytest.raises(ChunkUnavailableError):
            await _fetcher({"arion": fetch_one}, attempts=3).fetch([("arion", "a")], "addr")
    assert seen == ["read_timeout"] * 3 + ["connect_timeout"] * 3 + ["pool_timeout"]


@pytest.mark.asyncio
async def test_a_timed_out_attempt_is_logged_under_its_outcome_label(caplog: pytest.LogCaptureFixture) -> None:
    # The log line and the metric share one vocabulary, so a responder can grep for the label
    # the dashboard shows.
    import logging

    import httpx

    async def slow(identifier: str, address: str) -> bytes:
        raise httpx.ReadTimeout("slow")

    with caplog.at_level(logging.WARNING, logger="hippius_s3.reader.backend_fetch"):
        with pytest.raises(ChunkUnavailableError):
            await _fetcher({"arion": slow}, attempts=2).fetch([("arion", "a")], "addr")
    kinds = [r.getMessage().split("kind=")[1].split(" ")[0] for r in caplog.records if "kind=" in r.getMessage()]
    assert kinds == ["read_timeout", "read_timeout"]


@pytest.mark.parametrize(
    ("exc", "outcome"),
    [
        (httpx.PoolTimeout("pool"), "pool_timeout"),
        (httpx.ConnectTimeout("no"), "connect_timeout"),
        (httpx.ReadTimeout("slow"), "read_timeout"),
        (httpx.WriteTimeout("slow"), "read_timeout"),
        (ConnectionError("blip"), "error"),
    ],
    ids=["pool", "connect", "read", "write", "other"],
)
def test_outcome_of_keeps_the_timeout_subclasses_apart(exc: Exception, outcome: str) -> None:
    # PoolTimeout, ConnectTimeout, ReadTimeout and WriteTimeout are all TimeoutException; the
    # mapping must check the specific classes, or every timeout collapses into one label.
    from hippius_s3.reader.backend_fetch import _outcome_of

    assert _outcome_of(exc) == outcome


@pytest.mark.asyncio
async def test_slot_gauges_track_inflight_and_waiting(monkeypatch: pytest.MonkeyPatch) -> None:
    # The gauges come from the fetcher's own accounting, not httpx pool internals: a wedged worker
    # shows inflight pinned at the concurrency cap while the ok-outcome counter goes flat.
    from hippius_s3.reader import backend_fetch

    snapshots: list[tuple[int, int]] = []
    monkeypatch.setattr(
        backend_fetch, "_publish_slots", lambda inflight, waiting: snapshots.append((inflight, waiting))
    )

    gate = asyncio.Event()

    async def blocked(identifier: str, address: str) -> bytes:
        await gate.wait()
        return b"cipher"

    fetcher = _fetcher({"arion": blocked}, concurrency=1)
    t1 = asyncio.create_task(fetcher.fetch([("arion", "a")], "addr"))
    t2 = asyncio.create_task(fetcher.fetch([("arion", "b")], "addr"))
    await asyncio.sleep(0.05)
    assert (1, 1) in snapshots, "one fetch holds the only slot, one is waiting"
    gate.set()
    await asyncio.gather(t1, t2)
    assert snapshots[-1] == (0, 0)


@pytest.mark.asyncio
async def test_a_queue_timeout_leaves_no_waiter_behind(monkeypatch: pytest.MonkeyPatch) -> None:
    from hippius_s3.reader import backend_fetch

    snapshots: list[tuple[int, int]] = []
    monkeypatch.setattr(
        backend_fetch, "_publish_slots", lambda inflight, waiting: snapshots.append((inflight, waiting))
    )

    gate = asyncio.Event()

    async def blocked(identifier: str, address: str) -> bytes:
        await gate.wait()
        return b"cipher"

    fetcher = BackendChunkFetcher(
        {"arion": blocked}, concurrency=1, attempts=1, base_sleep=0.0, jitter=0.0, queue_timeout=0.05
    )
    holder = asyncio.create_task(fetcher.fetch([("arion", "a")], "addr"))
    await asyncio.sleep(0.01)
    with pytest.raises(ChunkUnavailableError, match="saturated"):
        await fetcher.fetch([("arion", "b")], "addr")
    assert snapshots[-1] == (1, 0), "the timed-out waiter is no longer counted; the holder still is"
    gate.set()
    await holder
    assert snapshots[-1] == (0, 0)


@pytest.mark.asyncio
async def test_a_requester_cancelled_while_queued_leaves_no_waiter_behind(monkeypatch: pytest.MonkeyPatch) -> None:
    # A client that disconnects before its fetch gets a slot is cancelled inside the wait. If that
    # path skipped the accounting, `backend_fetch_waiting` would read one waiter too many for the
    # life of the process — a phantom queue in the gauge that exists to show a real one.
    from hippius_s3.reader import backend_fetch

    snapshots: list[tuple[int, int]] = []
    monkeypatch.setattr(
        backend_fetch, "_publish_slots", lambda inflight, waiting: snapshots.append((inflight, waiting))
    )

    gate = asyncio.Event()

    async def blocked(identifier: str, address: str) -> bytes:
        await gate.wait()
        return b"cipher"

    fetcher = _fetcher({"arion": blocked}, concurrency=1)
    holder = asyncio.create_task(fetcher.fetch([("arion", "a")], "addr"))
    queued = asyncio.create_task(fetcher.fetch([("arion", "b")], "addr"))
    await asyncio.sleep(0.01)
    assert snapshots[-1] == (1, 1)

    queued.cancel()
    with pytest.raises(asyncio.CancelledError):
        await queued
    assert snapshots[-1] == (1, 0), "the cancelled waiter is no longer counted; the holder still is"

    gate.set()
    await holder
    assert snapshots[-1] == (0, 0)
