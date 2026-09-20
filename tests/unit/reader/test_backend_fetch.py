"""The read path's backend tier: fetch a ciphertext chunk into memory, in backend order, with bounded
retries and one concurrency budget per process. Nothing here writes to any cache — that is the point.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
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
async def test_a_bounded_timeout_is_not_retried_within_the_fetch() -> None:
    # After a 4-14 s bounded failure a second attempt cannot finish inside the reader's 25 s
    # first-chunk bound; retrying only reproduces the silent cancel the bounds exist to remove.
    import httpx

    calls: list[str] = []

    async def slow(identifier: str, address: str) -> bytes:
        calls.append("arion")
        raise httpx.ReadTimeout("slow")

    with pytest.raises(ChunkUnavailableError, match="timed out"):
        await _fetcher({"arion": slow}, attempts=3).fetch([("arion", "a")], "addr")
    assert calls == ["arion"]


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

    counter = iter(range(1, 10))
    holder = _ReplaceableArionClient(lambda: FakeClient(next(counter)))
    first = holder.client
    await holder.reset()
    assert holder.client is not first
    assert holder.client.n == 2
    assert events == ["close-1"]


@pytest.mark.asyncio
async def test_a_failed_rebuild_keeps_the_previous_client_and_raises() -> None:
    from hippius_s3.reader.backend_fetch import _ReplaceableArionClient

    class Fine:
        async def close(self) -> None:
            raise AssertionError("the old client must not be closed when its replacement failed to build")

    def make() -> Fine:
        if made:
            raise RuntimeError("cannot build")
        made.append(1)
        return Fine()

    made: list[int] = []
    holder = _ReplaceableArionClient(make)
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
async def test_a_requester_cancelled_mid_reset_releases_its_slot_and_the_reset_completes() -> None:
    # The reset is decoupled from the GET that tripped it: a client disconnecting while the rebuild
    # runs must neither abort the rebuild nor leak the slot its fetch held.
    import httpx

    started = asyncio.Event()
    release = asyncio.Event()
    events: list[str] = []

    async def close_old() -> None:
        started.set()
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
    task = asyncio.create_task(fetcher.fetch([("arion", "a")], "addr"))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert fetcher._inflight == 0, "the cancelled fetch gave its slot back"

    release.set()
    assert len(fetcher._reset_tasks) == 1, "the deferred close is held while it runs"
    await asyncio.gather(*fetcher._reset_tasks)
    assert events == ["swapped", "closed"], "the deferred close outlived the requester that tripped it"
    assert fetcher._reset_tasks == set(), "a finished close is dropped from the store"


@pytest.mark.asyncio
async def test_reset_does_not_wait_forever_on_a_client_that_will_not_close() -> None:
    from hippius_s3.reader.backend_fetch import _ReplaceableArionClient

    class Hanging:
        async def close(self) -> None:
            await asyncio.sleep(3600)

    class Fresh:
        async def close(self) -> None:
            pass

    async def gave_up(awaitable: Coroutine[Any, Any, None], **_bound: float) -> None:
        # Stand in for the bound expiring; close the coroutine so the loop does not warn that
        # the hanging close() was never awaited.
        awaitable.close()
        raise asyncio.TimeoutError

    made = iter([Hanging(), Fresh()])
    holder = _ReplaceableArionClient(lambda: next(made))
    with patch("hippius_s3.reader.backend_fetch.asyncio.wait_for", gave_up):
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
async def test_terminal_timeouts_record_their_outcome(monkeypatch: pytest.MonkeyPatch) -> None:
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
    assert seen == ["read_timeout", "connect_timeout", "pool_timeout"]


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
