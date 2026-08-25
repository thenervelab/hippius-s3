"""RQ-1: one pub/sub subscription per stream, demuxed to per-chunk waiters.

The per-chunk `wait_for_chunk` opens and tears down a fresh Redis pubsub for every cold chunk —
O(chunks) subscribe/unsubscribe churn on a multi-chunk cold read. `stream_subscription` opens ONE
pattern subscription for the whole (object, version) and a single listener demuxes notifications to
per-chunk events. These tests pin the correctness-sensitive parts the plan calls out: out-of-order
demux, the post-subscribe FS re-check race guard, resilience to a missed wakeup, the transient-miss
retry, and the terminal timeout.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from hippius_s3.cache import notifier as notifier_mod
from hippius_s3.cache.notifier import ChunkNotifier
from hippius_s3.cache.notifier import build_chunk_key


OBJ = "11111111-2222-3333-4444-555555555555"


class FakePubSub:
    """Pattern pubsub fed pmessage dicts from the test."""

    def __init__(self) -> None:
        self._messages: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.psub_calls = 0
        self.punsub_calls = 0
        self.aclose_calls = 0

    async def psubscribe(self, *_patterns: str) -> None:
        self.psub_calls += 1

    async def punsubscribe(self, *_patterns: str) -> None:
        self.punsub_calls += 1

    async def aclose(self) -> None:
        self.aclose_calls += 1

    async def listen(self):
        while True:
            yield await self._messages.get()

    def inject(self, channel: str) -> None:
        self._messages.put_nowait({"type": "pmessage", "pattern": b"pat", "channel": channel, "data": b"1"})


class FakeRedis:
    def __init__(self) -> None:
        self.pubsubs: list[FakePubSub] = []

    def pubsub(self) -> FakePubSub:
        ps = FakePubSub()
        self.pubsubs.append(ps)
        return ps


def _channel(pn: int, ci: int) -> str:
    return f"notify:{build_chunk_key(OBJ, 1, pn, ci)}"


@pytest.mark.asyncio
async def test_single_subscription_serves_out_of_order_chunks() -> None:
    """One subscription for the whole stream resolves multiple chunks, even out of arrival order."""
    redis = FakeRedis()
    notifier = ChunkNotifier(redis)
    fs: dict[tuple[int, int], bytes] = {}

    async def fetch(_oid: str, _v: int, pn: int, ci: int) -> bytes | None:
        return fs.get((pn, ci))

    async with notifier.stream_subscription(OBJ, 1, fetch_fn=fetch) as sub:

        async def land() -> None:
            await asyncio.sleep(0.01)
            fs[(0, 1)] = b"chunk-one"
            redis.pubsubs[0].inject(_channel(0, 1))
            await asyncio.sleep(0.01)
            fs[(0, 0)] = b"chunk-zero"
            redis.pubsubs[0].inject(_channel(0, 0))

        landing = asyncio.create_task(land())
        # Await chunk 1 first (it lands first), then chunk 0.
        got1 = await sub.wait_for_chunk(0, 1, timeout=2.0)
        got0 = await sub.wait_for_chunk(0, 0, timeout=2.0)
        await landing

    assert got1 == b"chunk-one"
    assert got0 == b"chunk-zero"
    assert len(redis.pubsubs) == 1, "the whole stream must use exactly one pubsub"
    assert redis.pubsubs[0].psub_calls == 1


@pytest.mark.asyncio
async def test_fast_path_returns_without_waiting() -> None:
    """A chunk already on the FS returns immediately with no notification."""
    redis = FakeRedis()
    notifier = ChunkNotifier(redis)
    fs = {(0, 0): b"already-here"}

    async def fetch(_oid: str, _v: int, pn: int, ci: int) -> bytes | None:
        return fs.get((pn, ci))

    async with notifier.stream_subscription(OBJ, 1, fetch_fn=fetch) as sub:
        got = await sub.wait_for_chunk(0, 0, timeout=2.0)

    assert got == b"already-here"


@pytest.mark.asyncio
async def test_periodic_recheck_resolves_missed_wakeup(monkeypatch: Any) -> None:
    """If the chunk lands but NO notification is delivered (listener hiccup / missed publish), the
    periodic FS re-check must still resolve it within the timeout."""
    monkeypatch.setattr(notifier_mod, "_STREAM_RECHECK_INTERVAL_SECONDS", 0.02)
    redis = FakeRedis()
    notifier = ChunkNotifier(redis)
    fs: dict[tuple[int, int], bytes] = {}

    async def fetch(_oid: str, _v: int, pn: int, ci: int) -> bytes | None:
        return fs.get((pn, ci))

    async with notifier.stream_subscription(OBJ, 1, fetch_fn=fetch) as sub:

        async def land_silently() -> None:
            await asyncio.sleep(0.05)
            fs[(0, 0)] = b"no-notify"  # deliberately never inject a message

        landing = asyncio.create_task(land_silently())
        got = await sub.wait_for_chunk(0, 0, timeout=2.0)
        await landing

    assert got == b"no-notify"


@pytest.mark.asyncio
async def test_transient_miss_after_notify_retries() -> None:
    """Notification fires but the first post-notify fetch misses (janitor/replication lag); a retry
    within the same subscription resolves it."""
    redis = FakeRedis()
    notifier = ChunkNotifier(redis)
    calls = {"n": 0}

    async def fetch(_oid: str, _v: int, pn: int, ci: int) -> bytes | None:
        calls["n"] += 1
        # fast-path miss, post-subscribe miss, post-notify miss, then hit.
        return b"eventually" if calls["n"] >= 4 else None

    async with notifier.stream_subscription(OBJ, 1, fetch_fn=fetch) as sub:

        async def notify() -> None:
            await asyncio.sleep(0.01)
            redis.pubsubs[0].inject(_channel(0, 0))

        n = asyncio.create_task(notify())
        got = await sub.wait_for_chunk(0, 0, timeout=2.0)
        await n

    assert got == b"eventually"


@pytest.mark.asyncio
async def test_terminal_timeout_raises() -> None:
    """A chunk that never lands and is never notified raises TimeoutError at the deadline."""
    redis = FakeRedis()
    notifier = ChunkNotifier(redis)

    async def fetch(_oid: str, _v: int, _pn: int, _ci: int) -> bytes | None:
        return None

    async with notifier.stream_subscription(OBJ, 1, fetch_fn=fetch) as sub:
        with pytest.raises(asyncio.TimeoutError):
            await sub.wait_for_chunk(0, 0, timeout=0.15)


@pytest.mark.asyncio
async def test_subscription_cleans_up_on_exit() -> None:
    """Leaving the context tears the single subscription down (punsubscribe + aclose)."""
    redis = FakeRedis()
    notifier = ChunkNotifier(redis)
    fs = {(0, 0): b"x"}

    async def fetch(_oid: str, _v: int, pn: int, ci: int) -> bytes | None:
        return fs.get((pn, ci))

    async with notifier.stream_subscription(OBJ, 1, fetch_fn=fetch) as sub:
        await sub.wait_for_chunk(0, 0, timeout=1.0)

    assert redis.pubsubs[0].punsub_calls == 1
    assert redis.pubsubs[0].aclose_calls == 1
