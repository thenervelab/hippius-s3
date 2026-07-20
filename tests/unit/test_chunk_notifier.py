"""ChunkNotifier tests — pub/sub wait/notify contract.

Uses an in-memory fake Redis pubsub so we test the notifier's race-safe
subscribe/recheck/wait pattern without depending on a real Redis.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest
from redis.exceptions import TimeoutError as RedisTimeoutError

from hippius_s3.cache.notifier import ChunkNotifier
from hippius_s3.cache.notifier import ChunkNotReadyError
from hippius_s3.cache.notifier import build_chunk_key


OBJ = "11111111-2222-3333-4444-555555555555"


class FakePubSub:
    """Minimal pubsub that can be fed messages."""

    def __init__(self) -> None:
        self._messages: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.subscribe = AsyncMock()
        self.unsubscribe = AsyncMock()
        self.aclose = AsyncMock()

    async def listen(self):
        while True:
            msg = await self._messages.get()
            yield msg

    def inject_message(self, channel: str) -> None:
        self._messages.put_nowait({"type": "message", "channel": channel, "data": b"1"})


class FakeRedis:
    """Minimal async Redis surface used by ChunkNotifier: pubsub() + publish()."""

    def __init__(self) -> None:
        self.pubsub_instance = FakePubSub()
        self.published: list[tuple[str, bytes]] = []

    def pubsub(self) -> FakePubSub:
        return self.pubsub_instance

    async def publish(self, channel: str, message: bytes | str) -> None:
        self.published.append((channel, message if isinstance(message, bytes) else message.encode()))


# -------- build_chunk_key ----------


def test_build_chunk_key_format() -> None:
    key = build_chunk_key(OBJ, 7, 3, 42)
    assert key == f"obj:{OBJ}:v:7:part:3:chunk:42"


# -------- notify ----------


@pytest.mark.asyncio
async def test_notify_publishes_on_correct_channel() -> None:
    redis = FakeRedis()
    notifier = ChunkNotifier(redis)

    await notifier.notify(OBJ, 1, 2, 3)

    expected_channel = f"notify:{build_chunk_key(OBJ, 1, 2, 3)}"
    assert redis.published == [(expected_channel, b"1")]


# -------- wait_for_chunk ----------


@pytest.mark.asyncio
async def test_wait_fast_path_no_subscribe_needed() -> None:
    """If fetch_fn returns data immediately, no pubsub activity happens."""
    redis = FakeRedis()
    notifier = ChunkNotifier(redis)

    async def fetch(oid, v, pn, ci):
        return b"hello"

    result = await notifier.wait_for_chunk(OBJ, 1, 1, 0, fetch_fn=fetch, timeout=1.0)

    assert result == b"hello"
    redis.pubsub_instance.subscribe.assert_not_awaited()
    redis.pubsub_instance.aclose.assert_not_awaited()


@pytest.mark.asyncio
async def test_wait_slow_path_subscribes_then_returns_on_notify() -> None:
    """Reader subscribes, worker publishes, reader fetches and returns."""
    redis = FakeRedis()
    notifier = ChunkNotifier(redis)

    available = False

    async def fetch(oid, v, pn, ci):
        return b"data" if available else None

    async def simulate_worker():
        nonlocal available
        await asyncio.sleep(0.01)
        available = True
        redis.pubsub_instance.inject_message(f"notify:{build_chunk_key(OBJ, 1, 1, 0)}")

    worker = asyncio.create_task(simulate_worker())
    result = await notifier.wait_for_chunk(OBJ, 1, 1, 0, fetch_fn=fetch, timeout=1.0)
    await worker

    assert result == b"data"
    redis.pubsub_instance.subscribe.assert_awaited_once()
    redis.pubsub_instance.unsubscribe.assert_awaited_once()
    redis.pubsub_instance.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_wait_race_safe_recheck_after_subscribe() -> None:
    """If the chunk appears between fast-path and subscribe, the post-subscribe
    re-check should pick it up without needing a notification."""
    redis = FakeRedis()
    notifier = ChunkNotifier(redis)

    call_count = 0

    async def fetch(oid, v, pn, ci):
        nonlocal call_count
        call_count += 1
        # First call (fast path) returns None, second (post-subscribe recheck)
        # returns the data. No message is ever injected.
        if call_count == 1:
            return None
        return b"recheck-win"

    result = await notifier.wait_for_chunk(OBJ, 1, 1, 0, fetch_fn=fetch, timeout=1.0)

    assert result == b"recheck-win"
    assert call_count == 2


@pytest.mark.asyncio
async def test_wait_timeout_raises() -> None:
    """If no notification arrives within the timeout, TimeoutError propagates."""
    redis = FakeRedis()
    notifier = ChunkNotifier(redis)

    async def fetch(oid, v, pn, ci):
        return None

    with pytest.raises(asyncio.TimeoutError):
        await notifier.wait_for_chunk(OBJ, 1, 1, 0, fetch_fn=fetch, timeout=0.05)


@pytest.mark.asyncio
async def test_wait_transient_miss_retries_once() -> None:
    """Worker publishes but chunk is missing on first post-notify fetch
    (e.g. janitor evicted it). Notifier retries once after a small sleep."""
    redis = FakeRedis()
    notifier = ChunkNotifier(redis)

    call_log: list[None] = []

    async def fetch(oid, v, pn, ci):
        # Pattern: fast-path miss, post-subscribe miss, post-notify miss, retry HIT
        call_log.append(None)
        if len(call_log) >= 4:
            return b"retry-win"
        return None

    async def simulate_worker():
        await asyncio.sleep(0.01)
        redis.pubsub_instance.inject_message(f"notify:{build_chunk_key(OBJ, 1, 1, 0)}")

    worker = asyncio.create_task(simulate_worker())
    result = await notifier.wait_for_chunk(OBJ, 1, 1, 0, fetch_fn=fetch, timeout=1.0)
    await worker

    assert result == b"retry-win"
    assert len(call_log) == 4


@pytest.mark.asyncio
async def test_wait_survives_transient_redis_read_timeout() -> None:
    """A redis socket read timeout from listen() must NOT kill the wait.

    Reproduces the incident where the pub/sub connection's short socket read
    timeout fired (redis.exceptions.TimeoutError) before the slow downloader
    delivered the chunk. The wait should swallow it, re-check the FS, and keep
    waiting up to the real timeout — then succeed once the chunk lands.
    """
    redis = FakeRedis()
    notifier = ChunkNotifier(redis)

    # listen() raises a redis TimeoutError on its first call (the socket read
    # window), then on the second call blocks until the worker injects a message.
    listen_calls = 0

    async def flaky_listen():
        nonlocal listen_calls
        listen_calls += 1
        if listen_calls == 1:
            raise RedisTimeoutError("Timeout reading from redis-queues:6379")
        while True:
            msg = await redis.pubsub_instance._messages.get()
            yield msg

    redis.pubsub_instance.listen = flaky_listen  # type: ignore[method-assign]

    available = False

    async def fetch(oid, v, pn, ci):
        return b"late-chunk" if available else None

    async def simulate_slow_worker():
        nonlocal available
        await asyncio.sleep(0.02)
        available = True
        redis.pubsub_instance.inject_message(f"notify:{build_chunk_key(OBJ, 1, 1, 0)}")

    worker = asyncio.create_task(simulate_slow_worker())
    result = await notifier.wait_for_chunk(OBJ, 1, 1, 0, fetch_fn=fetch, timeout=2.0)
    await worker

    assert result == b"late-chunk"
    assert listen_calls >= 2  # proves we retried after the transient timeout


@pytest.mark.asyncio
async def test_wait_repolls_fs_for_a_chunk_landed_without_notification(monkeypatch: pytest.MonkeyPatch) -> None:
    """A chunk made readable by a NON-notifying producer must be picked up via the periodic FS
    re-poll, not block until the caller `timeout`.

    Reproduces the fresh-object cross-node read hang: the Rust drain lands a part in the CephFS
    pool (readable via the pool fallback) WITHOUT publishing `notify:`, and the queues pub/sub
    client has no socket read timeout. So no notification ever arrives and the socket-timeout
    re-poll never fires — the wait is driven purely by the periodic FS re-read. Before the fix
    (notification-only wait) this blocked the full `timeout` (up to the 90s first-chunk bound →
    a client-visible ~35s hang) even though the chunk was already in the pool.
    """
    import hippius_s3.cache.notifier as notifier_mod

    monkeypatch.setattr(notifier_mod, "_FS_REPOLL_INTERVAL_SECONDS", 0.02)

    redis = FakeRedis()
    notifier = ChunkNotifier(redis)

    available = False

    async def fetch(oid: str, v: int, pn: int, ci: int) -> bytes | None:
        return b"drained-copy" if available else None

    async def drain_lands_it_without_notifying() -> None:
        await asyncio.sleep(0.1)  # the drain copies the part to the pool a bit later
        nonlocal available
        available = True
        # DELIBERATELY no inject_message — the drain does not publish a chunk-ready notification.

    worker = asyncio.create_task(drain_lands_it_without_notifying())
    loop = asyncio.get_event_loop()
    t0 = loop.time()
    # A large timeout: without the re-poll this blocks the full 30s (nothing wakes the waiter),
    # so returning quickly is the proof the FS re-poll works.
    result = await notifier.wait_for_chunk(OBJ, 1, 1, 0, fetch_fn=fetch, timeout=30.0)
    elapsed = loop.time() - t0
    await worker

    assert result == b"drained-copy"
    assert elapsed < 2.0, f"re-poll should return ~1 interval after the chunk lands, not block; took {elapsed:.2f}s"
    assert redis.published == [], "no notification was published — pickup was via the FS re-poll"


@pytest.mark.asyncio
async def test_wait_gives_up_if_retry_also_misses() -> None:
    """If even the retry fails, wait_for_chunk raises RuntimeError."""
    redis = FakeRedis()
    notifier = ChunkNotifier(redis)

    async def fetch(oid, v, pn, ci):
        return None

    async def simulate_worker():
        await asyncio.sleep(0.01)
        redis.pubsub_instance.inject_message(f"notify:{build_chunk_key(OBJ, 1, 1, 0)}")

    worker = asyncio.create_task(simulate_worker())
    with pytest.raises(ChunkNotReadyError, match="missing after pub/sub"):
        await notifier.wait_for_chunk(OBJ, 1, 1, 0, fetch_fn=fetch, timeout=1.0)
    await worker
