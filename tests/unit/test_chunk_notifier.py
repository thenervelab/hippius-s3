"""ChunkNotifier tests — pub/sub wait/notify contract.

Uses an in-memory fake Redis pubsub so we test the notifier's race-safe
subscribe/recheck/wait pattern without depending on a real Redis.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest

from hippius_s3.cache.notifier import ChunkNotifier
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
    with pytest.raises(RuntimeError, match="missing after pub/sub"):
        await notifier.wait_for_chunk(OBJ, 1, 1, 0, fetch_fn=fetch, timeout=1.0)
    await worker
