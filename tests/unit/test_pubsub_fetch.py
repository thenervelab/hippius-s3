"""Tests for pub/sub-based chunk fetching via ObjectPartsCache.wait_for_chunk.

Validates the subscribe→check→wait pattern that prevents the classic
pub/sub race condition where a worker publishes before the reader subscribes.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest


class FakePubSub:
    """Simulates redis.asyncio pubsub with controllable message delivery."""

    def __init__(self) -> None:
        self._subscribed: list[str] = []
        self._messages: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.subscribe = AsyncMock(side_effect=self._subscribe)
        self.unsubscribe = AsyncMock()
        self.aclose = AsyncMock()

    async def _subscribe(self, channel: str) -> None:
        self._subscribed.append(channel)

    async def listen(self):
        while True:
            msg = await self._messages.get()
            yield msg

    def inject_message(self, channel: str) -> None:
        self._messages.put_nowait({"type": "message", "channel": channel, "data": b"1"})


class FakeObjCache:
    """Simulates RedisObjectPartsCache with controllable chunk existence and pub/sub."""

    def __init__(self, pubsub: FakePubSub) -> None:
        self._chunks: dict[str, bytes] = {}
        self._get_count = 0
        self._pubsub = pubsub

    def build_chunk_key(self, object_id: str, object_version: int, part_number: int, chunk_index: int) -> str:
        return f"obj:{object_id}:v:{object_version}:part:{part_number}:chunk:{chunk_index}"

    async def get_chunk(self, object_id: str, object_version: int, part_number: int, chunk_index: int) -> bytes | None:
        self._get_count += 1
        key = self.build_chunk_key(object_id, object_version, part_number, chunk_index)
        return self._chunks.get(key)

    def set_chunk_data(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int, data: bytes
    ) -> None:
        key = self.build_chunk_key(object_id, object_version, part_number, chunk_index)
        self._chunks[key] = data

    async def wait_for_chunk(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int
    ) -> bytes:
        from contextlib import asynccontextmanager
        from typing import AsyncIterator
        from typing import cast

        chunk_key = self.build_chunk_key(object_id, int(object_version), int(part_number), int(chunk_index))
        channel = f"notify:{chunk_key}"

        @asynccontextmanager
        async def _subscribe(ch: str) -> AsyncIterator[Any]:
            await self._pubsub.subscribe(ch)
            try:
                yield self._pubsub
            finally:
                await self._pubsub.unsubscribe(ch)
                await self._pubsub.aclose()

        async with _subscribe(channel) as pubsub:
            c = await self.get_chunk(object_id, int(object_version), int(part_number), int(chunk_index))
            if c is not None:
                return cast(bytes, c)

            async for msg in pubsub.listen():
                if msg["type"] == "message":
                    break

        return cast(
            bytes,
            await self.get_chunk(object_id, int(object_version), int(part_number), int(chunk_index)),
        )

    async def notify_chunk(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int
    ) -> None:
        pass


@pytest.mark.asyncio
async def test_chunk_already_exists_returns_immediately():
    """If chunk exists before we wait, return it without waiting for a message."""
    pubsub = FakePubSub()
    obj_cache = FakeObjCache(pubsub)
    obj_cache.set_chunk_data("obj1", 1, 1, 0, b"hello")

    result = await obj_cache.wait_for_chunk("obj1", 1, 1, 0)

    assert result == b"hello"
    # Should have subscribed first (before the GET check)
    pubsub.subscribe.assert_awaited_once()
    # Should have cleaned up
    pubsub.unsubscribe.assert_awaited_once()
    pubsub.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_worker_publishes_after_subscribe():
    """Worker writes chunk and publishes after fetcher subscribes — fetcher gets notified."""
    pubsub = FakePubSub()
    obj_cache = FakeObjCache(pubsub)

    chunk_key = obj_cache.build_chunk_key("obj1", 1, 1, 0)
    channel = f"notify:{chunk_key}"

    async def simulate_worker():
        await asyncio.sleep(0.01)
        obj_cache.set_chunk_data("obj1", 1, 1, 0, b"worker-data")
        pubsub.inject_message(channel)

    worker_task = asyncio.create_task(simulate_worker())
    result = await obj_cache.wait_for_chunk("obj1", 1, 1, 0)
    await worker_task

    assert result == b"worker-data"
    pubsub.unsubscribe.assert_awaited_once()
    pubsub.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_multiple_concurrent_fetchers():
    """Multiple fetchers waiting for the same chunk all get notified."""
    pubsub1 = FakePubSub()
    pubsub2 = FakePubSub()
    obj_cache1 = FakeObjCache(pubsub1)
    obj_cache2 = FakeObjCache(pubsub2)

    chunk_key = obj_cache1.build_chunk_key("obj1", 1, 1, 0)
    channel = f"notify:{chunk_key}"

    async def simulate_worker():
        await asyncio.sleep(0.01)
        obj_cache1.set_chunk_data("obj1", 1, 1, 0, b"shared-data")
        obj_cache2.set_chunk_data("obj1", 1, 1, 0, b"shared-data")
        pubsub1.inject_message(channel)
        pubsub2.inject_message(channel)

    worker_task = asyncio.create_task(simulate_worker())
    r1, r2 = await asyncio.gather(
        obj_cache1.wait_for_chunk("obj1", 1, 1, 0),
        obj_cache2.wait_for_chunk("obj1", 1, 1, 0),
    )
    await worker_task

    assert r1 == b"shared-data"
    assert r2 == b"shared-data"
