"""Chunk notification pub/sub.

Thin Redis pub/sub wrapper: when a worker finishes writing a chunk to the
filesystem cache, it calls `notify(...)`. Streamers that need a chunk call
`wait_for_chunk(...)` which subscribes to the channel, then re-reads the
chunk from a caller-supplied `fetch_fn` (typically `fs_store.get_chunk`).

Separated from FileSystemPartsStore so chunk I/O stays pure (no Redis
dependency) and the pub/sub concern is isolated.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any
from typing import AsyncIterator
from typing import Awaitable
from typing import Callable


logger = logging.getLogger(__name__)


def build_chunk_key(object_id: str, object_version: int, part_number: int, chunk_index: int) -> str:
    """Deterministic cache key used for pub/sub channels.

    Kept as a module-level function (not a method) so callers can construct
    keys without needing a notifier instance.
    """
    return f"obj:{object_id}:v:{int(object_version)}:part:{int(part_number)}:chunk:{int(chunk_index)}"


class ChunkNotifier:
    """Redis pub/sub for chunk-ready notifications."""

    def __init__(self, redis_client: Any) -> None:
        """Initialize with a Redis client used for publish/subscribe.

        Typically pass the queues Redis (redis_queues_client) so pub/sub
        traffic is isolated from chunk/cache Redis.
        """
        self._redis = redis_client

    async def notify(self, object_id: str, object_version: int, part_number: int, chunk_index: int) -> None:
        """Publish a chunk-ready notification."""
        chunk_key = build_chunk_key(object_id, object_version, part_number, chunk_index)
        await self._redis.publish(f"notify:{chunk_key}", "1")

    @asynccontextmanager
    async def _subscribe(self, channel: str) -> AsyncIterator[Any]:
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(channel)
        try:
            yield pubsub
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()

    async def wait_for_chunk(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        chunk_index: int,
        *,
        fetch_fn: Callable[[str, int, int, int], Awaitable[bytes | None]],
        timeout: float,  # noqa: ASYNC109
    ) -> bytes:
        """Return chunk bytes, waiting on pub/sub if not yet present.

        Flow:
        1. Fast path: call fetch_fn; if it returns data, done.
        2. Slow path: subscribe to the chunk channel, re-check (race safety),
           then wait for a notification. On notification, fetch again.
           One extra retry with a small sleep covers transient misses
           (janitor deleting between notification and fetch, etc.).

        Args:
            object_id: Object UUID.
            object_version: Object version.
            part_number: Part number.
            chunk_index: Chunk index within the part.
            fetch_fn: Async callable (oid, v, pn, ci) -> bytes | None.
            timeout: Max seconds to wait for a notification.

        Returns:
            Chunk bytes.

        Raises:
            RuntimeError: If the chunk is still missing after notification + retry.
            asyncio.TimeoutError: If no notification arrives within `timeout`.
        """
        # Fast path
        data = await fetch_fn(object_id, int(object_version), int(part_number), int(chunk_index))
        if data is not None:
            return data

        chunk_key = build_chunk_key(object_id, object_version, part_number, chunk_index)
        channel = f"notify:{chunk_key}"

        async with self._subscribe(channel) as pubsub:
            # Re-check after subscribe (race: worker may have finished between
            # our fast-path fetch and our subscribe).
            data = await fetch_fn(object_id, int(object_version), int(part_number), int(chunk_index))
            if data is not None:
                return data

            async def _listen() -> None:
                async for msg in pubsub.listen():
                    if msg["type"] == "message":
                        return

            await asyncio.wait_for(_listen(), timeout=timeout)

        # Worker published — fetch it. Retry once on transient miss (e.g. a
        # janitor delete or CephFS replication lag).
        data = await fetch_fn(object_id, int(object_version), int(part_number), int(chunk_index))
        if data is None:
            logger.warning("Chunk transient miss after notification, retrying: %s", chunk_key)
            await asyncio.sleep(0.1)
            data = await fetch_fn(object_id, int(object_version), int(part_number), int(chunk_index))
        if data is None:
            raise RuntimeError(f"Chunk missing after pub/sub notification: {chunk_key}")
        return data
