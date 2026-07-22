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
import contextlib
import logging
from contextlib import asynccontextmanager
from typing import Any
from typing import AsyncIterator
from typing import Awaitable
from typing import Callable

from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError


logger = logging.getLogger(__name__)

# How often `wait_for_chunk` re-reads the FS while blocked, INDEPENDENT of pub/sub
# notifications. A chunk can become readable from a producer that does NOT publish a
# chunk-ready notification — notably the Rust drain, which lands a part in the CephFS pool
# (making it readable via the pool fallback) WITHOUT publishing `notify:`. Without this
# periodic re-read, a reader blocked on a fresh cross-node object waits out the whole caller
# `timeout` (up to the 90s first-chunk bound → a client-visible ~35s+ hang) even though the
# chunk is already in the pool. One poll interval bounds that to ~1s. The queues pub/sub
# client is built without a socket_timeout, so the RedisTimeoutError re-poll path never fires
# on its own — this tick is the primary FS re-read while waiting.
_FS_REPOLL_INTERVAL_SECONDS = 1.0

# RQ-1: with one subscription per stream the demux is event-driven, but a missed wakeup (listener
# socket hiccup, a publish that raced the subscribe) must not hang the stream. Each waiter also
# re-checks the FS on this cadence as a safety net, bounding worst-case latency to this interval.
_STREAM_RECHECK_INTERVAL_SECONDS = 1.0


class ChunkNotReadyError(Exception):
    """A chunk is still absent after a pub/sub notification woke the waiter.

    Distinct from `asyncio.TimeoutError` (no notification arrived in time): here the downloader
    DID notify — typically after giving up fast on a backend miss for an undrained object — but the
    chunk never materialized. Typed so the read-path peek can map it to a retryable 503 instead of
    letting a bare `RuntimeError` escape to a 500. See PR #235 task B1.
    """


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

    def stream_subscription(
        self,
        object_id: str,
        object_version: int,
        *,
        fetch_fn: Callable[[str, int, int, int], Awaitable[bytes | None]],
    ) -> _StreamSubscription:
        """Open ONE pattern subscription covering every chunk of `(object_id, object_version)`.

        Use as `async with notifier.stream_subscription(...) as sub:` for the lifetime of a stream,
        then call `sub.wait_for_chunk(part_number, chunk_index, timeout=...)` per chunk. Replaces the
        per-chunk subscribe/unsubscribe churn of `wait_for_chunk` with a single subscription and a
        listener that demuxes notifications to per-chunk events.
        """
        return _StreamSubscription(self._redis, object_id, int(object_version), fetch_fn)

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
            ChunkNotReadyError: If the chunk is still missing after notification + retry.
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

            # The pub/sub connection has its own (short) socket read timeout, so a
            # blocking `listen()` can raise redis TimeoutError/ConnectionError long
            # before `timeout` if the downloader is simply slower than that socket
            # read window. Treat those as transient: re-check the FS (the chunk may
            # have landed) and keep waiting up to the real `timeout` deadline rather
            # than killing the stream. See the 5s-read-timeout incident.
            loop = asyncio.get_running_loop()
            deadline = loop.time() + timeout
            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise asyncio.TimeoutError(f"timed out waiting for chunk {chunk_key}")
                # Cap each listen to a short poll so we ALSO re-read the FS on every tick, not
                # only when a notification arrives — see `_FS_REPOLL_INTERVAL_SECONDS`. This is
                # what lets a reader pick up a chunk landed by a non-notifying producer (the
                # drain) within ~1 poll interval instead of blocking to `timeout`.
                try:
                    await asyncio.wait_for(_listen(), timeout=min(_FS_REPOLL_INTERVAL_SECONDS, remaining))
                    break
                except asyncio.TimeoutError:
                    # Poll tick (no notification this interval): the chunk may have been landed
                    # by the drain without a notify:. Re-read; keep waiting up to the deadline.
                    data = await fetch_fn(object_id, int(object_version), int(part_number), int(chunk_index))
                    if data is not None:
                        return data
                except (RedisTimeoutError, RedisConnectionError) as exc:
                    data = await fetch_fn(object_id, int(object_version), int(part_number), int(chunk_index))
                    if data is not None:
                        return data
                    logger.debug("pubsub read interrupted for %s (%s); still waiting", chunk_key, exc)

        # Worker published — fetch it. Retry once on transient miss (e.g. a
        # janitor delete or CephFS replication lag).
        data = await fetch_fn(object_id, int(object_version), int(part_number), int(chunk_index))
        if data is None:
            logger.warning("Chunk transient miss after notification, retrying: %s", chunk_key)
            await asyncio.sleep(0.1)
            data = await fetch_fn(object_id, int(object_version), int(part_number), int(chunk_index))
        if data is None:
            raise ChunkNotReadyError(f"Chunk missing after pub/sub notification: {chunk_key}")
        return data


class _StreamSubscription:
    """One pattern subscription for a whole `(object_id, object_version)`, demuxed to per-chunk events.

    A single background listener reads `pmessage`s off the pattern and sets the `asyncio.Event` for
    the notified chunk; `wait_for_chunk` waits on its chunk's event. Correctness rests on two things
    carried over from the per-chunk `wait_for_chunk`:

    - **Race guard**: the event is created (so any concurrent notification is captured) *before* the
      post-subscribe FS re-check, so a chunk that lands between subscribe and wait is never missed.
    - **Safety net**: each waiter also re-checks the FS every `_STREAM_RECHECK_INTERVAL_SECONDS`, so a
      dropped notification (listener socket hiccup, publish that raced the subscribe) degrades to a
      bounded poll instead of hanging the stream.
    """

    def __init__(
        self,
        redis_client: Any,
        object_id: str,
        object_version: int,
        fetch_fn: Callable[[str, int, int, int], Awaitable[bytes | None]],
    ) -> None:
        self._redis = redis_client
        self._object_id = object_id
        self._object_version = int(object_version)
        self._fetch_fn = fetch_fn
        self._pattern = f"notify:obj:{object_id}:v:{self._object_version}:part:*:chunk:*"
        self._events: dict[str, asyncio.Event] = {}
        self._pubsub: Any = None
        self._listener: asyncio.Task[None] | None = None

    def _event_for(self, chunk_key: str) -> asyncio.Event:
        ev = self._events.get(chunk_key)
        if ev is None:
            ev = asyncio.Event()
            self._events[chunk_key] = ev
        return ev

    async def __aenter__(self) -> _StreamSubscription:
        self._pubsub = self._redis.pubsub()
        await self._pubsub.psubscribe(self._pattern)
        self._listener = asyncio.create_task(self._run_listener())
        return self

    async def __aexit__(self, *_exc: Any) -> bool:
        if self._listener is not None:
            self._listener.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._listener
        with contextlib.suppress(Exception):
            await self._pubsub.punsubscribe(self._pattern)
        with contextlib.suppress(Exception):
            await self._pubsub.aclose()
        return False

    async def _run_listener(self) -> None:
        """Demux pattern messages to per-chunk events, surviving transient socket errors.

        A `TimeoutError` is just the socket read window elapsing — re-listen. A `ConnectionError`
        drops the subscription, so re-`psubscribe` before re-listening. Any missed wakeup in the gap
        is covered by each waiter's periodic FS re-check.
        """
        while True:
            try:
                async for msg in self._pubsub.listen():
                    if msg.get("type") != "pmessage":
                        continue
                    channel = msg.get("channel")
                    if isinstance(channel, (bytes, bytearray)):
                        channel = channel.decode()
                    if not isinstance(channel, str) or not channel.startswith("notify:"):
                        continue
                    self._event_for(channel[len("notify:") :]).set()
            except RedisTimeoutError:
                continue
            except RedisConnectionError as exc:
                logger.debug("stream pubsub connection dropped (%s); re-subscribing", exc)
                with contextlib.suppress(Exception):
                    await self._pubsub.psubscribe(self._pattern)
                continue

    async def _fetch(self, part_number: int, chunk_index: int) -> bytes | None:
        return await self._fetch_fn(self._object_id, self._object_version, int(part_number), int(chunk_index))

    async def wait_for_chunk(
        self,
        part_number: int,
        chunk_index: int,
        *,
        timeout: float,  # noqa: ASYNC109
    ) -> bytes:
        """Return chunk bytes, waiting on this stream's shared subscription if not yet present."""
        data = await self._fetch(part_number, chunk_index)
        if data is not None:
            return data

        chunk_key = build_chunk_key(self._object_id, self._object_version, int(part_number), int(chunk_index))
        # Create the event BEFORE the re-check so a notification racing the subscribe is not lost.
        event = self._event_for(chunk_key)
        data = await self._fetch(part_number, chunk_index)
        if data is not None:
            return data

        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise asyncio.TimeoutError(f"timed out waiting for chunk {chunk_key}")
            try:
                await asyncio.wait_for(event.wait(), timeout=min(remaining, _STREAM_RECHECK_INTERVAL_SECONDS))
                fired = True
            except asyncio.TimeoutError:
                fired = False  # periodic re-check tick, not the real deadline

            data = await self._fetch(part_number, chunk_index)
            if data is not None:
                return data
            if fired:
                # Notified but the chunk is missing (janitor delete / replication lag). Re-arm and
                # retry once quickly before falling back to the periodic re-check.
                event.clear()
                await asyncio.sleep(0.1)
                data = await self._fetch(part_number, chunk_index)
                if data is not None:
                    return data
