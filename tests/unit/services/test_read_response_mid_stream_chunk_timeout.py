"""B2: the read path bounds EVERY chunk wait, not just the first.

`test_read_response_first_chunk_timeout.py` already locks in the FIRST-chunk bound
(`stream_first_chunk_timeout_seconds`). This is the sibling regression for a stall on a LATER chunk:
after chunk 0 streams fine, chunk 1 never lands (no worker fills it, no pub/sub notification). The
stream must abort at the per-chunk bound (`stream_chunk_timeout_seconds`, threaded into `stream_plan`
as `chunk_timeout` and enforced deep in `ChunkNotifier.wait_for_chunk`) rather than hanging up to
`cache_ttl_seconds` (~1h).

The test drives the REAL bounding path end to end: the real `stream_plan` (which passes
`timeout=chunk_timeout` per chunk) over the real `ChunkNotifier` (whose deadline loop raises
`asyncio.TimeoutError`). Only `decrypt_chunk_if_needed` is stubbed — decryption is orthogonal to the
timeout under test.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest

from hippius_s3.cache.notifier import ChunkNotifier
from hippius_s3.reader import streamer
from hippius_s3.reader.types import ChunkPlanItem


OBJ = "11111111-2222-3333-4444-555555555555"


class _BlockingPubSub:
    """A pubsub whose listen() blocks forever — no chunk-ready message is ever delivered.

    This is the mid-stream stall: the downloader never fills the chunk and never publishes, so the
    notifier's wait must fall through to its own timeout deadline.
    """

    def __init__(self) -> None:
        self._messages: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.subscribe = AsyncMock()
        self.unsubscribe = AsyncMock()
        self.aclose = AsyncMock()

    async def listen(self) -> Any:
        while True:
            msg = await self._messages.get()  # never resolves — nothing is injected
            yield msg


class _SilentRedis:
    """Minimal Redis surface for ChunkNotifier: a pubsub that never notifies."""

    def __init__(self) -> None:
        self._ps = _BlockingPubSub()

    def pubsub(self) -> _BlockingPubSub:
        return self._ps

    async def publish(self, channel: str, message: bytes | str) -> None:
        return None


class _CacheStallsAfterFirstChunk:
    """obj_cache adapter mirroring RedisObjectPartsCache.wait_for_chunk: delegate to the real
    ChunkNotifier with a fetch_fn that serves chunk 0 and permanently misses every later chunk."""

    def __init__(self, notifier: ChunkNotifier, first_chunk: bytes) -> None:
        self._notifier = notifier
        self._first = first_chunk

    async def _fetch(self, oid: str, v: int, pn: int, ci: int) -> bytes | None:
        return self._first if int(ci) == 0 else None

    async def wait_for_chunk(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        chunk_index: int,
        *,
        timeout: float | None = None,  # noqa: ASYNC109 (mirrors RedisObjectPartsCache.wait_for_chunk)
    ) -> bytes:
        return await self._notifier.wait_for_chunk(
            object_id,
            int(object_version),
            int(part_number),
            int(chunk_index),
            fetch_fn=self._fetch,
            timeout=float(timeout),
        )


async def _identity_decrypt(c: bytes, **_kw: Any) -> bytes:
    return c


@pytest.mark.asyncio
async def test_mid_stream_chunk_stall_is_bounded_by_per_chunk_timeout() -> None:
    """Chunk 0 streams; chunk 1 stalls → abort at the per-chunk bound, not a ~1h hang."""
    obj_cache = _CacheStallsAfterFirstChunk(ChunkNotifier(_SilentRedis()), first_chunk=b"chunk-zero")

    plan = [
        ChunkPlanItem(part_number=1, chunk_index=0),
        ChunkPlanItem(part_number=1, chunk_index=1),  # never lands
    ]
    per_chunk_bound = 0.2

    with patch.object(streamer, "decrypt_chunk_if_needed", new=_identity_decrypt):
        gen = streamer.stream_plan(
            obj_cache=obj_cache,
            object_id=OBJ,
            object_version=1,
            plan=plan,
            storage_version=5,
            key_bytes=None,
            suite_id="hip-enc/aes256gcm",
            bucket_id="bkt",
            upload_id="",
            prefetch_chunks=0,
            chunk_timeout=per_chunk_bound,
        )
        try:
            first = await gen.__anext__()
            assert first == b"chunk-zero", "first chunk must stream before the mid-stream stall"

            loop = asyncio.get_running_loop()
            t0 = loop.time()
            with pytest.raises(asyncio.TimeoutError):
                # The outer wait_for is a SAFETY ceiling so a genuinely unbounded wait fails the test
                # (loudly) instead of hanging the suite. The elapsed assertion below is what proves the
                # abort came from the 0.2s per-chunk bound, not from this 10s ceiling.
                await asyncio.wait_for(gen.__anext__(), timeout=10.0)
            elapsed = loop.time() - t0
            assert elapsed < 5.0, f"mid-stream chunk wait was not bounded by the per-chunk timeout (took {elapsed:.2f}s)"
        finally:
            await gen.aclose()
