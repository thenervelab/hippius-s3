"""A3: a mid-stream chunk stall is bounded by the per-chunk timeout.

`test_read_response_first_chunk_timeout.py` locks in the FIRST-chunk bound (the 503 before headers
are committed). This pins the other half: once the response is streaming, a later chunk whose
backend fetch never returns must abort at the per-chunk bound (`stream_chunk_timeout_seconds`,
threaded into `stream_plan` as `chunk_timeout`) rather than hanging the open response.

Drives the REAL `stream_plan`: an obj_cache that serves chunk 0 from the local tier and misses
chunk 1, and a `fetch_missing` (the backend tier) that never returns.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import patch

import pytest

from hippius_s3.reader import streamer
from hippius_s3.reader.types import ChunkPlanItem


OBJ = "11111111-2222-3333-4444-555555555555"


class _CacheServesOnlyChunkZero:
    async def get_chunk(self, oid: str, v: int, pn: int, ci: int) -> bytes | None:
        return b"chunk-zero" if int(ci) == 0 else None


async def _backend_never_answers(item: ChunkPlanItem) -> bytes:
    await asyncio.sleep(3600)
    return b"unreachable"


async def _identity_decrypt(c: bytes, **_kw: Any) -> bytes:
    return c


@pytest.mark.asyncio
async def test_mid_stream_chunk_stall_is_bounded_by_per_chunk_timeout() -> None:
    """Chunk 0 streams; chunk 1's backend fetch stalls → abort at the per-chunk bound, not a hang."""
    plan = [
        ChunkPlanItem(part_number=1, chunk_index=0),
        ChunkPlanItem(part_number=1, chunk_index=1),  # never lands
    ]
    per_chunk_bound = 0.2

    with patch.object(streamer, "decrypt_chunk_if_needed", new=_identity_decrypt):
        gen = streamer.stream_plan(
            obj_cache=_CacheServesOnlyChunkZero(),
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
            fetch_missing=_backend_never_answers,
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
