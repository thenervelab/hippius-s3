"""Tests for stream_plan download cache cleanup behavior.

Validates that consumed chunks are batch-deleted from the download cache
during streaming (every _CLEANUP_BATCH_SIZE items) and on completion/disconnect.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from hippius_s3.reader.streamer import _CLEANUP_BATCH_SIZE
from hippius_s3.reader.streamer import stream_plan
from hippius_s3.reader.types import ChunkPlanItem


async def _noop_decrypt(cbytes, **kwargs):
    return cbytes


_decrypt_patch = patch("hippius_s3.reader.streamer.decrypt_chunk_if_needed", side_effect=_noop_decrypt)


class FakeObjCache:
    """Minimal obj_cache mock that tracks delete_download_chunks calls."""

    def __init__(self, chunk_data: bytes = b"\x00" * 16) -> None:
        self._chunk_data = chunk_data
        self.delete_calls: list[list[Any]] = []

    async def wait_for_chunk(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int
    ) -> bytes:
        return self._chunk_data

    async def delete_download_chunks(
        self, object_id: str, object_version: int, plan_items: list[Any]
    ) -> None:
        self.delete_calls.append(list(plan_items))


def _make_plan(n: int) -> list[ChunkPlanItem]:
    return [ChunkPlanItem(part_number=1, chunk_index=i) for i in range(n)]


async def _consume_stream(gen) -> list[bytes]:
    """Fully consume an async generator."""
    result = []
    async for chunk in gen:
        result.append(chunk)
    return result


@pytest.mark.asyncio
@_decrypt_patch
async def test_cleanup_after_small_stream(_mock):
    """Small stream (< batch size) cleans up in finally."""
    cache = FakeObjCache(b"data")
    plan = _make_plan(5)

    gen = stream_plan(
        obj_cache=cache,
        object_id="obj1",
        object_version=1,
        plan=plan,
        storage_version=5,
        key_bytes=None,
        suite_id=None,
        bucket_id="b1",
        upload_id="u1",
    )
    await _consume_stream(gen)

    assert len(cache.delete_calls) == 1
    assert len(cache.delete_calls[0]) == 5


@pytest.mark.asyncio
@_decrypt_patch
async def test_cleanup_batches_during_large_stream(_mock):
    """Large stream (> batch size) deletes in batches during streaming."""
    n = _CLEANUP_BATCH_SIZE * 3 + 10
    cache = FakeObjCache(b"data")
    plan = _make_plan(n)

    gen = stream_plan(
        obj_cache=cache,
        object_id="obj1",
        object_version=1,
        plan=plan,
        storage_version=5,
        key_bytes=None,
        suite_id=None,
        bucket_id="b1",
        upload_id="u1",
    )
    await _consume_stream(gen)

    # 3 full batches + 1 final flush of the remainder
    assert len(cache.delete_calls) == 4
    assert len(cache.delete_calls[0]) == _CLEANUP_BATCH_SIZE
    assert len(cache.delete_calls[1]) == _CLEANUP_BATCH_SIZE
    assert len(cache.delete_calls[2]) == _CLEANUP_BATCH_SIZE
    assert len(cache.delete_calls[3]) == 10

    total_deleted = sum(len(batch) for batch in cache.delete_calls)
    assert total_deleted == n


@pytest.mark.asyncio
@_decrypt_patch
async def test_cleanup_exactly_batch_size(_mock):
    """Stream with exactly batch_size chunks: 1 batch + 1 empty final flush."""
    n = _CLEANUP_BATCH_SIZE
    cache = FakeObjCache(b"data")
    plan = _make_plan(n)

    gen = stream_plan(
        obj_cache=cache,
        object_id="obj1",
        object_version=1,
        plan=plan,
        storage_version=5,
        key_bytes=None,
        suite_id=None,
        bucket_id="b1",
        upload_id="u1",
    )
    await _consume_stream(gen)

    # 1 full batch during streaming, final flush has 0 items (items.clear() already ran)
    assert len(cache.delete_calls) == 1
    assert len(cache.delete_calls[0]) == _CLEANUP_BATCH_SIZE


@pytest.mark.asyncio
@_decrypt_patch
async def test_cleanup_on_client_disconnect(_mock):
    """If client disconnects mid-stream, consumed chunks still get cleaned up."""
    n = _CLEANUP_BATCH_SIZE + 50
    cache = FakeObjCache(b"data")
    plan = _make_plan(n)

    gen = stream_plan(
        obj_cache=cache,
        object_id="obj1",
        object_version=1,
        plan=plan,
        storage_version=5,
        key_bytes=None,
        suite_id=None,
        bucket_id="b1",
        upload_id="u1",
    )

    # Consume only half the stream then close
    consumed = 0
    async for _ in gen:
        consumed += 1
        if consumed == _CLEANUP_BATCH_SIZE + 20:
            break
    await gen.aclose()

    total_deleted = sum(len(batch) for batch in cache.delete_calls)
    # At least the full batch was flushed, plus remaining items on close
    assert total_deleted >= _CLEANUP_BATCH_SIZE


@pytest.mark.asyncio
@_decrypt_patch
async def test_cleanup_empty_stream(_mock):
    """Empty stream has no cleanup calls."""
    cache = FakeObjCache(b"data")
    plan: list[ChunkPlanItem] = []

    gen = stream_plan(
        obj_cache=cache,
        object_id="obj1",
        object_version=1,
        plan=plan,
        storage_version=5,
        key_bytes=None,
        suite_id=None,
        bucket_id="b1",
        upload_id="u1",
    )
    await _consume_stream(gen)

    assert len(cache.delete_calls) == 0


@pytest.mark.asyncio
@_decrypt_patch
async def test_cleanup_with_prefetch(_mock):
    """Pipelined path (prefetch > 0) also batch-deletes."""
    n = _CLEANUP_BATCH_SIZE + 30
    cache = FakeObjCache(b"data")
    plan = _make_plan(n)

    gen = stream_plan(
        obj_cache=cache,
        object_id="obj1",
        object_version=1,
        plan=plan,
        storage_version=5,
        key_bytes=None,
        suite_id=None,
        bucket_id="b1",
        upload_id="u1",
        prefetch_chunks=4,
    )
    await _consume_stream(gen)

    # 1 full batch + final remainder
    assert len(cache.delete_calls) == 2
    assert len(cache.delete_calls[0]) == _CLEANUP_BATCH_SIZE
    assert len(cache.delete_calls[1]) == 30

    total_deleted = sum(len(batch) for batch in cache.delete_calls)
    assert total_deleted == n
