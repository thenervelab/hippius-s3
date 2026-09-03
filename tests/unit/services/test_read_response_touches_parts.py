"""read_response bulk-stamps read recency for multi-part plans before streaming starts.

The per-chunk stamp in the store only reaches a part when its first chunk streams; on a long
read that is minutes later, after the evictor may already have taken the tail parts of the very
object being read. The stamp runs once, up front, and never for a single-part plan (the per-chunk
path covers that with no extra round trip).
"""

from __future__ import annotations

import contextlib
from types import SimpleNamespace
from typing import Any
from typing import Iterator
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest

from hippius_s3.services import object_reader


OBJ = "11111111-2222-3333-4444-555555555555"


def _ctx(part_numbers: list[int]) -> object_reader.StreamContext:
    return object_reader.StreamContext(
        plan=[SimpleNamespace(part_number=pn, chunk_index=0) for pn in part_numbers],
        object_version=4,
        storage_version=5,
        source="cache",
        key_bytes=None,
        suite_id="hip-enc/aes256gcm",
        bucket_id="bkt",
        upload_id="",
    )


class _Recorder:
    def __init__(self) -> None:
        self.touched: list[tuple] = []

    async def touch_parts(self, object_id: str, object_version: int, part_numbers: list[int]) -> None:
        self.touched.append((object_id, object_version, part_numbers))


@contextlib.contextmanager
def _patched(part_numbers: list[int], recorder: _Recorder | None) -> Iterator[None]:
    async def _one():
        yield b"x"

    cfg = SimpleNamespace(
        stream_first_chunk_timeout_seconds=5, stream_chunk_timeout_seconds=300, http_stream_prefetch_chunks=0
    )
    with (
        patch.object(object_reader, "build_stream_context", new=AsyncMock(return_value=_ctx(part_numbers))),
        patch.object(object_reader, "get_config", return_value=cfg),
        patch.object(object_reader, "build_headers", return_value={}),
        patch.object(object_reader, "stream_plan", new=lambda **kw: _one()),
        patch.object(object_reader, "get_read_recency_recorder", return_value=recorder),
    ):
        yield


async def _read() -> Any:
    info = {"object_id": OBJ, "bucket_name": "bkt", "content_type": "application/octet-stream", "metadata": {}}
    return await object_reader.read_response(
        db=None, redis=None, obj_cache=None, info=info, read_mode="auto", rng=None, address="a"
    )


@pytest.mark.asyncio
async def test_a_multi_part_plan_touches_every_distinct_part_once() -> None:
    recorder = _Recorder()
    with _patched([1, 1, 2, 3, 3], recorder):
        resp = await _read()
    assert resp.status_code == 200
    assert recorder.touched == [(OBJ, 4, [1, 2, 3])]


@pytest.mark.asyncio
async def test_a_single_part_plan_is_left_to_the_per_chunk_stamp() -> None:
    recorder = _Recorder()
    with _patched([1, 1], recorder):
        await _read()
    assert recorder.touched == []


@pytest.mark.asyncio
async def test_no_recorder_means_no_stamp_and_no_error() -> None:
    with _patched([1, 2], None):
        resp = await _read()
    assert resp.status_code == 200
