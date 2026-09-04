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


def _ctx(part_numbers: list[int], plan: list[object] | None = None) -> object_reader.StreamContext:
    return object_reader.StreamContext(
        plan=[SimpleNamespace(part_number=pn, chunk_index=0) for pn in part_numbers] if plan is None else plan,
        object_version=4,
        storage_version=5,
        source="cache",
        key_bytes=None,
        suite_id="hip-enc/aes256gcm",
        bucket_id="bkt",
        upload_id="",
    )


class _Recorder:
    def __init__(self, events: list[str] | None = None) -> None:
        self.touched: list[tuple] = []
        self._events = events

    async def touch_parts(self, object_id: str, object_version: int, part_numbers: list[int]) -> None:
        self.touched.append((object_id, object_version, part_numbers))
        if self._events is not None:
            self._events.append("touch")


@contextlib.contextmanager
def _patched(
    part_numbers: list[int],
    recorder: object | None,
    *,
    plan: list[object] | None = None,
    events: list[str] | None = None,
) -> Iterator[None]:
    async def _one():
        if events is not None:
            events.append("first-chunk")
        yield b"x"

    cfg = SimpleNamespace(
        stream_first_chunk_timeout_seconds=5, stream_chunk_timeout_seconds=300, http_stream_prefetch_chunks=0
    )
    with (
        patch.object(object_reader, "build_stream_context", new=AsyncMock(return_value=_ctx(part_numbers, plan))),
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


@pytest.mark.asyncio
async def test_the_touch_lands_before_the_first_chunk_is_waited_on() -> None:
    """The whole point is to beat the evictor to the tail parts: a stamp issued after the first
    chunk peek would sit behind a cold read's download wait, which is exactly the window it exists
    to close."""
    events: list[str] = []
    with _patched([1, 2], _Recorder(events), events=events):
        await _read()
    assert events == ["touch", "first-chunk"]


@pytest.mark.asyncio
async def test_plan_items_without_a_part_number_are_skipped_not_fatal() -> None:
    """Other read_response tests drive it with bare `object()` plan items; the touch must tolerate
    them the way the coalesce-lock release below it already does."""
    recorder = _Recorder()
    with _patched([], recorder, plan=[object(), object()]):
        resp = await _read()
    assert resp.status_code == 200
    assert recorder.touched == []


@pytest.mark.asyncio
async def test_a_recorder_whose_pool_is_down_never_fails_the_read() -> None:
    """A real recorder against a dead pool: the read is served, the sample is lost."""
    from hippius_s3.cache.read_recency import ReadRecencyRecorder

    class DownPool:
        def acquire(self, *, timeout: float | None = None) -> object:  # noqa: ASYNC109
            raise RuntimeError("pool is closing")

    with _patched([1, 2, 3], ReadRecencyRecorder(DownPool(), "node-a")):  # type: ignore[arg-type]
        resp = await _read()
    assert resp.status_code == 200


def test_an_empty_node_name_yields_no_recorder_so_nothing_is_touched() -> None:
    """A pod without NODE_NAME (workers, a single-tier deployment) has no row of its own to stamp;
    the factory returns None and read_response takes the no-recorder branch above."""
    from hippius_s3.cache.read_recency import create_read_recency_recorder

    assert create_read_recency_recorder(object(), "") is None  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_stream_object_touches_a_multi_part_source_too() -> None:
    """A streaming CopyObject reads its whole source through stream_object, not read_response, and
    its tail parts face the same evictor while the head is still being copied."""
    recorder = _Recorder()
    with _patched([1, 2], recorder):
        gen = await object_reader.stream_object(
            db=None,
            redis=None,
            obj_cache=None,
            info={"object_id": OBJ, "bucket_name": "bkt"},
            rng=None,
            address="a",
        )
    assert [c async for c in gen] == [b"x"]
    assert recorder.touched == [(OBJ, 4, [1, 2])]
