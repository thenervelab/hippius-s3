"""F1: the streamer re-enqueues a part's download when a chunk is missing from FS at stream time.

A cache-source read whose later part is evicted mid-stream (by the janitor) used to wait on a
`notify:` no producer would ever publish and time out INSIDE the response body — silent truncation.
The fix: on a genuine FS miss the streamer calls `ensure_part_fn(part_number)` (idempotent download
re-enqueue) before waiting on pub/sub, so a producer actually fills + notifies the chunk.

These tests isolate the streamer's miss->enqueue wiring with a fake cache; decrypt is a passthrough.
"""

from __future__ import annotations

import contextlib
from typing import Any

import pytest

from hippius_s3.reader import streamer
from hippius_s3.reader.types import ChunkPlanItem


OBJ = "11111111-2222-3333-4444-555555555555"


def _plan() -> list[ChunkPlanItem]:
    # Part 1 is warm (both chunks present); part 2 was evicted mid-stream (both chunks missing).
    return [
        ChunkPlanItem(part_number=1, chunk_index=0, slice_start=None, slice_end_excl=None),
        ChunkPlanItem(part_number=1, chunk_index=1, slice_start=None, slice_end_excl=None),
        ChunkPlanItem(part_number=2, chunk_index=0, slice_start=None, slice_end_excl=None),
        ChunkPlanItem(part_number=2, chunk_index=1, slice_start=None, slice_end_excl=None),
    ]


_DATA = {(1, 0): b"aaaa", (1, 1): b"bbbb", (2, 0): b"cccc", (2, 1): b"dddd"}


class _FakeSub:
    def __init__(self, data: dict[tuple[int, int], bytes]) -> None:
        self._data = data
        self.waits: list[tuple[int, int]] = []

    async def wait_for_chunk(self, part_number: int, chunk_index: int, *, timeout: float) -> bytes:  # noqa: ASYNC109
        self.waits.append((part_number, chunk_index))
        return self._data[(part_number, chunk_index)]


class _FakeCache:
    def __init__(self, data: dict[tuple[int, int], bytes], present: set[tuple[int, int]]) -> None:
        self._data = data
        self._present = present
        self.exists_checks: list[tuple[int, int]] = []
        self.subs: list[_FakeSub] = []

    async def chunk_exists(self, _oid: str, _v: int, pn: int, ci: int) -> bool:
        self.exists_checks.append((pn, ci))
        return (pn, ci) in self._present

    async def wait_for_chunk(self, _oid: str, _v: int, pn: int, ci: int, *, timeout: Any) -> bytes:  # noqa: ASYNC109
        return self._data[(pn, ci)]

    def stream_subscription(self, _object_id: str, _object_version: int) -> Any:
        sub = _FakeSub(self._data)
        self.subs.append(sub)

        @contextlib.asynccontextmanager
        async def _cm() -> Any:
            yield sub

        return _cm()


async def _run(cache: _FakeCache, *, ensure_part_fn: Any, prefetch: int, monkeypatch: Any) -> bytes:
    async def _identity(cbytes: bytes, **_kw: Any) -> bytes:
        return cbytes

    monkeypatch.setattr(streamer, "decrypt_chunk_if_needed", _identity)
    gen = streamer.stream_plan(
        obj_cache=cache,
        object_id=OBJ,
        object_version=1,
        plan=_plan(),
        storage_version=5,
        key_bytes=b"\x11" * 32,
        suite_id="hip-enc/aes256gcm",
        bucket_id="bkt",
        upload_id="",
        prefetch_chunks=prefetch,
        chunk_timeout=5.0,
        ensure_part_fn=ensure_part_fn,
    )
    return b"".join([chunk async for chunk in gen])


@pytest.mark.asyncio
@pytest.mark.parametrize("single_sub", [False, True])
@pytest.mark.parametrize("prefetch", [0, 4])
async def test_missing_part_triggers_single_reenqueue(monkeypatch: Any, single_sub: bool, prefetch: int) -> None:
    monkeypatch.setattr(streamer, "_single_subscription_enabled", lambda: single_sub)
    present = {(1, 0), (1, 1)}  # part 2 evicted mid-stream
    cache = _FakeCache(_DATA, present)

    ensured: list[int] = []

    async def _ensure(part_number: int) -> None:
        ensured.append(int(part_number))

    out = await _run(cache, ensure_part_fn=_ensure, prefetch=prefetch, monkeypatch=monkeypatch)

    # Stream completes byte-exact (with the fix a producer is re-enqueued instead of hanging).
    assert out == b"aaaabbbbccccdddd"
    # Only the evicted part is re-enqueued, and exactly once despite both its chunks missing.
    assert ensured == [2], "the evicted part must be re-enqueued exactly once"


@pytest.mark.asyncio
@pytest.mark.parametrize("single_sub", [False, True])
async def test_all_present_never_enqueues(monkeypatch: Any, single_sub: bool) -> None:
    """The cache-source fast path (every chunk present) must never call the enqueue hook."""
    monkeypatch.setattr(streamer, "_single_subscription_enabled", lambda: single_sub)
    present = {(1, 0), (1, 1), (2, 0), (2, 1)}
    cache = _FakeCache(_DATA, present)

    ensured: list[int] = []

    async def _ensure(part_number: int) -> None:
        ensured.append(int(part_number))

    out = await _run(cache, ensure_part_fn=_ensure, prefetch=0, monkeypatch=monkeypatch)

    assert out == b"aaaabbbbccccdddd"
    assert ensured == [], "a fully-cached stream must not re-enqueue any part"


@pytest.mark.asyncio
async def test_no_ensure_fn_keeps_pure_wait_path(monkeypatch: Any) -> None:
    """Callers that pass no ensure_part_fn (HEAD/copy/migrate) keep the pure wait-on-pub/sub path:
    no existence pre-check at all."""
    monkeypatch.setattr(streamer, "_single_subscription_enabled", lambda: False)
    present: set[tuple[int, int]] = set()
    cache = _FakeCache(_DATA, present)

    out = await _run(cache, ensure_part_fn=None, prefetch=0, monkeypatch=monkeypatch)

    assert out == b"aaaabbbbccccdddd"
    assert cache.exists_checks == [], "no ensure_part_fn must skip the per-chunk existence pre-check"
