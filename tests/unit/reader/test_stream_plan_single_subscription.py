"""RQ-1 wiring: `stream_plan` uses one subscription per stream when the flag is on.

Verifies the flag routes the streamer through `obj_cache.stream_subscription(...)` (one subscription
for the whole plan) and that the emitted bytes are identical to the per-chunk path. Decryption is a
no-op here (legacy passthrough: `key_bytes=None`), so this isolates the wait/subscription wiring.
"""

from __future__ import annotations

import contextlib
from typing import Any

import pytest

from hippius_s3.reader import streamer
from hippius_s3.reader.types import ChunkPlanItem


OBJ = "11111111-2222-3333-4444-555555555555"


def _plan() -> list[ChunkPlanItem]:
    return [
        ChunkPlanItem(part_number=1, chunk_index=0, slice_start=None, slice_end_excl=None),
        ChunkPlanItem(part_number=1, chunk_index=1, slice_start=None, slice_end_excl=None),
        ChunkPlanItem(part_number=2, chunk_index=0, slice_start=None, slice_end_excl=None),
    ]


class _FakeSub:
    def __init__(self, data: dict[tuple[int, int], bytes]) -> None:
        self._data = data
        self.waits: list[tuple[int, int]] = []

    async def wait_for_chunk(self, part_number: int, chunk_index: int, *, timeout: float) -> bytes:  # noqa: ASYNC109
        self.waits.append((part_number, chunk_index))
        return self._data[(part_number, chunk_index)]


class _FakeCache:
    def __init__(self, data: dict[tuple[int, int], bytes]) -> None:
        self._data = data
        self.subs: list[_FakeSub] = []
        self.per_chunk_waits = 0

    def stream_subscription(self, _object_id: str, _object_version: int) -> Any:
        sub = _FakeSub(self._data)
        self.subs.append(sub)

        @contextlib.asynccontextmanager
        async def _cm() -> Any:
            yield sub

        return _cm()

    async def wait_for_chunk(self, _oid: str, _v: int, pn: int, ci: int, *, timeout: Any) -> bytes:  # noqa: ASYNC109
        self.per_chunk_waits += 1
        return self._data[(pn, ci)]


async def _run(cache: _FakeCache, prefetch: int, monkeypatch: Any) -> bytes:
    # Isolate the wait/subscription wiring: decrypt is a pure passthrough here.
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
    )
    return b"".join([chunk async for chunk in gen])


@pytest.mark.asyncio
@pytest.mark.parametrize("prefetch", [0, 4])
async def test_flag_on_uses_single_subscription(monkeypatch: Any, prefetch: int) -> None:
    monkeypatch.setattr(streamer, "_single_subscription_enabled", lambda: True)
    data = {(1, 0): b"aaaa", (1, 1): b"bbbb", (2, 0): b"cccc"}
    cache = _FakeCache(data)

    out = await _run(cache, prefetch, monkeypatch)

    assert out == b"aaaabbbbcccc"
    assert len(cache.subs) == 1, "the whole stream must open exactly one subscription"
    assert sorted(cache.subs[0].waits) == [(1, 0), (1, 1), (2, 0)]
    assert cache.per_chunk_waits == 0, "flag-on must not use the per-chunk wait path"


@pytest.mark.asyncio
@pytest.mark.parametrize("prefetch", [0, 4])
async def test_flag_off_uses_per_chunk_path(monkeypatch: Any, prefetch: int) -> None:
    monkeypatch.setattr(streamer, "_single_subscription_enabled", lambda: False)
    data = {(1, 0): b"aaaa", (1, 1): b"bbbb", (2, 0): b"cccc"}
    cache = _FakeCache(data)

    out = await _run(cache, prefetch, monkeypatch)

    assert out == b"aaaabbbbcccc"
    assert len(cache.subs) == 0, "flag-off must not open a subscription"
    assert cache.per_chunk_waits == 3
