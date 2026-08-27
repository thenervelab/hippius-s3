"""Which tiers each DualFileSystemPartsStore method is allowed to consult.

`get_chunk` walks local -> peer -> pool. The lookups (`get_meta`, `chunk_exists`,
`chunks_exist_batch`) walk local -> pool and stop. That difference is a decision, not an
oversight, and it is the kind of decision a later reader "tidies up" — the existence check looks
like it is simply missing a tier.

What makes it load-bearing is the DIRECTION each answer can be wrong in. These lookups gate whether
the read path enqueues a repair. Saying "missing" when a peer holds the part costs a redundant
background fetch and nothing else: `wait_for_chunk` calls `get_chunk` on its fast path, so the peer
serves the bytes and the reader never waits on that request. Saying "present" on a peer's word and
skipping the repair costs a stalled stream when that peer turns out not to have it. Wasteful is
recoverable; stalled is an outage.

These tests fail if someone adds a peer pass to a lookup, and they also fail if someone removes the
peer from `get_chunk` — the asymmetry is pinned from both sides, so neither half can drift alone.
"""

from __future__ import annotations

import os
from typing import Any

import pytest

from hippius_s3.cache.dual_fs_store import DualFileSystemPartsStore


PART = (1, 0)  # (part_number, chunk_index)
# fs_store rejects anything that is not a UUID — the path-traversal guard on object_id.
OBJ = "3f6c1e2a-0000-4000-8000-0000000000aa"


def _store(tmp_path: Any, peer_calls: list[tuple], *, promote: bool = True) -> DualFileSystemPartsStore:
    """A dual store whose peer tier records every call and always claims to hold the chunk.

    `promote=True` on purpose: promotion is the loudest side effect on the read path, so a lookup
    that wrongly went through the peer would leave evidence on the local tier as well as in the
    call log.
    """
    primary = tmp_path / "local"
    fallback = tmp_path / "pool"
    for d in (primary, fallback):
        os.makedirs(d, exist_ok=True)

    async def peer_fetch(object_id: str, object_version: int, part_number: int, chunk_index: int) -> bytes:
        peer_calls.append((object_id, object_version, part_number, chunk_index))
        return b"peer-bytes"

    return DualFileSystemPartsStore(str(primary), str(fallback), promote=promote, peer_fetch=peer_fetch)


@pytest.mark.asyncio
async def test_get_chunk_does_consult_the_peer(tmp_path: Any) -> None:
    """The control. Without this the other three tests would also pass on a store whose peer tier
    was simply broken, and they would be proving nothing."""
    calls: list[tuple] = []
    store = _store(tmp_path, calls, promote=False)

    result = await store.get_chunk(OBJ, 1, *PART)

    assert result == b"peer-bytes"
    assert calls == [(OBJ, 1, *PART)]


@pytest.mark.asyncio
async def test_chunks_exist_batch_does_not_consult_the_peer(tmp_path: Any) -> None:
    """The one that matters: this answer decides cache-vs-pipeline for the whole read."""
    calls: list[tuple] = []
    store = _store(tmp_path, calls)

    present = await store.chunks_exist_batch(OBJ, 1, [PART])

    assert present == [False], "neither local nor pool holds it, so the honest answer is missing"
    assert calls == [], "a peer pass here would let a stale 'present' suppress the repair"


@pytest.mark.asyncio
async def test_chunk_exists_does_not_consult_the_peer(tmp_path: Any) -> None:
    calls: list[tuple] = []
    store = _store(tmp_path, calls)

    assert await store.chunk_exists(OBJ, 1, *PART) is False
    assert calls == []


@pytest.mark.asyncio
async def test_get_meta_does_not_consult_the_peer(tmp_path: Any) -> None:
    calls: list[tuple] = []
    store = _store(tmp_path, calls)

    assert await store.get_meta(OBJ, 1, PART[0]) is None
    assert calls == []


@pytest.mark.asyncio
async def test_a_lookup_never_promotes(tmp_path: Any) -> None:
    """Promotion is `get_chunk`'s side effect, not a lookup's.

    A lookup that promoted would copy chunks onto this node's flash that nobody asked to read, and
    would move the evictor's recency signal for a read that never happened — so the tier would fill
    with, and then preferentially retain, data on the strength of existence checks alone.
    """
    calls: list[tuple] = []
    store = _store(tmp_path, calls)
    local_before = sorted(p for p in (tmp_path / "local").rglob("*") if p.is_file())

    await store.chunks_exist_batch(OBJ, 1, [PART])
    await store.chunk_exists(OBJ, 1, *PART)
    await store.get_meta(OBJ, 1, PART[0])

    assert sorted(p for p in (tmp_path / "local").rglob("*") if p.is_file()) == local_before
    assert calls == []
