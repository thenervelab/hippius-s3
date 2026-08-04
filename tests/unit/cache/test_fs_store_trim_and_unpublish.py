"""fs_store helpers backing the publish-time trim and the append CAS-loser un-publish.

The drain replicates a part only when the SSD chunk set is EXACTLY
{0..meta.num_chunks-1} (partdrain.rs IncompleteSource gate) — a stale chunk tail
left by a larger earlier attempt strands the part forever: never replicated,
never evicted. `trim_chunks_from` enforces the exact set at publish time.
`delete_meta` un-publishes a part dir (append CAS loser) without touching chunks.
"""

from __future__ import annotations

import logging
import uuid
from pathlib import Path

import pytest

from hippius_s3.cache import FileSystemPartsStore


@pytest.fixture()
def store(tmp_path) -> FileSystemPartsStore:
    return FileSystemPartsStore(str(tmp_path))


async def _seed_part(store: FileSystemPartsStore, object_id: str, *, indices: list[int], with_meta: bool) -> Path:
    for i in indices:
        await store.set_chunk(object_id, 1, 1, i, f"chunk-{i}".encode())
    if with_meta:
        await store.set_meta(object_id, 1, 1, chunk_size=4, num_chunks=len(indices), size_bytes=8)
    return Path(store.part_path(object_id, 1, 1))


@pytest.mark.asyncio
async def test_trim_removes_only_tail_indices(store):
    object_id = str(uuid.uuid4())
    part_dir = await _seed_part(store, object_id, indices=[0, 1, 2, 3, 4], with_meta=True)

    removed = await store.trim_chunks_from(object_id, 1, 1, 2)

    assert removed == 3
    assert sorted(p.name for p in part_dir.iterdir()) == ["chunk_0.bin", "chunk_1.bin", "meta.json"]


@pytest.mark.asyncio
async def test_trim_noop_when_no_tail(store):
    object_id = str(uuid.uuid4())
    part_dir = await _seed_part(store, object_id, indices=[0, 1], with_meta=True)

    removed = await store.trim_chunks_from(object_id, 1, 1, 2)

    assert removed == 0
    assert sorted(p.name for p in part_dir.iterdir()) == ["chunk_0.bin", "chunk_1.bin", "meta.json"]


@pytest.mark.asyncio
async def test_trim_noop_on_missing_dir(store):
    removed = await store.trim_chunks_from(str(uuid.uuid4()), 1, 1, 0)
    assert removed == 0


@pytest.mark.asyncio
async def test_trim_per_file_failure_logs_error_and_continues(store, caplog):
    object_id = str(uuid.uuid4())
    part_dir = await _seed_part(store, object_id, indices=[0, 1, 2], with_meta=True)
    # A directory named like a chunk makes unlink raise — a real-FS failure injection.
    (part_dir / "chunk_3.bin").mkdir()

    with caplog.at_level(logging.ERROR, logger="hippius_s3.cache.fs_store"):
        removed = await store.trim_chunks_from(object_id, 1, 1, 2)

    assert removed == 1  # chunk_2 removed despite chunk_3 failing
    assert not (part_dir / "chunk_2.bin").exists()
    error_lines = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert error_lines, "surviving stale tail must be loud (stranded-part risk)"
    assert any(object_id in r.getMessage() for r in error_lines)


@pytest.mark.asyncio
async def test_delete_meta_unpublishes_but_keeps_chunks(store):
    object_id = str(uuid.uuid4())
    part_dir = await _seed_part(store, object_id, indices=[0, 1], with_meta=True)

    await store.delete_meta(object_id, 1, 1)

    assert not (part_dir / "meta.json").exists()
    assert (part_dir / "chunk_0.bin").exists()
    assert (part_dir / "chunk_1.bin").exists()
    assert await store.get_meta(object_id, 1, 1) is None
    # meta-gated reads now miss: the dir is un-published.
    assert await store.get_chunk(object_id, 1, 1, 0) is None


@pytest.mark.asyncio
async def test_delete_meta_idempotent_on_missing(store):
    object_id = str(uuid.uuid4())
    await store.delete_meta(object_id, 1, 1)  # no dir at all — must not raise
    await _seed_part(store, object_id, indices=[0], with_meta=False)
    await store.delete_meta(object_id, 1, 1)  # dir without meta — must not raise
