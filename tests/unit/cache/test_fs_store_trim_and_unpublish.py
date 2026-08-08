"""fs_store's part publish (staged set + exact-set trim) and the append CAS-loser un-publish.

The drain replicates a part only when the SSD chunk set is EXACTLY
{0..meta.num_chunks-1} (partdrain.rs IncompleteSource gate) — a stale chunk tail
left by a larger earlier attempt strands the part forever: never replicated,
never evicted. `publish_part` enforces the exact set as it promotes one attempt's
staged chunks. `delete_meta` un-publishes a part dir (append CAS loser) without
touching chunks.
"""

from __future__ import annotations

import logging
import uuid
from pathlib import Path

import pytest

from hippius_s3.cache import FileSystemPartsStore


ATTEMPT = "0123456789abcdef"


@pytest.fixture()
def store(tmp_path) -> FileSystemPartsStore:
    return FileSystemPartsStore(str(tmp_path))


async def _seed_part(store: FileSystemPartsStore, object_id: str, *, indices: list[int], with_meta: bool) -> Path:
    for i in indices:
        await store.set_chunk(object_id, 1, 1, i, f"chunk-{i}".encode())
    if with_meta:
        await store.set_meta(object_id, 1, 1, chunk_size=4, num_chunks=len(indices), size_bytes=8)
    return Path(store.part_path(object_id, 1, 1))


async def _stage(store: FileSystemPartsStore, object_id: str, count: int) -> None:
    for i in range(count):
        await store.stage_chunk(object_id, 1, 1, i, f"new-{i}".encode(), attempt_id=ATTEMPT)


async def _publish(store: FileSystemPartsStore, object_id: str, num_chunks: int) -> None:
    await store.publish_part(
        object_id, 1, 1, attempt_id=ATTEMPT, chunk_size=4, num_chunks=num_chunks, size_bytes=8
    )


@pytest.mark.asyncio
async def test_publish_removes_the_tail_of_a_larger_earlier_attempt(store):
    object_id = str(uuid.uuid4())
    part_dir = await _seed_part(store, object_id, indices=[0, 1, 2, 3, 4], with_meta=True)

    await _stage(store, object_id, 2)
    await _publish(store, object_id, 2)

    assert sorted(p.name for p in part_dir.iterdir()) == ["chunk_0.bin", "chunk_1.bin", "meta.json"]
    assert (part_dir / "chunk_0.bin").read_bytes() == b"new-0"


@pytest.mark.asyncio
async def test_publish_of_an_equal_sized_set_leaves_no_extra_files(store):
    object_id = str(uuid.uuid4())
    part_dir = await _seed_part(store, object_id, indices=[0, 1], with_meta=True)

    await _stage(store, object_id, 2)
    await _publish(store, object_id, 2)

    assert sorted(p.name for p in part_dir.iterdir()) == ["chunk_0.bin", "chunk_1.bin", "meta.json"]


@pytest.mark.asyncio
async def test_publish_without_staged_chunks_raises_before_touching_the_part(store):
    """The pre-flight is what keeps a doomed publish from half-swapping the part: a whole-part
    eviction between staging and publish must abort the publish, not corrupt what is there."""
    object_id = str(uuid.uuid4())
    part_dir = await _seed_part(store, object_id, indices=[0, 1], with_meta=True)

    with pytest.raises(FileNotFoundError):
        await _publish(store, object_id, 2)

    assert (part_dir / "meta.json").exists()
    assert (part_dir / "chunk_0.bin").read_bytes() == b"chunk-0"


@pytest.mark.asyncio
async def test_publish_survives_an_untrimmable_tail_but_logs_it(store, caplog):
    object_id = str(uuid.uuid4())
    part_dir = await _seed_part(store, object_id, indices=[0, 1, 2], with_meta=True)
    # A directory named like a chunk makes unlink raise — a real-FS failure injection.
    (part_dir / "chunk_3.bin").mkdir()

    await _stage(store, object_id, 2)
    with caplog.at_level(logging.ERROR, logger="hippius_s3.cache.fs_store"):
        await _publish(store, object_id, 2)

    assert not (part_dir / "chunk_2.bin").exists()  # trimmed despite chunk_3 failing
    assert (part_dir / "meta.json").exists()  # the client's success does not hinge on the tail
    error_lines = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert error_lines, "surviving stale tail must be loud (stranded-part risk)"
    assert any(object_id in r.getMessage() for r in error_lines)


@pytest.mark.asyncio
async def test_discard_staged_removes_only_this_attempts_files(store):
    object_id = str(uuid.uuid4())
    part_dir = await _seed_part(store, object_id, indices=[0, 1], with_meta=True)
    await _stage(store, object_id, 2)
    await store.stage_chunk(object_id, 1, 1, 0, b"other", attempt_id="fedcba9876543210")

    removed = await store.discard_staged(object_id, 1, 1, attempt_id=ATTEMPT)

    assert removed == 2
    assert sorted(p.name for p in part_dir.iterdir()) == [
        "chunk_0.bin",
        "chunk_0.bin.staged.fedcba9876543210",
        "chunk_1.bin",
        "meta.json",
    ]


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
