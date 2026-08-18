"""Publishing a part excludes other PROCESSES, not just other coroutines.

`_publish_locks` is a module-level registry of `asyncio.Lock`, so it is per-interpreter. The api
runs uvicorn with `UVICORN_WORKERS=4` in both namespaces, and the four workers share one listening
socket — so two `UploadPart` attempts at the same part land in different interpreters most of the
time and never see each other's lock.

That matters because publishing swaps a SET of files and a set-rename has no atomic primitive. Two
promotions interleaving produce a part whose chunks come from both attempts, and since nonces are
random per chunk (#402) and the AAD binds `(bucket, object, part, chunk)` rather than attempt
identity, every chunk in that mixture decrypts cleanly under a valid tag. The corruption is silent.

`flock` is keyed on the open file description, not the process, so two `open()` calls contend even
within one interpreter. These tests use that to exercise the cross-process path without spawning a
second one — the lock a sibling fd here observes is the same lock a sibling worker observes.
"""

from __future__ import annotations

import asyncio
import fcntl
import os
from pathlib import Path

import pytest

from hippius_s3.cache.fs_store import FileSystemPartsStore


OBJ = "466916c0-d61b-4518-b81b-9576b574270a"
CHUNK = b"0123456789abcdef"


async def _stage_one_chunk(store: FileSystemPartsStore, attempt: str) -> None:
    await store.stage_chunk(OBJ, 1, 1, 0, CHUNK, attempt_id=attempt)


def _part_dir(store: FileSystemPartsStore) -> Path:
    return Path(store.part_path(OBJ, 1, 1))


@pytest.mark.asyncio
async def test_publish_waits_for_a_lock_another_process_holds(tmp_path) -> None:
    """A publish must not proceed while another interpreter is mid-swap.

    Held from a second file description, which is what a second uvicorn worker is from this one's
    point of view. Without the flock the publish sails through and the assertion on `done` fails.
    """
    store = FileSystemPartsStore(str(tmp_path))
    await _stage_one_chunk(store, "attemptaa")
    part_dir = _part_dir(store)

    # Stand in for the other worker: hold the directory lock on an independent description.
    other = os.open(part_dir, os.O_RDONLY)
    fcntl.flock(other, fcntl.LOCK_EX)
    try:
        task = asyncio.create_task(
            store.publish_part(
                OBJ, 1, 1, chunk_size=len(CHUNK), num_chunks=1, size_bytes=len(CHUNK), attempt_id="attemptaa"
            )
        )
        # Long enough that a publish which ignored the lock would have finished: the whole swap is
        # a handful of local renames.
        await asyncio.sleep(0.35)
        assert not task.done(), "publish went ahead while another process held the part lock"

        fcntl.flock(other, fcntl.LOCK_UN)
        await asyncio.wait_for(task, timeout=5)
    finally:
        os.close(other)

    assert await store.get_chunk(OBJ, 1, 1, 0) == CHUNK, "and it still publishes once the lock frees"


@pytest.mark.asyncio
async def test_the_lock_is_released_after_publishing(tmp_path) -> None:
    """A held-forever lock would wedge every later attempt at that part on that node."""
    store = FileSystemPartsStore(str(tmp_path))
    await _stage_one_chunk(store, "attemptaa")
    await store.publish_part(
        OBJ, 1, 1, chunk_size=len(CHUNK), num_chunks=1, size_bytes=len(CHUNK), attempt_id="attemptaa"
    )

    probe = os.open(_part_dir(store), os.O_RDONLY)
    try:
        fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)  # raises if still held
        fcntl.flock(probe, fcntl.LOCK_UN)
    finally:
        os.close(probe)


@pytest.mark.asyncio
async def test_a_failed_publish_still_releases_the_lock(tmp_path) -> None:
    """The failure path is the one that strands a lock, so it is the one worth pinning.

    Staging is deleted underneath the publish, so the pre-flight raises inside the critical
    section — the lock has to come back regardless.
    """
    store = FileSystemPartsStore(str(tmp_path))
    await _stage_one_chunk(store, "attemptaa")
    part_dir = _part_dir(store)
    for staged in part_dir.glob("*.staged.*"):
        staged.unlink()

    with pytest.raises(FileNotFoundError):
        await store.publish_part(
            OBJ, 1, 1, chunk_size=len(CHUNK), num_chunks=1, size_bytes=len(CHUNK), attempt_id="attemptaa"
        )

    probe = os.open(part_dir, os.O_RDONLY)
    try:
        fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(probe, fcntl.LOCK_UN)
    finally:
        os.close(probe)


@pytest.mark.asyncio
async def test_different_parts_do_not_block_each_other(tmp_path) -> None:
    """The lock is per part dir. Serializing unrelated parts would cost the whole ingest path."""
    store = FileSystemPartsStore(str(tmp_path))
    await store.stage_chunk(OBJ, 1, 1, 0, CHUNK, attempt_id="aa")
    await store.stage_chunk(OBJ, 1, 2, 0, CHUNK, attempt_id="aa")

    held = os.open(Path(store.part_path(OBJ, 1, 1)), os.O_RDONLY)
    fcntl.flock(held, fcntl.LOCK_EX)
    try:
        await asyncio.wait_for(
            store.publish_part(OBJ, 1, 2, chunk_size=len(CHUNK), num_chunks=1, size_bytes=len(CHUNK), attempt_id="aa"),
            timeout=5,
        )
    finally:
        fcntl.flock(held, fcntl.LOCK_UN)
        os.close(held)

    assert await store.get_chunk(OBJ, 1, 2, 0) == CHUNK
