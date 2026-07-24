"""delete_part must do all filesystem I/O off the event loop thread.

Each on-loop stat/rmdir is a CephFS metadata roundtrip that serializes every
janitor worker behind MDS latency — the whole exists/rmtree/prune sequence has
to happen inside one thread hop.
"""

from __future__ import annotations

import logging
import os
import threading
from pathlib import Path

import pytest

from hippius_s3.cache.fs_store import FileSystemPartsStore


OBJ = "11111111-2222-3333-4444-555555555555"


@pytest.fixture
def fs(tmp_path: Path) -> FileSystemPartsStore:
    return FileSystemPartsStore(str(tmp_path))


@pytest.mark.asyncio
async def test_delete_part_does_all_fs_io_off_the_event_loop(
    fs: FileSystemPartsStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    await fs.set_chunk(OBJ, 1, 1, 0, b"x")
    await fs.set_meta(OBJ, 1, 1, chunk_size=1, num_chunks=1, size_bytes=1)
    part_dir = Path(fs.part_path(OBJ, 1, 1))

    loop_thread = threading.get_ident()
    offending: list[str] = []
    real_stat = os.stat

    def spy_stat(path, *args, **kwargs):  # type: ignore[no-untyped-def]
        # Path.exists() resolves to os.stat at call time on 3.11+, so any
        # on-loop existence check surfaces here.
        if threading.get_ident() == loop_thread:
            offending.append(str(path))
        return real_stat(path, *args, **kwargs)

    monkeypatch.setattr(os, "stat", spy_stat)
    await fs.delete_part(OBJ, 1, 1)
    # Snapshot before our own asserts stat the path from the loop thread.
    on_loop_hits = [p for p in offending if OBJ in p]

    assert not part_dir.exists()
    assert not on_loop_hits, f"on-loop FS calls: {on_loop_hits}"


@pytest.mark.asyncio
async def test_delete_part_missing_is_noop(fs: FileSystemPartsStore) -> None:
    await fs.delete_part(OBJ, 1, 99)  # must not raise


@pytest.mark.asyncio
async def test_delete_part_failure_logs_warning_and_does_not_raise(
    fs: FileSystemPartsStore, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    await fs.set_chunk(OBJ, 1, 1, 0, b"x")
    await fs.set_meta(OBJ, 1, 1, chunk_size=1, num_chunks=1, size_bytes=1)

    def boom(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise PermissionError("simulated permission failure")

    monkeypatch.setattr("hippius_s3.cache.fs_store.shutil.rmtree", boom)

    with caplog.at_level(logging.WARNING, logger="hippius_s3.cache.fs_store"):
        await fs.delete_part(OBJ, 1, 1)

    assert any("failed to delete part" in r.message for r in caplog.records)
    # Data untouched — a failed delete must never leave the part half-gone silently.
    assert await fs.chunk_exists(OBJ, 1, 1, 0)


@pytest.mark.asyncio
async def test_delete_part_prunes_empty_version_and_object_dirs(fs: FileSystemPartsStore) -> None:
    await fs.set_chunk(OBJ, 1, 1, 0, b"x")
    await fs.set_meta(OBJ, 1, 1, chunk_size=1, num_chunks=1, size_bytes=1)

    await fs.delete_part(OBJ, 1, 1)

    version_dir = Path(fs.part_path(OBJ, 1, 1)).parent
    assert not version_dir.exists()
    assert not version_dir.parent.exists()


@pytest.mark.asyncio
async def test_delete_part_keeps_non_empty_version_dir(fs: FileSystemPartsStore) -> None:
    await fs.set_chunk(OBJ, 1, 1, 0, b"x")
    await fs.set_meta(OBJ, 1, 1, chunk_size=1, num_chunks=1, size_bytes=1)
    await fs.set_chunk(OBJ, 1, 2, 0, b"y")
    await fs.set_meta(OBJ, 1, 2, chunk_size=1, num_chunks=1, size_bytes=1)

    await fs.delete_part(OBJ, 1, 1)

    version_dir = Path(fs.part_path(OBJ, 1, 2)).parent
    assert version_dir.exists()
    assert (version_dir / "part_2").exists()
