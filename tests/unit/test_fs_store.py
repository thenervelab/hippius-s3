"""FileSystemPartsStore tests.

Covers:
- atomic chunk/meta writes (unique tmp + rename)
- concurrent writers (two workers, same chunk path)
- touch_chunk / touch_part updating atime/mtime
- chunks_exist_batch correctness
- crash-safety: partial writes don't leak as visible chunks
- delete_part idempotency and empty-dir pruning
"""

from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path

import pytest

from hippius_s3.cache.fs_store import FileSystemPartsStore


OBJ = "11111111-2222-3333-4444-555555555555"


@pytest.fixture
def fs(tmp_path: Path) -> FileSystemPartsStore:
    return FileSystemPartsStore(str(tmp_path))


# -------- safety / validation ----------


@pytest.mark.asyncio
async def test_invalid_object_id_rejected(fs: FileSystemPartsStore) -> None:
    with pytest.raises(ValueError):
        await fs.set_chunk("not-a-uuid", 1, 0, 0, b"data")


@pytest.mark.asyncio
async def test_negative_part_or_chunk_rejected(fs: FileSystemPartsStore) -> None:
    with pytest.raises(ValueError):
        await fs.set_chunk(OBJ, 1, -1, 0, b"data")
    with pytest.raises(ValueError):
        await fs.set_chunk(OBJ, 1, 0, -1, b"data")


# -------- basic I/O ----------


@pytest.mark.asyncio
async def test_set_and_get_chunk_roundtrip(fs: FileSystemPartsStore) -> None:
    await fs.set_chunk(OBJ, 1, 1, 0, b"payload")
    # meta gate: without meta, get_chunk returns None
    assert await fs.get_chunk(OBJ, 1, 1, 0) is None

    await fs.set_meta(OBJ, 1, 1, chunk_size=7, num_chunks=1, size_bytes=7)
    assert await fs.get_chunk(OBJ, 1, 1, 0) == b"payload"


@pytest.mark.asyncio
async def test_set_and_get_meta_roundtrip(fs: FileSystemPartsStore) -> None:
    await fs.set_meta(OBJ, 2, 3, chunk_size=4096, num_chunks=5, size_bytes=20000)
    meta = await fs.get_meta(OBJ, 2, 3)
    assert meta == {"chunk_size": 4096, "num_chunks": 5, "size_bytes": 20000}


@pytest.mark.asyncio
async def test_get_chunk_missing_returns_none(fs: FileSystemPartsStore) -> None:
    assert await fs.get_chunk(OBJ, 1, 99, 0) is None


@pytest.mark.asyncio
async def test_get_meta_missing_returns_none(fs: FileSystemPartsStore) -> None:
    assert await fs.get_meta(OBJ, 1, 99) is None


# -------- atomic writes ----------


@pytest.mark.asyncio
async def test_atomic_write_leaves_no_tmp_files(fs: FileSystemPartsStore) -> None:
    """After a successful write, only the final file exists — no .tmp.* debris."""
    await fs.set_chunk(OBJ, 1, 1, 0, b"payload")
    await fs.set_meta(OBJ, 1, 1, chunk_size=7, num_chunks=1, size_bytes=7)

    part_dir = Path(fs.part_path(OBJ, 1, 1))
    entries = sorted(p.name for p in part_dir.iterdir())
    assert entries == ["chunk_0.bin", "meta.json"]


@pytest.mark.asyncio
async def test_concurrent_writers_no_corruption(fs: FileSystemPartsStore) -> None:
    """Two workers writing the same chunk concurrently — last rename wins, no partial writes."""
    payload_a = b"A" * 4096
    payload_b = b"B" * 4096

    async def worker(data: bytes) -> None:
        for _ in range(10):
            await fs.set_chunk(OBJ, 1, 1, 0, data)

    await asyncio.gather(worker(payload_a), worker(payload_b))
    await fs.set_meta(OBJ, 1, 1, chunk_size=4096, num_chunks=1, size_bytes=4096)

    result = await fs.get_chunk(OBJ, 1, 1, 0)
    # Must be one of the complete payloads (never a mix)
    assert result in (payload_a, payload_b)
    assert len(result) == 4096

    # No temp debris left behind
    part_dir = Path(fs.part_path(OBJ, 1, 1))
    tmp_files = [p for p in part_dir.iterdir() if ".tmp." in p.name]
    assert tmp_files == []


@pytest.mark.asyncio
async def test_concurrent_writers_identical_content(fs: FileSystemPartsStore) -> None:
    """Realistic race: same backend → identical encrypted bytes → content is stable."""
    payload = b"identical-payload-from-same-backend" * 128

    async def worker() -> None:
        await fs.set_chunk(OBJ, 1, 1, 0, payload)

    await asyncio.gather(*[worker() for _ in range(20)])
    await fs.set_meta(OBJ, 1, 1, chunk_size=len(payload), num_chunks=1, size_bytes=len(payload))

    assert await fs.get_chunk(OBJ, 1, 1, 0) == payload


@pytest.mark.asyncio
async def test_temp_file_uses_unique_suffix(fs: FileSystemPartsStore) -> None:
    """_unique_tmp() builds distinct tempfile names on each call."""
    chunk_path = Path(fs.part_path(OBJ, 1, 1)) / "chunk_0.bin"
    a = fs._unique_tmp(chunk_path)
    b = fs._unique_tmp(chunk_path)
    assert a != b
    assert a.name.startswith("chunk_0.bin.tmp.")
    assert b.name.startswith("chunk_0.bin.tmp.")


@pytest.mark.asyncio
async def test_set_chunk_cleans_tmp_on_failure(tmp_path: Path, monkeypatch) -> None:
    """If the rename step raises, the tempfile must not be left on disk."""
    fs = FileSystemPartsStore(str(tmp_path))

    original_replace = Path.replace

    def boom(self, target):
        raise OSError("simulated disk failure")

    monkeypatch.setattr(Path, "replace", boom)

    with pytest.raises(OSError):
        await fs.set_chunk(OBJ, 1, 1, 0, b"will-fail")

    # Restore so teardown isn't broken
    monkeypatch.setattr(Path, "replace", original_replace)

    part_dir = Path(fs.part_path(OBJ, 1, 1))
    if part_dir.exists():
        leftovers = list(part_dir.iterdir())
        assert leftovers == [], f"Orphan tmp files left behind: {leftovers}"


# -------- meta gating for reads ----------


@pytest.mark.asyncio
async def test_chunk_exists_requires_meta(fs: FileSystemPartsStore) -> None:
    """Chunk file alone is not enough — meta.json must also exist."""
    await fs.set_chunk(OBJ, 1, 1, 0, b"data")
    assert await fs.chunk_exists(OBJ, 1, 1, 0) is False

    await fs.set_meta(OBJ, 1, 1, chunk_size=4, num_chunks=1, size_bytes=4)
    assert await fs.chunk_exists(OBJ, 1, 1, 0) is True


@pytest.mark.asyncio
async def test_partial_fill_readable_via_eager_meta(fs: FileSystemPartsStore) -> None:
    """Downloader writes meta eagerly — chunks become readable as they land."""
    await fs.set_meta(OBJ, 1, 1, chunk_size=4, num_chunks=5, size_bytes=20)
    # Only write chunks 0 and 2
    await fs.set_chunk(OBJ, 1, 1, 0, b"AAAA")
    await fs.set_chunk(OBJ, 1, 1, 2, b"CCCC")

    assert await fs.chunk_exists(OBJ, 1, 1, 0) is True
    assert await fs.chunk_exists(OBJ, 1, 1, 1) is False
    assert await fs.chunk_exists(OBJ, 1, 1, 2) is True
    assert await fs.get_chunk(OBJ, 1, 1, 0) == b"AAAA"
    assert await fs.get_chunk(OBJ, 1, 1, 1) is None


# -------- batch existence ----------


@pytest.mark.asyncio
async def test_chunks_exist_batch_empty(fs: FileSystemPartsStore) -> None:
    assert await fs.chunks_exist_batch(OBJ, 1, []) == []


@pytest.mark.asyncio
async def test_chunks_exist_batch_mixed(fs: FileSystemPartsStore) -> None:
    await fs.set_meta(OBJ, 1, 1, chunk_size=4, num_chunks=3, size_bytes=12)
    await fs.set_chunk(OBJ, 1, 1, 0, b"aaaa")
    await fs.set_chunk(OBJ, 1, 1, 2, b"cccc")

    result = await fs.chunks_exist_batch(OBJ, 1, [(1, 0), (1, 1), (1, 2), (2, 0)])
    assert result == [True, False, True, False]


# -------- touch / hot-retention ----------


@pytest.mark.asyncio
async def test_touch_chunk_updates_atime_mtime(fs: FileSystemPartsStore) -> None:
    await fs.set_chunk(OBJ, 1, 1, 0, b"hello")
    await fs.set_meta(OBJ, 1, 1, chunk_size=5, num_chunks=1, size_bytes=5)

    chunk_path = Path(fs.part_path(OBJ, 1, 1)) / "chunk_0.bin"
    old_stat = chunk_path.stat()
    await asyncio.sleep(0.05)
    await fs.touch_chunk(OBJ, 1, 1, 0)
    new_stat = chunk_path.stat()

    assert new_stat.st_mtime >= old_stat.st_mtime
    assert new_stat.st_atime >= old_stat.st_atime
    # At least one of them must have strictly advanced
    assert new_stat.st_mtime > old_stat.st_mtime or new_stat.st_atime > old_stat.st_atime


@pytest.mark.asyncio
async def test_touch_chunk_missing_is_noop(fs: FileSystemPartsStore) -> None:
    # Should not raise even though the chunk was never created
    await fs.touch_chunk(OBJ, 1, 99, 0)


@pytest.mark.asyncio
async def test_get_chunk_touches_file(fs: FileSystemPartsStore) -> None:
    """Every successful read updates atime/mtime — the janitor's hot-retention
    signal — without needing filesystem-level `strictatime`."""
    await fs.set_chunk(OBJ, 1, 1, 0, b"payload")
    await fs.set_meta(OBJ, 1, 1, chunk_size=7, num_chunks=1, size_bytes=7)

    chunk_path = Path(fs.part_path(OBJ, 1, 1)) / "chunk_0.bin"
    # Rewind both times to clearly distant past
    past = time.time() - 3600
    os.utime(chunk_path, (past, past))
    old_mtime = chunk_path.stat().st_mtime

    data = await fs.get_chunk(OBJ, 1, 1, 0)
    assert data == b"payload"

    new_mtime = chunk_path.stat().st_mtime
    assert new_mtime > old_mtime


@pytest.mark.asyncio
async def test_touch_part_updates_all_files(fs: FileSystemPartsStore) -> None:
    for i in range(3):
        await fs.set_chunk(OBJ, 1, 5, i, b"x" * 16)
    await fs.set_meta(OBJ, 1, 5, chunk_size=16, num_chunks=3, size_bytes=48)

    part_dir = Path(fs.part_path(OBJ, 1, 5))
    past = time.time() - 3600
    for entry in part_dir.iterdir():
        os.utime(entry, (past, past))

    await fs.touch_part(OBJ, 1, 5)

    for entry in part_dir.iterdir():
        assert entry.stat().st_mtime > past


@pytest.mark.asyncio
async def test_touch_part_missing_is_noop(fs: FileSystemPartsStore) -> None:
    await fs.touch_part(OBJ, 1, 999)  # should not raise


# -------- delete ----------


@pytest.mark.asyncio
async def test_delete_part_idempotent(fs: FileSystemPartsStore) -> None:
    await fs.set_chunk(OBJ, 1, 1, 0, b"x")
    await fs.set_meta(OBJ, 1, 1, chunk_size=1, num_chunks=1, size_bytes=1)

    await fs.delete_part(OBJ, 1, 1)
    await fs.delete_part(OBJ, 1, 1)  # second call is a no-op


@pytest.mark.asyncio
async def test_delete_part_prunes_empty_parents(fs: FileSystemPartsStore) -> None:
    await fs.set_chunk(OBJ, 1, 1, 0, b"x")
    await fs.set_meta(OBJ, 1, 1, chunk_size=1, num_chunks=1, size_bytes=1)

    await fs.delete_part(OBJ, 1, 1)

    version_dir = Path(fs.part_path(OBJ, 1, 1)).parent
    object_dir = version_dir.parent
    assert not version_dir.exists()
    assert not object_dir.exists()


@pytest.mark.asyncio
async def test_delete_part_keeps_non_empty_parent(fs: FileSystemPartsStore) -> None:
    await fs.set_chunk(OBJ, 1, 1, 0, b"x")
    await fs.set_meta(OBJ, 1, 1, chunk_size=1, num_chunks=1, size_bytes=1)
    await fs.set_chunk(OBJ, 1, 2, 0, b"y")
    await fs.set_meta(OBJ, 1, 2, chunk_size=1, num_chunks=1, size_bytes=1)

    await fs.delete_part(OBJ, 1, 1)

    # Version dir still has part 2, so it survives
    version_dir = Path(fs.part_path(OBJ, 1, 2)).parent
    assert version_dir.exists()
    assert (version_dir / "part_2").exists()


@pytest.mark.asyncio
async def test_delete_object_all_versions(fs: FileSystemPartsStore) -> None:
    await fs.set_chunk(OBJ, 1, 1, 0, b"v1")
    await fs.set_meta(OBJ, 1, 1, chunk_size=2, num_chunks=1, size_bytes=2)
    await fs.set_chunk(OBJ, 2, 1, 0, b"v2")
    await fs.set_meta(OBJ, 2, 1, chunk_size=2, num_chunks=1, size_bytes=2)

    await fs.delete_object(OBJ)

    assert not Path(fs.part_path(OBJ, 1, 1)).parent.parent.exists()


@pytest.mark.asyncio
async def test_delete_object_specific_version(fs: FileSystemPartsStore) -> None:
    await fs.set_chunk(OBJ, 1, 1, 0, b"v1")
    await fs.set_meta(OBJ, 1, 1, chunk_size=2, num_chunks=1, size_bytes=2)
    await fs.set_chunk(OBJ, 2, 1, 0, b"v2")
    await fs.set_meta(OBJ, 2, 1, chunk_size=2, num_chunks=1, size_bytes=2)

    await fs.delete_object(OBJ, object_version=1)

    assert not Path(fs.part_path(OBJ, 1, 1)).parent.exists()
    # v2 untouched
    assert await fs.chunk_exists(OBJ, 2, 1, 0)
