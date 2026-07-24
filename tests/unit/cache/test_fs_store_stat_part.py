"""stat_part is the janitor SQL-eviction candidate check: meta.json-else-dir, None if gone.

It mirrors the walk's readiness rule (meta.json is the part-complete signal, fall back to the
part dir) so a candidate's atime and existence are read the same way the walk reads them. A None
tells the caller the inventory row is stale (the part dir is already gone) and must be self-healed.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from hippius_s3.cache.fs_store import FileSystemPartsStore


OBJ = "11111111-2222-3333-4444-555555555555"


@pytest.fixture
def fs(tmp_path: Path) -> FileSystemPartsStore:
    return FileSystemPartsStore(str(tmp_path))


@pytest.mark.asyncio
async def test_stat_part_present_stats_meta(fs: FileSystemPartsStore) -> None:
    await fs.set_chunk(OBJ, 1, 1, 0, b"x")
    await fs.set_meta(OBJ, 1, 1, chunk_size=1, num_chunks=1, size_bytes=1)

    st = fs.stat_part(OBJ, 1, 1)

    assert st is not None
    meta = Path(fs.part_path(OBJ, 1, 1)) / "meta.json"
    assert st.st_ino == meta.stat().st_ino  # it stats meta.json, not the dir


@pytest.mark.asyncio
async def test_stat_part_meta_missing_falls_back_to_dir(fs: FileSystemPartsStore) -> None:
    # A part dir that exists but has no meta.json yet (mid-materialization) still stats — the
    # walk falls back to the dir here, and so must the candidate check.
    part_dir = Path(fs.part_path(OBJ, 1, 1))
    part_dir.mkdir(parents=True)
    (part_dir / "chunk_0.bin").write_bytes(b"x")

    st = fs.stat_part(OBJ, 1, 1)

    assert st is not None
    assert st.st_ino == part_dir.stat().st_ino


def test_stat_part_absent_returns_none(fs: FileSystemPartsStore) -> None:
    assert fs.stat_part(OBJ, 1, 99) is None
