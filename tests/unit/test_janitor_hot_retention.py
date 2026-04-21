"""Tests for the janitor's hot-retention + disk-pressure behaviors.

The janitor's age-based cleanup has three new behaviors:
1. Parts with atime within `fs_cache_hot_retention_seconds` are kept.
2. Under elevated disk pressure (>=85%), hot retention is halved.
3. Under critical pressure (>=95%), hot retention is 0; parts past half the
   age threshold are deleted even without replication.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from workers import run_janitor_in_loop as janitor


OBJ = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def _make_part(
    fs_root: Path, object_id: str, version: int, part_number: int, *, mtime_offset: float = 0, atime_offset: float = 0
) -> Path:
    """Create a fake part directory (chunk + meta) with adjustable timestamps.

    mtime_offset / atime_offset are seconds **before now**.
    """
    part_dir = fs_root / object_id / f"v{version}" / f"part_{part_number}"
    part_dir.mkdir(parents=True, exist_ok=True)

    chunk = part_dir / "chunk_0.bin"
    chunk.write_bytes(b"payload")

    meta = part_dir / "meta.json"
    meta.write_text('{"chunk_size": 7, "num_chunks": 1, "size_bytes": 7}')

    now = time.time()
    target = (now - atime_offset, now - mtime_offset)
    os.utime(chunk, target)
    os.utime(meta, target)

    return part_dir


class _FakeFsStore:
    def __init__(self, root: Path) -> None:
        self.root = root


@pytest.fixture
def fs_root(tmp_path: Path) -> Path:
    return tmp_path


@pytest.fixture
def fs_store(fs_root: Path) -> _FakeFsStore:
    return _FakeFsStore(fs_root)


@pytest.fixture
def redis_mock():
    client = MagicMock()
    client.lrange = AsyncMock(return_value=[])
    return client


@pytest.fixture
def db_mock():
    db = AsyncMock()
    db.fetchrow = AsyncMock(return_value=None)
    db.fetch = AsyncMock(return_value=[])
    db.fetchval = AsyncMock(return_value=None)
    return db


# Helper — patch in config values for each test
def _patch_config(hot_retention: int = 10800, gc_max_age: int = 604800) -> None:
    janitor.config.fs_cache_hot_retention_seconds = hot_retention
    janitor.config.fs_cache_gc_max_age_seconds = gc_max_age
    janitor.config.mpu_stale_seconds = 86400
    # Avoid touching real config.upload_backends during test
    janitor.config.upload_backends = ["arion"]


@pytest.mark.asyncio
async def test_normal_pressure_keeps_hot_files(fs_root, fs_store, redis_mock, db_mock):
    """Recently-read part (atime within hot window) survives GC."""
    _patch_config(hot_retention=10800, gc_max_age=60)

    # mtime is ancient (past GC cutoff), atime is recent (within hot window)
    _make_part(fs_root, OBJ, 1, 1, mtime_offset=3600, atime_offset=60)

    with patch.object(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True)):
        count = await janitor.cleanup_old_parts_by_mtime(db_mock, fs_store, redis_mock)

    assert count == 0
    assert (fs_root / OBJ / "v1" / "part_1").exists()
    assert janitor._fs_hot_parts == 1
    assert janitor._fs_pressure_mode == 0


@pytest.mark.asyncio
async def test_normal_pressure_deletes_cold_replicated(fs_root, fs_store, redis_mock, db_mock):
    """Old + replicated + cold atime → deleted."""
    _patch_config(hot_retention=10800, gc_max_age=60)

    # atime_offset must exceed hot_retention (10800s) to count as cold
    _make_part(fs_root, OBJ, 1, 1, mtime_offset=20000, atime_offset=20000)

    with patch.object(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True)):
        count = await janitor.cleanup_old_parts_by_mtime(db_mock, fs_store, redis_mock)

    assert count == 1
    assert not (fs_root / OBJ / "v1" / "part_1").exists()


@pytest.mark.asyncio
async def test_normal_pressure_keeps_not_replicated_not_old(fs_root, fs_store, redis_mock, db_mock):
    """Young part not yet replicated — keep it."""
    _patch_config(hot_retention=10800, gc_max_age=604800)

    _make_part(fs_root, OBJ, 1, 1, mtime_offset=60, atime_offset=60)

    with patch.object(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=False)):
        count = await janitor.cleanup_old_parts_by_mtime(db_mock, fs_store, redis_mock)

    assert count == 0
    assert (fs_root / OBJ / "v1" / "part_1").exists()


@pytest.mark.asyncio
async def test_hot_beats_replication(fs_root, fs_store, redis_mock, db_mock):
    """A file that's replicated AND old but ALSO hot is kept."""
    _patch_config(hot_retention=10800, gc_max_age=60)

    _make_part(fs_root, OBJ, 1, 1, mtime_offset=7200, atime_offset=600)

    with patch.object(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True)):
        count = await janitor.cleanup_old_parts_by_mtime(db_mock, fs_store, redis_mock)

    assert count == 0
    assert (fs_root / OBJ / "v1" / "part_1").exists()


@pytest.mark.asyncio
async def test_elevated_pressure_halves_hot_window(fs_root, fs_store, redis_mock, db_mock, monkeypatch):
    """At 90% disk usage, hot window halves: a part read 8000s ago (past half-hot)
    but within original 10800s is now treated as cold."""
    _patch_config(hot_retention=10800, gc_max_age=60)

    # Pretend pressure is elevated (simulate 90% disk)
    monkeypatch.setattr(janitor, "_pressure_mode", lambda root: 1)

    _make_part(fs_root, OBJ, 1, 1, mtime_offset=7200, atime_offset=8000)

    with patch.object(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True)):
        count = await janitor.cleanup_old_parts_by_mtime(db_mock, fs_store, redis_mock)

    assert count == 1  # elevated halves hot_window to 5400s, so 8000s > 5400s = cold
    assert janitor._fs_pressure_mode == 1


@pytest.mark.asyncio
async def test_elevated_pressure_still_keeps_very_recent(fs_root, fs_store, redis_mock, db_mock, monkeypatch):
    """Elevated pressure — a part read 60s ago is still hot (60s < 5400s)."""
    _patch_config(hot_retention=10800, gc_max_age=60)
    monkeypatch.setattr(janitor, "_pressure_mode", lambda root: 1)

    _make_part(fs_root, OBJ, 1, 1, mtime_offset=7200, atime_offset=60)

    with patch.object(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True)):
        count = await janitor.cleanup_old_parts_by_mtime(db_mock, fs_store, redis_mock)

    assert count == 0


@pytest.mark.asyncio
async def test_critical_pressure_ignores_hot_retention(fs_root, fs_store, redis_mock, db_mock, monkeypatch):
    """At 95%+ disk usage, hot retention is 0 — even very hot files are eligible."""
    _patch_config(hot_retention=10800, gc_max_age=60)
    monkeypatch.setattr(janitor, "_pressure_mode", lambda root: 2)

    _make_part(fs_root, OBJ, 1, 1, mtime_offset=3600, atime_offset=10)

    with patch.object(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True)):
        count = await janitor.cleanup_old_parts_by_mtime(db_mock, fs_store, redis_mock)

    assert count == 1


@pytest.mark.asyncio
async def test_critical_pressure_refuses_to_delete_non_replicated(fs_root, fs_store, redis_mock, db_mock, monkeypatch):
    """Absolute safety rule: even at 95%+ disk, a non-replicated part is
    NEVER deleted. The janitor logs ERROR and refuses to touch it rather
    than risk destroying the only copy of data.
    """
    _patch_config(hot_retention=10800, gc_max_age=600)
    monkeypatch.setattr(janitor, "_pressure_mode", lambda root: 2)

    # Very old, NOT replicated. Old janitor would have deleted this.
    _make_part(fs_root, OBJ, 1, 1, mtime_offset=3600, atime_offset=3600)

    with patch.object(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=False)):
        count = await janitor.cleanup_old_parts_by_mtime(db_mock, fs_store, redis_mock)

    assert count == 0
    assert (fs_root / OBJ / "v1" / "part_1").exists()


@pytest.mark.asyncio
async def test_critical_pressure_evicts_replicated_cold_parts(fs_root, fs_store, redis_mock, db_mock, monkeypatch):
    """Under critical pressure, fully-replicated cold parts ARE eligible for
    eviction regardless of age (hot_window=0 at critical pressure).
    """
    _patch_config(hot_retention=10800, gc_max_age=600)
    monkeypatch.setattr(janitor, "_pressure_mode", lambda root: 2)

    _make_part(fs_root, OBJ, 1, 1, mtime_offset=60, atime_offset=60)

    with patch.object(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True)):
        count = await janitor.cleanup_old_parts_by_mtime(db_mock, fs_store, redis_mock)

    assert count == 1


@pytest.mark.asyncio
async def test_backup_backends_unioned_into_replication_check(fs_root, fs_store, redis_mock, db_mock, monkeypatch):
    """When HIPPIUS_BACKUP_BACKENDS is set, those backends must be required
    for the replication check too. This guards against deleting FS chunks
    before s3-backup has pushed them to OVH.
    """
    _patch_config(hot_retention=0, gc_max_age=60)
    # Pretend OVH backup is required in addition to arion upload
    janitor.config.upload_backends = ["arion"]
    janitor.config.backup_backends = ["ovh"]
    monkeypatch.setattr(janitor, "_pressure_mode", lambda root: 0)

    _make_part(fs_root, OBJ, 1, 1, mtime_offset=7200, atime_offset=7200)

    # Record the `expected` list passed to the SQL so we can assert the union.
    captured_expected: list[list[str]] = []

    async def fake_fetchrow(sql, *args):
        if "object_versions" in sql:
            return {"version_type": None, "upload_backends": ["arion"]}
        if "count_chunk_backends" in sql or "chunk_backend" in sql:
            expected_arg = args[3] if len(args) >= 4 else []
            captured_expected.append(list(expected_arg))
            # Say: total==replicated==expected so the part is "replicated"
            return {"total_chunks": 1, "replicated_chunks": 1, "expected_chunks": 1}
        return None

    # Bypass get_query and drive fetchrow with our fake via the db_mock.
    db_mock.fetchrow = AsyncMock(side_effect=fake_fetchrow)

    # Mock get_query to return a dummy SQL string flagged for our faker.
    with patch.object(janitor, "get_query", return_value="SELECT count_chunk_backends"):
        count = await janitor.cleanup_old_parts_by_mtime(db_mock, fs_store, redis_mock)

    # Janitor called is_replicated_on_all_backends, which called
    # count_chunk_backends with an `expected` list containing BOTH arion and ovh.
    assert captured_expected, "replication check never ran"
    assert set(captured_expected[0]) == {"arion", "ovh"}
    # And because we claimed full replication, the eviction actually proceeds.
    assert count == 1


# -------- orphan .tmp cleanup ----------


@pytest.mark.asyncio
async def test_cleans_orphan_tmp_files(fs_root, fs_store):
    """Stale .tmp.* files (older than 1h) are removed."""
    part_dir = fs_root / OBJ / "v1" / "part_1"
    part_dir.mkdir(parents=True, exist_ok=True)

    old_tmp = part_dir / "chunk_0.bin.tmp.abc123"
    old_tmp.write_bytes(b"orphan")
    past = time.time() - 7200  # 2h ago
    os.utime(old_tmp, (past, past))

    fresh_tmp = part_dir / "chunk_0.bin.tmp.def456"
    fresh_tmp.write_bytes(b"fresh")
    # default mtime/atime = now

    count = await janitor.cleanup_orphan_tmp_files(fs_store)

    assert count == 1
    assert not old_tmp.exists()
    assert fresh_tmp.exists()  # too fresh to touch


@pytest.mark.asyncio
async def test_tmp_cleanup_empty_root_is_safe(tmp_path: Path):
    fs_store = _FakeFsStore(tmp_path / "does-not-exist")
    count = await janitor.cleanup_orphan_tmp_files(fs_store)
    assert count == 0


# -------- pressure mode detection ----------


def test_pressure_mode_normal(monkeypatch):
    class FakeUsage:
        used = 100
        total = 1000  # 10% used

    monkeypatch.setattr(janitor.shutil, "disk_usage", lambda p: FakeUsage())
    assert janitor._pressure_mode(Path("/tmp")) == 0


def test_pressure_mode_elevated(monkeypatch):
    class FakeUsage:
        used = 900
        total = 1000  # 90% used

    monkeypatch.setattr(janitor.shutil, "disk_usage", lambda p: FakeUsage())
    assert janitor._pressure_mode(Path("/tmp")) == 1


def test_pressure_mode_critical(monkeypatch):
    class FakeUsage:
        used = 970
        total = 1000  # 97% used

    monkeypatch.setattr(janitor.shutil, "disk_usage", lambda p: FakeUsage())
    assert janitor._pressure_mode(Path("/tmp")) == 2


def test_effective_hot_retention_scaling():
    _patch_config(hot_retention=10800)
    assert janitor._effective_hot_retention(0) == 10800
    assert janitor._effective_hot_retention(1) == 5400
    assert janitor._effective_hot_retention(2) == 0.0
