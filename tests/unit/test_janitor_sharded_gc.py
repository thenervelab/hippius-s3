"""Sharding + budgeting of the janitor FS-walk phases, and the sweep-scoped census.

The starvation fix makes each FS-walk phase cover one hash-shard of the tree per cycle
(full sweep every `shards` cycles) under a wall-clock budget, so the cycle always completes
and the DB-only durability phases (moved to the front) always run. The safety-critical
property is that sharding changes only WHICH objects a cycle visits, never the per-part
deletion decision: the union of deletions over all shards must equal the un-sharded run.
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


def _make_part(fs_root: Path, object_id: str, version: int, part: int, *, mtime_offset: float = 0) -> None:
    d = fs_root / object_id / f"v{version}" / f"part_{part}"
    d.mkdir(parents=True, exist_ok=True)
    (d / "chunk_0.bin").write_bytes(b"payload")
    (d / "meta.json").write_text('{"chunk_size": 7, "num_chunks": 1, "size_bytes": 7}')
    now = time.time()
    os.utime(d / "meta.json", (now - mtime_offset, now - mtime_offset))


class _PoolCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _FakePool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        return _PoolCtx(self._conn)


class _FakeFsStore:
    def __init__(self, root: Path):
        self.root = root
        self.deleted: list[tuple[str, int, int]] = []

    async def delete_part(self, object_id: str, object_version: int, part_number: int) -> None:
        import shutil

        self.deleted.append((object_id, object_version, part_number))
        p = self.root / object_id / f"v{object_version}" / f"part_{part_number}"
        if p.exists():
            shutil.rmtree(p)


@pytest.fixture(autouse=True)
def _reset_state():
    # hot_retention=1s so the parts these coverage tests create (mtime/atime ~1h ago) are cold;
    # they isolate the age+replication decision from hot-retention (tested separately).
    janitor.config.fs_cache_hot_retention_seconds = 1
    janitor.config.fs_cache_gc_max_age_seconds = 60
    janitor.config.mpu_stale_seconds = 86400
    janitor.config.upload_backends = ["arion"]
    janitor._reset_census_accum()
    janitor._walk_shard = 0
    yield


def _db():
    db = AsyncMock()
    db.fetchrow = AsyncMock(return_value=None)
    db.fetch = AsyncMock(return_value=[])
    db.fetchval = AsyncMock(return_value=None)
    return db


def _redis():
    r = MagicMock()
    r.lrange = AsyncMock(return_value=[])
    return r


def _n_objects(fs_root: Path, n: int, *, mtime_offset: float = 3600) -> None:
    for i in range(n):
        _make_part(fs_root, f"{i:032x}", 1, 1, mtime_offset=mtime_offset)


# ---- the safety invariant: shard union == unsharded ----


@pytest.mark.asyncio
async def test_shard_union_deletes_exactly_the_unsharded_set(tmp_path: Path, monkeypatch):
    """The whole point: sharding must not change WHAT gets deleted, only across how many cycles."""
    monkeypatch.setattr(janitor, "_pressure_mode", lambda root: 0)

    # Unsharded reference run.
    ref_root = tmp_path / "ref"
    _n_objects(ref_root, 60)
    ref_store = _FakeFsStore(ref_root)
    with patch.object(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True)):
        await janitor.cleanup_old_parts_by_mtime(_FakePool(_db()), ref_store, _redis(), shards=1, walk_concurrency=4)
    ref_deleted = set(ref_store.deleted)
    assert ref_deleted, "reference run should have deleted the cold replicated parts"

    # Sharded run: same tree, all shards, deletions accumulated.
    sh_root = tmp_path / "sh"
    _n_objects(sh_root, 60)
    sh_store = _FakeFsStore(sh_root)
    shards = 6
    with patch.object(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True)):
        for s in range(shards):
            await janitor.cleanup_old_parts_by_mtime(
                _FakePool(_db()), sh_store, _redis(), shard=s, shards=shards, walk_concurrency=4, publish_sweep=False
            )
    assert set(sh_store.deleted) == ref_deleted


@pytest.mark.asyncio
async def test_stale_cleanup_shard_union_matches(tmp_path: Path):
    """Same invariant for cleanup_stale_parts (orphan / no-parts-row reap path)."""
    ref_root = tmp_path / "ref"
    _n_objects(ref_root, 40, mtime_offset=janitor.config.mpu_stale_seconds + 100)
    ref_store = _FakeFsStore(ref_root)
    db = _db()  # fetchrow None => no parts row => orphan => reap
    await janitor.cleanup_stale_parts(_FakePool(db), ref_store, _redis(), shards=1, walk_concurrency=4)
    ref = set(ref_store.deleted)
    assert ref

    sh_root = tmp_path / "sh"
    _n_objects(sh_root, 40, mtime_offset=janitor.config.mpu_stale_seconds + 100)
    sh_store = _FakeFsStore(sh_root)
    shards = 5
    for s in range(shards):
        await janitor.cleanup_stale_parts(
            _FakePool(_db()), sh_store, _redis(), shard=s, shards=shards, walk_concurrency=4
        )
    assert set(sh_store.deleted) == ref


# ---- census is sweep-scoped ----


@pytest.mark.asyncio
async def test_census_publishes_only_on_full_sweep(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(janitor, "_pressure_mode", lambda root: 0)
    _n_objects(tmp_path, 30)
    store = _FakeFsStore(tmp_path)
    janitor._fs_parts_on_disk = -1  # sentinel: unchanged means "not published"
    shards = 3
    with patch.object(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=False)):
        # shards 0 and 1: accumulate, do NOT publish
        for s in range(2):
            await janitor.cleanup_old_parts_by_mtime(
                _FakePool(_db()), store, _redis(), shard=s, shards=shards, walk_concurrency=4, publish_sweep=False
            )
        assert janitor._fs_parts_on_disk == -1, "census must not publish mid-sweep"
        # last shard: publish the accumulated full-tree census
        await janitor.cleanup_old_parts_by_mtime(
            _FakePool(_db()), store, _redis(), shard=2, shards=shards, walk_concurrency=4, publish_sweep=True
        )
    assert janitor._fs_parts_on_disk == 30, "a completed sweep must publish the whole-tree count"


@pytest.mark.asyncio
async def test_shard_zero_resets_accumulator_so_a_mid_sweep_shard_change_cannot_inflate(tmp_path: Path, monkeypatch):
    """If pressure flips `shards` to 1 mid-sweep, the next shard-0 walk must start a clean
    accumulator — otherwise the prior partial sweep's counts blend in and inflate the census."""
    monkeypatch.setattr(janitor, "_pressure_mode", lambda root: 0)
    _n_objects(tmp_path, 40)
    store = _FakeFsStore(tmp_path)
    with patch.object(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=False)):
        # Partial 4-shard sweep: cover shards 0,1 (accumulates ~half the tree), never published.
        for s in (0, 1):
            await janitor.cleanup_old_parts_by_mtime(
                _FakePool(_db()), store, _redis(), shard=s, shards=4, walk_concurrency=4, publish_sweep=False
            )
        # Pressure kicks in → shards=1, shard=0, publish. shard-0 reset must discard the partial
        # accumulation and publish exactly the whole tree, not tree + the earlier half.
        await janitor.cleanup_old_parts_by_mtime(
            _FakePool(_db()), store, _redis(), shard=0, shards=1, walk_concurrency=4, publish_sweep=True
        )
    assert janitor._fs_parts_on_disk == 40, "shard-0 reset must prevent the mid-sweep count from inflating"


@pytest.mark.asyncio
async def test_truncated_sweep_does_not_publish_partial_census(tmp_path: Path, monkeypatch):
    """A budget-truncated shard taints the sweep; the gauge must hold its last good value."""
    monkeypatch.setattr(janitor, "_pressure_mode", lambda root: 0)
    _n_objects(tmp_path, 50)
    store = _FakeFsStore(tmp_path)
    janitor._fs_parts_on_disk = 999  # last-good value
    import asyncio

    loop = asyncio.get_running_loop()
    with patch.object(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=False)):
        # single-shard sweep, already-expired deadline => truncated
        await janitor.cleanup_old_parts_by_mtime(
            _FakePool(_db()),
            store,
            _redis(),
            shard=0,
            shards=1,
            walk_concurrency=2,
            deadline=loop.time() - 1.0,
            publish_sweep=True,
        )
    assert janitor._fs_parts_on_disk == 999, "a truncated sweep must not overwrite the census"


# ---- deadline helper ----


def test_walk_deadline_lifts_under_critical_pressure():
    import asyncio

    async def _check():
        loop = asyncio.get_running_loop()
        assert janitor._walk_deadline(loop, pressure=2, budget=300) is None  # critical => unbounded
        assert janitor._walk_deadline(loop, pressure=0, budget=0) is None  # disabled
        d = janitor._walk_deadline(loop, pressure=0, budget=300)
        assert d is not None and d > loop.time()
        d1 = janitor._walk_deadline(loop, pressure=1, budget=300)  # elevated still bounded
        assert d1 is not None

    asyncio.run(_check())


# ---- the core durability fix: DB-only phases run BEFORE the FS walks ----


@pytest.mark.asyncio
async def test_durability_phases_run_before_fs_walks(monkeypatch):
    """The starvation bug: the replication sentinel + aged-orphan gauge ran LAST, behind two
    full-tree FS walks that never finished, so on prod they never ran. They must now run first,
    unconditionally, so no FS-walk cost can starve them."""
    from pathlib import Path

    order: list[str] = []

    async def _rec(name, ret=0):
        order.append(name)
        return ret

    monkeypatch.setattr(
        janitor, "_update_disk_metrics", AsyncMock(side_effect=lambda root: order.append("disk_metrics"))
    )
    monkeypatch.setattr(janitor, "_pressure_mode", lambda root: 0)
    monkeypatch.setattr(janitor, "check_replication_sentinel", lambda *a, **k: _rec("sentinel"))
    monkeypatch.setattr(janitor, "get_all_dlq_object_ids", lambda *a, **k: _rec("dlq", set()))
    monkeypatch.setattr(janitor, "check_aged_pending_orphans", lambda *a, **k: _rec("aged_orphans"))
    monkeypatch.setattr(janitor, "cleanup_stale_parts", lambda *a, **k: _rec("fs_stale"))
    monkeypatch.setattr(janitor, "cleanup_old_parts_by_mtime", lambda *a, **k: _rec("fs_gc"))
    monkeypatch.setattr(janitor, "cleanup_orphan_tmp_files", lambda *a, **k: _rec("fs_tmp"))
    monkeypatch.setattr(janitor, "gc_soft_deleted_objects", lambda *a, **k: _rec("hard_delete"))
    monkeypatch.setattr(janitor, "_setup_janitor_metrics", lambda: None)
    monkeypatch.setattr(janitor, "create_fs_store", lambda config: MagicMock(root=Path("/tmp")))
    monkeypatch.setattr(janitor.asyncpg, "create_pool", AsyncMock(return_value=AsyncMock()))
    monkeypatch.setattr(janitor.Redis, "from_url", lambda url: AsyncMock())

    class _Stop(Exception):
        pass

    async def _sleep_then_stop(_seconds):
        raise _Stop

    monkeypatch.setattr(janitor.asyncio, "sleep", _sleep_then_stop)

    with pytest.raises(_Stop):
        await janitor.run_janitor_loop()

    assert "sentinel" in order and "aged_orphans" in order
    # both durability signals must precede every FS-walk phase
    first_fs = min(order.index(p) for p in ("fs_stale", "fs_gc", "fs_tmp"))
    assert order.index("sentinel") < first_fs, f"sentinel must run before FS walks; got {order}"
    assert order.index("aged_orphans") < first_fs, f"aged-orphan gauge must run before FS walks; got {order}"


@pytest.mark.asyncio
async def test_disk_pressure_collapses_the_walk_to_a_single_whole_tree_shard(monkeypatch):
    """Safety rule (config.janitor_walk_shards): under ANY disk pressure the janitor must collapse
    sharding to shards=1 so ONE cycle walks the WHOLE tree and eviction sees every deletable part —
    never a 1/Nth slice while the disk fills. A regression that kept the normal shard count under
    pressure would silently slow eviction exactly when it matters most; pin it here."""

    async def _shards_passed_at_pressure(pressure: int) -> dict:
        captured: dict = {}

        async def _capture_stale(*a, **k):
            captured["stale"] = k.get("shards")
            return 0

        async def _capture_gc(*a, **k):
            captured["gc"] = k.get("shards")
            return 0

        monkeypatch.setattr(janitor, "_update_disk_metrics", AsyncMock(return_value=None))
        monkeypatch.setattr(janitor, "_pressure_mode", lambda root: pressure)
        monkeypatch.setattr(janitor, "check_replication_sentinel", AsyncMock(return_value=0))
        monkeypatch.setattr(janitor, "get_all_dlq_object_ids", AsyncMock(return_value=set()))
        monkeypatch.setattr(janitor, "check_aged_pending_orphans", AsyncMock(return_value=0))
        monkeypatch.setattr(janitor, "cleanup_stale_parts", _capture_stale)
        monkeypatch.setattr(janitor, "cleanup_old_parts_by_mtime", _capture_gc)
        monkeypatch.setattr(janitor, "cleanup_orphan_tmp_files", AsyncMock(return_value=0))
        monkeypatch.setattr(janitor, "gc_soft_deleted_objects", AsyncMock(return_value=0))
        monkeypatch.setattr(janitor, "_setup_janitor_metrics", lambda: None)
        monkeypatch.setattr(janitor, "create_fs_store", lambda config: MagicMock(root=Path("/tmp")))
        monkeypatch.setattr(janitor.asyncpg, "create_pool", AsyncMock(return_value=AsyncMock()))
        monkeypatch.setattr(janitor.Redis, "from_url", lambda url: AsyncMock())

        class _Stop(Exception):
            pass

        async def _sleep_then_stop(_seconds):
            raise _Stop

        monkeypatch.setattr(janitor.asyncio, "sleep", _sleep_then_stop)
        with pytest.raises(_Stop):
            await janitor.run_janitor_loop()
        return captured

    # Elevated (1) AND critical (2) both force the whole-tree single-shard walk.
    for pressure in (1, 2):
        captured = await _shards_passed_at_pressure(pressure)
        assert captured["stale"] == 1, f"pressure={pressure} must force shards=1 (stale phase), got {captured}"
        assert captured["gc"] == 1, f"pressure={pressure} must force shards=1 (age-GC phase), got {captured}"

    # Normal pressure keeps the configured sharded sweep.
    normal = await _shards_passed_at_pressure(0)
    expected = max(1, janitor.config.janitor_walk_shards)
    assert normal["stale"] == expected, f"normal pressure must use the configured shard count, got {normal}"
    assert normal["gc"] == expected, f"normal pressure must use the configured shard count, got {normal}"
