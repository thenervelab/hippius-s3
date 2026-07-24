"""Union-equivalence tests for the unified janitor FS walk.

The janitor used to run THREE separate full-tree FS walks per cycle — `cleanup_stale_parts`,
`cleanup_old_parts_by_mtime` (age-GC + census), `cleanup_orphan_tmp_files` — each independently
crawling the shard via `iter_part_dirs`. On prod that metadata-crawled a ~15.6M-object CephFS tree
3× per cycle over the same MDS that serves live GET/PUT. `cleanup_parts_unified` merges them into
ONE walk, applying every phase's rule per part dir.

The safety-critical property these tests pin: the WALK is unified but the deletion RULES are
byte-for-byte identical. The union of what the unified walk deletes must EXACTLY equal what the
three old phases delete when run in sequence — across every category (stale orphans,
terminally-abandoned parts, replicated-cold age-GC deletes, hot-protected, under-replicated,
DLQ-protected, and orphan `.tmp.*` files) and under sharding.
"""

from __future__ import annotations

import json
import os
import shutil
import time
import uuid
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from workers import run_janitor_in_loop as janitor


# ---- object-id catalogue (well-formed 32-hex names so the walk yields them) ----

ORPHAN_STALE = f"{0x01:032x}"  # stale-eligible, no parts row -> reap (stale_mtime)
ABANDONED = f"{0x02:032x}"  # stale-eligible, failed+unservable -> reap (abandoned)
PROTECTED_PENDING = f"{0x03:032x}"  # stale-eligible, old row, not replicated, not abandoned -> keep
GC_REPLICATED_COLD = f"{0x04:032x}"  # gc-eligible, replicated, cold -> delete (gc_age)
HOT_PROTECTED = f"{0x05:032x}"  # gc-aged mtime but recently read -> hot -> keep
UNDER_REPLICATED = f"{0x06:032x}"  # gc-eligible but NOT replicated -> replication gate -> keep
DLQ_PROTECTED = f"{0x07:032x}"  # stale+gc eligible but in DLQ -> keep (both rules honour DLQ)
TMP_SURVIVOR = f"{0x08:032x}"  # recent, ineligible; carries an orphan tmp that must be reaped

EXPECTED_DELETED = {(ORPHAN_STALE, 1, 1), (ABANDONED, 1, 1), (GC_REPLICATED_COLD, 1, 1)}
EXPECTED_TMP = 3  # tmp files live ONLY in surviving dirs (PROTECTED_PENDING, UNDER_REPLICATED, TMP_SURVIVOR)

REPLICATED_OIDS = {GC_REPLICATED_COLD, DLQ_PROTECTED}
ABANDONED_OIDS = {ABANDONED}
# parts-row state for the stale query: object_id -> None (no row) | {"recent": bool}
PARTS_ROWS: dict[str, dict | None] = {ABANDONED: {"recent": False}, PROTECTED_PENDING: {"recent": False}}


def _make_part(root: Path, oid: str, *, mtime_ago: float, atime_ago: float, tmp_ago: float | None = None) -> None:
    d = root / oid / "v1" / "part_1"
    d.mkdir(parents=True, exist_ok=True)
    (d / "chunk_0.bin").write_bytes(b"payload")
    (d / "meta.json").write_text('{"chunk_size": 7, "num_chunks": 1, "size_bytes": 7}')
    now = time.time()
    os.utime(d / "meta.json", (now - atime_ago, now - mtime_ago))
    if tmp_ago is not None:
        t = d / f"chunk_0.bin.tmp.{uuid.uuid4()}"
        t.write_bytes(b"partial")
        os.utime(t, (now - tmp_ago, now - tmp_ago))


def _build_catalogue(root: Path) -> None:
    old = 200_000  # older than the 86_400 stale threshold
    gc_aged = 3600  # older than the 60s gc cutoff, younger than the stale threshold
    recent = 30  # younger than everything
    tmp_old = 3600  # older than TMP_FILE_MAX_AGE_SECONDS (1800)

    _make_part(root, ORPHAN_STALE, mtime_ago=old, atime_ago=old)
    _make_part(root, ABANDONED, mtime_ago=old, atime_ago=old)
    _make_part(root, PROTECTED_PENDING, mtime_ago=old, atime_ago=old, tmp_ago=tmp_old)
    _make_part(root, GC_REPLICATED_COLD, mtime_ago=gc_aged, atime_ago=gc_aged)
    _make_part(root, HOT_PROTECTED, mtime_ago=gc_aged, atime_ago=0)  # atime now -> hot
    _make_part(root, UNDER_REPLICATED, mtime_ago=gc_aged, atime_ago=gc_aged, tmp_ago=tmp_old)
    _make_part(root, DLQ_PROTECTED, mtime_ago=old, atime_ago=old)
    _make_part(root, TMP_SURVIVOR, mtime_ago=recent, atime_ago=recent, tmp_ago=tmp_old)


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


class _FakeConn:
    """Answers only the stale-phase `parts` recency query; replication / abandoned checks are
    patched at module level so they never touch this conn."""

    async def fetchrow(self, query, *args):
        object_id = args[0]
        return PARTS_ROWS.get(object_id)


class _FakeFsStore:
    def __init__(self, root: Path):
        self.root = root
        self.deleted: list[tuple[str, int, int]] = []

    async def delete_part(self, object_id: str, object_version: int, part_number: int) -> None:
        self.deleted.append((object_id, int(object_version), int(part_number)))
        p = self.root / object_id / f"v{object_version}" / f"part_{part_number}"
        if p.exists():
            shutil.rmtree(p)


def _redis(dlq_oids: tuple[str, ...] = ()):
    r = MagicMock()
    entries = [json.dumps({"object_id": o}) for o in dlq_oids]

    async def _lrange(key, start, end):
        return entries if key == "arion_upload_requests:dlq" else []

    r.lrange = AsyncMock(side_effect=_lrange)
    return r


async def _fake_replicated(conn, oid, ov, pn):
    return oid in REPLICATED_OIDS


async def _fake_abandoned(conn, oid, ov, pn):
    return oid in ABANDONED_OIDS


@pytest.fixture(autouse=True)
def _cfg():
    janitor.config.fs_cache_hot_retention_seconds = 1  # tight window: only atime==now is "hot"
    janitor.config.fs_cache_gc_max_age_seconds = 60
    janitor.config.mpu_stale_seconds = 86_400
    janitor.config.upload_backends = ["arion"]
    janitor.config.backup_backends = []
    janitor._reset_census_accum()
    janitor._walk_shard = 0
    yield


async def _run_three_phases_sequential(store, redis, **walk_kw) -> int:
    """The OLD loop body: stale -> age-GC -> orphan-tmp, in sequence, on one tree."""
    await janitor.cleanup_stale_parts(_FakePool(_FakeConn()), store, redis, **walk_kw)
    await janitor.cleanup_old_parts_by_mtime(_FakePool(_FakeConn()), store, redis, **walk_kw)
    return await janitor.cleanup_orphan_tmp_files(store, **walk_kw)


# --------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unified_walk_deletes_exactly_the_three_phase_union(tmp_path: Path, monkeypatch):
    """The core property: one walk deletes EXACTLY the set (and reaps exactly the tmp files) that
    the three separate phases delete when run in sequence — across every category."""
    monkeypatch.setattr(janitor, "_pressure_mode", lambda root: 0)

    walk_kw = {"shard": 0, "shards": 1, "walk_concurrency": 4, "deadline": None}

    # Reference: run the three old phases sequentially.
    ref_root = tmp_path / "ref"
    _build_catalogue(ref_root)
    ref_store = _FakeFsStore(ref_root)
    with (
        patch.object(janitor, "is_replicated_on_all_backends", _fake_replicated),
        patch.object(janitor, "is_terminally_abandoned", _fake_abandoned),
    ):
        ref_tmp = await _run_three_phases_sequential(ref_store, _redis((DLQ_PROTECTED,)), **walk_kw)

    assert set(ref_store.deleted) == EXPECTED_DELETED
    assert ref_tmp == EXPECTED_TMP

    # Unified: one walk over an identical tree.
    uni_root = tmp_path / "uni"
    _build_catalogue(uni_root)
    uni_store = _FakeFsStore(uni_root)
    with (
        patch.object(janitor, "is_replicated_on_all_backends", _fake_replicated),
        patch.object(janitor, "is_terminally_abandoned", _fake_abandoned),
    ):
        res = await janitor.cleanup_parts_unified(
            _FakePool(_FakeConn()), uni_store, _redis((DLQ_PROTECTED,)), pressure=0, **walk_kw
        )

    assert set(uni_store.deleted) == set(ref_store.deleted), "unified deletions must equal the 3-phase union"
    assert res["tmp"] == ref_tmp
    # And the reason attribution is correct.
    assert res["stale_mtime"] == 1  # ORPHAN_STALE
    assert res["abandoned"] == 1  # ABANDONED
    assert res["gc"] == 1  # GC_REPLICATED_COLD

    # The survivors really survive on disk.
    for oid in (PROTECTED_PENDING, HOT_PROTECTED, UNDER_REPLICATED, DLQ_PROTECTED, TMP_SURVIVOR):
        assert (uni_root / oid / "v1" / "part_1").exists(), f"{oid} must not be deleted"
    # Orphan tmp files are gone from the survivors that carried them.
    for oid in (PROTECTED_PENDING, UNDER_REPLICATED, TMP_SURVIVOR):
        remaining = list((uni_root / oid / "v1" / "part_1").glob("*.tmp.*"))
        assert remaining == [], f"{oid} orphan tmp must be reaped by the unified walk"


@pytest.mark.asyncio
async def test_unified_walk_dlq_unavailable_skips_stale_but_still_age_gcs(tmp_path: Path, monkeypatch):
    """Invariant 2 & 3: on a DLQ-read failure the unified walk SKIPS the (non-replication-gated)
    stale-reap entirely (fail-closed), but STILL performs replication-gated age-GC eviction
    (fail-open) — a fully-replicated part is safe to evict regardless of DLQ availability."""
    monkeypatch.setattr(janitor, "_pressure_mode", lambda root: 0)

    root = tmp_path / "t"
    _build_catalogue(root)
    store = _FakeFsStore(root)

    async def _raise(_redis):
        raise janitor.DLQProtectionUnavailable("redis-queues down")

    with (
        patch.object(janitor, "is_replicated_on_all_backends", _fake_replicated),
        patch.object(janitor, "is_terminally_abandoned", _fake_abandoned),
        patch.object(janitor, "get_all_dlq_object_ids", _raise),
    ):
        res = await janitor.cleanup_parts_unified(
            _FakePool(_FakeConn()), store, _redis(), pressure=0, shard=0, shards=1, walk_concurrency=4
        )

    deleted = set(store.deleted)
    # Stale-reap is skipped: ORPHAN_STALE and ABANDONED (both non-replicated) survive.
    assert (ORPHAN_STALE, 1, 1) not in deleted
    assert (ABANDONED, 1, 1) not in deleted
    assert res["stale_mtime"] == 0 and res["abandoned"] == 0
    # Age-GC still runs replication-gate-only: the replicated-cold part is evicted.
    assert (GC_REPLICATED_COLD, 1, 1) in deleted
    # DLQ set is empty (unavailable) but DLQ_PROTECTED is replicated + gc-aged, so age-GC evicts it
    # too — proving age-GC no longer depends on the DLQ dimension when it is down.
    assert (DLQ_PROTECTED, 1, 1) in deleted
    assert res["gc"] == 2  # GC_REPLICATED_COLD + DLQ_PROTECTED (no longer DLQ-shielded when DLQ is down)
    # tmp cleanup is independent of the DB/DLQ and still runs.
    assert res["tmp"] == EXPECTED_TMP


@pytest.mark.asyncio
async def test_unified_shard_union_equals_unsharded(tmp_path: Path, monkeypatch):
    """Invariant 9: sharding changes only WHICH objects a cycle visits, never the per-part
    decision — the union of unified deletions over all shards equals the unsharded unified run."""
    monkeypatch.setattr(janitor, "_pressure_mode", lambda root: 0)

    # Unsharded reference.
    ref_root = tmp_path / "ref"
    _build_catalogue(ref_root)
    ref_store = _FakeFsStore(ref_root)
    with (
        patch.object(janitor, "is_replicated_on_all_backends", _fake_replicated),
        patch.object(janitor, "is_terminally_abandoned", _fake_abandoned),
    ):
        await janitor.cleanup_parts_unified(
            _FakePool(_FakeConn()), ref_store, _redis((DLQ_PROTECTED,)), pressure=0, shards=1, walk_concurrency=4
        )
    ref_deleted = set(ref_store.deleted)
    assert ref_deleted == EXPECTED_DELETED

    # Sharded: same tree, every shard, deletions accumulated.
    sh_root = tmp_path / "sh"
    _build_catalogue(sh_root)
    sh_store = _FakeFsStore(sh_root)
    shards = 5
    with (
        patch.object(janitor, "is_replicated_on_all_backends", _fake_replicated),
        patch.object(janitor, "is_terminally_abandoned", _fake_abandoned),
    ):
        for s in range(shards):
            await janitor.cleanup_parts_unified(
                _FakePool(_FakeConn()),
                sh_store,
                _redis((DLQ_PROTECTED,)),
                pressure=0,
                shard=s,
                shards=shards,
                walk_concurrency=4,
                publish_sweep=(s == shards - 1),
            )

    assert set(sh_store.deleted) == ref_deleted, "unified shard union must equal the unsharded unified run"


@pytest.mark.asyncio
async def test_unified_tmp_only_no_part_deletes(tmp_path: Path, monkeypatch):
    """A tree with nothing deletable but stale orphan tmp files: the unified walk deletes no part
    yet still reaps the tmp files in the SAME pass (no separate tmp crawl)."""
    monkeypatch.setattr(janitor, "_pressure_mode", lambda root: 0)

    root = tmp_path / "t"
    # A single recent, ineligible part carrying an old orphan tmp file.
    _make_part(root, TMP_SURVIVOR, mtime_ago=10, atime_ago=10, tmp_ago=3600)
    store = _FakeFsStore(root)

    with (
        patch.object(janitor, "is_replicated_on_all_backends", _fake_replicated),
        patch.object(janitor, "is_terminally_abandoned", _fake_abandoned),
    ):
        res = await janitor.cleanup_parts_unified(
            _FakePool(_FakeConn()), store, _redis(), pressure=0, shard=0, shards=1, walk_concurrency=2
        )

    assert store.deleted == []
    assert res == {"stale_mtime": 0, "abandoned": 0, "gc": 0, "tmp": 1}
    assert list((root / TMP_SURVIVOR / "v1" / "part_1").glob("*.tmp.*")) == []
