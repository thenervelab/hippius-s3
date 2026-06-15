"""Tests for the janitor's bounded-concurrency cleanup machinery.

Phase 1/2 used to walk the FS and run per-part DB checks + deletes on a single
connection, serially. At 3M+ parts that pass could not finish in a day. The
cleanup now feeds candidates from a producer into a pool of workers
(`_run_worker_pool`), and disk-usage metrics are refreshed up front
(`_update_disk_metrics`) so visibility never waits on the GC walk.

These tests pin down:
- the worker pool processes every item, bounds in-flight work to `concurrency`,
  and returns the count of truthy handles;
- failures in one item don't strand the rest;
- disk metrics are populated independent of any cleanup pass;
- Phase 1 actually fans out across multiple pooled connections.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from workers import run_janitor_in_loop as janitor


class _TrackingPool:
    """Pool whose acquire() context tracks peak concurrency precisely.

    If `conn` is given, every acquire() yields that same configured connection
    (so a test's `fetchrow`/`fetch` behavior is preserved); otherwise a fresh
    MagicMock is handed out (fine for tests whose handle ignores the conn).
    """

    def __init__(self, conn=None) -> None:
        self.in_use = 0
        self.max_in_use = 0
        self.total_acquired = 0
        self._conn = conn

    def acquire(self):
        pool = self

        class _Ctx:
            async def __aenter__(self_):
                pool.in_use += 1
                pool.total_acquired += 1
                pool.max_in_use = max(pool.max_in_use, pool.in_use)
                return pool._conn if pool._conn is not None else MagicMock()

            async def __aexit__(self_, *exc):
                pool.in_use -= 1
                return False

        return _Ctx()


async def _agen(items):
    for it in items:
        yield it


# ----------------------------------------------------------- _run_worker_pool


@pytest.mark.asyncio
async def test_worker_pool_processes_every_item():
    pool = _TrackingPool()
    seen: list[int] = []

    async def handle(_conn, item) -> bool:
        seen.append(item)
        return True

    cleaned = await janitor._run_worker_pool(pool, _agen(range(50)), handle, concurrency=8)

    assert cleaned == 50
    assert sorted(seen) == list(range(50))
    assert pool.total_acquired == 50  # one acquire per item


@pytest.mark.asyncio
async def test_worker_pool_counts_only_truthy_handles():
    pool = _TrackingPool()

    async def handle(_conn, item) -> bool:
        return item % 2 == 0  # delete evens only

    cleaned = await janitor._run_worker_pool(pool, _agen(range(10)), handle, concurrency=4)

    assert cleaned == 5  # 0,2,4,6,8


@pytest.mark.asyncio
async def test_worker_pool_bounds_concurrency():
    """No more than `concurrency` handles run at once even with many items."""
    pool = _TrackingPool()
    concurrency = 5
    live = 0
    peak = 0

    async def handle(_conn, _item) -> bool:
        nonlocal live, peak
        live += 1
        peak = max(peak, live)
        await asyncio.sleep(0.01)  # hold the slot so overlap is observable
        live -= 1
        return True

    cleaned = await janitor._run_worker_pool(pool, _agen(range(40)), handle, concurrency=concurrency)

    assert cleaned == 40
    assert peak <= concurrency, f"ran {peak} handles at once, cap was {concurrency}"
    assert pool.max_in_use <= concurrency


@pytest.mark.asyncio
async def test_worker_pool_empty_producer_returns_zero():
    pool = _TrackingPool()

    async def handle(_conn, _item) -> bool:  # pragma: no cover - never called
        return True

    cleaned = await janitor._run_worker_pool(pool, _agen([]), handle, concurrency=8)

    assert cleaned == 0
    assert pool.total_acquired == 0


@pytest.mark.asyncio
async def test_worker_pool_survives_handle_exception():
    """A raising handle (e.g. a transient pool.acquire/DB blip on one part) must
    NOT kill the worker or wedge the pool: it's logged and the sweep continues,
    so every other item is still processed and counted."""
    pool = _TrackingPool()
    processed: list[int] = []

    async def handle(_conn, item) -> bool:
        processed.append(item)
        if item == 3:
            raise RuntimeError("infra blip on item 3")
        return True

    cleaned = await janitor._run_worker_pool(pool, _agen(range(20)), handle, concurrency=4)

    assert set(processed) == set(range(20)), "every item must still be handled"
    assert cleaned == 19, "only the raising item is not counted"


@pytest.mark.asyncio
async def test_worker_pool_survives_producer_exception():
    """The producer walks the tree while workers delete from it, so a concurrent
    prune can raise mid-walk. The pool must drain cleanly (no orphaned/pending
    worker tasks, no hang) and items yielded before the error must be processed.
    The producer error then surfaces to the phase handler."""
    pool = _TrackingPool()
    handled: list[int] = []

    async def handle(_conn, item) -> bool:
        handled.append(item)
        return True

    async def exploding_producer():
        for i in range(5):
            yield i
        raise FileNotFoundError("dir vanished mid-walk")

    with pytest.raises(FileNotFoundError):
        await janitor._run_worker_pool(pool, exploding_producer(), handle, concurrency=4)

    # The 5 pre-error items were still handed off and processed (no deadlock).
    assert set(handled) == {0, 1, 2, 3, 4}
    # All worker tasks were drained/awaited — no lingering pending tasks.
    pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
    assert pending == [], f"workers leaked: {pending}"


@pytest.mark.asyncio
async def test_worker_pool_clamps_nonpositive_concurrency():
    """concurrency<=0 must not yield an unbounded queue + zero workers (a silent
    no-op that fails to GC under disk pressure). It is clamped to 1."""
    pool = _TrackingPool()
    seen: list[int] = []

    async def handle(_conn, item) -> bool:
        seen.append(item)
        return True

    cleaned = await janitor._run_worker_pool(pool, _agen(range(12)), handle, concurrency=0)

    assert cleaned == 12
    assert sorted(seen) == list(range(12))
    assert pool.max_in_use == 1  # clamped to a single worker


# --------------------------------------------------------- _update_disk_metrics


def test_update_disk_metrics_populates_gauges_without_cleanup(monkeypatch):
    """Disk + pressure gauges are set from a single statvfs, not from a GC pass."""

    class FakeUsage:
        used = 850
        total = 1000  # 85% → elevated

    monkeypatch.setattr(janitor.shutil, "disk_usage", lambda p: FakeUsage())

    janitor._fs_disk_used_bytes = 0
    janitor._fs_disk_total_bytes = 0
    janitor._fs_pressure_mode = 0

    janitor._update_disk_metrics(Path("/tmp"))

    assert janitor._fs_disk_used_bytes == 850
    assert janitor._fs_disk_total_bytes == 1000
    assert janitor._fs_pressure_mode == 1  # 85% == elevated boundary


def test_update_disk_metrics_critical(monkeypatch):
    class FakeUsage:
        used = 990
        total = 1000  # 99% → critical

    monkeypatch.setattr(janitor.shutil, "disk_usage", lambda p: FakeUsage())
    janitor._update_disk_metrics(Path("/tmp"))
    assert janitor._fs_pressure_mode == 2


def test_update_disk_metrics_normal(monkeypatch):
    class FakeUsage:
        used = 100
        total = 1000  # 10% → normal

    monkeypatch.setattr(janitor.shutil, "disk_usage", lambda p: FakeUsage())
    janitor._update_disk_metrics(Path("/tmp"))
    assert janitor._fs_pressure_mode == 0


class _RmtreeFsStore:
    """fs_store whose delete_part really removes the part dir on disk."""

    def __init__(self, root: Path) -> None:
        import shutil

        self.root = root
        self.deleted: list[tuple[str, int, int]] = []
        self._rmtree = shutil.rmtree

    async def delete_part(self, oid, ov, pn) -> None:
        self.deleted.append((oid, int(ov), int(pn)))
        p = self.root / oid / f"v{ov}" / f"part_{pn}"
        if p.exists():
            self._rmtree(p)


def _make_old_parts(tmp_path: Path, n: int) -> None:
    """Create `n` old (well past stale/GC cutoff) single-part objects on disk."""
    import os
    import time

    old = time.time() - 200000
    for i in range(n):
        oid = f"{i:08d}-0000-0000-0000-000000000000"
        part = tmp_path / oid / "v1" / "part_1"
        part.mkdir(parents=True)
        (part / "chunk_0.bin").write_bytes(b"x")
        meta = part / "meta.json"
        meta.write_text("{}")
        os.utime(part / "chunk_0.bin", (old, old))
        os.utime(meta, (old, old))


# ----------------------------------------------- Phase 1 & 2 fan out across the pool


def _gated_repl(expected_width: int):
    """Return (fake_repl, peak_reached, release) for deterministic fan-out tests.

    Each call increments a live counter and parks on `release`; once
    `expected_width` calls are simultaneously parked, `peak_reached` is set. The
    test waits on `peak_reached`, asserts the pool's peak concurrency, then sets
    `release` to let everything finish — fully deterministic, no timing.
    """
    peak_reached = asyncio.Event()
    release = asyncio.Event()
    live = 0

    async def fake_repl(_conn, _oid, _ov, _pn) -> bool:
        nonlocal live
        live += 1
        if live >= expected_width:
            peak_reached.set()
        await release.wait()
        return True

    return fake_repl, peak_reached, release


@pytest.mark.asyncio
async def test_cleanup_stale_parts_fans_out_across_pool(tmp_path: Path):
    """Phase 1 end-to-end: stale replicated parts are processed concurrently
    (exactly `janitor_concurrency` at peak) and all are reclaimed."""
    janitor.config.mpu_stale_seconds = 86400
    janitor.config.upload_backends = ["arion"]
    janitor.config.backup_backends = []
    janitor.config.janitor_concurrency = 8

    n = 30
    width = janitor.config.janitor_concurrency
    _make_old_parts(tmp_path, n)
    fs_store = _RmtreeFsStore(tmp_path)

    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value={"recent": False})  # exists, not recent
    pool = _TrackingPool(conn=conn)

    redis_mock = MagicMock()
    redis_mock.lrange = AsyncMock(return_value=[])

    fake_repl, peak_reached, release = _gated_repl(width)
    with patch.object(janitor, "is_replicated_on_all_backends", AsyncMock(side_effect=fake_repl)):
        task = asyncio.create_task(janitor.cleanup_stale_parts(pool, fs_store, redis_mock))
        await asyncio.wait_for(peak_reached.wait(), timeout=5)
        assert pool.max_in_use == width, "exactly janitor_concurrency handles should run at peak"
        release.set()
        cleaned = await task

    assert cleaned == n
    assert len(fs_store.deleted) == n
    assert pool.total_acquired == n  # one acquire per processed candidate


@pytest.mark.asyncio
async def test_cleanup_old_parts_fans_out_and_accounts_stats(tmp_path: Path, monkeypatch):
    """Phase 2 end-to-end: cold replicated parts are GC'd concurrently AND the
    producer's stats accounting (parts_seen) survives the fan-out."""
    janitor.config.fs_cache_hot_retention_seconds = 10800
    janitor.config.fs_cache_gc_max_age_seconds = 60
    janitor.config.upload_backends = ["arion"]
    janitor.config.backup_backends = []
    janitor.config.janitor_concurrency = 8
    monkeypatch.setattr(janitor, "_pressure_mode", lambda root: 0)  # pin: runner disk may be >85%

    n = 25
    width = janitor.config.janitor_concurrency
    _make_old_parts(tmp_path, n)  # mtime/atime 200000s ago → cold + old
    fs_store = _RmtreeFsStore(tmp_path)

    pool = _TrackingPool(conn=AsyncMock())
    redis_mock = MagicMock()
    redis_mock.lrange = AsyncMock(return_value=[])

    fake_repl, peak_reached, release = _gated_repl(width)
    with patch.object(janitor, "is_replicated_on_all_backends", AsyncMock(side_effect=fake_repl)):
        task = asyncio.create_task(janitor.cleanup_old_parts_by_mtime(pool, fs_store, redis_mock))
        await asyncio.wait_for(peak_reached.wait(), timeout=5)
        assert pool.max_in_use == width
        release.set()
        cleaned = await task

    assert cleaned == n
    assert len(fs_store.deleted) == n
    assert janitor._fs_parts_on_disk == n, "producer stats must count every part under concurrency"


# --------------------------------------- main loop refreshes disk metrics up front


@pytest.mark.asyncio
async def test_main_loop_refreshes_disk_metrics_before_phases(monkeypatch):
    """The whole point of the PR: `_update_disk_metrics` runs at the TOP of each
    cycle, before the (potentially multi-hour) GC phases, so disk visibility
    never waits on the walk. Guards against a refactor silently dropping it."""
    order: list[str] = []

    async def _phase_stub(name):
        order.append(name)
        return 0

    monkeypatch.setattr(janitor, "_update_disk_metrics", lambda root: order.append("disk_metrics"))
    monkeypatch.setattr(janitor, "cleanup_stale_parts", lambda *a, **k: _phase_stub("phase1"))
    monkeypatch.setattr(janitor, "cleanup_old_parts_by_mtime", lambda *a, **k: _phase_stub("phase2"))
    monkeypatch.setattr(janitor, "cleanup_orphan_tmp_files", lambda *a, **k: _phase_stub("phase3"))
    monkeypatch.setattr(janitor, "gc_soft_deleted_objects", lambda *a, **k: _phase_stub("phase4"))
    monkeypatch.setattr(janitor, "_setup_janitor_metrics", lambda: None)
    monkeypatch.setattr(janitor, "create_fs_store", lambda config: MagicMock(root=Path("/tmp")))
    monkeypatch.setattr(janitor, "_pressure_mode", lambda root: 0)

    fake_pool = AsyncMock()
    monkeypatch.setattr(janitor.asyncpg, "create_pool", AsyncMock(return_value=fake_pool))
    monkeypatch.setattr(janitor.Redis, "from_url", lambda url: AsyncMock())

    # Break the infinite loop after the first cycle via the cycle-end sleep.
    class _StopLoop(Exception):
        pass

    async def _sleep_then_stop(_seconds):
        raise _StopLoop

    monkeypatch.setattr(janitor.asyncio, "sleep", _sleep_then_stop)

    with pytest.raises(_StopLoop):
        await janitor.run_janitor_loop()

    assert order[0] == "disk_metrics", f"disk metrics must refresh before any phase; got {order}"
    assert order[:3] == ["disk_metrics", "phase1", "phase2"]
