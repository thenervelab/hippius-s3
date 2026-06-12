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
    """Pool whose acquire() context tracks peak concurrency precisely."""

    def __init__(self) -> None:
        self.in_use = 0
        self.max_in_use = 0
        self.total_acquired = 0

    def acquire(self):
        pool = self

        class _Ctx:
            async def __aenter__(self_):
                pool.in_use += 1
                pool.total_acquired += 1
                pool.max_in_use = max(pool.max_in_use, pool.in_use)
                return MagicMock()

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
async def test_worker_pool_propagates_handle_exception():
    """A raising handle is surfaced (fail-loud): the GC phase aborts and the main
    loop logs + retries next cycle. Production handlers never raise — they catch
    their own DB errors and return False — so this only fires on genuine infra
    failures (e.g. a dead pool), where aborting the cycle is the right call."""
    pool = _TrackingPool()

    async def handle(_conn, item) -> bool:
        if item == 3:
            raise RuntimeError("infra down")
        return True

    with pytest.raises(RuntimeError, match="infra down"):
        await janitor._run_worker_pool(pool, _agen(range(20)), handle, concurrency=4)


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


# ----------------------------------------------- Phase 1 fans out across the pool


@pytest.mark.asyncio
async def test_cleanup_stale_parts_fans_out_across_pool(tmp_path: Path, monkeypatch):
    """End-to-end: many stale parts are processed concurrently, each on its own
    acquired connection, and the replicated ones are reclaimed."""
    import shutil
    import time

    janitor.config.mpu_stale_seconds = 86400
    janitor.config.upload_backends = ["arion"]
    janitor.config.backup_backends = []
    janitor.config.janitor_concurrency = 8

    class _FsStore:
        def __init__(self, root: Path) -> None:
            self.root = root
            self.deleted: list[tuple[str, int, int]] = []

        async def delete_part(self, oid, ov, pn) -> None:
            self.deleted.append((oid, int(ov), int(pn)))
            p = self.root / oid / f"v{ov}" / f"part_{pn}"
            if p.exists():
                shutil.rmtree(p)

    # 30 old parts across distinct objects.
    n = 30
    oids = [f"{i:08d}-0000-0000-0000-000000000000" for i in range(n)]
    old = time.time() - 200000
    for oid in oids:
        part = tmp_path / oid / "v1" / "part_1"
        part.mkdir(parents=True)
        (part / "chunk_0.bin").write_bytes(b"x")
        meta = part / "meta.json"
        meta.write_text("{}")
        import os

        os.utime(part / "chunk_0.bin", (old, old))
        os.utime(meta, (old, old))

    fs_store = _FsStore(tmp_path)

    pool = _TrackingPool()
    redis_mock = MagicMock()
    redis_mock.lrange = AsyncMock(return_value=[])

    # All have an old (non-recent) parts row and are fully replicated. The tiny
    # sleep forces handles to overlap so we can observe genuine fan-out.
    async def fake_repl(_conn, _oid, _ov, _pn) -> bool:
        await asyncio.sleep(0.01)
        return True

    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value={"recent": False})

    # Make the tracking pool hand out the configured conn.
    def acquire():
        class _Ctx:
            async def __aenter__(self_):
                pool.in_use += 1
                pool.total_acquired += 1
                pool.max_in_use = max(pool.max_in_use, pool.in_use)
                return conn

            async def __aexit__(self_, *exc):
                pool.in_use -= 1
                return False

        return _Ctx()

    pool.acquire = acquire  # type: ignore[method-assign]

    with patch.object(janitor, "is_replicated_on_all_backends", AsyncMock(side_effect=fake_repl)):
        cleaned = await janitor.cleanup_stale_parts(pool, fs_store, redis_mock)

    assert cleaned == n
    assert len(fs_store.deleted) == n
    assert pool.total_acquired == n  # one acquire per processed candidate
    assert pool.max_in_use > 1, "work should overlap, not run serially"
    assert pool.max_in_use <= janitor.config.janitor_concurrency
