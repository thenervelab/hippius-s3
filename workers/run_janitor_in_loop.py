#!/usr/bin/env python3
"""Janitor task: clean up stale / aged parts from the shared FS cache.

Runs periodically and:
- Deletes stale MPU parts (aborted uploads) older than `mpu_stale_seconds`.
- GC aged parts that have been replicated to every required backend
  (upload + backup). Replication is an ABSOLUTE gate: a chunk that hasn't
  been backed up to every required backend is NEVER deleted, under any
  conditions.
- Keeps "hot" parts (atime within `fs_cache_hot_retention_seconds`) —
  these are recently read and worth keeping on NVMe.
- Cleans orphan `.tmp.*` files (from worker crashes during atomic write).
- Hard-deletes soft-deleted objects whose backends have confirmed unpin.

Disk-pressure modes (all still replication-gated):
- Normal   (<85%):  honor hot retention; evict only replicated + aged + cold.
- Elevated (85-95%): halve hot retention; evict replicated + cold regardless of age.
- Critical (>=95%): disable hot retention; evict replicated + cold regardless of age.
  If no replicated parts exist, the janitor logs an ERROR and does nothing.
  It will never delete non-replicated data to free space. Operator paging
  is the answer, not data loss.
"""

import asyncio
import contextlib
import json
import logging
import os
import shutil
import sys
import time
import uuid
import zlib
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

import asyncpg
import httpx
from opentelemetry import metrics as otel_metrics
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from redis.asyncio import Redis


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.cache import FileSystemPartsStore
from hippius_s3.cache import create_fs_store
from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.otel_setup import build_resource
from hippius_s3.pressure_signal import PRESSURE_CRITICAL
from hippius_s3.pressure_signal import PRESSURE_CRITICAL_EXIT
from hippius_s3.pressure_signal import PRESSURE_ELEVATED
from hippius_s3.pressure_signal import PRESSURE_ELEVATED_EXIT
from hippius_s3.pressure_signal import PressurePublisher
from hippius_s3.pressure_signal import parse_pool_percent_used
from hippius_s3.queue_metrics import QueueDepthSampler
from hippius_s3.repositories import fs_cache_inventory
from hippius_s3.repositories.fs_cache_inventory import clear_cached
from hippius_s3.repositories.fs_cache_inventory import get_janitor_state
from hippius_s3.repositories.fs_cache_inventory import set_janitor_state
from hippius_s3.sentry import init_sentry
from hippius_s3.utils import get_query
from hippius_s3.workers.shutdown import run_worker


config = get_config()
setup_loki_logging(config, "janitor", include_ray_id=False)
logger = logging.getLogger(__name__)
init_sentry("janitor", is_worker=True)

# --- Janitor-owned OTel metrics ---

AGE_BUCKET_BOUNDARIES = [
    ("0-1h", 3600),
    ("1-6h", 21600),
    ("6-24h", 86400),
    ("1-3d", 259200),
    ("3-7d", 604800),
]
AGE_BUCKET_NAMES = [b[0] for b in AGE_BUCKET_BOUNDARIES] + ["7d+"]

# Disk pressure thresholds live in hippius_s3.pressure_signal (imported below):
# the janitor's cycle gating and the published fs_cache:pressure signal must
# key off the SAME watermarks or consumers would disagree with the evictor.

# Maximum age of an orphan `.tmp.*` file before we delete it. Atomic writes
# finish in milliseconds; anything older than this is a crashed-write orphan.
# 30min: atomic writes complete in ms, so half an hour is already many orders of
# magnitude of slack — no reason to sit on crashed-write bytes for a full hour.
TMP_FILE_MAX_AGE_SECONDS = 1800  # 30m
# Cap on the G2 sentinel scan: it needs only to DETECT a durability gap and sample a few
# offenders, not enumerate every one, so a bounded page keeps the read-only query cheap.
SENTINEL_SCAN_LIMIT = 500
# How many kept-part inventory rows the unified walk buffers before flushing one
# `record_cached_batch`. A module constant (not a config knob) so tests can shrink it to exercise
# the flush boundary without a live 500-part tree; there is no operational reason to tune it.
INVENTORY_BACKFILL_BATCH_SIZE = 500
# The idle grace before a pending/draining orphan counts toward the aged-orphan gauge is
# `config.mpu_sweep_grace_seconds` — the SAME window the reaper's orphan sweep
# (list_orphan_replication_versions.sql) uses. They MUST match: the gauge is only meaningful
# if it counts exactly the population the sweep can clear, otherwise it reads non-zero forever
# (an orphan aged past the gauge grace but not yet past a larger sweep grace) and the soak
# gate's slope≈0/bounded assertion watches a phantom backlog.

_fs_parts_on_disk = 0
_fs_oldest_age_seconds = 0.0
_fs_disk_used_bytes = 0
_fs_disk_total_bytes = 0
_fs_hot_parts = 0
_fs_pressure_mode = 0  # 0 = normal, 1 = elevated, 2 = critical
_prev_pressure_mode = 0  # C2: last mode returned by _pressure_mode, for hysteresis (single-instance janitor)
# Fullest configured Ceph pool's %USED (0.0-1.0), refreshed once per cycle by _update_disk_metrics
# from the mgr exporter. None when pool gating is unconfigured or the probe failed this cycle — in
# which case _pressure_mode falls back to statvfs alone. See _fetch_pool_percent_used.
_fs_pool_percent_used: float | None = None
_fs_age_buckets: dict[str, int] = dict.fromkeys(AGE_BUCKET_NAMES, 0)
# Census is now accumulated across a full sharded sweep (the age-GC walk covers 1/shards of
# the tree per cycle), then published to the gauges above only when a sweep completes without
# a budget truncation — so `_fs_parts_on_disk` etc. reflect the whole cache, never one shard
# or a half-finished pass. Between sweeps the gauges hold the last complete sweep's values.
_census_accum: dict[str, Any] = {
    "parts_seen": 0,
    "hot_parts": 0,
    "oldest_mtime": None,
    "age_counts": dict.fromkeys(AGE_BUCKET_NAMES, 0),
}
_census_accum_complete = True
# Which hash-shard the FS walk covers this cycle. Advances by one every cycle; a full sweep
# of the tree completes every `janitor_walk_shards` cycles. See config.janitor_walk_shards.
_walk_shard = 0
# G2 replication-gate sentinel: live/serveable chunks lacking full-union backend coverage
# (the population the gate must never reclaim). Any nonzero value is a standing durability
# alarm; sampled/capped by SENTINEL_SCAN_LIMIT, so a value at the cap means ">=".
_replication_sentinel_violations = 0
# Aged pending/draining orphan count (A21 leak backlog): the soak-gate feed the replicated-
# only gate is blind to. A standing or rising value means orphans are accumulating faster
# than the sweep clears them — a re-introduced leak.
_aged_pending_orphans = 0

# Cycle progress. The census gauges below (parts_on_disk, age buckets, hot parts) are only
# written at the END of the GC phase, so if an earlier phase runs long they report 0 forever
# and read as "cache is empty" rather than "never measured". Prod 2026-07-23: cleanup_stale_parts
# ran >1h48m without returning, so every one of those gauges sat at 0 while the janitor was
# deleting 18,737 parts. These two make that state visible instead of silent.
_janitor_phase = 0  # index into JANITOR_PHASES; 0 = idle/sleeping
_janitor_last_cycle_completed_at = 0.0
_janitor_cycle_seconds = 0.0

JANITOR_PHASES = (
    "idle",
    "parts_unified",
    "soft_deleted",
    "sentinel",
    "aged_orphans",
    "sql_evict",
)

_janitor_deleted_counter = None  # set by _setup_janitor_metrics
_janitor_tmp_deleted_counter = None
_janitor_abandoned_deleted_counter = None


def _obs_parts_on_disk(_: object) -> list[otel_metrics.Observation]:
    return [otel_metrics.Observation(_fs_parts_on_disk, {})]


def _obs_oldest_age(_: object) -> list[otel_metrics.Observation]:
    return [otel_metrics.Observation(_fs_oldest_age_seconds, {})]


def _obs_disk_used(_: object) -> list[otel_metrics.Observation]:
    return [otel_metrics.Observation(_fs_disk_used_bytes, {})]


def _obs_disk_total(_: object) -> list[otel_metrics.Observation]:
    return [otel_metrics.Observation(_fs_disk_total_bytes, {})]


def _obs_hot_parts(_: object) -> list[otel_metrics.Observation]:
    return [otel_metrics.Observation(_fs_hot_parts, {})]


def _obs_pressure_mode(_: object) -> list[otel_metrics.Observation]:
    return [otel_metrics.Observation(_fs_pressure_mode, {})]


def _obs_pool_percent_used(_: object) -> list[otel_metrics.Observation]:
    # -1 = pool gating unconfigured or the probe failed this cycle (falling back to statvfs).
    value = _fs_pool_percent_used if _fs_pool_percent_used is not None else -1.0
    return [otel_metrics.Observation(value, {})]


def _obs_age_buckets(_: object) -> list[otel_metrics.Observation]:
    return [otel_metrics.Observation(count, {"age_bucket": bucket}) for bucket, count in _fs_age_buckets.items()]


def _obs_replication_sentinel(_: object) -> list[otel_metrics.Observation]:
    return [otel_metrics.Observation(_replication_sentinel_violations, {})]


def _obs_aged_pending_orphans(_: object) -> list[otel_metrics.Observation]:
    return [otel_metrics.Observation(_aged_pending_orphans, {})]


def _obs_janitor_phase(_: object) -> list[otel_metrics.Observation]:
    return [otel_metrics.Observation(_janitor_phase, {"phase": JANITOR_PHASES[_janitor_phase]})]


def _obs_cycle_age(_: object) -> list[otel_metrics.Observation]:
    # Age of the last COMPLETED cycle. Rises without bound while a phase is stuck, which is
    # the signal that the census gauges are stale rather than genuinely zero.
    if _janitor_last_cycle_completed_at <= 0:
        return [otel_metrics.Observation(-1.0, {})]
    return [otel_metrics.Observation(max(0.0, time.time() - _janitor_last_cycle_completed_at), {})]


def _obs_cycle_seconds(_: object) -> list[otel_metrics.Observation]:
    return [otel_metrics.Observation(_janitor_cycle_seconds, {})]


def _classify_age_bucket(age_seconds: float) -> str:
    for name, upper in AGE_BUCKET_BOUNDARIES:
        if age_seconds < upper:
            return name
    return "7d+"


async def _fetch_pool_percent_used() -> float | None:
    """The fullest configured Ceph pool's %USED (0.0-1.0) from the mgr exporter, or None.

    statvfs on the cache mount (what _pressure_mode reads locally) sees the CephFS *PVC quota*,
    not the backing pool: on 2026-07-24 statvfs read 0.69 while ceph-filesystem-data0 sat at 0.94,
    so the janitor never left Normal mode as the pool filled to the read-only cliff. This reads the
    same `ceph_pool_percent_used` signal PR #337 gave the drain allocator.

    Returns None — the caller then falls back to statvfs alone — when pool gating is unconfigured,
    the mgr is unreachable, or any configured pool is absent (a missing pool must not silently
    shrink the gate). The pool signal only ever RAISES pressure via the max in _pressure_mode; on
    failure the janitor is no worse off than its pre-incident statvfs-only behavior, but it logs.
    """
    url = config.janitor_ceph_mgr_metrics_url
    pools = [p.strip() for p in config.janitor_ceph_pools.split(",") if p.strip()]
    if not url or not pools:
        return None
    try:
        async with httpx.AsyncClient(timeout=config.janitor_ceph_probe_timeout_seconds) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            body = resp.text
    except httpx.HTTPError as exc:
        logger.warning("janitor pool-fullness probe failed (%s); falling back to statvfs", exc)
        return None
    return parse_pool_percent_used(body, pools)


def _pressure_mode(root: Path) -> int:
    """Return the current disk-pressure mode (0/1/2) with hysteresis.

    C2: a mode is entered at its (higher) enter threshold and only released once the disk drops
    below the (lower) exit threshold, using the previous mode. This stops a disk sitting right at
    0.85 or 0.95 from oscillating the mode — and the hot-retention window and loop sleep that key
    off it — on every cycle. The janitor is single-instance, so a module-global previous-mode is
    safe. On a stat error we hold the previous mode rather than snapping to normal.

    The ratio is the MAX of the local statvfs used-fraction and the backing Ceph pool's %USED
    (`_fs_pool_percent_used`, refreshed once per cycle). statvfs alone sees the CephFS PVC quota,
    not the pool, so it under-reports fullness for a pool whose CRUSH subtree fills ahead of the
    quota — the 2026-07-24 incident. `None` (pool gating off / probe failed) uses statvfs alone.
    """
    global _prev_pressure_mode
    try:
        usage = shutil.disk_usage(root)
        local_ratio = usage.used / usage.total if usage.total else 0.0
    except OSError:
        return _prev_pressure_mode
    pool_ratio = _fs_pool_percent_used if _fs_pool_percent_used is not None else 0.0
    ratio = max(local_ratio, pool_ratio)
    prev = _prev_pressure_mode
    if ratio >= PRESSURE_CRITICAL or (prev == 2 and ratio >= PRESSURE_CRITICAL_EXIT):
        mode = 2
    elif ratio >= PRESSURE_ELEVATED or (prev >= 1 and ratio >= PRESSURE_ELEVATED_EXIT):
        mode = 1
    else:
        mode = 0
    _prev_pressure_mode = mode
    return mode


def _effective_hot_retention(mode: int) -> float:
    """Effective hot-retention window (seconds) given pressure mode."""
    base = float(getattr(config, "fs_cache_hot_retention_seconds", 10800))
    if mode == 1:
        return base / 2
    if mode == 2:
        return 0.0  # disable hot retention under critical pressure
    return base


async def _clear_inventory_after_delete(
    conn: asyncpg.Connection, object_id: str, object_version: int, part_number: int
) -> None:
    """Drop a just-evicted part from `fs_cache_inventory`, swallowing any clear failure.

    `clear_cached` RAISES by design, but its ONE janitor caller must not let a clear failure that
    lands AFTER a successful `delete_part` flip the delete's result to False: the part is already
    gone from disk, so re-counting it as "not deleted" is the wrong outcome. A stale inventory row
    is self-healing — the walk sweep re-walks kept parts, and the SQL-eviction re-check tolerates a
    part that is already absent — so a swallowed clear costs at most a delayed row cleanup, never a
    resurrected candidate for data that still exists.
    """
    try:
        await fs_cache_inventory.clear_cached(conn, object_id, object_version, part_number)
    except Exception as exc:
        logger.warning(
            "fs_cache_inventory clear failed after eviction (row self-heals via next walk sweep): %s v%s p%s: %s",
            object_id,
            object_version,
            part_number,
            exc,
        )


def _is_uuid_name(name: str) -> bool:
    """True iff a walked cache dirname parses as a UUID.

    The walk yields raw directory names, but Task 4.1's eviction query casts
    `fs_cache_inventory.object_id::uuid` and a single non-UUID row would abort the whole candidate
    page. So only UUID-shaped dirnames may be backfilled into the inventory.
    """
    try:
        uuid.UUID(name)
    except (ValueError, AttributeError, TypeError):
        return False
    return True


def _safe_iterdir(path: Path) -> Iterator[Path]:
    """Lazily list a directory, tolerating concurrent removal.

    The cleanup workers delete (and prune empty parent) directories while the
    producer is still walking the tree, so a vanished dir/entry is expected.
    Stays lazy — the cache root can hold millions of object dirs, so we must not
    materialize it — and swallows the OSError rather than aborting the walk.
    """
    try:
        it = iter(path.iterdir())
    except OSError:
        return
    while True:
        try:
            entry = next(it)
        except StopIteration:
            return
        except OSError:
            return
        yield entry


@dataclass(frozen=True)
class PartDirInfo:
    """One part directory the walk found, with the stat the phases gate on.

    `mtime`/`atime` are read from `meta.json` when present, else the part dir — the same
    "part complete signal or fall back to the dir" rule the serial walk used.
    """

    object_id: str
    object_version: int
    part_number: int
    mtime: float
    atime: float


@dataclass
class WalkState:
    """Out-of-band result of a walk: whether the wall-clock budget truncated it, how
    many object dirs (in this shard) it reached, and how many orphan `.tmp.*` files it
    unlinked (only when the walk was asked to clean tmp — see `iter_part_dirs`'s
    `tmp_cutoff`). The census is only trustworthy for a full (untruncated) sweep, so
    callers check `truncated` before publishing gauges."""

    truncated: bool = False
    objects_scanned: int = 0
    tmp_removed: int = 0


def _object_in_shard(object_id: str, shard: int, shards: int) -> bool:
    # crc32 is deterministic per name, so a given object falls in the same shard every sweep —
    # rotating `shard` per cycle covers the whole tree over `shards` cycles without a cursor.
    if shards <= 1:
        return True
    return zlib.crc32(object_id.encode("utf-8", "surrogatepass")) % shards == shard % shards


def _read_dir_batch(scan_it: Any, n: int) -> tuple[list[str], bool]:
    """Pull up to `n` sub-directory names from an os.scandir iterator. Returns
    (names, exhausted). Runs in a worker thread so a slow CephFS readdir never blocks the loop.
    A vanished/odd entry is skipped, not fatal (the tree mutates under the walk)."""
    names: list[str] = []
    for _ in range(n):
        try:
            entry = next(scan_it)
        except StopIteration:
            return names, True
        except OSError:
            return names, True
        try:
            if entry.is_dir():
                names.append(entry.name)
        except OSError:
            continue
    return names, False


def _unlink_old_tmp_files(part_path: str, cutoff: float) -> int:
    """Unlink `*.tmp.*` files older than `cutoff` directly under one part dir. Returns how many
    were removed. Every FS error is a skip — the tree mutates underneath us. This is the
    per-part-dir tmp-reap the standalone tmp walk used, extracted so the unified walk can apply
    it in the SAME descent that gathers part stats (one visit per part dir, not two)."""
    removed = 0
    try:
        file_scan = os.scandir(part_path)  # noqa: PTH208
    except OSError:
        return 0
    with file_scan:
        for f in file_scan:
            if ".tmp." not in f.name:
                continue
            try:
                if not f.is_file():
                    continue
                if f.stat().st_mtime > cutoff:
                    continue
                os.unlink(f.path)  # noqa: PTH108
                removed += 1
            except OSError:
                continue
    return removed


def _descend_object(root_str: str, object_name: str, tmp_cutoff: float | None = None) -> tuple[list[PartDirInfo], int]:
    """Blocking descent of ONE object dir → its part dirs, run in a walk thread.

    Mirrors the serial walk exactly: `v<n>` version dirs, `part_<n>` part dirs, stat
    `meta.json` if present else the part dir. Every FS error is swallowed to a skip — the
    tree is mutating underneath us. Returns `(parts, tmp_removed)`: the parts found (possibly
    empty), and — when `tmp_cutoff` is not None — the count of orphan `.tmp.*` files older than
    the cutoff unlinked from those part dirs (0 when `tmp_cutoff` is None; that keeps the legacy
    stat-only callers byte-for-byte unchanged)."""
    out: list[PartDirInfo] = []
    tmp_removed = 0
    obj_path = os.path.join(root_str, object_name)  # noqa: PTH118 — hot walk path, os.* avoids per-entry Path alloc
    try:
        version_scan = os.scandir(obj_path)  # noqa: PTH208
    except OSError:
        return out, tmp_removed
    with version_scan:
        for vd in version_scan:
            name = vd.name
            if not name.startswith("v"):
                continue
            try:
                if not vd.is_dir():
                    continue
            except OSError:
                continue
            try:
                object_version = int(name[1:])
            except ValueError:
                continue
            try:
                part_scan = os.scandir(vd.path)  # noqa: PTH208
            except OSError:
                continue
            with part_scan:
                for pd in part_scan:
                    pname = pd.name
                    if not pname.startswith("part_"):
                        continue
                    try:
                        if not pd.is_dir():
                            continue
                    except OSError:
                        continue
                    # tmp reap runs on every valid part dir, BEFORE the part-number parse guard,
                    # so it covers the same part dirs the standalone tmp walk did.
                    if tmp_cutoff is not None:
                        tmp_removed += _unlink_old_tmp_files(pd.path, tmp_cutoff)
                    try:
                        part_number = int(pname.split("_")[1])
                    except (ValueError, IndexError):
                        continue
                    meta = os.path.join(pd.path, "meta.json")  # noqa: PTH118
                    try:
                        st = os.stat(meta)  # noqa: PTH116
                    except OSError:
                        try:
                            st = os.stat(pd.path)  # noqa: PTH116
                        except OSError:
                            continue
                    out.append(PartDirInfo(object_name, object_version, part_number, st.st_mtime, st.st_atime))
    return out, tmp_removed


async def _stream_shard_object_names(
    root_str: str,
    shard: int,
    shards: int,
    deadline: float | None,
    state: WalkState,
) -> AsyncIterator[str]:
    """Stream this shard's object-dir names off the event loop, stopping at the budget deadline."""
    loop = asyncio.get_running_loop()
    try:
        scan_it = await asyncio.to_thread(os.scandir, root_str)
    except OSError:
        return
    try:
        while True:
            if deadline is not None and loop.time() >= deadline:
                state.truncated = True
                return
            names, exhausted = await asyncio.to_thread(_read_dir_batch, scan_it, 512)
            for name in names:
                if _object_in_shard(name, shard, shards):
                    state.objects_scanned += 1
                    yield name
            if exhausted:
                return
    finally:
        await asyncio.to_thread(scan_it.close)


async def iter_part_dirs(
    root: Path,
    *,
    concurrency: int,
    shard: int,
    shards: int,
    deadline: float | None,
    state: WalkState,
    tmp_cutoff: float | None = None,
) -> AsyncIterator[PartDirInfo]:
    """Walk `root` and yield every part dir in the current shard, descending object dirs
    across a bounded thread pool so many CephFS metadata roundtrips are in flight at once.

    This replaces the single-threaded, event-loop-blocking `_safe_iterdir` walk that could
    not complete a pass over ~2.8M object dirs inside a cycle (measured ~40 objects/s serial
    on prod). Callers apply their own per-part gating + census to the yielded records; the
    deletion logic downstream is unchanged.

    When `tmp_cutoff` is not None the SAME descent also unlinks orphan `.tmp.*` files older
    than the cutoff from every part dir it visits, accumulating the count into
    `state.tmp_removed` — so the unified walk folds the old standalone tmp sweep into this one
    pass instead of a third full-tree crawl. `None` leaves the walk stat-only (legacy callers).

    `concurrency<=1` degrades to a serial descent (legacy behaviour, one object at a time).
    """
    concurrency = max(1, concurrency)
    root_str = str(root)
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="janitor-walk")
    inflight: set[asyncio.Future[tuple[list[PartDirInfo], int]]] = set()
    try:
        async for name in _stream_shard_object_names(root_str, shard, shards, deadline, state):
            inflight.add(loop.run_in_executor(executor, _descend_object, root_str, name, tmp_cutoff))
            if len(inflight) >= concurrency:
                done, inflight = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
                for fut in done:
                    parts, tmp_removed = fut.result()
                    state.tmp_removed += tmp_removed
                    for info in parts:
                        yield info
        while inflight:
            done, inflight = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
            for fut in done:
                parts, tmp_removed = fut.result()
                state.tmp_removed += tmp_removed
                for info in parts:
                    yield info
    finally:
        for fut in inflight:
            fut.cancel()
        executor.shutdown(wait=False)


def _walk_deadline(loop: asyncio.AbstractEventLoop, pressure: int, budget: int) -> float | None:
    """The wall-clock deadline for one FS-walk phase. None (unbounded) under CRITICAL pressure
    or when the budget is disabled — freeing space must never be capped by a clock."""
    if pressure >= 2 or budget <= 0:
        return None
    return loop.time() + budget


def _shards_for_pressure(pressure: int) -> int:
    """How many hash-shards the walk rotates through at this pressure level. CRITICAL collapses
    to a single whole-tree walk (paired with the unbounded budget above). ELEVATED keeps rotating
    a small shard count — collapsing to 1 there made every budget-truncated cycle re-walk the same
    readdir head and starve the tail of the tree. NORMAL keeps the full fair sweep."""
    if pressure >= 2:
        return 1
    if pressure == 1:
        return max(1, config.janitor_elevated_walk_shards)
    return max(1, config.janitor_walk_shards)


def _reset_census_accum() -> None:
    global _census_accum, _census_accum_complete
    _census_accum = {
        "parts_seen": 0,
        "hot_parts": 0,
        "oldest_mtime": None,
        "age_counts": dict.fromkeys(AGE_BUCKET_NAMES, 0),
    }
    _census_accum_complete = True


def _accumulate_census(stats: dict[str, Any], shard_complete: bool) -> None:
    """Fold one shard's census into the in-progress sweep. `shard_complete` is False if the
    shard's walk hit the budget, which taints the whole sweep's completeness."""
    global _census_accum_complete
    _census_accum["parts_seen"] += stats["parts_seen"]
    _census_accum["hot_parts"] += stats["hot_parts"]
    for bucket, count in stats["age_counts"].items():
        _census_accum["age_counts"][bucket] += count
    om = stats["oldest_mtime"]
    if om is not None and (_census_accum["oldest_mtime"] is None or om < _census_accum["oldest_mtime"]):
        _census_accum["oldest_mtime"] = om
    if not shard_complete:
        _census_accum_complete = False


def _publish_census(now: float) -> None:
    """Publish the completed sweep's census to the gauges. A sweep tainted by a budget
    truncation is dropped (gauges hold their last complete value) rather than reported as a
    full census that undercounts. The accumulator is reset at the NEXT sweep's shard 0, not
    here, so a same-cycle re-read stays consistent. The pressure gauge is owned by
    _update_disk_metrics (refreshed every cycle top), so it is not touched here."""
    global _fs_parts_on_disk, _fs_oldest_age_seconds, _fs_age_buckets, _fs_hot_parts
    if _census_accum_complete:
        oldest_mtime = _census_accum["oldest_mtime"]
        _fs_parts_on_disk = _census_accum["parts_seen"]
        _fs_oldest_age_seconds = max(0.0, now - float(oldest_mtime)) if oldest_mtime is not None else 0.0
        _fs_age_buckets = dict(_census_accum["age_counts"])
        _fs_hot_parts = _census_accum["hot_parts"]
    else:
        logger.info(
            "Census sweep truncated by walk budget — holding last complete census (partial parts_seen=%d)",
            _census_accum["parts_seen"],
        )


async def _update_disk_metrics(root: Path) -> None:
    """Refresh disk-usage + pressure gauges from a single statvfs.

    Called at the top of every cycle so disk visibility never depends on a full
    GC pass completing — the GC walk over millions of parts can take hours, and
    operators need to see a filling disk long before then.

    The pool-fullness probe is refreshed HERE (once per cycle) into a module global that
    _pressure_mode reads, so the many per-cycle _pressure_mode calls (each GC phase re-reads it)
    share one mgr scrape rather than hammering the exporter.
    """
    global _fs_disk_used_bytes, _fs_disk_total_bytes, _fs_pressure_mode, _fs_pool_percent_used
    _fs_pool_percent_used = await _fetch_pool_percent_used()
    usage = shutil.disk_usage(root)
    _fs_disk_used_bytes = usage.used
    _fs_disk_total_bytes = usage.total
    _fs_pressure_mode = _pressure_mode(root)


async def _run_worker_pool(
    pool: asyncpg.Pool,
    producer: AsyncIterator[Any],
    handle: Callable[[Any, Any], Awaitable[bool]],
    concurrency: int,
) -> int:
    """Drain an async `producer` of candidate items through `handle(conn, item)`
    with bounded concurrency over a connection pool.

    The producer walks the FS (cheap, stat-only) and yields candidates; N
    workers run the expensive per-part DB checks + deletes in parallel, each on
    its own pooled connection. This is what turns the serial, single-connection
    deletion loop (DB-roundtrip bound at ~25/s) into a parallel one.

    Returns the number of items for which `handle` returned True (i.e. deleted).

    Resilience: workers swallow per-item errors (a dead pool / DB blip on one
    part must not wedge the whole sweep), and the producer drain is wrapped so
    sentinels are always sent and every worker is always awaited — even if the
    producer itself raises (it walks the tree while workers delete from it, so
    a concurrent prune can surface FileNotFoundError mid-walk). This guarantees
    no orphaned tasks and no deadlock.
    """
    concurrency = max(1, concurrency)  # 0/neg would mean an unbounded queue + no workers (silent no-op)
    queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=concurrency * 4)
    cleaned = 0

    async def worker() -> None:
        nonlocal cleaned
        while True:
            item = await queue.get()
            try:
                if item is None:
                    return
                async with pool.acquire() as conn:
                    if await handle(conn, item):
                        cleaned += 1
            except Exception as e:
                # Never let one item kill a worker — that would strand the queue
                # and could deadlock the producer on a full queue.
                logger.warning(f"Janitor worker item failed: {e}")
            finally:
                queue.task_done()

    workers = [asyncio.create_task(worker()) for _ in range(concurrency)]
    try:
        async for item in producer:
            await queue.put(item)
    finally:
        for _ in workers:
            await queue.put(None)
        await asyncio.gather(*workers)
    return cleaned


def _setup_janitor_metrics() -> None:
    global _janitor_deleted_counter, _janitor_tmp_deleted_counter, _janitor_abandoned_deleted_counter

    if os.getenv("ENABLE_MONITORING", "false").lower() not in ("true", "1", "yes"):
        logger.info("Monitoring disabled for janitor")
        return

    # If auto-instrumentation already set a MeterProvider, use it.
    # Only create our own if none exists.
    existing = otel_metrics.get_meter_provider()
    if not isinstance(existing, MeterProvider):
        endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
        service_name = os.getenv("OTEL_SERVICE_NAME", "hippius-s3")

        resource = build_resource(service_name)
        metric_reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(endpoint=endpoint, insecure=True),
            export_interval_millis=10000,
        )
        provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
        otel_metrics.set_meter_provider(provider)

    meter = otel_metrics.get_meter("janitor")

    meter.create_observable_gauge(
        name="fs_store_parts_on_disk",
        callbacks=[_obs_parts_on_disk],
        description="Total part dirs on disk",
    )
    meter.create_observable_gauge(
        name="fs_store_oldest_age_seconds",
        callbacks=[_obs_oldest_age],
        description="Age of oldest part in seconds",
    )
    meter.create_observable_gauge(
        name="fs_cache_disk_used_bytes",
        callbacks=[_obs_disk_used],
        description="Bytes used on cache filesystem",
    )
    meter.create_observable_gauge(
        name="fs_cache_disk_total_bytes",
        callbacks=[_obs_disk_total],
        description="Total bytes on cache filesystem",
    )
    meter.create_observable_gauge(
        name="fs_cache_hot_parts",
        callbacks=[_obs_hot_parts],
        description="Parts retained because atime is within hot-retention window",
    )
    meter.create_observable_gauge(
        name="fs_cache_pressure_mode",
        callbacks=[_obs_pressure_mode],
        description="0=normal, 1=elevated, 2=critical",
    )
    meter.create_observable_gauge(
        name="fs_cache_pool_percent_used",
        callbacks=[_obs_pool_percent_used],
        description="Backing Ceph pool %USED (0.0-1.0) from the mgr exporter; -1 = pool gating off or probe failed (statvfs fallback). The real fullness signal statvfs misses.",
    )
    meter.create_observable_gauge(
        name="fs_cache_age_bucket_parts",
        callbacks=[_obs_age_buckets],
        description="Number of parts per age bucket",
    )
    meter.create_observable_gauge(
        name="janitor_underreplicated_live_chunks",
        callbacks=[_obs_replication_sentinel],
        description="Live serveable chunks lacking full-union backend coverage (G2 sentinel; nonzero = durability gap)",
    )
    meter.create_observable_gauge(
        name="janitor_aged_pending_orphans",
        callbacks=[_obs_aged_pending_orphans],
        description="Aged pending/draining unservable orphan versions (A21 leak backlog; the soak gate asserts bounded / slope ~ 0)",
    )
    meter.create_observable_gauge(
        name="fs_janitor_phase",
        callbacks=[_obs_janitor_phase],
        description="Janitor phase currently executing (0=idle); label carries the phase name",
    )
    meter.create_observable_gauge(
        name="fs_janitor_last_cycle_age_seconds",
        callbacks=[_obs_cycle_age],
        description="Seconds since the last COMPLETED janitor cycle (-1 = none since start). Rising without bound means a phase is stuck and the census gauges are stale, not zero",
    )
    meter.create_observable_gauge(
        name="fs_janitor_cycle_seconds",
        callbacks=[_obs_cycle_seconds],
        description="Duration of the last completed janitor cycle",
    )
    _janitor_deleted_counter = meter.create_counter(
        name="fs_janitor_deleted_total",
        description="FS parts deleted by the janitor, by reason (gc_age|stale_mtime|abandoned|sql_evict)",
        unit="1",
    )
    _janitor_tmp_deleted_counter = meter.create_counter(
        name="fs_janitor_tmp_deleted_total",
        description="Total number of orphan .tmp files deleted by the janitor",
        unit="1",
    )
    _janitor_abandoned_deleted_counter = meter.create_counter(
        name="fs_janitor_abandoned_reclaimed_total",
        description="CephFS-pool parts reclaimed as terminally-abandoned uploads (failed + unservable)",
        unit="1",
    )


class DLQProtectionUnavailable(Exception):
    """A15: a DLQ read failed, so the protection set is INCOMPLETE. Raised so the caller
    fails CLOSED — skipping the destructive reap rather than running it with partial (or empty)
    protection exactly when redis-queues is down and a DLQ'd object's data must NOT be reaped."""


async def get_all_dlq_object_ids(redis_client: Redis) -> set[str]:
    """Fetch all object_ids currently in both upload and unpin DLQs.

    Returns the set of protected object_ids. Raises [`DLQProtectionUnavailable`] if ANY DLQ
    list read fails — the janitor's reaps use this set to PROTECT in-flight DLQ objects from
    eviction, so an incomplete set must never be treated as "nothing to protect" (fail-closed).
    """
    object_ids = set()

    # Enumerate DLQ keys dynamically from the configured upload backends so
    # a new backend (e.g. ipfs) doesn't silently bypass protection.
    dlq_keys = [f"{b}_upload_requests:dlq" for b in config.upload_backends]
    dlq_keys.append("unpin_requests:dlq")

    for dlq_key in dlq_keys:
        try:
            dlq_entries = await asyncio.wait_for(redis_client.lrange(dlq_key, 0, -1), timeout=5.0)
        except asyncio.TimeoutError as exc:
            raise DLQProtectionUnavailable(f"{dlq_key} fetch timeout (5s)") from exc
        except Exception as exc:
            raise DLQProtectionUnavailable(f"failed to fetch {dlq_key}: {exc}") from exc
        for entry_json in dlq_entries:
            try:
                entry = json.loads(entry_json)
                if obj_id := entry.get("object_id"):
                    object_ids.add(str(obj_id))
            except json.JSONDecodeError:
                # A single malformed entry does not compromise the rest of the set — skip it.
                logger.warning(f"Invalid JSON in {dlq_key}: {entry_json[:100]}")

    if object_ids:
        logger.info(f"Found {len(object_ids)} unique object_ids protected across all DLQs")
    return object_ids


def _stale_fs_eligible(
    mtime: float,
    mtime_cutoff: float,
    object_id: str,
    dlq_available: bool,
    dlq_object_ids: set[str],
) -> bool:
    """FS-level stale-reap gate (byte-for-byte the stale phase's producer filter).

    A part is a stale-reap candidate iff the DLQ protection set is available (fail-CLOSED:
    with no complete DLQ set we never reap the non-replication-gated stale path), its
    meta/dir mtime is older than the stale threshold, and its object is not DLQ-protected.
    The DB 3-state decision (`_stale_reap_decision`) is only consulted for parts that pass here.
    """
    if not dlq_available:
        return False
    if mtime > mtime_cutoff:
        return False
    return object_id not in dlq_object_ids


async def _stale_reap_decision(
    conn: asyncpg.Connection,
    object_id: str,
    object_version: int,
    part_number: int,
    stale_threshold_seconds: int,
) -> tuple[bool, bool]:
    """The stale phase's DB-side 3-state decision, extracted VERBATIM so the unified walk and
    the standalone `cleanup_stale_parts` share one implementation.

    Returns `(should_delete, abandoned)`:
      row is None        → no `parts` row → orphan → reap  → (True, False).
      row["recent"] true → row exists, recently written    → keep → (False, False).
      row old + replicated → safe to reap (fully backed up)→ (True, False).
      row old + not replicated:
          terminally-abandoned (failed + unservable)       → reclaim → (True, True).
          otherwise (pending/in-flight/aborted)            → protect → (False, False).
    Any DB error is conservative: (False, False) — never delete on an incomplete read.
    """
    cutoff_sql = "NOW() - INTERVAL '1 second' * $4"
    abandoned = False
    try:
        row = await conn.fetchrow(
            """
            SELECT (uploaded_at > """
            + cutoff_sql
            + """) AS recent
            FROM parts
            WHERE object_id = $1 AND object_version = $2 AND part_number = $3
            LIMIT 1""",
            object_id,
            object_version,
            part_number,
            stale_threshold_seconds,
        )
        if row is not None:
            if row["recent"]:
                return (False, False)
            if not await is_replicated_on_all_backends(conn, object_id, object_version, part_number):
                # Not replicated → normally protect (pending / in-flight / aborted).
                # The ONE exception: a terminally-abandoned upload — the reaper or an
                # abort marked the part 'failed' AND its version is unservable. The
                # drain never re-claims a 'failed' part, so its pool bytes leak
                # forever otherwise; an unservable version can never be served by a
                # GET, so reclaiming is safe (see is_terminally_abandoned).
                if not await is_terminally_abandoned(conn, object_id, object_version, part_number):
                    return (False, False)
                abandoned = True
    except Exception:
        # If any DB check fails, be extra conservative: skip deletion
        return (False, False)
    return (True, abandoned)


async def cleanup_stale_parts(
    pool: asyncpg.Pool,
    fs_store: FileSystemPartsStore,
    redis_client: Redis,
    *,
    shard: int = 0,
    shards: int = 1,
    walk_concurrency: int = 1,
    deadline: float | None = None,
) -> int:
    """Conservative cleanup of stale parts: rely on FS mtime only for now.

    Rationale: DB schemas for tracking MPU progress vary across deployments.
    To avoid accidental deletion of active uploads, we prefer a conservative
    approach: remove only parts whose meta/dir mtime is older than the
    configured stale threshold and which have no recent DB part activity.

    The FS walk (parallel, stat-only) runs in the producer via `iter_part_dirs`; the
    per-part DB checks and deletes run with bounded concurrency over a connection pool
    (`_run_worker_pool`). The walk is sharded + budgeted so it cannot monopolise the cycle:
    it descends only this cycle's shard and stops at `deadline`. Deletion logic is unchanged.
    """
    stale_threshold_seconds = config.mpu_stale_seconds

    try:
        dlq_object_ids = await get_all_dlq_object_ids(redis_client)
    except DLQProtectionUnavailable as exc:
        # A15 fail-closed: without a complete DLQ protection set we cannot know which parts
        # belong to in-flight DLQ operations, so skip this reap entirely rather than delete
        # unprotected. Retry next cycle when redis-queues is back.
        logger.error(f"Skipping stale-parts cleanup — DLQ protection unavailable: {exc}")
        return 0
    if dlq_object_ids:
        logger.info(f"Protecting {len(dlq_object_ids)} DLQ objects from stale cleanup")

    root = fs_store.root
    if not root.exists():
        return 0

    mtime_cutoff = time.time() - stale_threshold_seconds

    walk_state = WalkState()

    async def candidates() -> AsyncIterator[tuple[str, int, int]]:
        async for part in iter_part_dirs(
            root,
            concurrency=walk_concurrency,
            shard=shard,
            shards=shards,
            deadline=deadline,
            state=walk_state,
        ):
            # dlq_available=True: we only reach here when get_all_dlq_object_ids succeeded
            # (the fail-closed early-return above handles the unavailable case).
            if not _stale_fs_eligible(part.mtime, mtime_cutoff, part.object_id, True, dlq_object_ids):
                continue
            yield (part.object_id, part.object_version, part.part_number)

    async def handle(conn: asyncpg.Connection, item: tuple[str, int, int]) -> bool:
        object_id, object_version, part_number = item

        # Decide, in one query, which of three states this part is in (see
        # _stale_reap_decision): no `parts` row → orphan → reap; recent row → keep;
        # old row → reap only if fully replicated, or terminally-abandoned when not.
        should_delete, abandoned = await _stale_reap_decision(
            conn, object_id, object_version, part_number, stale_threshold_seconds
        )
        if not should_delete:
            return False

        try:
            await fs_store.delete_part(object_id, object_version, part_number)
            await _clear_inventory_after_delete(conn, object_id, object_version, part_number)
            if abandoned:
                logger.info(
                    "Reclaimed terminally-abandoned part (failed+unservable): "
                    f"object_id={object_id} v={object_version} part={part_number}"
                )
                if _janitor_abandoned_deleted_counter is not None:
                    _janitor_abandoned_deleted_counter.add(1)
                if _janitor_deleted_counter is not None:
                    _janitor_deleted_counter.add(1, attributes={"reason": "abandoned"})
            else:
                logger.debug(
                    f"Cleaned stale part by mtime: object_id={object_id} v={object_version} part={part_number}"
                )
                if _janitor_deleted_counter is not None:
                    _janitor_deleted_counter.add(1, attributes={"reason": "stale_mtime"})
            return True
        except Exception as e:
            logger.warning(f"Failed to clean part: object_id={object_id} v={object_version} part={part_number}: {e}")
            return False

    parts_cleaned = await _run_worker_pool(pool, candidates(), handle, config.janitor_concurrency)
    logger.info(
        "Janitor cleaned %d stale parts by mtime threshold (shard=%d/%d objects_scanned=%d truncated=%s)",
        parts_cleaned,
        shard,
        shards,
        walk_state.objects_scanned,
        walk_state.truncated,
    )
    return parts_cleaned


async def is_replicated_on_all_backends(
    db: asyncpg.Connection,
    object_id: str,
    object_version: int,
    part_number: int,
) -> bool:
    """Check if all chunks for a given part are replicated on all expected backends.

    Uses the chunk_backend table to verify that every chunk has rows for all
    expected backends (e.g. ["ipfs", "arion"]).

    Args:
        db: Database connection
        object_id: Object UUID
        object_version: Object version number
        part_number: Part number

    Returns:
        True if ALL chunks have all expected backends registered in chunk_backend,
        False otherwise (including if no chunks exist or chunk count doesn't match expected)
    """
    # Read the upload_backends persisted at version-creation time.
    # Falls back to config.upload_backends for rows created before the column existed.
    row = await db.fetchrow(
        """SELECT version_type, upload_backends FROM object_versions
           WHERE object_id = $1 AND object_version = $2""",
        object_id,
        object_version,
    )
    version_type = row["version_type"] if row else None
    if version_type == "migration":
        expected: list[str] = ["ipfs"]
    elif row and row["upload_backends"]:
        expected = list(row["upload_backends"])
    else:
        expected = list(config.upload_backends)

    # Union in any configured backup backends. The janitor must not delete a
    # part until every required backend — upload AND backup — has a live
    # chunk_backend row for every chunk.
    backup_backends = list(getattr(config, "backup_backends", []) or [])
    for b in backup_backends:
        if b and b not in expected:
            expected.append(b)

    result = await db.fetchrow(
        get_query("count_chunk_backends"),
        object_id,
        object_version,
        part_number,
        expected,
    )
    if not result or result["total_chunks"] == 0:
        return False
    expected_count = result["expected_chunks"] or 0
    if result["total_chunks"] < expected_count:
        return False
    return result["total_chunks"] == result["replicated_chunks"]


async def check_replication_sentinel(db_pool: asyncpg.Pool, pressure: int) -> int:
    """G2 read-only durability sentinel: count live/serveable chunks lacking full-union
    backend coverage and publish the gauge.

    This is the inverse of the janitor's reclaim gate: it finds the chunks the gate would
    (correctly) refuse to reclaim because they are under-replicated — i.e. chunks at
    data-loss risk the moment their SSD/pool copy is evicted. The required backend set
    mirrors ``is_replicated_on_all_backends`` (per-version ∪ backup), so this also catches
    the C10 divergence. A ``replication_sla_seconds`` grace excludes chunks whose part
    landed recently (normal in-flight replication) so only GENUINELY-STUCK chunks are
    counted — without it every servable upload trips the sentinel while it replicates and
    the alarm pages continuously. Purely a SELECT — it never deletes — so it is safe to run
    every cycle. A breach logs ERROR at critical disk pressure (a reclaim is imminent), WARN
    otherwise; a clean scan logs nothing. Returns the number of violations found (capped
    at ``SENTINEL_SCAN_LIMIT``).
    """
    global _replication_sentinel_violations
    backup_backends = list(getattr(config, "backup_backends", []) or [])
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            get_query("find_underreplicated_live_chunks"),
            backup_backends,
            list(config.upload_backends),
            SENTINEL_SCAN_LIMIT,
            config.replication_sla_seconds,
        )
    violations = len(rows)
    _replication_sentinel_violations = violations
    if violations:
        capped = ">=" if violations >= SENTINEL_SCAN_LIMIT else ""
        sample = [(str(r["object_id"]), r["object_version"], r["chunk_id"]) for r in rows[:5]]
        # Critical pressure = the janitor is actively evicting, so an under-replicated
        # chunk is one reclaim away from loss: page it. Otherwise warn (a standing gap).
        log = logger.error if pressure >= 2 else logger.warning
        log(
            "REPLICATION-GATE SENTINEL: %s%d live chunk(s) lack full-union backend coverage "
            "(upload=%s backup=%s); sample=%s",
            capped,
            violations,
            list(config.upload_backends),
            backup_backends,
            sample,
        )
    return violations


async def check_aged_pending_orphans(db_pool: asyncpg.Pool, dlq_object_ids: set[str]) -> int:
    """A21 soak-gate feed: count the standing aged pending/draining unservable orphan
    versions and publish the gauge.

    The 6h-soak gate asserts only the `replicated`-on-SSD count, so it is blind to A21
    orphans (which never reach `replicated`). This publishes the population the sweep
    (`sweep_orphan_replication_versions`) exists to clear, so the soak gate can assert it
    is bounded and its slope is ~ 0 — a rising value means orphans accrue faster than the
    sweep drains them (a re-introduced leak). Purely a SELECT, safe every cycle.

    ``dlq_object_ids`` MUST be excluded to keep the gauge in lockstep with the sweep: the sweep
    skips any object_id parked in a DLQ, so a DLQ-parked orphan is one it will never clear —
    counting it here would be a permanent phantom backlog. Passed as $2 (text[]); the SQL matches
    ``crs.object_id`` byte-for-byte against the reaper's ``str(object_id)`` membership test.

    Unlike the G2 sentinel this does NOT log on a nonzero value: a transient backlog between
    a leak and the next sweep is normal, so alerting is left to the gauge's slope/sustained
    threshold (see the ``aged-pending-orphan-backlog`` Grafana rule), not a per-cycle log.
    Returns the count.
    """
    global _aged_pending_orphans
    async with db_pool.acquire() as conn:
        count = await conn.fetchval(
            get_query("count_aged_pending_orphans"),
            config.aged_orphan_gauge_grace_seconds,
            list(dlq_object_ids),
        )
    _aged_pending_orphans = int(count or 0)
    return _aged_pending_orphans


async def is_terminally_abandoned(
    db: asyncpg.Connection,
    object_id: str,
    object_version: int,
    part_number: int,
) -> bool:
    """True iff this part is a terminally-abandoned upload that is SAFE to reclaim.

    Safety-critical. Returns True only when BOTH hold (see
    `janitor_part_terminally_abandoned.sql`):
      (a) `cephor_replication_status.status = 'failed'` — the MPU reaper or an abort
          marked the part terminal. The drain never re-claims a 'failed' row, so its
          CephFS-pool copy is dead weight that leaks forever otherwise.
      (b) the object version is UNSERVABLE — `address` was never written AND the GET
          download filter (`size_bytes > 0 OR md5_hash <> ''`) cannot be satisfied.

    Why both: 'failed' alone is NOT sufficient, because the drain's corruption-path
    `mark_failed` has no servability guard and can mark a part of a *servable*
    simple-PUT version 'failed'. Requiring (b) — the reaper's own `address IS NULL`
    predicate plus the literal download-servability filter — guarantees the janitor
    never deletes bytes a live GET could serve.
    """
    row = await db.fetchrow(
        get_query("janitor_part_terminally_abandoned"),
        object_id,
        object_version,
        part_number,
    )
    return bool(row and row["abandoned"])


def _descend_object_tmp(root_str: str, object_name: str, cutoff: float) -> int:
    """Blocking: unlink `*.tmp.*` files older than `cutoff` under one object dir's part dirs.
    Runs in a walk thread. Returns how many it removed. Every FS error is a skip."""
    removed = 0
    obj_path = os.path.join(root_str, object_name)  # noqa: PTH118 — hot walk path
    try:
        version_scan = os.scandir(obj_path)
    except OSError:
        return 0
    with version_scan:
        for vd in version_scan:
            if not vd.name.startswith("v"):
                continue
            try:
                if not vd.is_dir():
                    continue
            except OSError:
                continue
            try:
                part_scan = os.scandir(vd.path)
            except OSError:
                continue
            with part_scan:
                for pd in part_scan:
                    if not pd.name.startswith("part_"):
                        continue
                    try:
                        if not pd.is_dir():
                            continue
                    except OSError:
                        continue
                    removed += _unlink_old_tmp_files(pd.path, cutoff)
    return removed


async def cleanup_orphan_tmp_files(
    fs_store: FileSystemPartsStore,
    *,
    shard: int = 0,
    shards: int = 1,
    walk_concurrency: int = 1,
    deadline: float | None = None,
) -> int:
    """Remove orphan atomic-write temp files that outlived a crashed worker.

    Workers use `<target>.tmp.<uuid>` as the tempfile for atomic rename. A crash between
    create and rename leaves it behind; delete anything named `*.tmp.*` older than
    `TMP_FILE_MAX_AGE_SECONDS`. Uses the same sharded, budgeted, parallel descent as the GC
    walk — the previous `root.rglob("*.tmp.*")` was a full-tree walk on the event loop, so on
    a multi-million-object cache it would block the loop for hours (the same starvation the GC
    walk had), never mind that it ran after the phase that already never returned.
    """
    root = fs_store.root
    if not root.exists():
        return 0

    root_str = str(root)
    cutoff = time.time() - TMP_FILE_MAX_AGE_SECONDS
    loop = asyncio.get_running_loop()
    concurrency = max(1, walk_concurrency)
    executor = ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="janitor-tmp")
    state = WalkState()
    removed = 0
    inflight: set[asyncio.Future[int]] = set()
    try:
        async for name in _stream_shard_object_names(root_str, shard, shards, deadline, state):
            inflight.add(loop.run_in_executor(executor, _descend_object_tmp, root_str, name, cutoff))
            if len(inflight) >= concurrency:
                done, inflight = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
                removed += sum(f.result() for f in done)
        while inflight:
            done, inflight = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
            removed += sum(f.result() for f in done)
    finally:
        for fut in inflight:
            fut.cancel()
        executor.shutdown(wait=False)

    if removed > 0 and _janitor_tmp_deleted_counter is not None:
        _janitor_tmp_deleted_counter.add(removed)
    if removed > 0:
        logger.info(f"Janitor removed {removed} orphan tmp files (shard={shard}/{shards} truncated={state.truncated})")
    return removed


def _age_gc_decision(
    mtime: float,
    atime: float,
    now: float,
    hot_window: float,
    cutoff_time: float,
    pressure: int,
    is_dlq_protected: bool,
) -> tuple[bool, bool, bool]:
    """The age-GC phase's FS-level eligibility gate, extracted VERBATIM so the unified walk and
    the standalone `cleanup_old_parts_by_mtime` share one rule.

    Returns `(is_hot, gc_candidate, old_enough)`:
    - is_hot: atime within the pressure-adjusted hot-retention window (computed for census even
      when the part is skipped — hot parts are counted regardless of DLQ/eligibility).
    - gc_candidate: passes all FS gates → hand to the worker's replication check. False when
      DLQ-protected, hot, or (under normal pressure) not yet old enough.
    - old_enough: mtime older than the GC max-age cutoff (for logging).

    The replication gate itself is NOT here — it is the absolute DB check the worker applies to
    every gc_candidate. This gate only decides which replicated parts are ELIGIBLE, never
    overriding replication.
    """
    is_hot = hot_window > 0 and atime > (now - hot_window)
    if is_dlq_protected:
        return (is_hot, False, False)
    # Hot files are protected. Under critical pressure hot_window is 0, which forces
    # is_hot=False, letting fully-replicated parts become eligible even if recently read.
    if is_hot:
        return (is_hot, False, False)
    # A fully-replicated, cold, non-DLQ part is safe to evict. Under normal pressure we
    # additionally require age > cutoff so we don't thrash; under any pressure level we evict
    # replicated cold parts regardless of age. The replication gate is enforced in the worker.
    old_enough = mtime < cutoff_time
    if pressure == 0 and not old_enough:
        return (is_hot, False, False)
    return (is_hot, True, old_enough)


async def cleanup_old_parts_by_mtime(
    pool: asyncpg.Pool,
    fs_store: FileSystemPartsStore,
    redis_client: Redis,
    *,
    shard: int = 0,
    shards: int = 1,
    walk_concurrency: int = 1,
    deadline: float | None = None,
    publish_sweep: bool = True,
) -> int:
    """Safe, replication-gated GC.

    ABSOLUTE RULE: never delete a part that isn't fully replicated to every
    required backend (upload + any configured backup backends). Age, disk
    pressure, and hot-retention policies only relax what's eligible among
    already-replicated parts — they never override the replication check.

    Deletion rule:
        delete <=> fully_replicated AND NOT hot AND NOT dlq_protected

    Where:
    - fully_replicated: every chunk has a live `chunk_backend` row for every
      backend in upload_backends ∪ backup_backends.
    - hot: atime within the pressure-adjusted hot-retention window.
      - Normal pressure:   hot_window = config.fs_cache_hot_retention_seconds
      - Elevated (>=85%):  hot_window halves
      - Critical (>=95%):  hot_window = 0 (hot protection disabled; all
        replicated parts become eligible for eviction)

    Under critical pressure with nothing replicated, the janitor is stuck.
    That is the correct outcome — it logs an ERROR so operators page.

    Metric accumulation (parts_seen, age buckets, hot parts) happens in the
    producer walk; the replication check + delete run with bounded concurrency
    over the connection pool. The FS-level gates (DLQ, hot, age-under-normal)
    are applied in the producer so only genuine deletion candidates reach a
    worker — the replication gate itself is unchanged.
    """
    max_age_seconds = config.fs_cache_gc_max_age_seconds
    logger.info(f"Scanning FS parts eligible for GC (max_age={max_age_seconds}s, replication-gated)")

    try:
        dlq_object_ids = await get_all_dlq_object_ids(redis_client)
    except DLQProtectionUnavailable as exc:
        # C1 (refines A15): do NOT skip the whole age-GC pass when the DLQ set is unavailable.
        # This pass only ever deletes FULLY-REPLICATED parts (the replication gate below is the
        # hard safety net), and a part that is live on every required backend is safe to evict
        # from SSD regardless of any DLQ entry — its bytes can be re-fetched. Skipping entirely
        # meant a redis-queues outage froze eviction at ANY disk level; combined with the drain
        # (also on redis-queues) being down, that is a disk-fill spiral precisely when the janitor
        # is the only thing that can free space. Fall back to replication-gate-only eviction (no
        # DLQ dimension). The non-replication-gated cleanup_stale_parts pass stays fail-closed.
        logger.error(f"DLQ protection unavailable — age-GC falling back to replication-gate-only eviction: {exc}")
        dlq_object_ids = set()
    if dlq_object_ids:
        logger.info(f"Protecting {len(dlq_object_ids)} DLQ objects from GC")

    root = fs_store.root
    if not root.exists():
        return 0

    cutoff_time = time.time() - max_age_seconds
    pressure = _pressure_mode(root)
    hot_window = _effective_hot_retention(pressure)
    if pressure > 0:
        logger.warning(
            f"Disk pressure={pressure} ({'elevated' if pressure == 1 else 'critical'}); hot_window={hot_window}s"
        )

    global _fs_parts_on_disk, _fs_oldest_age_seconds
    global _fs_age_buckets, _fs_hot_parts, _fs_pressure_mode

    now = time.time()
    # Accumulated by the producer walk; read back after the pool drains.
    stats: dict[str, Any] = {
        "parts_seen": 0,
        "hot_parts": 0,
        "oldest_mtime": None,
        "age_counts": dict.fromkeys(AGE_BUCKET_NAMES, 0),
    }
    walk_state = WalkState()

    async def candidates() -> AsyncIterator[tuple[str, int, int, bool]]:
        # Walk the FS hierarchy <root>/<object_id>/v<version>/part_<n>/ via the parallel,
        # sharded, budgeted walker. The mtime/atime the census + gating use come straight
        # from the walk (meta.json if present else the part dir) — identical to the old
        # inline stat, just done in the walk threads.
        async for part in iter_part_dirs(
            root,
            concurrency=walk_concurrency,
            shard=shard,
            shards=shards,
            deadline=deadline,
            state=walk_state,
        ):
            object_id = part.object_id
            object_version = part.object_version
            part_number = part.part_number
            is_dlq_protected = object_id in dlq_object_ids

            stats["parts_seen"] += 1

            # Check mtime (for age) and atime (for hot retention).
            #
            # ZFS + noatime note: in prod the local-cache volume is a ZFS
            # dataset mounted `noatime` (see `mount | grep local_object_cache`
            # → `rw,noatime,xattr,noacl,casesensitive`). `noatime` only
            # blocks VFS-triggered atime updates on reads (the
            # `file_accessed() → atime_needs_update() → dirty_inode()`
            # path). It does NOT block explicit `utimensat(2)` metadata
            # writes, which go through `setattr()`. Our reader refreshes
            # atime on every chunk read via `os.utime(path, None)` in
            # `fs_store.get_chunk`, so hot-retention works correctly here.
            # OpenZFS PR #4482 ("Fix atime handling and relatime") made
            # this behaviour consistent — atime is handled purely by VFS
            # and explicit setattr writes are always honoured regardless
            # of the mount's noatime flag.
            #
            # Side effect: `os.utime(path, None)` sets BOTH atime and
            # mtime to "now" (UTIME_NOW on both). Recently-read chunks
            # therefore show `atime == mtime` to the nanosecond — that's
            # expected, not a bug. It also means reads push mtime
            # forward, so the mtime-based age check below is more
            # conservative on actively-read content (treats hot chunks
            # as younger than their original landing time). Replication
            # is still the absolute gate, so this only relaxes, never
            # tightens, what we delete.
            try:
                mtime = part.mtime
                atime = part.atime
                if stats["oldest_mtime"] is None or mtime < stats["oldest_mtime"]:
                    stats["oldest_mtime"] = mtime

                part_age = now - mtime
                stats["age_counts"][_classify_age_bucket(part_age)] += 1

                is_hot, gc_candidate, old_enough = _age_gc_decision(
                    mtime, atime, now, hot_window, cutoff_time, pressure, is_dlq_protected
                )
                if is_hot:
                    stats["hot_parts"] += 1
                if not gc_candidate:
                    continue
            except Exception as e:
                logger.warning(
                    f"Failed to classify part object_id={object_id} v={object_version} part={part_number}: {e}"
                )
                continue

            yield (object_id, object_version, part_number, old_enough)

    async def handle(conn: asyncpg.Connection, item: tuple[str, int, int, bool]) -> bool:
        object_id, object_version, part_number, old_enough = item

        # ABSOLUTE safety gate: never delete non-replicated data.
        try:
            fully_replicated = await is_replicated_on_all_backends(conn, object_id, object_version, part_number)
        except Exception as e:
            logger.warning(f"Replication check failed for {object_id} v{object_version} part{part_number}: {e}")
            return False
        if not fully_replicated:
            return False

        try:
            await fs_store.delete_part(object_id, object_version, part_number)
            await _clear_inventory_after_delete(conn, object_id, object_version, part_number)
            logger.debug(
                f"GC cleaned part: object_id={object_id} v={object_version} part={part_number} "
                f"replicated=True pressure={pressure} {old_enough=}"
            )
            return True
        except Exception as e:
            logger.warning(f"Failed to clean part: object_id={object_id} v={object_version} part={part_number}: {e}")
            return False

    parts_cleaned = await _run_worker_pool(pool, candidates(), handle, config.janitor_concurrency)

    # Census is accumulated across the sharded sweep (this cycle covered shard `shard` of
    # `shards`) and only published to the gauges when the sweep completes — see
    # _accumulate_census / _publish_census. Disk-usage + pressure gauges are owned by
    # _update_disk_metrics (refreshed at cycle top); don't re-stat the disk here.
    # Pressure is a per-cycle fact (not per-sweep), so publish it every call regardless of the
    # sharded census; _update_disk_metrics also sets it at cycle top, this keeps it in sync with
    # the pressure this GC pass actually acted on.
    global _fs_pressure_mode
    _fs_pressure_mode = pressure

    # A sweep always starts at shard 0, so reset the accumulator there. This bounds the
    # accumulator to exactly one sweep even if `shards` changed mid-sweep (e.g. a pressure
    # transition flips it to 1) — without it the old partial sweep's counts would blend into
    # the new one and inflate the published census once.
    if shard == 0:
        _reset_census_accum()
    _accumulate_census(stats, shard_complete=not walk_state.truncated)
    if publish_sweep:
        _publish_census(now)

    if parts_cleaned > 0 and _janitor_deleted_counter is not None:
        _janitor_deleted_counter.add(parts_cleaned, attributes={"reason": "gc_age"})

    logger.info(f"GC cleaned {parts_cleaned=} hot_parts={stats['hot_parts']} pressure={pressure}")

    # If we're under critical disk pressure but couldn't free any space,
    # every on-disk part is either hot (ignored under critical — nothing to
    # free) or non-replicated (we refuse to delete). Page the operator.
    if pressure == 2 and parts_cleaned == 0 and stats["parts_seen"] > 0:
        logger.error(
            "JANITOR_CRITICAL_PRESSURE_BLOCKED parts_seen=%d hot_parts=%d — "
            "disk is >=95%% full but all remaining parts are non-replicated. "
            "Operator action required; refusing to delete unreplicated data.",
            stats["parts_seen"],
            stats["hot_parts"],
        )

    return parts_cleaned


@dataclass(frozen=True)
class _UnifiedCandidate:
    """One part the unified walk found that qualifies for AT LEAST one deletion rule. The two
    `*_candidate` flags carry the FS-level pre-decision so the worker only runs the DB checks a
    part actually needs; a part can qualify for both (deleted once, stale attributed first)."""

    object_id: str
    object_version: int
    part_number: int
    stale_candidate: bool
    gc_candidate: bool
    gc_old_enough: bool


async def cleanup_parts_unified(
    pool: asyncpg.Pool,
    fs_store: FileSystemPartsStore,
    redis_client: Redis,
    *,
    pressure: int,
    shard: int = 0,
    shards: int = 1,
    walk_concurrency: int = 1,
    deadline: float | None = None,
    publish_sweep: bool = True,
) -> dict[str, int]:
    """ONE FS walk that applies all three old FS-walk phases per part dir.

    This collapses `cleanup_stale_parts` + `cleanup_old_parts_by_mtime` + `cleanup_orphan_tmp_files`
    — which each independently crawled the whole shard via `iter_part_dirs` — into a single pass.
    On prod that turned a 3× metadata crawl of a ~15.6M-object CephFS tree (contending with live
    GET/PUT on the MDS) into 1×, for the same coverage. The WALK is unified; the deletion RULES are
    the SAME byte-for-byte helpers the standalone phases use:
      - stale-reap:  `_stale_fs_eligible` (producer) + `_stale_reap_decision` (worker).
      - age-GC:      `_age_gc_decision` (producer) + `is_replicated_on_all_backends` (worker).
      - tmp:         `_unlink_old_tmp_files`, folded into the walk via `iter_part_dirs(tmp_cutoff=)`.
      - census:      accumulated in the producer, published only on an untruncated full sweep.

    A part is deleted if EITHER the stale rule OR the age-GC rule fires; both are evaluated and it
    is deleted once, attributing the correct reason (abandoned / stale_mtime / gc_age). The one
    walk gets ONE budget (`deadline`), unbounded at Critical — that is the ~3× cycle-time win.

    DLQ semantics (invariants 2 & 3): the protection set is fetched ONCE.
      - On success: stale-reap requires the object be absent from it; age-GC honours it too.
      - On DLQProtectionUnavailable: stale-reap is SKIPPED entirely this cycle (fail-CLOSED),
        while age-GC still runs replication-gate-only (fail-OPEN) — a fully-replicated part is
        safe to evict regardless of any DLQ entry.

    Returns a dict of per-reason delete counts plus tmp: {stale_mtime, abandoned, gc, tmp}.
    """
    root = fs_store.root
    if not root.exists():
        return {"stale_mtime": 0, "abandoned": 0, "gc": 0, "tmp": 0}

    stale_threshold_seconds = config.mpu_stale_seconds
    max_age_seconds = config.fs_cache_gc_max_age_seconds
    logger.info(
        "Unified janitor walk: stale_threshold=%ds gc_max_age=%ds tmp_max_age=%ds pressure=%d (replication-gated)",
        stale_threshold_seconds,
        max_age_seconds,
        TMP_FILE_MAX_AGE_SECONDS,
        pressure,
    )

    # A15/C1: one DLQ fetch drives both rules. dlq_available=False means the stale-reap path is
    # skipped this cycle (fail-closed); age-GC proceeds against an empty set (fail-open) — its
    # replication gate is the hard safety net.
    dlq_available = True
    try:
        dlq_object_ids = await get_all_dlq_object_ids(redis_client)
    except DLQProtectionUnavailable as exc:
        logger.error(
            "DLQ protection unavailable — stale-reap SKIPPED this cycle (fail-closed); "
            "age-GC falls back to replication-gate-only eviction: %s",
            exc,
        )
        dlq_object_ids = set()
        dlq_available = False
    if dlq_object_ids:
        logger.info(f"Protecting {len(dlq_object_ids)} DLQ objects from the unified walk")

    now = time.time()
    mtime_cutoff = now - stale_threshold_seconds
    cutoff_time = now - max_age_seconds
    tmp_cutoff = now - TMP_FILE_MAX_AGE_SECONDS
    hot_window = _effective_hot_retention(pressure)
    if pressure > 0:
        logger.warning(
            f"Disk pressure={pressure} ({'elevated' if pressure == 1 else 'critical'}); hot_window={hot_window}s"
        )

    stats: dict[str, Any] = {
        "parts_seen": 0,
        "hot_parts": 0,
        "oldest_mtime": None,
        "age_counts": dict.fromkeys(AGE_BUCKET_NAMES, 0),
    }
    reason_counts = {"stale_mtime": 0, "abandoned": 0, "gc": 0}
    walk_state = WalkState()

    async def candidates() -> AsyncIterator[_UnifiedCandidate]:
        # The walk doubles as the inventory backfill/reconciler: every part that SURVIVES all gates
        # (i.e. will REMAIN on disk) is recorded into fs_cache_inventory, so the first full sweep
        # after deploy populates the table and later sweeps repair any row a materialization
        # pipeline (Task 3.1) failed to write. Deletion candidates are deliberately NOT backfilled
        # here — see the asymmetry note at the survivor branch below.
        backfill_batch: list[tuple[str, int, int]] = []

        async def _flush_backfill() -> None:
            nonlocal backfill_batch
            if not backfill_batch:
                return
            # The producer holds no pooled connection of its own, so acquire one per flush.
            # Best-effort end to end: record_cached_batch swallows its own errors, and the acquire
            # is wrapped too — a pool failure here would otherwise escape the candidates() generator
            # and abort the whole walk for an advisory write (next sweep backfills what was lost).
            try:
                async with pool.acquire() as conn:
                    await fs_cache_inventory.record_cached_batch(conn, backfill_batch)
            except Exception as e:
                logger.warning(f"Inventory backfill flush failed (next sweep re-covers): {e}")
            backfill_batch = []

        async for part in iter_part_dirs(
            root,
            concurrency=walk_concurrency,
            shard=shard,
            shards=shards,
            deadline=deadline,
            state=walk_state,
            tmp_cutoff=tmp_cutoff,  # folds the old orphan-tmp sweep into this one walk
        ):
            object_id = part.object_id
            object_version = part.object_version
            part_number = part.part_number
            is_dlq_protected = object_id in dlq_object_ids

            stats["parts_seen"] += 1
            try:
                mtime = part.mtime
                atime = part.atime
                if stats["oldest_mtime"] is None or mtime < stats["oldest_mtime"]:
                    stats["oldest_mtime"] = mtime
                stats["age_counts"][_classify_age_bucket(now - mtime)] += 1

                # Age-GC gate (also yields is_hot for the census — counted for every part,
                # even DLQ-protected/hot ones, exactly as the standalone census did).
                is_hot, gc_candidate, gc_old_enough = _age_gc_decision(
                    mtime, atime, now, hot_window, cutoff_time, pressure, is_dlq_protected
                )
                if is_hot:
                    stats["hot_parts"] += 1

                # Stale-reap gate (mtime-only; does NOT honour hot retention, matching the
                # standalone stale phase). Skipped wholesale when the DLQ set is unavailable.
                stale_candidate = _stale_fs_eligible(mtime, mtime_cutoff, object_id, dlq_available, dlq_object_ids)
            except Exception as e:
                logger.warning(
                    f"Failed to classify part object_id={object_id} v={object_version} part={part_number}: {e}"
                )
                continue

            if not stale_candidate and not gc_candidate:
                # Survivor: stays on disk this sweep → backfill its inventory row. Only UUID-shaped
                # dirnames — Task 4.1's eviction query casts object_id::uuid and one non-UUID row
                # would abort the whole candidate page. A yielded candidate that later survives the
                # worker's gate re-check (e.g. an under-replicated cold part) is NOT recorded here;
                # it is re-walked and re-evaluated next sweep, so it only enters the inventory once
                # it is genuinely at rest — which is fine, since it is not evictable until then.
                if _is_uuid_name(object_id):
                    backfill_batch.append((object_id, object_version, part_number))
                    if len(backfill_batch) >= INVENTORY_BACKFILL_BATCH_SIZE:
                        await _flush_backfill()
                continue
            yield _UnifiedCandidate(
                object_id, object_version, part_number, stale_candidate, gc_candidate, gc_old_enough
            )

        await _flush_backfill()  # final partial batch of kept parts

    async def handle(conn: asyncpg.Connection, item: _UnifiedCandidate) -> bool:
        # Evaluate stale-reap FIRST (matches the old serial order: stale phase ran before age-GC,
        # so a part deletable by both is attributed to stale). If stale protects it, fall through
        # to age-GC — a not-replicated part is protected by both, a stale-recent-but-replicated
        # part is still an age-GC delete.
        if item.stale_candidate:
            should_delete, abandoned = await _stale_reap_decision(
                conn, item.object_id, item.object_version, item.part_number, stale_threshold_seconds
            )
            if should_delete:
                try:
                    await fs_store.delete_part(item.object_id, item.object_version, item.part_number)
                except Exception as e:
                    logger.warning(
                        f"Failed to clean part: object_id={item.object_id} "
                        f"v={item.object_version} part={item.part_number}: {e}"
                    )
                    return False
                await _clear_inventory_after_delete(conn, item.object_id, item.object_version, item.part_number)
                if abandoned:
                    logger.info(
                        "Reclaimed terminally-abandoned part (failed+unservable): "
                        f"object_id={item.object_id} v={item.object_version} part={item.part_number}"
                    )
                    reason_counts["abandoned"] += 1
                    if _janitor_abandoned_deleted_counter is not None:
                        _janitor_abandoned_deleted_counter.add(1)
                    if _janitor_deleted_counter is not None:
                        _janitor_deleted_counter.add(1, attributes={"reason": "abandoned"})
                else:
                    logger.debug(
                        f"Cleaned stale part by mtime: object_id={item.object_id} "
                        f"v={item.object_version} part={item.part_number}"
                    )
                    reason_counts["stale_mtime"] += 1
                    if _janitor_deleted_counter is not None:
                        _janitor_deleted_counter.add(1, attributes={"reason": "stale_mtime"})
                return True

        if item.gc_candidate:
            # ABSOLUTE safety gate: never delete non-replicated data.
            try:
                fully_replicated = await is_replicated_on_all_backends(
                    conn, item.object_id, item.object_version, item.part_number
                )
            except Exception as e:
                logger.warning(
                    f"Replication check failed for {item.object_id} v{item.object_version} part{item.part_number}: {e}"
                )
                return False
            if not fully_replicated:
                return False
            try:
                await fs_store.delete_part(item.object_id, item.object_version, item.part_number)
            except Exception as e:
                logger.warning(
                    f"Failed to clean part: object_id={item.object_id} "
                    f"v={item.object_version} part={item.part_number}: {e}"
                )
                return False
            await _clear_inventory_after_delete(conn, item.object_id, item.object_version, item.part_number)
            logger.debug(
                f"GC cleaned part: object_id={item.object_id} v={item.object_version} part={item.part_number} "
                f"replicated=True pressure={pressure} old_enough={item.gc_old_enough}"
            )
            reason_counts["gc"] += 1
            if _janitor_deleted_counter is not None:
                _janitor_deleted_counter.add(1, attributes={"reason": "gc_age"})
            return True

        return False

    parts_cleaned = await _run_worker_pool(pool, candidates(), handle, config.janitor_concurrency)

    # Census: accumulated across the sharded sweep, published only on an untruncated full sweep.
    # A sweep starts at shard 0, so reset the accumulator there (bounds it to one sweep even if
    # `shards` changed mid-sweep). Pressure is a per-cycle fact — publish it every call.
    global _fs_pressure_mode
    _fs_pressure_mode = pressure
    if shard == 0:
        _reset_census_accum()
    _accumulate_census(stats, shard_complete=not walk_state.truncated)
    if publish_sweep:
        _publish_census(now)

    tmp_removed = walk_state.tmp_removed
    if tmp_removed > 0 and _janitor_tmp_deleted_counter is not None:
        _janitor_tmp_deleted_counter.add(tmp_removed)

    logger.info(
        "Unified walk cleaned: stale=%d abandoned=%d gc=%d tmp=%d hot_parts=%d parts_seen=%d "
        "(shard=%d/%d objects_scanned=%d truncated=%s pressure=%d)",
        reason_counts["stale_mtime"],
        reason_counts["abandoned"],
        reason_counts["gc"],
        tmp_removed,
        stats["hot_parts"],
        stats["parts_seen"],
        shard,
        shards,
        walk_state.objects_scanned,
        walk_state.truncated,
        pressure,
    )

    # Critical pressure but nothing freed: every remaining part is hot (ignored under critical —
    # nothing to free) or non-replicated (we refuse to delete). parts_cleaned counts BOTH stale
    # and age-GC part deletions, so this fires only when the walk genuinely freed no part space.
    if pressure == 2 and parts_cleaned == 0 and stats["parts_seen"] > 0:
        logger.error(
            "JANITOR_CRITICAL_PRESSURE_BLOCKED parts_seen=%d hot_parts=%d — "
            "disk is >=95%% full but all remaining parts are non-replicated. "
            "Operator action required; refusing to delete unreplicated data.",
            stats["parts_seen"],
            stats["hot_parts"],
        )

    return {**reason_counts, "tmp": tmp_removed}


# The keyset cursor's cold-start position: sorts strictly before every real row in
# (cached_at, object_id, object_version, part_number) order, so the first page begins at the
# oldest inventory row. $6 is a NATIVE timestamptz param — asyncpg rejects a string there — so the
# sentinel is the epoch (cached_at DEFAULTs to now(), so epoch is safely before every real row);
# $7-$9 are '' / 0 / 0. None is NEVER passed: a NULL in the row-value comparison drops every row.
_EVICT_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_EVICT_CURSOR_START: tuple[datetime, str, int, int] = (_EVICT_EPOCH, "", 0, 0)


def _load_evict_cursor(state: dict[str, Any] | None) -> tuple[datetime, str, int, int]:
    """Parse the durable keyset cursor from janitor_state, falling back to the epoch ring-start on
    any malformed value. cached_at is persisted as an ISO string (JSON has no datetime) and parsed
    back to a datetime here for the native-timestamptz $6. A corrupt cursor must delay/loop
    eviction, never crash the phase (invariant 6: a new failure mode degrades to "not evicted this
    cycle", never an unsafe delete)."""
    if not state:
        return _EVICT_CURSOR_START
    try:
        return (
            datetime.fromisoformat(state["cached_at"]),
            str(state["object_id"]),
            int(state["object_version"]),
            int(state["part_number"]),
        )
    except (TypeError, ValueError, KeyError):
        return _EVICT_CURSOR_START


async def evict_from_inventory(
    pool: asyncpg.Pool,
    fs_store: FileSystemPartsStore,
    redis_client: Redis,
    *,
    pressure: int,
) -> int:
    """SQL-driven eviction: SLICE-THEN-FILTER over fs_cache_inventory, stat only the candidates
    (existence + atime hot-check), then apply the UNCHANGED absolute replication gate per part
    before deleting. O(evictable) instead of O(resident) — the walk's ~36 obj/s CephFS-readdir
    bottleneck is replaced by indexed DB reads.

    Each page runs TWO bounded queries: janitor_inventory_slice (a pure keyset window of the
    inventory ring, index-only, cost bounded by page_size) then janitor_evictable_candidates
    (the coverage/age filter over exactly that window's tuples). THE CURSOR ADVANCES BY THE SLICE,
    not by the filter output: a window that is 100% non-candidates still advances the ring. This is
    the stall fix — the old single scan-and-filter query, on an inventory whose head is millions of
    non-candidates, re-scanned from the same cursor every cycle, deterministically re-timed-out, and
    never advanced, so the SQL phase permanently freed zero. Bounding the scan window guarantees
    forward progress at ANY sparseness.

    Cursor lives in janitor_state['sql_evict_cursor'] and only advances past a slice after the
    slice's candidates are fully processed, so a crash mid-page re-processes it (idempotent:
    delete_part no-ops on a missing dir, clear_cached no-ops on a missing row). The scan is a RING:
    an empty slice resets the cursor to the start, a short slice ends the cycle after advancing.

    Safety is the walk's exact model: the SQL filter is a PREFILTER only (invariant 1); the
    per-part is_replicated_on_all_backends gate is re-run on the worker connection before every
    delete. DLQ protection is honoured when available and fails OPEN (invariant 2) — the same
    age-GC-class rule, since a fully-replicated part is safe to evict regardless of any DLQ entry.
    """
    max_deletes = config.janitor_sql_max_deletes_per_cycle
    if max_deletes <= 0:
        return 0  # kill switch: phase disabled without a deploy (prod rollback)

    hot_window = _effective_hot_retention(pressure)
    ignore_age = pressure > 0
    now = time.time()

    try:
        dlq_object_ids = await get_all_dlq_object_ids(redis_client)
    except DLQProtectionUnavailable as exc:
        # C1 fail-open, same as age-GC: the replication gate below is the hard net, so a
        # redis-queues outage must not freeze eviction while the disk fills.
        logger.error(f"DLQ protection unavailable — SQL eviction replication-gate-only: {exc}")
        dlq_object_ids = set()

    backup_backends = list(getattr(config, "backup_backends", []) or [])
    upload_backends = list(config.upload_backends)
    page_size = config.janitor_sql_page_size
    query_timeout = config.janitor_sql_query_timeout_seconds

    async with pool.acquire() as conn:
        cursor = _load_evict_cursor(await get_janitor_state(conn, "sql_evict_cursor"))

    async def handle(conn: asyncpg.Connection, item: tuple[str, int, int]) -> bool:
        object_id, object_version, part_number = item
        st = await asyncio.to_thread(fs_store.stat_part, object_id, object_version, part_number)
        if st is None:
            await clear_cached(conn, object_id, object_version, part_number)  # stale row: self-heal
            return False
        if hot_window > 0 and st.st_atime > (now - hot_window):
            return False  # recently read — hot retention protects it (skipped when window==0)
        # ABSOLUTE safety gate — identical call to the walk's, never bypassed by the prefilter.
        try:
            fully_replicated = await is_replicated_on_all_backends(conn, object_id, object_version, part_number)
        except Exception as e:
            logger.warning(f"SQL evict replication check failed for {object_id} v{object_version} p{part_number}: {e}")
            return False
        if not fully_replicated:
            return False
        await fs_store.delete_part(object_id, object_version, part_number)
        try:
            await clear_cached(conn, object_id, object_version, part_number)
        except Exception as e:
            # The delete already happened; a retained inventory row self-heals on the next
            # stat-miss. Narrow the swallow to the clear so the delete is still counted.
            logger.warning(f"SQL evict clear_cached failed after delete {object_id} p{part_number}: {e}")
        if _janitor_deleted_counter is not None:
            _janitor_deleted_counter.add(1, attributes={"reason": "sql_evict"})
        return True

    async def candidates(page: list[Any]) -> AsyncIterator[tuple[str, int, int]]:
        for r in page:
            if r["object_id"] in dlq_object_ids:
                continue  # DLQ-parked: an in-flight op owns this object's data
            yield (r["object_id"], r["object_version"], r["part_number"])

    deleted_total = 0
    pages = 0
    while deleted_total < max_deletes:
        # Step 1 — SLICE: pure keyset window of the inventory ring (index-only, bounded by
        # page_size). This is the cursor-advancing scan; on timeout we leave the cursor UNMOVED
        # (no advance, no {} reset) so the next cycle resumes here. With the scan bounded, a
        # timeout now only fires on a genuinely degraded DB, not on a sparse ring head.
        try:
            async with pool.acquire() as conn:
                slice_rows = await conn.fetch(
                    get_query("janitor_inventory_slice"),
                    page_size,
                    *cursor,
                    timeout=query_timeout,
                )
        except asyncio.TimeoutError:
            logger.warning(
                "SQL eviction slice scan timed out after %ss; ending cycle at cursor "
                "cached_at=%s object_id=%s v=%s part=%s (resumes here next cycle)",
                query_timeout,
                cursor[0],
                cursor[1],
                cursor[2],
                cursor[3],
            )
            break
        if not slice_rows:
            async with pool.acquire() as conn:
                await set_janitor_state(conn, "sql_evict_cursor", {})  # ring wrap: restart at the head
            break
        pages += 1

        # Step 2 — FILTER: coverage/age evictability over exactly this slice's tuples. On timeout,
        # same semantics as the slice: cursor UNMOVED, cycle ends. Only reachable now if the DB is
        # genuinely degraded, since the filter operates on a bounded (page_size) tuple set.
        object_ids = [r["object_id"] for r in slice_rows]
        versions = [r["object_version"] for r in slice_rows]
        part_numbers = [r["part_number"] for r in slice_rows]
        try:
            async with pool.acquire() as conn:
                cand_rows = await conn.fetch(
                    get_query("janitor_evictable_candidates"),
                    object_ids,
                    versions,
                    part_numbers,
                    backup_backends,
                    upload_backends,
                    config.fs_cache_gc_max_age_seconds,
                    ignore_age,
                    timeout=query_timeout,
                )
        except asyncio.TimeoutError:
            logger.warning(
                "SQL eviction filter query timed out after %ss; ending cycle at cursor "
                "cached_at=%s object_id=%s v=%s part=%s (resumes here next cycle)",
                query_timeout,
                cursor[0],
                cursor[1],
                cursor[2],
                cursor[3],
            )
            break

        deleted_total += await _run_worker_pool(pool, candidates(cand_rows), handle, config.janitor_concurrency)

        # Advance the cursor to the LAST SLICE ROW — NOT the last candidate. This is the stall fix:
        # a slice with zero candidates still advances the ring by every row it scanned, so a head of
        # non-candidates can never re-pin the cursor. last["cached_at"] is a datetime (asyncpg
        # decodes timestamptz) — pass it straight to the next $2, isoformat only for the JSON cursor.
        last = slice_rows[-1]
        cursor = (last["cached_at"], last["object_id"], last["object_version"], last["part_number"])
        async with pool.acquire() as conn:
            await set_janitor_state(
                conn,
                "sql_evict_cursor",
                {
                    "cached_at": last["cached_at"].isoformat(),
                    "object_id": last["object_id"],
                    "object_version": last["object_version"],
                    "part_number": last["part_number"],
                },
            )
        if len(slice_rows) < page_size:
            break  # short slice = end of the ring this cycle

    logger.info(
        "SQL eviction cycle: deleted=%d pages=%d pressure=%d ignore_age=%s",
        deleted_total,
        pages,
        pressure,
        ignore_age,
    )
    return deleted_total


# The hard-delete ring cursor's cold-start position: sorts strictly before every real (deleted_at,
# object_id) pair, so a wrapped ring restarts at the oldest soft-deleted object. deleted_at is a
# NATIVE timestamptz param ($2) — asyncpg rejects a string there — so the sentinel is the epoch
# (soft-deletes are always after 1970); object_id ($3) is a uuid, so the sentinel is the nil-uuid.
_HARD_DELETE_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_HARD_DELETE_NIL_UUID = "00000000-0000-0000-0000-000000000000"
_HARD_DELETE_CURSOR_START: tuple[datetime, str] = (_HARD_DELETE_EPOCH, _HARD_DELETE_NIL_UUID)


def _load_hard_delete_cursor(state: dict[str, Any] | None) -> tuple[datetime, str]:
    """Parse the durable (deleted_at, object_id) ring cursor from janitor_state, falling back to the
    epoch/nil-uuid ring-start on any malformed value. deleted_at is persisted as an ISO string (JSON
    has no datetime) and parsed back to a datetime here for the native-timestamptz $2. A corrupt
    cursor must restart the ring, never crash the phase."""
    if not state:
        return _HARD_DELETE_CURSOR_START
    try:
        return (datetime.fromisoformat(state["deleted_at"]), str(state["object_id"]))
    except (TypeError, ValueError, KeyError):
        return _HARD_DELETE_CURSOR_START


async def gc_soft_deleted_objects(pool: asyncpg.Pool) -> int:
    """Hard-delete soft-deleted objects whose backends have confirmed unpin, walking a durable keyset
    RING so a permanently-unready head (e.g. a never-replicated CopyObject destination) can no longer
    block the whole batch forever. The finder returns a (deleted_at, object_id)-ordered SLICE of ALL
    soft-deleted candidates past grace — ready and not — with a per-row `ready` boolean; we hard-delete
    only the ready ones via the guarded delete but advance the cursor over the ENTIRE slice, so the
    next cycle resumes strictly after it. An empty slice wraps the ring back to the start."""
    async with pool.acquire() as db:
        cursor = _load_hard_delete_cursor(await get_janitor_state(db, "hard_delete_cursor"))
        rows = await db.fetch(
            get_query("find_objects_ready_for_hard_delete"),
            config.janitor_hard_delete_batch,
            *cursor,
        )
        if not rows:
            await set_janitor_state(db, "hard_delete_cursor", {})  # ring wrap: restart at the head
            logger.info("Hard-delete cycle: scanned=0 ready=0 deleted=0 skipped=0 wrapped=True")
            return 0

        ready = sum(1 for r in rows if r["ready"])
        deleted = 0
        skipped = 0
        for row in rows:
            if not row["ready"]:
                continue  # cursor still advances past it below — no head-of-line block
            try:
                # Guarded delete re-verifies readiness atomically (mirrors the finder), so a row
                # revived between the find and here — re-PUT clears deleted_at + adds live chunks —
                # is left untouched. "DELETE 0" => skipped, not deleted.
                tag = await db.execute(get_query("hard_delete_object"), row["object_id"])
                if tag == "DELETE 0":
                    skipped += 1
                    continue
                deleted += 1
            except Exception as e:
                logger.warning(f"Failed to hard-delete object {row['object_id']}: {e}")

        last = rows[-1]
        await set_janitor_state(
            db,
            "hard_delete_cursor",
            {"deleted_at": last["deleted_at"].isoformat(), "object_id": str(last["object_id"])},
        )
        logger.info(
            "Hard-delete cycle: scanned=%d ready=%d deleted=%d skipped=%d wrapped=False",
            len(rows),
            ready,
            deleted,
            skipped,
        )
    return deleted


async def run_janitor_loop():
    """Main janitor loop: periodically clean stale and old parts."""
    concurrency = max(1, config.janitor_concurrency)
    db_pool = await asyncpg.create_pool(config.database_url, min_size=2, max_size=concurrency + 4)
    fs_store = create_fs_store(config)
    redis_client = Redis.from_url(config.redis_queues_url)

    # Initialize janitor-owned OTel metrics
    _setup_janitor_metrics()

    # Queue depth/age gauges (2026-07-25 audit: 136k payloads accumulated
    # unseen in ovh_download_requests). Janitor hosts the sampler because it is
    # single-instance and already holds the queues-Redis client; the task runs
    # off the cycle path so a slow walk never blinds the queue gauges.
    queue_sampler = QueueDepthSampler(redis_client, config)
    queue_sampler_task = asyncio.create_task(queue_sampler.run())

    # Publish the shared pressure signal (fs_cache:pressure on the CACHE Redis
    # — where the api middleware and the s3-backup hydrator read it). Sampled
    # every 30s rather than once per cycle: a mass writer can move the pool
    # percent materially inside one ~20min cycle. The janitor's own per-cycle
    # _pressure_mode stays authoritative for eviction pacing; both key off the
    # same watermarks in hippius_s3.pressure_signal.
    cache_redis_client = Redis.from_url(config.redis_url)
    pressure_publisher = PressurePublisher(
        cache_redis_client,
        Path(config.object_cache_dir),
        mgr_metrics_url=config.janitor_ceph_mgr_metrics_url,
        pools=config.janitor_ceph_pools.split(","),
        probe_timeout_seconds=config.janitor_ceph_probe_timeout_seconds,
    )
    pressure_publish_task = asyncio.create_task(pressure_publisher.run())

    logger.info("Starting janitor service...")
    logger.info(f"FS store root: {config.object_cache_dir}")
    logger.info(f"MPU stale threshold: {config.mpu_stale_seconds}s")
    logger.info(
        f"Aged-pending-orphan gauge grace: {config.aged_orphan_gauge_grace_seconds}s "
        f"(soak-visibility window, decoupled from the {config.mpu_sweep_grace_seconds}s reaper sweep grace)"
    )
    logger.info(f"FS GC max age: {config.fs_cache_gc_max_age_seconds}s")
    logger.info(f"FS hot retention: {getattr(config, 'fs_cache_hot_retention_seconds', 10800)}s")
    logger.info(f"Cleanup concurrency: {concurrency}")
    if config.janitor_ceph_mgr_metrics_url and config.janitor_ceph_pools:
        logger.info(
            f"Pool-fullness gate ACTIVE: pools={config.janitor_ceph_pools} via {config.janitor_ceph_mgr_metrics_url} "
            f"(pressure = max(statvfs, fullest pool %USED))"
        )
    else:
        logger.warning(
            "Pool-fullness gate INACTIVE (HIPPIUS_JANITOR_CEPH_MGR_METRICS_URL / _POOLS unset); "
            "pressure keyed on statvfs, which sees the PVC quota not the backing pool — the 2026-07-24 blind spot"
        )

    # Sleep intervals: shorter under disk pressure to catch up
    sleep_normal = 600  # 10m
    sleep_pressure = max(1, config.janitor_pressure_sleep_seconds)

    global _walk_shard, _janitor_phase, _janitor_last_cycle_completed_at, _janitor_cycle_seconds
    loop = asyncio.get_running_loop()

    try:
        while True:
            _cycle_started = time.time()
            logger.info("Janitor cycle starting...")
            # Refresh disk/pressure gauges up front, and read pressure ONCE for the whole cycle.
            await _update_disk_metrics(fs_store.root)
            pressure = _pressure_mode(fs_store.root)

            # --- DB-only DURABILITY phases run FIRST ------------------------------------------
            # These used to run LAST, behind two full-tree FS walks. cleanup_stale_parts could
            # not finish a pass over ~2.8M object dirs inside a cycle, so on prod these never ran
            # at all — the replication-gate sentinel and the A21 aged-orphan leak gauge went dark.
            # They are single indexed DB reads; nothing about the FS cache should gate them.
            sentinel_violations = 0
            try:
                _janitor_phase = 3  # sentinel
                sentinel_violations = await check_replication_sentinel(db_pool, pressure)
            except Exception as e:
                logger.error(f"Replication sentinel error: {e}", exc_info=True)

            # Aged-pending orphan gauge (A21 soak-gate feed). The DLQ set is gathered fresh via
            # the fail-closed get_all_dlq_object_ids and excluded so the gauge counts EXACTLY the
            # population the sweep clears; on a DLQ-read failure the whole phase is skipped (gauge
            # holds its prior value) rather than over-counting against an empty set.
            aged_orphans = 0
            try:
                _janitor_phase = 4  # aged_orphans
                gauge_dlq_object_ids = await get_all_dlq_object_ids(redis_client)
                aged_orphans = await check_aged_pending_orphans(db_pool, gauge_dlq_object_ids)
            except Exception as e:
                logger.error(f"Aged-pending orphan gauge error: {e}", exc_info=True)

            # --- FS-walk phases: sharded + budgeted so the cycle ALWAYS completes -------------
            # Each cycle covers one hash-shard of the tree; a full sweep takes `shards` cycles.
            # ELEVATED pressure rotates a smaller shard count (see _shards_for_pressure); CRITICAL
            # walks the whole tree with the wall-clock budget lifted entirely — freeing space must
            # never be capped by a clock.
            shards = _shards_for_pressure(pressure)
            walk_shard = _walk_shard % shards
            publish_sweep = walk_shard == shards - 1  # census publishes when the sweep wraps
            walk_conc = max(1, config.janitor_walk_concurrency)
            budget = config.janitor_walk_budget_seconds

            stale_count = 0
            abandoned_count = 0
            gc_count = 0
            tmp_count = 0
            hard_deleted = 0
            sql_evicted = 0

            # Phase (SQL EVICT): keyset-cursored discovery over fs_cache_inventory evicts only the
            # fully-replicated, aged, cold candidates the index already knows about — O(evictable),
            # not the walk's O(resident) CephFS crawl. Runs BEFORE the walk: in Wave 4 both engines
            # coexist and are mutually idempotent (delete_part/clear_cached no-op on missing). The
            # kill switch (HIPPIUS_JANITOR_SQL_MAX_DELETES_PER_CYCLE=0) makes it a no-op for rollback.
            try:
                _janitor_phase = 5  # sql_evict
                sql_evicted = await evict_from_inventory(db_pool, fs_store, redis_client, pressure=pressure)
            except Exception as e:
                logger.error(f"SQL eviction error: {e}", exc_info=True)

            # Phase A (UNIFIED): ONE FS walk applies stale-reap + age-GC + census + orphan-tmp per
            # part dir. This replaces the three separate full-tree walks that each independently
            # crawled the shard (3× the CephFS MDS metadata load, ~3× the cycle time); the deletion
            # RULES are unchanged (shared helpers), only the WALK is merged. The single walk gets
            # ONE budget, unbounded at Critical — that is the ~3× cycle-time win.
            try:
                _janitor_phase = 1  # parts_unified
                unified = await cleanup_parts_unified(
                    db_pool,
                    fs_store,
                    redis_client,
                    pressure=pressure,
                    shard=walk_shard,
                    shards=shards,
                    walk_concurrency=walk_conc,
                    deadline=_walk_deadline(loop, pressure, budget),
                    publish_sweep=publish_sweep,
                )
                stale_count = unified["stale_mtime"]
                abandoned_count = unified["abandoned"]
                gc_count = unified["gc"]
                tmp_count = unified["tmp"]
            except Exception as e:
                logger.error(f"Unified parts cleanup error: {e}", exc_info=True)

            # Phase D: hard-delete soft-deleted objects where all unpins are confirmed (DB-bound,
            # batch-capped — cannot starve the cycle).
            try:
                _janitor_phase = 2  # soft_deleted
                hard_deleted = await gc_soft_deleted_objects(db_pool)
            except Exception as e:
                logger.error(f"Hard delete error: {e}", exc_info=True)

            _walk_shard += 1  # advance the shard for next cycle

            logger.info(
                f"Janitor cycle complete: shard={walk_shard}/{shards} publish_sweep={publish_sweep} "
                f"sql_evicted={sql_evicted} stale={stale_count} abandoned={abandoned_count} gc={gc_count} "
                f"tmp={tmp_count} hard_deleted={hard_deleted} sentinel_violations={sentinel_violations} "
                f"aged_orphans={aged_orphans}"
            )

            _janitor_cycle_seconds = time.time() - _cycle_started
            _janitor_last_cycle_completed_at = time.time()

            # Pick sleep interval based on current pressure.
            sleep_interval = sleep_pressure if pressure > 0 else sleep_normal
            logger.info(f"Janitor sleeping {sleep_interval}s (pressure={pressure})")
            _janitor_phase = 0
            await asyncio.sleep(sleep_interval)
    finally:
        for task in (queue_sampler_task, pressure_publish_task):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await cache_redis_client.close()
        if redis_client:
            await redis_client.close()
        if db_pool:
            await db_pool.close()


if __name__ == "__main__":
    run_worker(run_janitor_loop, "janitor")
