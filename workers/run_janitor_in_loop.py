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
import json
import logging
import os
import shutil
import sys
import time
import zlib
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import asyncpg
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
from hippius_s3.sentry import init_sentry
from hippius_s3.utils import get_query


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

# Disk pressure thresholds (fraction of total disk used). Enter thresholds are higher than exit
# thresholds (hysteresis) so a disk hovering at a boundary doesn't flap the mode — and with it the
# hot-retention window and loop-sleep — every cycle. C2.
PRESSURE_ELEVATED = 0.85
PRESSURE_ELEVATED_EXIT = 0.83
PRESSURE_CRITICAL = 0.95
PRESSURE_CRITICAL_EXIT = 0.93

# Maximum age of an orphan `.tmp.*` file before we delete it. Atomic writes
# finish in milliseconds; anything older than this is a crashed-write orphan.
TMP_FILE_MAX_AGE_SECONDS = 3600  # 1h
# Cap on the G2 sentinel scan: it needs only to DETECT a durability gap and sample a few
# offenders, not enumerate every one, so a bounded page keeps the read-only query cheap.
SENTINEL_SCAN_LIMIT = 500
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


def _obs_age_buckets(_: object) -> list[otel_metrics.Observation]:
    return [otel_metrics.Observation(count, {"age_bucket": bucket}) for bucket, count in _fs_age_buckets.items()]


def _obs_replication_sentinel(_: object) -> list[otel_metrics.Observation]:
    return [otel_metrics.Observation(_replication_sentinel_violations, {})]


def _obs_aged_pending_orphans(_: object) -> list[otel_metrics.Observation]:
    return [otel_metrics.Observation(_aged_pending_orphans, {})]


def _classify_age_bucket(age_seconds: float) -> str:
    for name, upper in AGE_BUCKET_BOUNDARIES:
        if age_seconds < upper:
            return name
    return "7d+"


def _pressure_mode(root: Path) -> int:
    """Return the current disk-pressure mode (0/1/2) with hysteresis.

    C2: a mode is entered at its (higher) enter threshold and only released once the disk drops
    below the (lower) exit threshold, using the previous mode. This stops a disk sitting right at
    0.85 or 0.95 from oscillating the mode — and the hot-retention window and loop sleep that key
    off it — on every cycle. The janitor is single-instance, so a module-global previous-mode is
    safe. On a stat error we hold the previous mode rather than snapping to normal.
    """
    global _prev_pressure_mode
    try:
        usage = shutil.disk_usage(root)
        ratio = usage.used / usage.total if usage.total else 0.0
    except OSError:
        return _prev_pressure_mode
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
    """Out-of-band result of a walk: whether the wall-clock budget truncated it, and how
    many object dirs (in this shard) it reached. The census is only trustworthy for a full
    (untruncated) sweep, so callers check `truncated` before publishing gauges."""

    truncated: bool = False
    objects_scanned: int = 0


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


def _descend_object(root_str: str, object_name: str) -> list[PartDirInfo]:
    """Blocking descent of ONE object dir → its part dirs, run in a walk thread.

    Mirrors the serial walk exactly: `v<n>` version dirs, `part_<n>` part dirs, stat
    `meta.json` if present else the part dir. Every FS error is swallowed to a skip — the
    tree is mutating underneath us. Returns the parts found (possibly empty)."""
    out: list[PartDirInfo] = []
    obj_path = os.path.join(root_str, object_name)  # noqa: PTH118 — hot walk path, os.* avoids per-entry Path alloc
    try:
        version_scan = os.scandir(obj_path)
    except OSError:
        return out
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
                part_scan = os.scandir(vd.path)
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
    return out


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
) -> AsyncIterator[PartDirInfo]:
    """Walk `root` and yield every part dir in the current shard, descending object dirs
    across a bounded thread pool so many CephFS metadata roundtrips are in flight at once.

    This replaces the single-threaded, event-loop-blocking `_safe_iterdir` walk that could
    not complete a pass over ~2.8M object dirs inside a cycle (measured ~40 objects/s serial
    on prod). Callers apply their own per-part gating + census to the yielded records; the
    deletion logic downstream is unchanged.

    `concurrency<=1` degrades to a serial descent (legacy behaviour, one object at a time).
    """
    concurrency = max(1, concurrency)
    root_str = str(root)
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="janitor-walk")
    inflight: set[asyncio.Future[list[PartDirInfo]]] = set()
    try:
        async for name in _stream_shard_object_names(root_str, shard, shards, deadline, state):
            inflight.add(loop.run_in_executor(executor, _descend_object, root_str, name))
            if len(inflight) >= concurrency:
                done, inflight = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
                for fut in done:
                    for info in fut.result():
                        yield info
        while inflight:
            done, inflight = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
            for fut in done:
                for info in fut.result():
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


def _update_disk_metrics(root: Path) -> None:
    """Refresh disk-usage + pressure gauges from a single statvfs.

    Called at the top of every cycle so disk visibility never depends on a full
    GC pass completing — the GC walk over millions of parts can take hours, and
    operators need to see a filling disk long before then.
    """
    global _fs_disk_used_bytes, _fs_disk_total_bytes, _fs_pressure_mode
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
    _janitor_deleted_counter = meter.create_counter(
        name="fs_janitor_deleted_total",
        description="Total number of FS parts deleted by the janitor",
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
    cutoff_sql = "NOW() - INTERVAL '1 second' * $4"

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
            if part.mtime > mtime_cutoff:
                # Recently touched, skip
                continue

            # Skip deletion if object is in DLQ
            if part.object_id in dlq_object_ids:
                logger.debug(
                    f"Skipping DLQ-protected part: object_id={part.object_id} "
                    f"v={part.object_version} part={part.part_number}"
                )
                continue

            yield (part.object_id, part.object_version, part.part_number)

    async def handle(conn: asyncpg.Connection, item: tuple[str, int, int]) -> bool:
        object_id, object_version, part_number = item

        # Decide, in one query, which of three states this part is in:
        #   row is None        → no `parts` row at all. The object's DB rows are
        #                        gone (Phase 4 hard-delete cascades parts →
        #                        part_chunks → chunk_backend) or this is orphaned
        #                        FS with no record. mtime>1d already rules out an
        #                        in-flight write (chunks land before the parts
        #                        row), so it is safe to reap → fall through.
        #   row["recent"] true → row exists and was (re)written recently → leave it.
        #   row["recent"] false→ row exists but is old → only reap once every chunk
        #                        is replicated to every required backend. A
        #                        not-yet-replicated part is a pending or aborted
        #                        upload and must be protected (no data loss) — with
        #                        ONE exception, the terminally-abandoned upload below.
        # Distinguishing "no DB row" (orphan, reap) from "DB row but not replicated"
        # (pending, protect) is what keeps deleted-object cache cleanup working
        # without ever deleting data that hasn't been backed up.
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
                    return False
                if not await is_replicated_on_all_backends(conn, object_id, object_version, part_number):
                    # Not replicated → normally protect (pending / in-flight / aborted).
                    # The ONE exception: a terminally-abandoned upload — the reaper or an
                    # abort marked the part 'failed' AND its version is unservable. The
                    # drain never re-claims a 'failed' part, so its pool bytes leak
                    # forever otherwise; an unservable version can never be served by a
                    # GET, so reclaiming is safe (see is_terminally_abandoned).
                    if not await is_terminally_abandoned(conn, object_id, object_version, part_number):
                        return False
                    abandoned = True
        except Exception:
            # If any DB check fails, be extra conservative: skip deletion
            return False

        try:
            await fs_store.delete_part(object_id, object_version, part_number)
            if abandoned:
                logger.info(
                    "Reclaimed terminally-abandoned part (failed+unservable): "
                    f"object_id={object_id} v={object_version} part={part_number}"
                )
                if _janitor_abandoned_deleted_counter is not None:
                    _janitor_abandoned_deleted_counter.add(1)
            else:
                logger.info(f"Cleaned stale part by mtime: object_id={object_id} v={object_version} part={part_number}")
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
                    try:
                        file_scan = os.scandir(pd.path)
                    except OSError:
                        continue
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

                is_hot = hot_window > 0 and atime > (now - hot_window)
                if is_hot:
                    stats["hot_parts"] += 1

                # Don't clean DLQ-protected parts (only count them for metrics)
                if is_dlq_protected:
                    continue

                # Hot files are protected. Under critical pressure
                # hot_window is 0, which forces is_hot=False, letting
                # fully-replicated parts become eligible even if recently read.
                if is_hot:
                    continue

                # A fully-replicated, cold, non-DLQ part is safe to evict.
                # Under normal pressure we additionally require age > cutoff
                # so we don't thrash. Under any pressure level we evict
                # replicated cold parts regardless of age. The replication
                # gate itself is enforced in the worker below.
                old_enough = mtime < cutoff_time
                if pressure == 0 and not old_enough:
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
            logger.info(
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
        _janitor_deleted_counter.add(parts_cleaned)

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


async def gc_soft_deleted_objects(pool: asyncpg.Pool) -> int:
    """Hard-delete objects where all backends have confirmed unpin."""
    async with pool.acquire() as db:
        rows = await db.fetch(get_query("find_objects_ready_for_hard_delete"), config.janitor_hard_delete_batch)
        deleted = 0
        skipped = 0
        for row in rows:
            try:
                # Guarded delete: re-verifies readiness atomically so a row revived
                # (re-PUT clears deleted_at + adds live chunks) between the find and
                # here is left untouched. "DELETE 0" => skipped, not deleted.
                tag = await db.execute(get_query("hard_delete_object"), row["object_id"])
                if tag == "DELETE 0":
                    skipped += 1
                    continue
                deleted += 1
                logger.info(f"Hard-deleted soft-deleted object: object_id={row['object_id']}")
            except Exception as e:
                logger.warning(f"Failed to hard-delete object {row['object_id']}: {e}")
        if skipped:
            logger.info(f"Hard-delete skipped {skipped} object(s) no longer ready (revived/in-flight)")
    return deleted


async def run_janitor_loop():
    """Main janitor loop: periodically clean stale and old parts."""
    concurrency = max(1, config.janitor_concurrency)
    db_pool = await asyncpg.create_pool(config.database_url, min_size=2, max_size=concurrency + 4)
    fs_store = create_fs_store(config)
    redis_client = Redis.from_url(config.redis_queues_url)

    # Initialize janitor-owned OTel metrics
    _setup_janitor_metrics()

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

    # Sleep intervals: shorter under disk pressure to catch up
    sleep_normal = 600  # 10m
    sleep_pressure = 120  # 2m

    global _walk_shard
    loop = asyncio.get_running_loop()

    try:
        while True:
            logger.info("Janitor cycle starting...")
            # Refresh disk/pressure gauges up front, and read pressure ONCE for the whole cycle.
            _update_disk_metrics(fs_store.root)
            pressure = _pressure_mode(fs_store.root)

            # --- DB-only DURABILITY phases run FIRST ------------------------------------------
            # These used to run LAST, behind two full-tree FS walks. cleanup_stale_parts could
            # not finish a pass over ~2.8M object dirs inside a cycle, so on prod these never ran
            # at all — the replication-gate sentinel and the A21 aged-orphan leak gauge went dark.
            # They are single indexed DB reads; nothing about the FS cache should gate them.
            sentinel_violations = 0
            try:
                sentinel_violations = await check_replication_sentinel(db_pool, pressure)
            except Exception as e:
                logger.error(f"Replication sentinel error: {e}", exc_info=True)

            # Aged-pending orphan gauge (A21 soak-gate feed). The DLQ set is gathered fresh via
            # the fail-closed get_all_dlq_object_ids and excluded so the gauge counts EXACTLY the
            # population the sweep clears; on a DLQ-read failure the whole phase is skipped (gauge
            # holds its prior value) rather than over-counting against an empty set.
            aged_orphans = 0
            try:
                gauge_dlq_object_ids = await get_all_dlq_object_ids(redis_client)
                aged_orphans = await check_aged_pending_orphans(db_pool, gauge_dlq_object_ids)
            except Exception as e:
                logger.error(f"Aged-pending orphan gauge error: {e}", exc_info=True)

            # --- FS-walk phases: sharded + budgeted so the cycle ALWAYS completes -------------
            # Each cycle covers one hash-shard of the tree; a full sweep takes `shards` cycles.
            # Under disk pressure we walk the whole tree every cycle (shards=1) and, at CRITICAL,
            # lift the wall-clock budget entirely — freeing space must never be capped by a clock.
            shards = 1 if pressure > 0 else max(1, config.janitor_walk_shards)
            walk_shard = _walk_shard % shards
            publish_sweep = walk_shard == shards - 1  # census publishes when the sweep wraps
            walk_conc = max(1, config.janitor_walk_concurrency)
            budget = config.janitor_walk_budget_seconds

            stale_count = 0
            gc_count = 0
            tmp_count = 0
            hard_deleted = 0

            # Phase A: clean stale/orphan/terminally-abandoned parts (each phase gets its OWN
            # fresh budget so the first walk can't starve the second — the bug we are fixing).
            try:
                stale_count = await cleanup_stale_parts(
                    db_pool,
                    fs_store,
                    redis_client,
                    shard=walk_shard,
                    shards=shards,
                    walk_concurrency=walk_conc,
                    deadline=_walk_deadline(loop, pressure, budget),
                )
            except Exception as e:
                logger.error(f"Stale cleanup error: {e}", exc_info=True)

            # Phase B: replication-gated age GC + census (census published only on a full sweep).
            try:
                gc_count = await cleanup_old_parts_by_mtime(
                    db_pool,
                    fs_store,
                    redis_client,
                    shard=walk_shard,
                    shards=shards,
                    walk_concurrency=walk_conc,
                    deadline=_walk_deadline(loop, pressure, budget),
                    publish_sweep=publish_sweep,
                )
            except Exception as e:
                logger.error(f"Age-GC error: {e}", exc_info=True)

            # Phase C: orphan .tmp.* files from crashed atomic writes.
            try:
                tmp_count = await cleanup_orphan_tmp_files(
                    fs_store,
                    shard=walk_shard,
                    shards=shards,
                    walk_concurrency=walk_conc,
                    deadline=_walk_deadline(loop, pressure, budget),
                )
            except Exception as e:
                logger.error(f"Tmp cleanup error: {e}", exc_info=True)

            # Phase D: hard-delete soft-deleted objects where all unpins are confirmed (DB-bound,
            # batch-capped — cannot starve the cycle).
            try:
                hard_deleted = await gc_soft_deleted_objects(db_pool)
            except Exception as e:
                logger.error(f"Hard delete error: {e}", exc_info=True)

            _walk_shard += 1  # advance the shard for next cycle

            logger.info(
                f"Janitor cycle complete: shard={walk_shard}/{shards} publish_sweep={publish_sweep} "
                f"stale={stale_count} gc={gc_count} tmp={tmp_count} hard_deleted={hard_deleted} "
                f"sentinel_violations={sentinel_violations} aged_orphans={aged_orphans}"
            )

            # Pick sleep interval based on current pressure.
            sleep_interval = sleep_pressure if pressure > 0 else sleep_normal
            logger.info(f"Janitor sleeping {sleep_interval}s (pressure={pressure})")
            await asyncio.sleep(sleep_interval)
    finally:
        if redis_client:
            await redis_client.close()
        if db_pool:
            await db_pool.close()


if __name__ == "__main__":
    asyncio.run(run_janitor_loop())
