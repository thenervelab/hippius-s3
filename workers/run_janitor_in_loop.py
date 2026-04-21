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
import socket
import sys
import time
from pathlib import Path

import asyncpg
from opentelemetry import metrics as otel_metrics
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from redis.asyncio import Redis


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.cache import FileSystemPartsStore
from hippius_s3.cache import create_fs_store
from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
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

# Disk pressure thresholds (fraction of total disk used).
PRESSURE_ELEVATED = 0.85
PRESSURE_CRITICAL = 0.95

# Maximum age of an orphan `.tmp.*` file before we delete it. Atomic writes
# finish in milliseconds; anything older than this is a crashed-write orphan.
TMP_FILE_MAX_AGE_SECONDS = 3600  # 1h

_fs_parts_on_disk = 0
_fs_oldest_age_seconds = 0.0
_fs_disk_used_bytes = 0
_fs_disk_total_bytes = 0
_fs_hot_parts = 0
_fs_pressure_mode = 0  # 0 = normal, 1 = elevated, 2 = critical
_fs_age_buckets: dict[str, int] = dict.fromkeys(AGE_BUCKET_NAMES, 0)

_janitor_deleted_counter = None  # set by _setup_janitor_metrics
_janitor_tmp_deleted_counter = None


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


def _classify_age_bucket(age_seconds: float) -> str:
    for name, upper in AGE_BUCKET_BOUNDARIES:
        if age_seconds < upper:
            return name
    return "7d+"


def _pressure_mode(root: Path) -> int:
    """Return the current disk-pressure mode (0/1/2)."""
    try:
        usage = shutil.disk_usage(root)
        ratio = usage.used / usage.total if usage.total else 0.0
    except OSError:
        return 0
    if ratio >= PRESSURE_CRITICAL:
        return 2
    if ratio >= PRESSURE_ELEVATED:
        return 1
    return 0


def _effective_hot_retention(mode: int) -> float:
    """Effective hot-retention window (seconds) given pressure mode."""
    base = float(getattr(config, "fs_cache_hot_retention_seconds", 10800))
    if mode == 1:
        return base / 2
    if mode == 2:
        return 0.0  # disable hot retention under critical pressure
    return base


def _setup_janitor_metrics() -> None:
    global _janitor_deleted_counter, _janitor_tmp_deleted_counter

    if os.getenv("ENABLE_MONITORING", "false").lower() not in ("true", "1", "yes"):
        logger.info("Monitoring disabled for janitor")
        return

    # If auto-instrumentation already set a MeterProvider, use it.
    # Only create our own if none exists.
    existing = otel_metrics.get_meter_provider()
    if not isinstance(existing, MeterProvider):
        endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
        service_name = os.getenv("OTEL_SERVICE_NAME", "hippius-s3")

        resource = Resource.create({"service.name": service_name, "service.instance.id": socket.gethostname()})
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


async def get_all_dlq_object_ids(redis_client: Redis) -> set[str]:
    """Fetch all object_ids currently in both upload and unpin DLQs.

    Returns:
        Set of object_id strings present in any DLQ
    """
    object_ids = set()

    # Enumerate DLQ keys dynamically from the configured upload backends so
    # a new backend (e.g. ipfs) doesn't silently bypass protection.
    dlq_keys = [f"{b}_upload_requests:dlq" for b in config.upload_backends]
    dlq_keys.append("unpin_requests:dlq")

    for dlq_key in dlq_keys:
        try:
            dlq_entries = await asyncio.wait_for(redis_client.lrange(dlq_key, 0, -1), timeout=5.0)
            for entry_json in dlq_entries:
                try:
                    entry = json.loads(entry_json)
                    if obj_id := entry.get("object_id"):
                        object_ids.add(str(obj_id))
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON in {dlq_key}: {entry_json[:100]}")
        except asyncio.TimeoutError:
            logger.error(f"{dlq_key} fetch timeout (5s)")
        except Exception as e:
            logger.error(f"Failed to fetch {dlq_key} object_ids: {e}")

    if object_ids:
        logger.info(f"Found {len(object_ids)} unique object_ids protected across all DLQs")
    return object_ids


async def cleanup_stale_parts(
    db: asyncpg.Connection,
    fs_store: FileSystemPartsStore,
    redis_client: Redis,
) -> int:
    """Conservative cleanup of stale parts: rely on FS mtime only for now.

    Rationale: DB schemas for tracking MPU progress vary across deployments.
    To avoid accidental deletion of active uploads, we prefer a conservative
    approach: remove only parts whose meta/dir mtime is older than the
    configured stale threshold and which have no recent DB part activity.
    """
    stale_threshold_seconds = config.mpu_stale_seconds
    cutoff_sql = "NOW() - INTERVAL '1 second' * $4"

    dlq_object_ids = await get_all_dlq_object_ids(redis_client)
    if dlq_object_ids:
        logger.info(f"Protecting {len(dlq_object_ids)} DLQ objects from stale cleanup")

    parts_cleaned = 0
    root = fs_store.root
    if not root.exists():
        return 0

    for object_dir in root.iterdir():
        if not object_dir.is_dir():
            continue
        for version_dir in object_dir.iterdir():
            if not version_dir.is_dir() or not version_dir.name.startswith("v"):
                continue
            try:
                object_id = object_dir.name
                object_version = int(version_dir.name[1:])
            except Exception:
                continue

            for part_dir in version_dir.iterdir():
                if not part_dir.is_dir() or not part_dir.name.startswith("part_"):
                    continue
                try:
                    part_number = int(part_dir.name.split("_")[1])
                except Exception:
                    continue

                meta_file = part_dir / "meta.json"
                check_path = meta_file if meta_file.exists() else part_dir
                try:
                    mtime = check_path.stat().st_mtime
                except Exception:
                    continue

                if mtime > (time.time() - stale_threshold_seconds):
                    # Recently touched, skip
                    continue

                # Skip deletion if object is in DLQ
                if object_id in dlq_object_ids:
                    logger.debug(
                        f"Skipping DLQ-protected part: object_id={object_id} v={object_version} part={part_number}"
                    )
                    continue

                # Cross-check DB: skip deletion if there was recent part activity
                try:
                    recent = await db.fetchval(
                        """
                        SELECT 1
                        FROM parts
                        WHERE object_id = $1 AND object_version = $2 AND part_number = $3
                          AND uploaded_at > """
                        + cutoff_sql
                        + " LIMIT 1",
                        object_id,
                        object_version,
                        part_number,
                        stale_threshold_seconds,
                    )
                    if recent:
                        continue
                except Exception:
                    # If DB check fails, be extra conservative: skip deletion
                    continue

                try:
                    await fs_store.delete_part(object_id, object_version, part_number)
                    parts_cleaned += 1
                    logger.info(
                        f"Cleaned stale part by mtime: object_id={object_id} v={object_version} part={part_number}"
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to clean part: object_id={object_id} v={object_version} part={part_number}: {e}"
                    )

    logger.info(f"Janitor cleaned {parts_cleaned} stale parts by mtime threshold")
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


async def cleanup_orphan_tmp_files(fs_store: FileSystemPartsStore) -> int:
    """Remove orphan atomic-write temp files that outlived a crashed worker.

    Workers use `<target>.tmp.<uuid>` as the tempfile for atomic rename.
    If the worker crashes between creating the tempfile and renaming it,
    the tempfile is left behind. Delete anything named `*.tmp.*` older than
    `TMP_FILE_MAX_AGE_SECONDS`.
    """
    root = fs_store.root
    if not root.exists():
        return 0

    now = time.time()
    cutoff = now - TMP_FILE_MAX_AGE_SECONDS
    removed = 0

    # rglob is fine; the FS is bounded by active objects
    for path in root.rglob("*.tmp.*"):
        try:
            if not path.is_file():
                continue
            if path.stat().st_mtime > cutoff:
                continue
            path.unlink()
            removed += 1
            logger.info(f"Removed orphan tmp file: {path}")
        except OSError as e:
            logger.debug(f"Skip orphan tmp {path}: {e}")

    if removed > 0 and _janitor_tmp_deleted_counter is not None:
        _janitor_tmp_deleted_counter.add(removed)
    if removed > 0:
        logger.info(f"Janitor removed {removed} orphan tmp files")
    return removed


async def cleanup_old_parts_by_mtime(
    db: asyncpg.Connection,
    fs_store: FileSystemPartsStore,
    redis_client: Redis,
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
    """
    max_age_seconds = config.fs_cache_gc_max_age_seconds
    logger.info(f"Scanning FS parts eligible for GC (max_age={max_age_seconds}s, replication-gated)")

    dlq_object_ids = await get_all_dlq_object_ids(redis_client)
    if dlq_object_ids:
        logger.info(f"Protecting {len(dlq_object_ids)} DLQ objects from GC")

    root = fs_store.root
    if not root.exists():
        return 0

    parts_cleaned = 0
    cutoff_time = time.time() - max_age_seconds
    pressure = _pressure_mode(root)
    hot_window = _effective_hot_retention(pressure)
    if pressure > 0:
        logger.warning(
            f"Disk pressure={pressure} ({'elevated' if pressure == 1 else 'critical'}); hot_window={hot_window}s"
        )

    global _fs_parts_on_disk, _fs_oldest_age_seconds, _fs_disk_used_bytes
    global _fs_disk_total_bytes, _fs_age_buckets, _fs_hot_parts, _fs_pressure_mode

    # Walk the FS hierarchy: <root>/<object_id>/v<version>/part_<n>/
    oldest_mtime = None
    parts_seen = 0
    hot_parts = 0
    age_counts: dict[str, int] = dict.fromkeys(AGE_BUCKET_NAMES, 0)
    now = time.time()

    for object_dir in root.iterdir():
        if not object_dir.is_dir():
            continue

        object_id = object_dir.name
        is_dlq_protected = object_id in dlq_object_ids

        for version_dir in object_dir.iterdir():
            if not version_dir.is_dir() or not version_dir.name.startswith("v"):
                continue

            try:
                object_version = int(version_dir.name[1:])
            except (ValueError, IndexError):
                continue

            for part_dir in version_dir.iterdir():
                if not part_dir.is_dir() or not part_dir.name.startswith("part_"):
                    continue
                parts_seen += 1

                try:
                    part_number = int(part_dir.name.split("_")[1])
                except (ValueError, IndexError):
                    continue

                # Check mtime (for age) and atime (for hot retention)
                meta_file = part_dir / "meta.json"
                check_path = meta_file if meta_file.exists() else part_dir

                try:
                    stat = check_path.stat()
                    mtime = stat.st_mtime
                    atime = stat.st_atime
                    if oldest_mtime is None or mtime < oldest_mtime:
                        oldest_mtime = mtime

                    part_age = now - mtime
                    age_counts[_classify_age_bucket(part_age)] += 1

                    is_hot = hot_window > 0 and atime > (now - hot_window)
                    if is_hot:
                        hot_parts += 1

                    # Don't clean DLQ-protected parts (only count them for metrics)
                    if is_dlq_protected:
                        continue

                    # Hot files are protected. Under critical pressure
                    # hot_window is 0, which forces is_hot=False, letting
                    # fully-replicated parts become eligible even if recently read.
                    if is_hot:
                        continue

                    # ABSOLUTE safety gate: never delete non-replicated data.
                    fully_replicated = await is_replicated_on_all_backends(
                        db,
                        object_id,
                        object_version,
                        part_number,
                    )
                    if not fully_replicated:
                        continue

                    # A fully-replicated, cold, non-DLQ part is safe to evict.
                    # Under normal pressure we additionally require age > cutoff
                    # so we don't thrash. Under any pressure level we evict
                    # replicated cold parts regardless of age.
                    old_enough = mtime < cutoff_time
                    if pressure == 0 and not old_enough:
                        continue

                    shutil.rmtree(part_dir)
                    parts_cleaned += 1
                    age = (now - mtime) / 3600
                    logger.info(
                        f"GC cleaned part: {part_dir} replicated=True "
                        f"pressure={pressure} {old_enough=} (mtime {age=:.1f}h)"
                    )

                    # Try to prune empty parents (idempotent; ignore if not empty)
                    with contextlib.suppress(OSError):
                        version_dir.rmdir()
                    with contextlib.suppress(OSError):
                        object_dir.rmdir()
                except Exception as e:
                    logger.warning(f"Failed to clean part {part_dir}: {e}")

    # Update module-level metric variables (read by OTel observable gauge callbacks)
    _fs_parts_on_disk = parts_seen
    _fs_oldest_age_seconds = max(0.0, now - float(oldest_mtime)) if oldest_mtime is not None else 0.0
    _fs_age_buckets = age_counts
    _fs_hot_parts = hot_parts
    _fs_pressure_mode = pressure

    # Disk usage
    disk = shutil.disk_usage(root)
    _fs_disk_used_bytes = disk.used
    _fs_disk_total_bytes = disk.total

    if parts_cleaned > 0 and _janitor_deleted_counter is not None:
        _janitor_deleted_counter.add(parts_cleaned)

    logger.info(f"GC cleaned {parts_cleaned=} hot_parts={hot_parts} pressure={pressure}")

    # If we're under critical disk pressure but couldn't free any space,
    # every on-disk part is either hot (ignored under critical — nothing to
    # free) or non-replicated (we refuse to delete). Page the operator.
    if pressure == 2 and parts_cleaned == 0 and parts_seen > 0:
        logger.error(
            "JANITOR_CRITICAL_PRESSURE_BLOCKED parts_seen=%d hot_parts=%d — "
            "disk is >=95%% full but all remaining parts are non-replicated. "
            "Operator action required; refusing to delete unreplicated data.",
            parts_seen,
            hot_parts,
        )

    return parts_cleaned


async def gc_soft_deleted_objects(db: asyncpg.Connection) -> int:
    """Hard-delete objects where all backends have confirmed unpin."""
    rows = await db.fetch(get_query("find_objects_ready_for_hard_delete"))
    deleted = 0
    for row in rows:
        try:
            await db.execute("DELETE FROM objects WHERE object_id = $1", row["object_id"])
            deleted += 1
            logger.info(f"Hard-deleted soft-deleted object: object_id={row['object_id']}")
        except Exception as e:
            logger.warning(f"Failed to hard-delete object {row['object_id']}: {e}")
    return deleted


async def run_janitor_loop():
    """Main janitor loop: periodically clean stale and old parts."""
    db = await asyncpg.connect(config.database_url)
    fs_store = create_fs_store(config)
    redis_client = Redis.from_url(config.redis_queues_url)

    # Initialize janitor-owned OTel metrics
    _setup_janitor_metrics()

    logger.info("Starting janitor service...")
    logger.info(f"FS store root: {config.object_cache_dir}")
    logger.info(f"MPU stale threshold: {config.mpu_stale_seconds}s")
    logger.info(f"FS GC max age: {config.fs_cache_gc_max_age_seconds}s")
    logger.info(f"FS hot retention: {getattr(config, 'fs_cache_hot_retention_seconds', 10800)}s")

    # Sleep intervals: shorter under disk pressure to catch up
    sleep_normal = 600  # 10m
    sleep_pressure = 120  # 2m

    try:
        while True:
            logger.info("Janitor cycle starting...")
            stale_count = 0
            gc_count = 0
            hard_deleted = 0
            tmp_count = 0

            # Phase 1: Clean stale MPU parts (aborted uploads)
            try:
                stale_count = await cleanup_stale_parts(db, fs_store, redis_client)
            except Exception as e:
                logger.error(f"Phase 1 (stale cleanup) error: {e}", exc_info=True)

            # Phase 2: GC old parts by mtime + update disk/cache metrics
            try:
                gc_count = await cleanup_old_parts_by_mtime(db, fs_store, redis_client)
            except Exception as e:
                logger.error(f"Phase 2 (GC) error: {e}", exc_info=True)

            # Phase 3: Clean orphan .tmp.* files from crashed atomic writes
            try:
                tmp_count = await cleanup_orphan_tmp_files(fs_store)
            except Exception as e:
                logger.error(f"Phase 3 (tmp cleanup) error: {e}", exc_info=True)

            # Phase 4: Hard-delete soft-deleted objects where all unpins are confirmed
            try:
                hard_deleted = await gc_soft_deleted_objects(db)
            except Exception as e:
                logger.error(f"Phase 4 (hard delete) error: {e}", exc_info=True)

            logger.info(
                f"Janitor cycle complete: stale={stale_count} gc={gc_count} tmp={tmp_count} hard_deleted={hard_deleted}"
            )

            # Pick sleep interval based on current pressure
            pressure = _pressure_mode(fs_store.root)
            sleep_interval = sleep_pressure if pressure > 0 else sleep_normal
            logger.info(f"Janitor sleeping {sleep_interval}s (pressure={pressure})")
            await asyncio.sleep(sleep_interval)
    finally:
        if redis_client:
            await redis_client.close()
        if db:
            await db.close()


if __name__ == "__main__":
    asyncio.run(run_janitor_loop())
