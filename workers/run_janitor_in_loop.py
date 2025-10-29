#!/usr/bin/env python3
"""Janitor task to clean up stale filesystem parts from aborted multipart uploads.

This background task runs periodically to identify and remove parts from the FS store
that belong to stale or abandoned multipart uploads, preventing disk bloat from
incomplete uploads that were never finalized or cleaned up.
"""

import asyncio
import logging
import sys
import time
from pathlib import Path

import asyncpg


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.cache import FileSystemPartsStore
from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.monitoring import initialize_metrics_collector


config = get_config()
setup_loki_logging(config, "janitor")
logger = logging.getLogger(__name__)


async def cleanup_stale_parts(db: asyncpg.Connection, fs_store: FileSystemPartsStore) -> int:
    """Conservative cleanup of stale parts: rely on FS mtime only for now.

    Rationale: DB schemas for tracking MPU progress vary across deployments.
    To avoid accidental deletion of active uploads, we prefer a conservative
    approach: remove only parts whose meta/dir mtime is older than the
    configured stale threshold and which have no recent DB part activity.
    """
    stale_threshold_seconds = config.mpu_stale_seconds
    cutoff_sql = "NOW() - INTERVAL '1 second' * $1"

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

                import time as _t

                if mtime > (_t.time() - stale_threshold_seconds):
                    # Recently touched, skip
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


async def cleanup_old_parts_by_mtime(fs_store: FileSystemPartsStore) -> int:
    """Clean up old parts from FS based on modification time (backup GC).

    This is a safety net to remove orphaned parts that weren't caught by the
    stale MPU cleanup (e.g., parts without DB entries).

    Args:
        fs_store: FileSystemPartsStore instance

    Returns:
        Number of parts cleaned up
    """
    max_age_seconds = config.fs_cache_gc_max_age_seconds
    logger.info(f"Scanning for orphaned FS parts older than {max_age_seconds}s")

    root = fs_store.root
    if not root.exists():
        return 0

    parts_cleaned = 0
    cutoff_time = time.time() - max_age_seconds

    # Walk the FS hierarchy: <root>/<object_id>/v<version>/part_<n>/
    oldest_mtime = None
    parts_seen = 0
    for object_dir in root.iterdir():
        if not object_dir.is_dir():
            continue

        for version_dir in object_dir.iterdir():
            if not version_dir.is_dir() or not version_dir.name.startswith("v"):
                continue

            for part_dir in version_dir.iterdir():
                if not part_dir.is_dir() or not part_dir.name.startswith("part_"):
                    continue
                parts_seen += 1

                # Check mtime of meta.json (if present) or the directory itself
                meta_file = part_dir / "meta.json"
                check_path = meta_file if meta_file.exists() else part_dir

                try:
                    mtime = check_path.stat().st_mtime
                    if oldest_mtime is None or mtime < oldest_mtime:
                        oldest_mtime = mtime
                    if mtime < cutoff_time:
                        # Old enough to clean
                        import shutil

                        shutil.rmtree(part_dir)
                        parts_cleaned += 1
                        logger.info(f"GC cleaned old part: {part_dir} (mtime age={(time.time() - mtime) / 3600:.1f}h)")

                        # Try to prune empty parents
                        try:
                            version_dir.rmdir()
                            object_dir.rmdir()
                        except OSError:
                            pass  # Not empty; ignore
                except Exception as e:
                    logger.warning(f"Failed to clean part {part_dir}: {e}")

    # Record metrics
    try:
        collector = get_metrics_collector()
        age_seconds = max(0.0, time.time() - float(oldest_mtime)) if oldest_mtime is not None else 0.0
        collector.set_fs_store_oldest_age_seconds(age_seconds)  # type: ignore[attr-defined]
        collector.set_fs_store_parts_on_disk(parts_seen)  # type: ignore[attr-defined]
        if parts_cleaned > 0:
            collector.fs_janitor_deleted_total.add(parts_cleaned)  # type: ignore[attr-defined]
    except Exception:
        logger.debug("Failed to record FS metrics", exc_info=True)

    logger.info(f"GC cleaned {parts_cleaned} orphaned parts by mtime")
    return parts_cleaned


async def run_janitor_loop():
    """Main janitor loop: periodically clean stale and old parts."""
    db = await asyncpg.connect(config.database_url)
    fs_store = FileSystemPartsStore(config.object_cache_dir)

    # Initialize metrics
    try:
        initialize_metrics_collector(None)  # type: ignore[arg-type]
    except Exception:
        logger.debug("Metrics initialization failed; continuing without metrics", exc_info=True)

    logger.info("Starting janitor service...")
    logger.info(f"Database: {config.database_url}")
    logger.info(f"FS store root: {config.object_cache_dir}")
    logger.info(f"MPU stale threshold: {config.mpu_stale_seconds}s")
    logger.info(f"FS GC max age: {config.fs_cache_gc_max_age_seconds}s")

    # Run immediately on start, then periodically
    sleep_interval = 3600  # 1 hour

    while True:
        try:
            logger.info("Janitor cycle starting...")

            # Phase 1: Clean stale MPU parts
            stale_count = await cleanup_stale_parts(db, fs_store)

            # Phase 2: GC old parts by mtime (backup safety net)
            gc_count = await cleanup_old_parts_by_mtime(fs_store)

            logger.info(f"Janitor cycle complete: stale={stale_count} gc={gc_count}")

        except Exception as e:
            logger.error(f"Janitor cycle error: {e}", exc_info=True)

        logger.info(f"Janitor sleeping {sleep_interval}s until next cycle...")
        await asyncio.sleep(sleep_interval)


if __name__ == "__main__":
    asyncio.run(run_janitor_loop())
