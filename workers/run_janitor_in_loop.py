#!/usr/bin/env python3
"""Janitor task to clean up stale filesystem parts from aborted multipart uploads.

This background task runs periodically to identify and remove parts from the FS store
that belong to stale or abandoned multipart uploads, preventing disk bloat from
incomplete uploads that were never finalized or cleaned up.
"""

import asyncio
import json
import logging
import sys
import time
from pathlib import Path

import asyncpg
import redis.asyncio as async_redis


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.cache import FileSystemPartsStore
from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.monitoring import initialize_metrics_collector


config = get_config()
setup_loki_logging(config, "janitor", include_ray_id=False)
logger = logging.getLogger(__name__)


async def get_all_dlq_object_ids(redis_client: async_redis.Redis) -> set[str]:
    """Fetch all object_ids currently in both upload and unpin DLQs.

    Returns:
        Set of object_id strings present in any DLQ
    """
    object_ids = set()

    for dlq_key in ["upload_requests:dlq", "unpin_requests:dlq"]:
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
    db: asyncpg.Connection, fs_store: FileSystemPartsStore, redis_client: async_redis.Redis
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

                import time as _t

                if mtime > (_t.time() - stale_threshold_seconds):
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


async def cleanup_old_parts_by_mtime(fs_store: FileSystemPartsStore, redis_client: async_redis.Redis) -> int:
    """Clean up old parts from FS based on modification time (backup GC).

    This is a safety net to remove orphaned parts that weren't caught by the
    stale MPU cleanup (e.g., parts without DB entries).

    Args:
        fs_store: FileSystemPartsStore instance
        redis_client: Redis client for DLQ checks

    Returns:
        Number of parts cleaned up
    """
    max_age_seconds = config.fs_cache_gc_max_age_seconds
    logger.info(f"Scanning for orphaned FS parts older than {max_age_seconds}s")

    dlq_object_ids = await get_all_dlq_object_ids(redis_client)
    if dlq_object_ids:
        logger.info(f"Protecting {len(dlq_object_ids)} DLQ objects from GC")

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

        object_id = object_dir.name

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
                        # Skip deletion if object is in DLQ
                        if object_id in dlq_object_ids:
                            logger.debug(f"Skipping DLQ-protected part (GC): {part_dir}")
                            continue

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


async def cleanup_uploaded_chunks(
    db: asyncpg.Connection,
    fs_store: FileSystemPartsStore,
    redis_client: async_redis.Redis,
) -> int:
    """Clean up chunks based on storage backend upload status.

    Deletion criteria:
    - storage_backends_uploaded >= total_number_of_storage_backends (fully replicated), OR
    - age > 7 days AND not in DLQ

    Deletes individual chunk_X.bin files, prunes empty directories.
    """
    logger.info("Starting uploaded chunks cleanup...")

    dlq_object_ids = await get_all_dlq_object_ids(redis_client)
    if dlq_object_ids:
        logger.info(f"Protecting {len(dlq_object_ids)} DLQ objects")

    chunks_cleaned = 0

    # Query chunks eligible for cleanup
    rows = await db.fetch(
        """
        SELECT
            pc.part_id,
            pc.chunk_index,
            p.object_id,
            p.object_version,
            p.part_number,
            pc.storage_backends_uploaded,
            pc.created_at
        FROM part_chunks pc
        JOIN parts p ON pc.part_id = p.part_id
        WHERE
            pc.storage_backends_uploaded >= $1
            OR pc.created_at < NOW() - INTERVAL '7 days'
        ORDER BY pc.created_at ASC
        LIMIT 10000
        """,
        config.total_number_of_storage_backends,
    )

    if not rows:
        logger.info("No uploaded chunks eligible for cleanup")
        return 0

    logger.info(f"Found {len(rows)} chunks eligible for cleanup")

    for row in rows:
        object_id = str(row["object_id"])
        if row["object_version"] is None:
            logger.warning(f"Skipping chunk with NULL object_version: object_id={object_id}")
            continue
        object_version = int(row["object_version"])
        part_number = int(row["part_number"])
        chunk_index = int(row["chunk_index"])
        backends_count = int(row["storage_backends_uploaded"])

        # Skip if object is in DLQ
        if object_id in dlq_object_ids:
            logger.debug(
                f"Skipping DLQ-protected chunk: {object_id} v={object_version} part={part_number} chunk={chunk_index}"
            )
            continue

        # Delete individual chunk file
        part_dir = Path(fs_store.part_path(object_id, object_version, part_number))
        chunk_file = part_dir / f"chunk_{chunk_index}.bin"

        if not chunk_file.exists():
            continue

        try:
            chunk_file.unlink()
            chunks_cleaned += 1
            logger.info(
                f"Cleaned chunk: {object_id} v={object_version} part={part_number} "
                f"chunk={chunk_index} backends={backends_count}"
            )

            # Prune empty directories after all chunks deleted
            remaining_chunks = list(part_dir.glob("chunk_*.bin"))
            if not remaining_chunks:
                meta_file = part_dir / "meta.json"
                if meta_file.exists():
                    meta_file.unlink()

                try:
                    part_dir.rmdir()
                    logger.info(f"Removed empty part directory: {part_dir}")

                    # Prune parent directories
                    version_dir = part_dir.parent
                    if version_dir.exists() and not list(version_dir.iterdir()):
                        version_dir.rmdir()

                        object_dir = version_dir.parent
                        if object_dir.exists() and not list(object_dir.iterdir()):
                            object_dir.rmdir()
                except OSError:
                    pass  # Directory not empty or race condition

        except Exception as e:
            logger.warning(
                f"Failed to clean chunk {object_id} v={object_version} part={part_number} chunk={chunk_index}: {e}"
            )

    logger.info(f"Cleaned {chunks_cleaned} uploaded chunks")
    return chunks_cleaned


async def run_janitor_loop():
    """Main janitor loop: periodically clean stale and old parts."""
    db = await asyncpg.connect(config.database_url)
    fs_store = FileSystemPartsStore(config.object_cache_dir)
    redis_client = async_redis.from_url(config.redis_queues_url)

    # Initialize metrics
    try:
        initialize_metrics_collector(None)  # type: ignore[arg-type]
    except Exception:
        logger.debug("Metrics initialization failed; continuing without metrics", exc_info=True)

    logger.info("Starting janitor service...")
    logger.info(f"Database: {config.database_url}")
    logger.info(f"Redis (for DLQ checks): {config.redis_queues_url}")
    logger.info(f"FS store root: {config.object_cache_dir}")
    logger.info(f"MPU stale threshold: {config.mpu_stale_seconds}s")
    logger.info(f"FS GC max age: {config.fs_cache_gc_max_age_seconds}s")

    # Run immediately on start, then periodically
    sleep_interval = 600  # 10m

    try:
        while True:
            try:
                logger.info("Janitor cycle starting...")

                # Phase 1: Clean uploaded chunks (primary cleanup path)
                uploaded_count = await cleanup_uploaded_chunks(db, fs_store, redis_client)

                # Phase 2: Clean stale MPU parts (aborted uploads)
                stale_count = await cleanup_stale_parts(db, fs_store, redis_client)

                # Phase 3: GC old parts by mtime (safety net for pre-migration chunks)
                gc_count = await cleanup_old_parts_by_mtime(fs_store, redis_client)

                logger.info(f"Janitor cycle complete: uploaded={uploaded_count} stale={stale_count} gc={gc_count}")

            except Exception as e:
                logger.error(f"Janitor cycle error: {e}", exc_info=True)

            logger.info(f"Janitor sleeping {sleep_interval}s until next cycle...")
            await asyncio.sleep(sleep_interval)
    finally:
        if redis_client:
            await redis_client.close()
        if db:
            await db.close()


if __name__ == "__main__":
    asyncio.run(run_janitor_loop())
