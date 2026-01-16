"""Filesystem cleanup utilities for removing parts after successful pin confirmation.

DEPRECATED: This module is being phased out in favor of chunk-level cleanup in janitor.
The cleanup_pinned_object_parts() function should no longer be called from uploader.
Cleanup is now handled by janitor based on storage_backends_uploaded counter.

This module remains for backward compatibility and manual cleanup operations.
"""

import logging
from typing import Any

from hippius_s3.cache import FileSystemPartsStore


logger = logging.getLogger(__name__)


async def cleanup_pinned_object_parts(
    fs_store: FileSystemPartsStore,
    db: Any,
    object_id: str,
    object_version: int,
) -> None:
    """Delete all parts for an object/version from FS after confirming all chunks are pinned.

    DEPRECATED: This function should no longer be called. Cleanup is now handled by janitor
    based on storage_backends_uploaded counter.

    This is called by the substrate pinner after successful pin submission and verification.
    It verifies that all chunk CIDs for all parts in the object are present in the DB
    (indicating successful upload and pinning), then deletes each part directory.

    Args:
        fs_store: FileSystemPartsStore instance
        db: Database connection
        object_id: Object UUID
        object_version: Object version number
    """
    logger.warning(
        f"cleanup_pinned_object_parts() is deprecated; cleanup handled by janitor. "
        f"object_id={object_id} v={object_version}"
    )
    try:
        # Fetch all parts for this object/version
        rows = await db.fetch(
            """
            SELECT p.part_number, COUNT(pc.chunk_index) as chunk_count
            FROM parts p
            LEFT JOIN part_chunks pc ON p.part_id = pc.part_id
            WHERE p.object_id = $1 AND p.object_version = $2
            GROUP BY p.part_number
            ORDER BY p.part_number
            """,
            object_id,
            object_version,
        )

        if not rows:
            logger.debug(f"No parts found for object_id={object_id} v={object_version}, skipping cleanup")
            return

        parts_deleted = 0
        for row in rows:
            part_number = int(row["part_number"])
            chunk_count = int(row["chunk_count"] or 0)

            # Only delete if we have chunk CIDs recorded (indicates successful upload)
            if chunk_count > 0:
                # Additional verification: check that all chunks have CIDs
                cid_check = await db.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM part_chunks pc
                    WHERE pc.part_id = (SELECT part_id FROM parts WHERE object_id = $1 AND object_version = $2 AND part_number = $3)
                      AND (pc.cid IS NULL OR pc.cid = '' OR pc.cid = 'pending')
                    """,
                    object_id,
                    object_version,
                    part_number,
                )

                if cid_check and int(cid_check) > 0:
                    logger.warning(
                        f"Part has unpinned chunks, skipping FS cleanup: object_id={object_id} v={object_version} part={part_number}"
                    )
                    continue

                # Safe to delete: all chunks have CIDs
                await fs_store.delete_part(object_id, object_version, part_number)
                parts_deleted += 1
                logger.info(f"Cleaned up FS part: object_id={object_id} v={object_version} part={part_number}")
            else:
                logger.debug(
                    f"Part has no chunks recorded, skipping cleanup: object_id={object_id} v={object_version} part={part_number}"
                )

        if parts_deleted > 0:
            logger.info(f"FS cleanup complete: object_id={object_id} v={object_version} deleted {parts_deleted} parts")

    except Exception as e:
        logger.error(f"FS cleanup failed for object_id={object_id} v={object_version}: {e}", exc_info=True)


async def cleanup_pinned_part(
    fs_store: FileSystemPartsStore,
    db: Any,
    object_id: str,
    object_version: int,
    part_number: int,
) -> None:
    """Delete a single part from FS after confirming all its chunks are pinned.

    This is a more granular version for per-part cleanup after individual part pin confirmation.

    Args:
        fs_store: FileSystemPartsStore instance
        db: Database connection
        object_id: Object UUID
        object_version: Object version number
        part_number: Part number
    """
    try:
        # Check that all chunks for this part have CIDs (not pending/null)
        unpinned_count = await db.fetchval(
            """
            SELECT COUNT(*)
            FROM part_chunks pc
            WHERE pc.part_id = (SELECT part_id FROM parts WHERE object_id = $1 AND object_version = $2 AND part_number = $3)
              AND (pc.cid IS NULL OR pc.cid = '' OR pc.cid = 'pending')
            """,
            object_id,
            object_version,
            part_number,
        )

        if unpinned_count and int(unpinned_count) > 0:
            logger.debug(
                f"Part has {unpinned_count} unpinned chunks, skipping FS cleanup: object_id={object_id} v={object_version} part={part_number}"
            )
            return

        # Safe to delete
        await fs_store.delete_part(object_id, object_version, part_number)
        logger.info(f"Cleaned up FS part: object_id={object_id} v={object_version} part={part_number}")

    except Exception as e:
        logger.error(
            f"FS cleanup failed for part: object_id={object_id} v={object_version} part={part_number}: {e}",
            exc_info=True,
        )
