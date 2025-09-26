"""
DLQ Storage Module

Handles filesystem operations for DLQ chunk persistence.
"""

import logging
import shutil
from pathlib import Path
from typing import List
from typing import Optional
from uuid import UUID

from hippius_s3.config import get_config


logger = logging.getLogger(__name__)


class DLQStorage:
    """Filesystem operations for DLQ chunk persistence."""

    def __init__(self, dlq_dir: Optional[str] = None, archive_dir: Optional[str] = None):
        config = get_config()
        self.dlq_dir = Path(dlq_dir or str(getattr(config, "dlq_dir", "/tmp/hippius_dlq")))
        self.archive_dir = Path(archive_dir or str(getattr(config, "dlq_archive_dir", "/tmp/hippius_dlq_archive")))

        # Ensure directories exist
        self.dlq_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)

    def _safe_id(self, s: str) -> str:
        """Validate object_id as UUID to prevent path traversal."""
        return str(UUID(s))

    def save_chunk(self, object_id: str, part_number: int, chunk_data: bytes) -> None:
        """Save chunk bytes to filesystem under DLQ directory."""
        object_dir = self.dlq_dir / self._safe_id(object_id)
        object_dir.mkdir(parents=True, exist_ok=True)

        part_file = object_dir / f"part_{part_number}"
        try:
            with part_file.open("wb") as f:
                f.write(chunk_data)
            logger.debug(f"Saved chunk to DLQ: {part_file}, size={len(chunk_data)}")
        except Exception as e:
            logger.error(f"Failed to save chunk to DLQ: {part_file}, error={e}")
            raise

    def load_chunk(self, object_id: str, part_number: int) -> Optional[bytes]:
        """Load chunk bytes from filesystem."""
        part_file = self.dlq_dir / self._safe_id(object_id) / f"part_{part_number}"
        try:
            with part_file.open("rb") as f:
                return f.read()
        except FileNotFoundError:
            logger.warning(f"Chunk not found in DLQ: {part_file}")
            return None
        except Exception as e:
            logger.error(f"Failed to load chunk from DLQ: {part_file}, error={e}")
            raise

    def list_chunks(self, object_id: str) -> List[int]:
        """List available part numbers for an object in DLQ."""
        object_dir = self.dlq_dir / self._safe_id(object_id)
        if not object_dir.exists():
            return []

        part_numbers = []
        try:
            for item in object_dir.iterdir():
                if item.is_file() and item.name.startswith("part_"):
                    try:
                        part_num = int(item.name.split("_", 1)[1])
                        part_numbers.append(part_num)
                    except ValueError:
                        logger.warning(f"Invalid part filename: {item.name}")
        except Exception as e:
            logger.error(f"Failed to list chunks for object {object_id}: {e}")
            raise

        return sorted(part_numbers)

    def has_object(self, object_id: str) -> bool:
        """Check if object has chunks in DLQ."""
        object_dir = self.dlq_dir / self._safe_id(object_id)
        return object_dir.exists() and any(object_dir.iterdir())

    def archive_object(self, object_id: str) -> None:
        """Move object directory from DLQ to archive."""
        src_dir = self.dlq_dir / self._safe_id(object_id)
        dst_dir = self.archive_dir / self._safe_id(object_id)

        if not src_dir.exists():
            logger.warning(f"Nothing to archive for object {object_id}")
            return

        try:
            # Remove existing archive if any
            if dst_dir.exists():
                shutil.rmtree(dst_dir)

            # Move to archive
            shutil.move(str(src_dir), str(dst_dir))
            logger.info(f"Archived DLQ data: {object_id} -> {dst_dir}")
        except Exception as e:
            logger.error(f"Failed to archive object {object_id}: {e}")
            raise

    def delete_archived_object(self, object_id: str) -> None:
        """Manually delete archived object data."""
        dst_dir = self.archive_dir / self._safe_id(object_id)

        if not dst_dir.exists():
            logger.warning(f"Archived object {object_id} not found")
            return

        try:
            shutil.rmtree(dst_dir)
            logger.info(f"Deleted archived DLQ data: {object_id}")
        except Exception as e:
            logger.error(f"Failed to delete archived object {object_id}: {e}")
            raise

    def cleanup_object(self, object_id: str) -> None:
        """Remove object directory from DLQ (no archive)."""
        object_dir = self.dlq_dir / self._safe_id(object_id)

        if not object_dir.exists():
            return

        try:
            shutil.rmtree(object_dir)
            logger.info(f"Cleaned up DLQ data: {object_id}")
        except Exception as e:
            logger.error(f"Failed to cleanup object {object_id}: {e}")
            raise
