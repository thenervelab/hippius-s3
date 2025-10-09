"""
DLQ Storage Module

Handles filesystem operations for DLQ chunk persistence.
"""

import contextlib
import json
import logging
import re
import shutil
from pathlib import Path
from typing import Dict
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

    def save_meta(self, object_id: str, part_number: int, meta: Dict) -> None:
        """Save cache metadata (chunk_size, num_chunks, size_bytes) as JSON sidecar.

        Enables exact Redis cache reconstruction without inferring chunk boundaries.
        """
        object_dir = self.dlq_dir / self._safe_id(object_id)
        object_dir.mkdir(parents=True, exist_ok=True)

        meta_file = object_dir / f"part_{part_number}.meta.json"
        try:
            with meta_file.open("w") as f:
                json.dump(meta, f)
            logger.debug(f"Saved meta to DLQ: {meta_file}")
        except Exception as e:
            logger.error(f"Failed to save meta to DLQ: {meta_file}, error={e}")
            raise

    def load_meta(self, object_id: str, part_number: int) -> Optional[Dict]:
        """Load cache metadata from JSON sidecar."""
        meta_file = self.dlq_dir / self._safe_id(object_id) / f"part_{part_number}.meta.json"
        try:
            with meta_file.open("r") as f:
                result = json.load(f)
                return dict(result) if isinstance(result, dict) else None
        except FileNotFoundError:
            logger.debug(f"Meta not found in DLQ: {meta_file}")
            return None
        except Exception as e:
            logger.error(f"Failed to load meta from DLQ: {meta_file}, error={e}")
            return None

    def save_chunk_piece(self, object_id: str, part_number: int, chunk_index: int, data: bytes) -> None:
        """Save a single chunk piece exactly as stored in Redis (ciphertext or plaintext)."""
        object_dir = self.dlq_dir / self._safe_id(object_id)
        object_dir.mkdir(parents=True, exist_ok=True)

        piece_file = object_dir / f"part_{part_number}.chunk_{chunk_index}"
        try:
            with piece_file.open("wb") as f:
                f.write(data)
            logger.debug(f"Saved chunk piece to DLQ: {piece_file}, size={len(data)}")
        except Exception as e:
            logger.error(f"Failed to save chunk piece to DLQ: {piece_file}, error={e}")
            raise

    def load_chunk_piece(self, object_id: str, part_number: int, chunk_index: int) -> Optional[bytes]:
        """Load a single chunk piece from DLQ, if present."""
        piece_file = self.dlq_dir / self._safe_id(object_id) / f"part_{part_number}.chunk_{chunk_index}"
        try:
            with piece_file.open("rb") as f:
                return f.read()
        except FileNotFoundError:
            return None
        except Exception as e:
            logger.error(f"Failed to load chunk piece from DLQ: {piece_file}, error={e}")
            raise

    def list_chunk_piece_indices(self, object_id: str, part_number: int) -> List[int]:
        """List available chunk piece indices for a given part in DLQ."""
        object_dir = self.dlq_dir / self._safe_id(object_id)
        if not object_dir.exists():
            return []
        indices: List[int] = []
        prefix = f"part_{part_number}.chunk_"
        try:
            for item in object_dir.iterdir():
                if item.is_file() and item.name.startswith(prefix):
                    try:
                        idx_str = item.name[len(prefix) :]
                        indices.append(int(idx_str))
                    except Exception:
                        continue
        except Exception as e:
            logger.error(f"Failed to list chunk pieces for object {object_id} part {part_number}: {e}")
            raise
        return sorted(indices)

    def part_size(self, object_id: str, part_number: int) -> int:
        """Return total bytes for a part, using meta or summing piece sizes. Legacy fallback to whole file."""
        # Prefer meta.size_bytes if present
        meta = self.load_meta(object_id, part_number)
        if isinstance(meta, dict):
            try:
                sb = int(meta.get("size_bytes", 0))
                if sb > 0:
                    return sb
            except Exception:
                pass
        # Sum all piece files
        indices = self.list_chunk_piece_indices(object_id, part_number)
        if indices:
            total = 0
            object_dir = self.dlq_dir / self._safe_id(object_id)
            for ci in indices:
                piece_file = object_dir / f"part_{part_number}.chunk_{ci}"
                try:
                    total += piece_file.stat().st_size
                except Exception:
                    b = self.load_chunk_piece(object_id, part_number, ci)
                    total += len(b) if isinstance(b, (bytes, bytearray)) else 0
            return total
        # Legacy whole file fallback
        data = self.load_chunk(object_id, part_number)
        return len(data) if isinstance(data, (bytes, bytearray)) else 0

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
        """List available part numbers (supports meta/pieces-only layout)."""
        object_dir = self.dlq_dir / self._safe_id(object_id)
        if not object_dir.exists():
            return []
        part_numbers: set = set()
        re_meta = re.compile(r"^part_(\d+)\.meta\.json$")
        re_piece = re.compile(r"^part_(\d+)\.chunk_\d+$")
        re_whole = re.compile(r"^part_(\d+)$")  # legacy
        try:
            for item in object_dir.iterdir():
                if not item.is_file():
                    continue
                name = item.name
                m = re_meta.match(name) or re_piece.match(name) or re_whole.match(name)
                if m:
                    with contextlib.suppress(Exception):
                        part_numbers.add(int(m.group(1)))
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
