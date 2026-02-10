"""Filesystem-backed parts store for object chunks and metadata.

Provides persistent storage for multipart upload chunks and metadata on a shared
volume, ensuring data availability beyond Redis TTL for long-running uploads.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import shutil
from pathlib import Path
from typing import Optional
from uuid import UUID


logger = logging.getLogger(__name__)


class FileSystemPartsStore:
    """Filesystem-backed store for object parts with atomic writes.

    Layout: <root>/<object_id>/v<object_version>/part_<part_number>/
              chunk_<index>.bin
              meta.json (presence indicates part is complete)

    All writes are atomic (tmp + rename). Readers check for meta.json existence
    before reading chunks.
    """

    def __init__(self, root_dir: str) -> None:
        """Initialize the store with a root directory path.

        Args:
            root_dir: Root directory path for object cache storage
        """
        self.root = Path(root_dir)
        self.root.mkdir(parents=True, exist_ok=True)

    def _safe_object_id(self, object_id: str) -> str:
        """Validate object_id as UUID to prevent path traversal."""
        try:
            return str(UUID(object_id))
        except (ValueError, AttributeError) as e:
            raise ValueError(f"Invalid object_id: {object_id}") from e

    def part_path(self, object_id: str, object_version: int, part_number: int) -> str:
        """Return the directory path for a specific part.

        Args:
            object_id: Object UUID
            object_version: Object version number
            part_number: Part number

        Returns:
            Absolute path to part directory
        """
        safe_id = self._safe_object_id(object_id)
        return str(self.root / safe_id / f"v{int(object_version)}" / f"part_{int(part_number)}")

    def _chunk_file(self, part_dir: Path, chunk_index: int) -> Path:
        """Return the path for a chunk file."""
        return part_dir / f"chunk_{int(chunk_index)}.bin"

    def _meta_file(self, part_dir: Path) -> Path:
        """Return the path for the metadata file."""
        return part_dir / "meta.json"

    async def set_chunk(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int, data: bytes
    ) -> None:
        """Write a chunk to filesystem atomically.

        Args:
            object_id: Object UUID
            object_version: Object version number
            part_number: Part number
            chunk_index: Chunk index within the part
            data: Chunk bytes (ciphertext)

        Raises:
            OSError: If write fails (fatal to request)
        """
        # Basic validation
        if int(part_number) < 0 or int(chunk_index) < 0:
            raise ValueError("part_number and chunk_index must be non-negative")

        part_dir = Path(self.part_path(object_id, object_version, part_number))
        part_dir.mkdir(parents=True, exist_ok=True)

        chunk_path = self._chunk_file(part_dir, chunk_index)
        tmp_path = chunk_path.with_suffix(".bin.tmp")

        try:
            # Write to temp file (off the event loop)
            def _write_chunk() -> None:
                with tmp_path.open("wb") as f:
                    f.write(data)
                    f.flush()
                tmp_path.replace(chunk_path)

            await asyncio.to_thread(_write_chunk)

            logger.debug(
                f"FS: wrote chunk object_id={object_id} v={object_version} part={part_number} chunk={chunk_index} size={len(data)}"
            )
        except Exception as e:
            # Clean up temp file if it exists
            if tmp_path.exists():
                tmp_path.unlink()
            logger.error(
                f"FS write failed: object_id={object_id} v={object_version} part={part_number} chunk={chunk_index}: {e}"
            )
            raise

    async def get_chunk(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int
    ) -> Optional[bytes]:
        """Read a chunk from filesystem.

        Args:
            object_id: Object UUID
            object_version: Object version number
            part_number: Part number
            chunk_index: Chunk index within the part

        Returns:
            Chunk bytes if present, None otherwise
        """
        part_dir = Path(self.part_path(object_id, object_version, part_number))
        meta_path = self._meta_file(part_dir)

        # Only read if meta.json exists (indicates part is complete)
        if not meta_path.exists():
            return None

        chunk_path = self._chunk_file(part_dir, chunk_index)
        if not chunk_path.exists():
            return None

        try:

            def _read_chunk() -> bytes:
                with chunk_path.open("rb") as f:
                    data = f.read()
                os.utime(chunk_path)
                return data

            data = await asyncio.to_thread(_read_chunk)
            logger.debug(
                f"FS: read chunk object_id={object_id} v={object_version} part={part_number} chunk={chunk_index} size={len(data)}"
            )
            return data
        except Exception as e:
            logger.warning(
                f"FS read failed: object_id={object_id} v={object_version} part={part_number} chunk={chunk_index}: {e}"
            )
            return None

    async def chunk_exists(self, object_id: str, object_version: int, part_number: int, chunk_index: int) -> bool:
        """Check if a chunk exists on filesystem.

        Args:
            object_id: Object UUID
            object_version: Object version number
            part_number: Part number
            chunk_index: Chunk index within the part

        Returns:
            True if chunk exists and part is complete (meta.json present)
        """
        part_dir = Path(self.part_path(object_id, object_version, part_number))
        meta_path = self._meta_file(part_dir)

        if not meta_path.exists():
            return False

        chunk_path = self._chunk_file(part_dir, chunk_index)
        return chunk_path.exists()

    async def set_meta(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        *,
        chunk_size: int,
        num_chunks: int,
        size_bytes: int,
    ) -> None:
        """Write metadata atomically. This is the 'complete' marker for a part.

        Args:
            object_id: Object UUID
            object_version: Object version number
            part_number: Part number
            chunk_size: Size of each chunk (bytes)
            num_chunks: Total number of chunks
            size_bytes: Total plaintext size (bytes)

        Raises:
            OSError: If write fails (fatal to request)
        """
        part_dir = Path(self.part_path(object_id, object_version, part_number))
        part_dir.mkdir(parents=True, exist_ok=True)

        meta_path = self._meta_file(part_dir)
        tmp_path = meta_path.with_suffix(".json.tmp")

        payload = {
            "chunk_size": int(chunk_size),
            "num_chunks": int(num_chunks),
            "size_bytes": int(size_bytes),
        }

        try:
            # Write to temp file and fsync file; then replace and fsync directory
            def _write_meta() -> None:
                with tmp_path.open("w") as f:
                    json.dump(payload, f)
                    f.flush()
                    os.fsync(f.fileno())
                tmp_path.replace(meta_path)

            await asyncio.to_thread(_write_meta)
            # Fsync the containing directory to ensure durability of the rename
            await self._fsync_dir_async(part_dir)

            logger.debug(
                f"FS: wrote meta object_id={object_id} v={object_version} part={part_number} num_chunks={num_chunks}"
            )
        except Exception as e:
            # Clean up temp file if it exists
            if tmp_path.exists():
                tmp_path.unlink()
            logger.error(f"FS meta write failed: object_id={object_id} v={object_version} part={part_number}: {e}")
            raise

    async def get_meta(self, object_id: str, object_version: int, part_number: int) -> Optional[dict]:
        """Read metadata from filesystem.

        Args:
            object_id: Object UUID
            object_version: Object version number
            part_number: Part number

        Returns:
            Metadata dict with chunk_size, num_chunks, size_bytes, or None if not present
        """
        part_dir = Path(self.part_path(object_id, object_version, part_number))
        meta_path = self._meta_file(part_dir)

        if not meta_path.exists():
            return None

        try:

            def _read_meta() -> dict:
                with meta_path.open("r") as f:
                    return dict(json.load(f))

            data = await asyncio.to_thread(_read_meta)
            logger.debug(f"FS: read meta object_id={object_id} v={object_version} part={part_number}")
            return data
        except Exception as e:
            logger.warning(f"FS meta read failed: object_id={object_id} v={object_version} part={part_number}: {e}")
            return None

    async def delete_part(self, object_id: str, object_version: int, part_number: int) -> None:
        """Delete a part directory and attempt to prune empty parent directories.

        This is idempotent and race-safe with concurrent append. Only removes
        directories if empty; ignores non-empty errors.

        Args:
            object_id: Object UUID
            object_version: Object version number
            part_number: Part number
        """
        part_dir = Path(self.part_path(object_id, object_version, part_number))

        if not part_dir.exists():
            logger.debug(
                f"FS: delete_part no-op (not present) object_id={object_id} v={object_version} part={part_number}"
            )
            return

        try:
            # Remove all files in the part directory
            await asyncio.to_thread(shutil.rmtree, part_dir)
            logger.info(f"FS: deleted part object_id={object_id} v={object_version} part={part_number}")
        except Exception as e:
            logger.warning(
                f"FS: failed to delete part object_id={object_id} v={object_version} part={part_number}: {e}"
            )
            return

        # Attempt to remove parent directories if empty (race-safe)
        try:
            version_dir = part_dir.parent
            if version_dir.exists():
                with contextlib.suppress(OSError):
                    version_dir.rmdir()  # Only succeeds if empty
                logger.debug(f"FS: pruned empty version dir {version_dir}")
        except OSError:
            pass  # Directory not empty or other race; ignore

        try:
            object_dir = version_dir.parent if version_dir else None
            if object_dir and object_dir.exists():
                with contextlib.suppress(OSError):
                    object_dir.rmdir()  # Only succeeds if empty
                logger.debug(f"FS: pruned empty object dir {object_dir}")
        except (OSError, AttributeError):
            pass  # Directory not empty or other race; ignore

    async def delete_object(self, object_id: str, object_version: Optional[int] = None) -> None:
        """Delete an entire object or specific version.

        Args:
            object_id: Object UUID
            object_version: If specified, delete only this version; otherwise delete entire object
        """
        safe_id = self._safe_object_id(object_id)
        object_dir = self.root / safe_id

        if object_version is not None:
            version_dir = object_dir / f"v{int(object_version)}"
            if version_dir.exists():
                try:
                    await asyncio.to_thread(shutil.rmtree, version_dir)
                    logger.info(f"FS: deleted version object_id={object_id} v={object_version}")
                except Exception as e:
                    logger.warning(f"FS: failed to delete version object_id={object_id} v={object_version}: {e}")
                    return

                # Try to prune object dir if empty
                try:
                    if object_dir.exists():
                        object_dir.rmdir()
                        logger.debug(f"FS: pruned empty object dir {object_dir}")
                except OSError:
                    pass  # Not empty; ignore
        else:
            # Delete entire object
            if object_dir.exists():
                try:
                    await asyncio.to_thread(shutil.rmtree, object_dir)
                    logger.info(f"FS: deleted object object_id={object_id}")
                except Exception as e:
                    logger.warning(f"FS: failed to delete object object_id={object_id}: {e}")

    async def _fsync_dir_async(self, directory: Path) -> None:
        """Fsync a directory (async wrapper) to ensure rename durability."""

        def _sync_dir() -> None:
            fd = os.open(str(directory), os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

        with contextlib.suppress(OSError):
            await asyncio.to_thread(_sync_dir)
