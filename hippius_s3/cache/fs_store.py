"""Filesystem-backed parts store for object chunks and metadata.

Provides persistent storage for multipart upload chunks and metadata on a shared
volume, ensuring data availability beyond Redis TTL for long-running uploads.
Also serves as the download cache — when workers fetch chunks from a backend,
they write them here (not to Redis) for the streamer to read.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import shutil
import uuid
from pathlib import Path
from typing import Any
from typing import Optional
from uuid import UUID

from hippius_s3.cache.access_tracker import get_access_tracker


logger = logging.getLogger(__name__)


class FileSystemPartsStore:
    """Filesystem-backed store for object parts with atomic writes.

    Layout: <root>/<object_id>/v<object_version>/part_<part_number>/
              chunk_<index>.bin
              meta.json (presence indicates part is complete)

    All writes are atomic (unique-tmp + rename) so concurrent writers from
    different worker pods don't corrupt files. Readers check for meta.json
    existence before reading chunks.
    """

    def __init__(self, root_dir: str) -> None:
        """Initialize the store with a root directory path.

        Args:
            root_dir: Root directory path for object cache storage
        """
        self.root = Path(root_dir)
        self.root.mkdir(parents=True, exist_ok=True)

    def _safe_object_id(self, object_id: Any) -> str:
        """Validate object_id as UUID to prevent path traversal.

        Accepts either a str (normalised via UUID parse) or a UUID instance
        (passed through via str()). Anything else raises ValueError. asyncpg
        may return UUID objects for UUID columns depending on codec setup.
        """
        if isinstance(object_id, UUID):
            return str(object_id)
        if not isinstance(object_id, str):
            raise ValueError(f"Invalid object_id type: {type(object_id).__name__}")
        try:
            return str(UUID(object_id.strip()))
        except (ValueError, AttributeError, TypeError) as e:
            raise ValueError(f"Invalid object_id: {object_id!r}") from e

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

    def _unique_tmp(self, target: Path) -> Path:
        """Build a unique temp filename alongside target.

        Using a uuid4 suffix guarantees two concurrent writers to the same
        final path never collide on the tempfile and interleave bytes.
        """
        return target.with_name(f"{target.name}.tmp.{uuid.uuid4().hex}")

    async def set_chunk(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int, data: bytes
    ) -> None:
        """Write a chunk to filesystem atomically.

        Concurrent writers to the same chunk path are safe: each uses a unique
        temp file, and the final `os.replace` is atomic. Last rename wins;
        content is deterministic per chunk so the "winner" doesn't matter.

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
        tmp_path = self._unique_tmp(chunk_path)

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
            with contextlib.suppress(OSError):
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

        Gated on meta.json existence — readers only see chunks once the part
        is marked ready. Read recency is recorded via the AccessTracker (into
        fs_cache_inventory.last_access_at) rather than atime; the hook lives
        HERE, at the store level, because the streamer passes fetch_fn =
        fs.get_chunk directly and bypasses every higher-level wrapper.

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
            # Read-recency for hot retention is recorded in
            # fs_cache_inventory.last_access_at (cache/access_tracker.py), not
            # via atime: the old per-read os.utime was silently dead on
            # read-only mounts (prod api-local) and an MDS metadata WRITE on
            # every read elsewhere. stat atime now reflects write recency only.

            def _read() -> bytes:
                with chunk_path.open("rb") as f:
                    return f.read()

            data = await asyncio.to_thread(_read)
            # Sync, sampled, no-op in processes that never initialize the
            # tracker (workers/janitor).
            tracker = get_access_tracker()
            if tracker is not None:
                tracker.note_read(object_id, int(object_version), int(part_number))
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

    async def chunks_exist_batch(
        self, object_id: str, object_version: int, checks: list[tuple[int, int]]
    ) -> list[bool]:
        """Batch existence check for many chunks (stat-based, no Redis).

        Groups checks by part_number so we only check meta.json once per part
        instead of once per chunk. For a part with 100 chunks this is 1 meta
        stat + 100 chunk stats instead of 100 meta stats + 100 chunk stats.

        Args:
            object_id: Object UUID
            object_version: Object version
            checks: list of (part_number, chunk_index) tuples

        Returns:
            List of booleans (same length and order as `checks`).
        """
        if not checks:
            return []

        def _check_all() -> list[bool]:
            meta_cache: dict[int, bool] = {}
            results: list[bool] = []
            for part_number, chunk_index in checks:
                # Resolve meta presence once per distinct part
                if part_number not in meta_cache:
                    part_dir = Path(self.part_path(object_id, object_version, part_number))
                    meta_cache[part_number] = self._meta_file(part_dir).exists()
                if not meta_cache[part_number]:
                    results.append(False)
                    continue
                part_dir = Path(self.part_path(object_id, object_version, part_number))
                chunk_path = self._chunk_file(part_dir, chunk_index)
                results.append(chunk_path.exists())
            return results

        return await asyncio.to_thread(_check_all)

    async def touch_chunk(self, object_id: str, object_version: int, part_number: int, chunk_index: int) -> None:
        """Update atime/mtime of a chunk to mark it as recently accessed.

        Safe to call on missing files (silently no-ops).
        """
        part_dir = Path(self.part_path(object_id, object_version, part_number))
        chunk_path = self._chunk_file(part_dir, chunk_index)
        meta_path = self._meta_file(part_dir)

        def _touch() -> None:
            with contextlib.suppress(OSError):
                os.utime(chunk_path, None)
            with contextlib.suppress(OSError):
                os.utime(meta_path, None)

        await asyncio.to_thread(_touch)

    async def touch_part(self, object_id: str, object_version: int, part_number: int) -> None:
        """Update atime/mtime of every chunk + meta in a part.

        Called by the uploader after successful backend upload — previously
        this refreshed the Redis TTL; now it signals the janitor to keep the
        part hot for the default age-based GC window.
        """
        part_dir = Path(self.part_path(object_id, object_version, part_number))
        if not part_dir.exists():
            return

        def _touch_all() -> None:
            for entry in part_dir.iterdir():
                if entry.is_file():
                    with contextlib.suppress(OSError):
                        os.utime(entry, None)

        await asyncio.to_thread(_touch_all)

    async def read_local_chunk(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int
    ) -> Optional[bytes]:
        """This store's OWN copy of a chunk, ignoring any fallback tier.

        Serves the peer-fetch endpoint, where "local" is the whole point: a peer that
        followed its fallback would put a network hop in front of the pool read the peer
        tier exists to avoid, and two nodes resolving each other could bounce a request
        between them rather than either just reading the pool.

        Calls the base implementation explicitly rather than `self.get_chunk`, so a
        subclass that adds fallback or peer tiers (DualFileSystemPartsStore) cannot change
        what this returns.
        """
        return await FileSystemPartsStore.get_chunk(self, object_id, object_version, part_number, chunk_index)

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

        For the download path, callers write meta EAGERLY at the start of part
        processing (using num_chunks/chunk_size from DB) so that partial fills
        become readable per-chunk as they land. For the upload path, meta is
        written AFTER all chunks — same method, different ordering.

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
        tmp_path = self._unique_tmp(meta_path)

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
            with contextlib.suppress(OSError):
                if tmp_path.exists():
                    tmp_path.unlink()
            logger.error(f"FS meta write failed: object_id={object_id} v={object_version} part={part_number}: {e}")
            raise

    async def trim_chunks_from(self, object_id: str, object_version: int, part_number: int, first_index: int) -> int:
        """Delete chunk files with index >= first_index; the publish-time exact-set trim.

        Called right after meta.json lands with num_chunks == first_index. The drain
        replicates a part only when the SSD chunk set is EXACTLY {0..num_chunks-1}
        (partdrain.rs completeness gate → IncompleteSource); a stale tail left by a
        larger earlier attempt would strand the part forever — never replicated, never
        evicted. Never touches meta.json or chunks below first_index.

        Per-file failures are logged at ERROR — a surviving tail IS the stranded-part
        risk and operators must see it — but never raised: the upload itself is durable
        on SSD and the client's success must not hinge on tail cleanup.

        Returns:
            Number of chunk files removed.
        """
        part_dir = Path(self.part_path(object_id, object_version, part_number))

        def _trim() -> int:
            removed = 0
            try:
                entries = list(part_dir.iterdir())
            except FileNotFoundError:
                return 0
            except OSError as e:
                # Same loudness as a per-file failure: an unscannable dir may hide a tail.
                logger.error(
                    f"FS trim failed — could not scan part dir, a stale chunk tail may strand the "
                    f"part as IncompleteSource in the drain: object_id={object_id} "
                    f"v={object_version} part={part_number}: {e}"
                )
                return 0
            for entry in entries:
                name = entry.name
                if not (name.startswith("chunk_") and name.endswith(".bin")):
                    continue
                try:
                    idx = int(name[len("chunk_") : -len(".bin")])
                except ValueError:
                    continue  # tmp files etc. — the janitor's orphan sweep owns those
                if idx < int(first_index):
                    continue
                try:
                    entry.unlink()
                    removed += 1
                except OSError as e:
                    logger.error(
                        f"FS trim failed — stale chunk tail survives and the part may strand as "
                        f"IncompleteSource in the drain: object_id={object_id} v={object_version} "
                        f"part={part_number} chunk={idx}: {e}"
                    )
            return removed

        removed = await asyncio.to_thread(_trim)
        if removed:
            logger.debug(
                f"FS: trimmed {removed} stale chunk(s) >= {first_index}: "
                f"object_id={object_id} v={object_version} part={part_number}"
            )
        return removed

    async def delete_meta(self, object_id: str, object_version: int, part_number: int) -> None:
        """Delete meta.json only, un-publishing a part dir while keeping its chunks.

        Used by the append CAS-loser cleanup: part-number reservation is FOR-UPDATE
        serialized, so no live winner shares the loser's dir — removing meta makes the
        dir invisible to readers and the drain reconciler again (restores meta-last)
        and stops a future append that reuses the number from inheriting stale
        readiness over mixed content. Idempotent; failures are logged at ERROR (stale
        readiness left behind) but not raised — the caller is already propagating the
        CAS failure.
        """
        part_dir = Path(self.part_path(object_id, object_version, part_number))
        meta_path = self._meta_file(part_dir)

        def _unlink() -> None:
            meta_path.unlink(missing_ok=True)

        try:
            await asyncio.to_thread(_unlink)
        except OSError as e:
            logger.error(
                f"FS meta delete failed — part stays published with stale readiness: "
                f"object_id={object_id} v={object_version} part={part_number}: {e}"
            )

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

    async def get_meta_with_wait(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        deadline_seconds: float = 30.0,
    ) -> Optional[dict]:
        # Poll get_meta with exponential backoff. The uploader writes meta last
        # (after all chunks), and the consumer may dequeue before the writer's
        # fsync has propagated across the shared cache mount. Within the
        # deadline, "missing" = "writer hasn't finished" = wait. After the
        # deadline, "missing" = genuine fault (writer crashed / FS-evicted /
        # never written) = let the caller raise an after-deadline error so the
        # classifier can route it as permanent.
        loop = asyncio.get_running_loop()
        deadline = loop.time() + deadline_seconds
        backoff = 0.1
        while True:
            meta = await self.get_meta(object_id, object_version, part_number)
            if meta is not None:
                return meta
            if loop.time() >= deadline:
                return None
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 2.0)

    def stat_part(self, object_id: str, object_version: int, part_number: int) -> os.stat_result | None:
        """Stat a part's readiness marker for the janitor's SQL-eviction candidate check.

        BLOCKING — callers wrap in `asyncio.to_thread` (each stat is a CephFS metadata
        roundtrip that must not run on the loop). Stats `meta.json` when present, else the part
        dir, returning None when neither exists — the same "meta.json is the part-complete
        signal, fall back to the dir" rule the walk gates on (see `_descend_object`). The
        returned atime drives the hot-retention skip; a None tells the caller the inventory row
        is stale (the dir is already gone) so it can self-heal by clearing the row.
        """
        part_dir = Path(self.part_path(object_id, object_version, part_number))
        meta_path = self._meta_file(part_dir)
        try:
            return meta_path.stat()
        except OSError:
            pass
        try:
            return part_dir.stat()
        except OSError:
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

        def _delete_and_prune() -> bool:
            # One thread hop for the whole sequence: the exists-check, rmtree,
            # and the empty-parent prunes are each a CephFS metadata roundtrip;
            # doing them on-loop serialized every janitor worker behind MDS
            # latency.
            if not part_dir.exists():
                return False
            shutil.rmtree(part_dir)
            for parent in (part_dir.parent, part_dir.parent.parent):
                with contextlib.suppress(OSError):
                    parent.rmdir()  # only succeeds if empty — race-safe by contract
            return True

        try:
            deleted = await asyncio.to_thread(_delete_and_prune)
        except Exception as e:
            logger.warning(
                f"FS: failed to delete part object_id={object_id} v={object_version} part={part_number}: {e}"
            )
            return
        if deleted:
            # DEBUG, not INFO — this fires ~11k times per janitor cycle on prod.
            logger.debug(f"FS: deleted part object_id={object_id} v={object_version} part={part_number}")
        else:
            logger.debug(
                f"FS: delete_part no-op (not present) object_id={object_id} v={object_version} part={part_number}"
            )

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
