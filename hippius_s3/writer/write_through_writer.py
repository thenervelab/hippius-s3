"""Parts writer: persists chunks and meta to the FS cache."""

from __future__ import annotations

from typing import Any


class WriteThroughPartsWriter:
    """Writes object parts to the filesystem cache (mandatory, fatal on failure).

    Historically this also mirrored writes to a Redis download cache. Since the
    2026-04-21 FS-cache migration the Redis cache delegates straight back to the
    same FS store, so the mirror was a redundant second disk write. Meta is always
    written last to indicate completeness.
    """

    def __init__(self, fs_store: Any, redis_cache: Any, ttl_seconds: int) -> None:
        """Initialize the parts writer.

        Args:
            fs_store: FileSystemPartsStore instance
            redis_cache: retained for call-site compatibility; no longer written to
            ttl_seconds: TTL for cache keys
        """
        self.fs_store = fs_store
        self.redis_cache = redis_cache
        self.ttl_seconds = int(ttl_seconds)

    async def write_meta(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        *,
        chunk_size: int,
        num_chunks: int,
        plain_size: int,
    ) -> None:
        """Write metadata to the FS cache (fatal on failure).

        Args:
            object_id: Object UUID
            object_version: Object version number
            part_number: Part number
            chunk_size: Size of each chunk (bytes)
            num_chunks: Total number of chunks
            plain_size: Total plaintext size (bytes)

        Raises:
            Exception: If FS write fails (fatal to request)
        """
        await self.fs_store.set_meta(
            object_id,
            int(object_version),
            int(part_number),
            chunk_size=int(chunk_size),
            num_chunks=int(num_chunks),
            size_bytes=int(plain_size),
        )

    async def write_chunks(self, object_id: str, object_version: int, part_number: int, chunks: list[bytes]) -> None:
        """Write chunks to the FS cache (fatal on failure).

        Args:
            object_id: Object UUID
            object_version: Object version number
            part_number: Part number
            chunks: List of ciphertext chunk bytes

        Raises:
            Exception: If any FS write fails (fatal to request)
        """
        for i, ct in enumerate(chunks):
            await self.fs_store.set_chunk(object_id, int(object_version), int(part_number), int(i), ct)
