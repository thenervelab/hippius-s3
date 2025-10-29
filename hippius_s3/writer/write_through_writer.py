"""Write-through parts writer: FS-first (fatal), then Redis (best-effort)."""

from __future__ import annotations

import logging
from typing import Any


logger = logging.getLogger(__name__)


class WriteThroughPartsWriter:
    """Writes object parts to both filesystem (mandatory) and Redis (best-effort).

    Write semantics:
    - FS writes are mandatory and fatal on failure
    - Redis writes are best-effort and only logged on failure
    - Meta is always written last to indicate completeness
    """

    def __init__(self, fs_store: Any, redis_cache: Any, ttl_seconds: int) -> None:
        """Initialize the write-through writer.

        Args:
            fs_store: FileSystemPartsStore instance
            redis_cache: ObjectPartsCache (Redis) instance
            ttl_seconds: TTL for Redis keys
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
        """Write metadata to FS (fatal) then Redis (best-effort).

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
        # FS first (fatal on failure)
        await self.fs_store.set_meta(
            object_id,
            int(object_version),
            int(part_number),
            chunk_size=int(chunk_size),
            num_chunks=int(num_chunks),
            size_bytes=int(plain_size),
        )

        # Redis second (best-effort)
        try:
            await self.redis_cache.set_meta(
                object_id,
                int(object_version),
                int(part_number),
                chunk_size=int(chunk_size),
                num_chunks=int(num_chunks),
                size_bytes=int(plain_size),
                ttl=self.ttl_seconds,
            )
        except Exception as e:
            logger.warning(
                f"Redis meta write failed (best-effort): object_id={object_id} v={object_version} part={part_number}: {e}",
                exc_info=True,
            )

    async def write_chunks(self, object_id: str, object_version: int, part_number: int, chunks: list[bytes]) -> None:
        """Write chunks to FS (fatal) then Redis (best-effort).

        Args:
            object_id: Object UUID
            object_version: Object version number
            part_number: Part number
            chunks: List of ciphertext chunk bytes

        Raises:
            Exception: If any FS write fails (fatal to request)
        """
        # FS first (fatal on failure for any chunk)
        for i, ct in enumerate(chunks):
            await self.fs_store.set_chunk(object_id, int(object_version), int(part_number), int(i), ct)

        # Redis second (best-effort for each chunk)
        for i, ct in enumerate(chunks):
            try:
                await self.redis_cache.set_chunk(
                    object_id, int(object_version), int(part_number), int(i), ct, ttl=self.ttl_seconds
                )
            except Exception as e:
                logger.warning(
                    f"Redis chunk write failed (best-effort): object_id={object_id} v={object_version} part={part_number} chunk={i}: {e}",
                    exc_info=True,
                )
