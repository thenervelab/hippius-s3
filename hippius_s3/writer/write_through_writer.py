"""Write-through parts writer: FS-only (fatal on failure)."""

from __future__ import annotations

import logging
from typing import Any


logger = logging.getLogger(__name__)


class WriteThroughPartsWriter:
    """Writes object parts to filesystem.

    Write semantics:
    - FS writes are mandatory and fatal on failure
    - Meta is always written last to indicate completeness
    """

    def __init__(self, fs_store: Any) -> None:
        """Initialize the writer.

        Args:
            fs_store: FileSystemPartsStore instance
        """
        self.fs_store = fs_store

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
        """Write metadata to FS (fatal on failure).

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
        """Write chunks to FS (fatal on failure).

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
