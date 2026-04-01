from __future__ import annotations

import logging
from typing import Optional

from hippius_s3.cache.fs_store import FileSystemPartsStore


logger = logging.getLogger(__name__)


class DualFileSystemPartsStore:
    """Composite store that writes to a primary and falls back to a secondary for reads.

    Used during migration from one storage backend to another. Writes go exclusively
    to the primary store. Reads check primary first, then fallback. Deletes only
    target the primary store.
    """

    def __init__(self, primary_dir: str, fallback_dir: str) -> None:
        self.primary = FileSystemPartsStore(primary_dir)
        self.fallback = FileSystemPartsStore(fallback_dir)
        self.root = self.primary.root

    def part_path(self, object_id: str, object_version: int, part_number: int) -> str:
        return self.primary.part_path(object_id, object_version, part_number)

    async def set_chunk(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int, data: bytes
    ) -> None:
        await self.primary.set_chunk(object_id, object_version, part_number, chunk_index, data)

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
        await self.primary.set_meta(
            object_id,
            object_version,
            part_number,
            chunk_size=chunk_size,
            num_chunks=num_chunks,
            size_bytes=size_bytes,
        )

    async def get_chunk(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int
    ) -> Optional[bytes]:
        result = await self.primary.get_chunk(object_id, object_version, part_number, chunk_index)
        if result is not None:
            return result
        return await self.fallback.get_chunk(object_id, object_version, part_number, chunk_index)

    async def get_meta(self, object_id: str, object_version: int, part_number: int) -> Optional[dict]:
        result = await self.primary.get_meta(object_id, object_version, part_number)
        if result is not None:
            return result
        return await self.fallback.get_meta(object_id, object_version, part_number)

    async def chunk_exists(self, object_id: str, object_version: int, part_number: int, chunk_index: int) -> bool:
        if await self.primary.chunk_exists(object_id, object_version, part_number, chunk_index):
            return True
        return await self.fallback.chunk_exists(object_id, object_version, part_number, chunk_index)

    async def delete_part(self, object_id: str, object_version: int, part_number: int) -> None:
        await self.primary.delete_part(object_id, object_version, part_number)

    async def delete_object(self, object_id: str, object_version: Optional[int] = None) -> None:
        await self.primary.delete_object(object_id, object_version)
