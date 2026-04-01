from __future__ import annotations

from typing import Optional

from hippius_s3.cache.fs_store import FileSystemPartsStore


class DualFileSystemPartsStore(FileSystemPartsStore):
    """Subclass that falls back to a secondary store for reads.

    Used during migration from one storage backend to another. Writes, deletes,
    and path operations are inherited from the parent (primary store). Reads
    check primary first, then fallback.
    """

    def __init__(self, primary_dir: str, fallback_dir: str) -> None:
        super().__init__(primary_dir)
        self.fallback = FileSystemPartsStore(fallback_dir)

    async def get_chunk(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int
    ) -> Optional[bytes]:
        result = await super().get_chunk(object_id, object_version, part_number, chunk_index)
        if result is not None:
            return result
        return await self.fallback.get_chunk(object_id, object_version, part_number, chunk_index)

    async def get_meta(self, object_id: str, object_version: int, part_number: int) -> Optional[dict]:
        result = await super().get_meta(object_id, object_version, part_number)
        if result is not None:
            return result
        return await self.fallback.get_meta(object_id, object_version, part_number)

    async def chunk_exists(self, object_id: str, object_version: int, part_number: int, chunk_index: int) -> bool:
        if await super().chunk_exists(object_id, object_version, part_number, chunk_index):
            return True
        return await self.fallback.chunk_exists(object_id, object_version, part_number, chunk_index)
