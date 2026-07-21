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

    async def chunks_exist_batch(
        self, object_id: str, object_version: int, checks: list[tuple[int, int]]
    ) -> list[bool]:
        # The read path decides cache-vs-pipeline from this batch check, so it must see
        # the fallback too: under drain-direct the drain unlinks the primary SSD copy
        # after replicating to the pool (the fallback tier), and a part present only on
        # the pool is durably available — it should read as cache, not be re-fetched
        # through the download pipeline. Only the primary misses are re-checked, so the
        # common all-present case stays a single primary pass.
        primary = await super().chunks_exist_batch(object_id, object_version, checks)
        missing = [check for check, present in zip(checks, primary, strict=False) if not present]
        if not missing:
            return primary
        fallback = await self.fallback.chunks_exist_batch(object_id, object_version, missing)
        found = {check for check, present in zip(missing, fallback, strict=False) if present}
        return [present or check in found for check, present in zip(checks, primary, strict=False)]
