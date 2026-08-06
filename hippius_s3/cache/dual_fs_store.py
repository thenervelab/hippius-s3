from __future__ import annotations

import logging
from typing import Awaitable
from typing import Callable
from typing import Optional

from hippius_s3.cache.fs_store import FileSystemPartsStore


logger = logging.getLogger(__name__)

# Called after a chunk is promoted onto the local tier, with (object_id, version,
# part_number, bytes). The api wires this to the drain's residency table so this node's
# evictor can reclaim the copy.
PromotionRecorder = Callable[[str, int, int, int], Awaitable[None]]


class DualFileSystemPartsStore(FileSystemPartsStore):
    """Primary (node-local NVMe) store with the shared CephFS pool as a read fallback.

    Writes, deletes, and path operations are inherited from the parent (the primary).
    Reads check primary first, then fallback.

    With `promote=True`, a read served from the fallback is copied onto the primary so
    the NEXT read of that chunk comes off local flash (~705 MB/s, ~6 ms per chunk) rather
    than the pool (~94 MB/s, ~40 ms, measured on node1 2026-08-06). Routing sends a GET to
    the node that ingested the part and therefore retains it; promotion is what covers the
    requests routing does not place — routing disabled, target node unready, or an object
    whose ingest node no longer holds it.
    """

    def __init__(
        self,
        primary_dir: str,
        fallback_dir: str,
        *,
        promote: bool = False,
        on_promote: Optional[PromotionRecorder] = None,
    ) -> None:
        super().__init__(primary_dir)
        self.fallback = FileSystemPartsStore(fallback_dir)
        self._promote = promote
        self._on_promote = on_promote

    async def get_chunk(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int
    ) -> Optional[bytes]:
        result = await super().get_chunk(object_id, object_version, part_number, chunk_index)
        if result is not None:
            return result
        result = await self.fallback.get_chunk(object_id, object_version, part_number, chunk_index)
        if result is not None and self._promote:
            await self._promote_chunk(object_id, object_version, part_number, chunk_index, result)
        return result

    async def _promote_chunk(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int, data: bytes
    ) -> None:
        """Copy a pool-served chunk onto the local tier. Best-effort, never fatal.

        The caller already holds the bytes and the pool copy is authoritative, so every
        failure here costs a cache warm and nothing else: a full disk, a read-only mount,
        or a race with the evictor unlinking the part must not turn a successful read into
        a failed one.

        Meta is written FIRST, matching the downloader rather than the uploader. Meta is the
        readiness gate, so writing it first makes each promoted chunk readable as it lands;
        writing it last would leave the whole part invisible until some read happened to
        promote the final chunk.
        """
        try:
            meta = await self.fallback.get_meta(object_id, object_version, part_number)
            if meta is None:
                return
            await self.set_meta(
                object_id,
                object_version,
                part_number,
                chunk_size=int(meta["chunk_size"]),
                num_chunks=int(meta["num_chunks"]),
                size_bytes=int(meta["size_bytes"]),
            )
            await self.set_chunk(object_id, object_version, part_number, chunk_index, data)
            if self._on_promote is not None:
                # Records residency so THIS node's evictor owns the copy. Without it the
                # part sits on a disk whose evictor is scoped to parts it ingested, and
                # nothing ever reclaims it.
                await self._on_promote(object_id, object_version, part_number, int(meta["size_bytes"]))
        except (OSError, KeyError, TypeError, ValueError) as exc:
            logger.debug(
                "promotion to the local tier failed for %s v%s part %s chunk %s: %s",
                object_id,
                object_version,
                part_number,
                chunk_index,
                exc,
            )

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
