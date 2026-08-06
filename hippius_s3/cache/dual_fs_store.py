from __future__ import annotations

import logging
from typing import Awaitable
from typing import Callable
from typing import Optional

from hippius_s3.cache.fs_store import FileSystemPartsStore
from hippius_s3.monitoring import ChunkReadTier


logger = logging.getLogger(__name__)


# Called after a chunk is promoted onto the local tier, with (object_id, version,
# part_number, bytes). The api wires this to the drain's residency table so this node's
# evictor can reclaim the copy.
PromotionRecorder = Callable[[str, int, int, int], Awaitable[None]]

# Fetches one chunk from the peer node that currently holds it on flash, with
# (object_id, version, part_number, chunk_index). Returns None when no peer has it.
PeerFetcher = Callable[[str, int, int, int], Awaitable[Optional[bytes]]]


def _record_tier(tier: ChunkReadTier) -> None:
    """Count which tier served a chunk. Never let observability break a read."""
    try:
        from hippius_s3.monitoring import get_metrics_collector

        collector = get_metrics_collector()
        if collector is not None:
            collector.record_chunk_read_tier(tier)
    except Exception:  # noqa: BLE001 - a metrics failure must not fail a read
        pass


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
        peer_fetch: Optional[PeerFetcher] = None,
    ) -> None:
        super().__init__(primary_dir)
        self.fallback = FileSystemPartsStore(fallback_dir)
        self._promote = promote
        self._on_promote = on_promote
        self._peer_fetch = peer_fetch
        # Chunks whose promotion is in flight right now, so concurrent readers of the same
        # cold chunk write it once. Holds only in-flight keys, so it drains itself and needs
        # no bound or TTL — unlike a "already done" memo, which the out-of-process evictor
        # could invalidate underneath us.
        self._promoting: set[tuple[str, int, int, int]] = set()

    async def get_chunk(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int
    ) -> Optional[bytes]:
        result = await super().get_chunk(object_id, object_version, part_number, chunk_index)
        if result is not None:
            _record_tier("local")
            return result

        # Peer tier, between local flash and the pool. A part lives on whichever node
        # ingested it (and on any node a read has promoted it to), and locality is resolved
        # per PART: on prod 2026-08-06 only 2% of multi-part objects had every part on one
        # node, so there is no single "right" node for a whole request to be sent to.
        peer_bytes = await self._fetch_from_peer(object_id, object_version, part_number, chunk_index)
        if peer_bytes is not None:
            _record_tier("peer")
            if self._promote:
                await self._promote_chunk(object_id, object_version, part_number, chunk_index, peer_bytes)
            return peer_bytes

        result = await self.fallback.get_chunk(object_id, object_version, part_number, chunk_index)
        if result is not None:
            _record_tier("pool")
            if self._promote:
                await self._promote_chunk(object_id, object_version, part_number, chunk_index, result)
        return result

    async def _fetch_from_peer(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int
    ) -> Optional[bytes]:
        """Ask the peer holding this part on flash. Best-effort; never raises.

        A peer is an optimisation over the pool, and the pool copy is authoritative and
        always present for a replicated part. So a peer that is down, slow, mid-eviction, or
        simply does not have the chunk must degrade to a pool read, never to a failed one.
        """
        if self._peer_fetch is None:
            return None
        try:
            return await self._peer_fetch(object_id, object_version, part_number, chunk_index)
        except Exception as exc:  # noqa: BLE001 - a peer must never be able to fail a read
            logger.debug(
                "peer fetch failed for %s v%s part %s chunk %s: %s",
                object_id,
                object_version,
                part_number,
                chunk_index,
                exc,
            )
            return None

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
        # Skip if another reader is already promoting this exact chunk. Skipping beats
        # waiting: this caller already holds the bytes, so blocking on someone else's write
        # would add latency and return the same result. Duplicate promotion is not merely
        # wasteful now that the residency upsert accumulates — it would double-count the
        # chunk's bytes and inflate the figure the evictor sums against its deficit.
        in_flight = (object_id, int(object_version), int(part_number), int(chunk_index))
        if in_flight in self._promoting:
            return
        self._promoting.add(in_flight)
        try:
            meta = await self.fallback.get_meta(object_id, object_version, part_number)
            if meta is None:
                return
            # Skip the rewrite only when meta is ACTUALLY on this node's disk — never on a
            # process-local memo. The evictor runs in a different process (drain-agent) and
            # unlinks the whole part dir, meta included; it cannot invalidate a memo held
            # here. A stale memo would skip the rewrite and leave chunks with no meta, and
            # meta is the readiness gate, so the promoted copy would be unreadable as well
            # as unrecorded. A local read is a cheap stat next to the fsync it avoids.
            if await FileSystemPartsStore.get_meta(self, object_id, object_version, part_number) is None:
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
                # Records residency so THIS node's evictor owns the copy: without it the part
                # sits on a disk whose evictor is scoped to the residency table, and nothing
                # ever reclaims it. Reported per CHUNK with the bytes actually written, not
                # the part's declared total — a range GET promotes only the chunks it touches,
                # so claiming the whole part's size would inflate the number the evictor sums
                # to decide it has freed enough, stopping a pass early while it reports success.
                await self._on_promote(object_id, object_version, part_number, len(data))
        except (OSError, KeyError, TypeError, ValueError) as exc:
            logger.debug(
                "promotion to the local tier failed for %s v%s part %s chunk %s: %s",
                object_id,
                object_version,
                part_number,
                chunk_index,
                exc,
            )
        finally:
            self._promoting.discard(in_flight)

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
