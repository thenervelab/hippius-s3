from __future__ import annotations

import asyncio
import logging
from contextvars import ContextVar
from pathlib import Path
from typing import Awaitable
from typing import Callable
from typing import Optional

from hippius_s3.cache.fs_store import FileSystemPartsStore
from hippius_s3.fs_pressure import FreeSpaceGate
from hippius_s3.monitoring import ChunkReadTier
from hippius_s3.monitoring import PromotionSkipReason


logger = logging.getLogger(__name__)


# Called BEFORE a chunk is promoted onto the local tier, with (object_id, version,
# part_number, bytes). The api wires this to the drain's residency table so this node's
# evictor can reclaim the copy. Returns whether the claim landed; `False` cancels the
# promotion, because a copy no evictor owns is worse than no copy at all.
PromotionRecorder = Callable[[str, int, int, int], Awaitable[bool]]

# Called when a claim landed but the copy's disk write then failed, with the same
# (object_id, version, part_number, bytes) the claim reported, so the recorder can subtract
# exactly what it added. Needed because the claim ACCUMULATES on conflict: without the
# give-back, every retried promotion against a persistently failing disk adds the chunk's
# bytes to the residency row again, unboundedly. Contract: must never raise — it runs on
# promotion's failure path, where an exception would fail a read that already has its bytes.
PromotionReleaser = Callable[[str, int, int, int], Awaitable[None]]

# Called after a chunk is served from THIS node's flash, with (object_id, version, part_number).
# Feeds cephor_ssd_residency.last_read_at, which is what makes eviction recency-ordered instead
# of FIFO. Sampled by the recorder, so it is cheap to call on every local hit.
LocalReadRecorder = Callable[[str, int, int], Awaitable[None]]

# Fetches one chunk from the peer node that currently holds it on flash, with
# (object_id, version, part_number, chunk_index). Returns None when no peer has it.
PeerFetcher = Callable[[str, int, int, int], Awaitable[Optional[bytes]]]

# Called with (object_id, version, part_number) on the AEAD-retry path only. True means the
# drain currently distrusts the part's pool copy (a redrive is in flight), so the retry must
# not be allowed to serve it. Contract: never raises; unknown degrades to False.
ReplicationSuspectFn = Callable[[str, int, int], Awaitable[bool]]

# Called AFTER a chunk has landed on the local tier through promotion, with (object_id, version,
# part_number, chunk_index). The api wires this to the chunk-ready pub/sub, so a reader that
# missed every tier and is parked in `wait_for_chunk` wakes as soon as a sibling reader's
# promotion makes the chunk readable here — nothing else publishes for a promoted chunk.
# A fault in it is contained by the store: it runs after a read already has its bytes, and a
# missed wakeup only costs the waiter its timeout re-check.
PromotedHook = Callable[[str, int, int, int], Awaitable[None]]

# Per-stream tier counts, set by the read path for the lifetime of one response so the stream
# can log how many of its chunks came off local flash, a peer, and the pool. None outside a
# stream (workers, tests), where `_record_tier` counts into metrics alone.
_stream_tiers: ContextVar[Optional[dict[str, int]]] = ContextVar("stream_tiers", default=None)


def _record_tier(tier: ChunkReadTier) -> None:
    """Count which tier served a chunk. Never let observability break a read."""
    counts = _stream_tiers.get()
    if counts is not None:
        counts[tier] = counts.get(tier, 0) + 1
    try:
        from hippius_s3.monitoring import get_metrics_collector

        collector = get_metrics_collector()
        if collector is not None:
            collector.record_chunk_read_tier(tier)
    except Exception:  # noqa: BLE001 - a metrics failure must not fail a read
        pass


def _record_promotion_skipped(reason: PromotionSkipReason) -> None:
    """Count a chunk served without warming local flash. Never fatal."""
    try:
        from hippius_s3.monitoring import get_metrics_collector

        collector = get_metrics_collector()
        if collector is not None:
            collector.record_promotion_skipped(reason)
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
        on_promote_release: Optional[PromotionReleaser] = None,
        peer_fetch: Optional[PeerFetcher] = None,
        space_gate: Optional[FreeSpaceGate] = None,
        on_local_read: Optional[LocalReadRecorder] = None,
        replication_suspect: Optional[ReplicationSuspectFn] = None,
        on_promoted: Optional[PromotedHook] = None,
        promote_max_part_number: int = 0,
    ) -> None:
        super().__init__(primary_dir)
        self.fallback = FileSystemPartsStore(fallback_dir)
        self._promote = promote
        self._on_promote = on_promote
        self._on_promote_release = on_promote_release
        self._on_promoted = on_promoted
        self._peer_fetch = peer_fetch
        # Consulted by `invalidate_local_chunk` alone, so the normal read path never pays for
        # it. `None` (workers, tests, deployments without a drain) keeps the pool-presence gate
        # as the only condition, which is exactly the pre-probe behaviour.
        self._replication_suspect = replication_suspect
        # Promotion yields to ingest: this is the only writer here that is pure optimisation,
        # and it shares the mount with the PUTs that `fs_cache_pressure` refuses when the disk
        # runs out. `None` means no gate (tests, and any deployment without a fallback tier).
        self._space_gate = space_gate
        # Records that a part was served locally, so the drain-agent evictor can order on USE
        # rather than on arrival. Sampled inside the recorder — this is a dict probe on all but
        # the first local read of a part per window.
        self._on_local_read = on_local_read
        # Parts above this number are never promoted; 0 means no cap. Mirrors the edge's
        # part-spread threshold — see `promote_max_part_number` in config.py.
        self._promote_max_part = int(promote_max_part_number)
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
            # Tell the evictor this part is still in use. Without it eviction orders on when a
            # part ARRIVED, which for a working set re-read every epoch tends to take exactly
            # the parts about to be read again. Sampled inside the recorder, so this is a dict
            # probe on all but the first read of a part per window.
            if self._on_local_read is not None:
                await self._on_local_read(object_id, int(object_version), int(part_number))
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

    async def peer_locate(self, object_id: str, object_version: int, part_number: int) -> tuple[Optional[str], bool]:
        """`(owner node or None, unreplicated)` for a part from the peer resolver; `(None, False)` without one.

        Best-effort on the same grounds as `_fetch_from_peer`: the peer tier is an optimisation
        and must never be able to fail a read, and this runs on the request path before the
        first byte.
        """
        locate = getattr(self._peer_fetch, "locate", None)
        if locate is None:
            return None, False
        try:
            owner, unreplicated = await locate(object_id, int(object_version), int(part_number))
        except Exception as exc:  # noqa: BLE001 - a peer must never be able to fail a read
            logger.debug("peer locate failed for %s v%s part %s: %s", object_id, object_version, part_number, exc)
            return None, False
        return owner, bool(unreplicated)

    async def peer_locate_many(
        self, object_id: str, object_version: int, part_numbers: list[int]
    ) -> dict[int, tuple[Optional[str], bool]]:
        """`peer_locate` for many parts through one resolver call; `(None, False)` wherever there is no owner.

        Best-effort on the same grounds as `peer_locate`. A resolver that has only the per-part
        `locate` is asked part by part, so the answer is the same either way — only the number
        of round trips differs.
        """
        parts = [int(n) for n in part_numbers]
        locate_many = getattr(self._peer_fetch, "locate_many", None)
        if locate_many is None:
            return {pn: await self.peer_locate(object_id, object_version, pn) for pn in parts}
        try:
            located = await locate_many(object_id, int(object_version), parts)
        except Exception as exc:  # noqa: BLE001 - a peer must never be able to fail a read
            logger.debug("peer locate failed for %s v%s parts %s: %s", object_id, object_version, parts, exc)
            return dict.fromkeys(parts, (None, False))
        answers: dict[int, tuple[Optional[str], bool]] = {}
        for pn in parts:
            owner, unreplicated = located.get(pn, (None, False))
            answers[pn] = (owner, bool(unreplicated))
        return answers

    def peer_last_owner(self, object_id: str, object_version: int, part_number: int) -> Optional[str]:
        """The peer resolver's memoised owner for a part, with no lookup. Safe inside a response body."""
        last_owner = getattr(self._peer_fetch, "last_owner", None)
        if last_owner is None:
            return None
        return last_owner(object_id, int(object_version), int(part_number))

    async def _promote_chunk(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int, data: bytes
    ) -> None:
        """Copy a pool-served chunk onto the local tier. Best-effort, never fatal.

        The caller already holds the bytes and the pool copy is authoritative, so every
        failure here costs a cache warm and nothing else: a full disk, a read-only mount,
        or a race with the evictor unlinking the part must not turn a successful read into
        a failed one.

        Residency is CLAIMED before anything is written, and a failed claim cancels the copy.
        That is fail-closed on an optimisation: a residency-DB outage disables promotion for its
        duration instead of leaking one unreclaimable copy per promoted chunk onto the disk whose
        filling makes `fs_cache_pressure` refuse every PUT. A copy with no residency row has no
        owner in either process — the drain's evictor is scoped to that table, and `ssd_reclaim`
        skips replicated parts as the read tier — so nothing frees it until some later read
        happens to promote the same chunk again.

        Claiming before writing means a failed write leaves a claim for bytes that never landed —
        and the claim ACCUMULATES on conflict, so without a give-back a persistently failing disk
        would add the same phantom bytes on every retried promotion, unboundedly. The failure path
        therefore releases exactly the bytes it claimed. The release is itself best-effort:
        leaving it un-issued takes a DB fault inside the very promotion whose claim just
        succeeded, and a fault that persists fails the claims themselves — which cancels
        promotion outright — so the residual over-count is bounded by one claim per such
        coincidence, and the evictor re-probes actual free space rather than trusting the sum.

        Meta is written FIRST of the two DISK writes, matching the downloader rather than the
        uploader. Meta is the readiness gate, so writing it first makes each promoted chunk
        readable as it lands; writing it last would leave the whole part invisible until some
        read happened to promote the final chunk.
        """
        # A tail part of a spread multipart object is on another node because the edge put it
        # there, so that no single node holds a whole large object. Promoting it onto the key
        # node — which every read of the object lands on — would undo the spread on the first
        # read. Checked before the space gate: it is a compare, and it applies at any free ratio.
        if 0 < self._promote_max_part < int(part_number):
            _record_promotion_skipped("part_cap")
            return

        # Yield to ingest before doing any work. Promotion competes for the same mount that
        # `fs_cache_pressure` refuses PUTs on, and it is the only writer here that is pure
        # optimisation — a cold cache costs latency, a full disk costs writes. The gate fires
        # above the drain evictor's floor, so promotion backs off before eviction is even
        # armed rather than racing it.
        # BOTH halves of this line matter and they came from different branches. `await` is
        # load-bearing: `allows()` became async when the floor started coming from the agent's
        # published band, and `not <coroutine>` is always False — dropping it does not fail, it
        # silently stops the gate from ever blocking and lets promotion write unthrottled to a
        # disk ingest is about to be refused on. The reason label is what makes a skip legible.
        if self._space_gate is not None and not await self._space_gate.allows():
            _record_promotion_skipped("disk_pressure")
            return

        # Skip if another reader is already promoting this exact chunk. Skipping beats
        # waiting: this caller already holds the bytes, so blocking on someone else's write
        # would add latency and return the same result. Duplicate promotion is not merely
        # wasteful now that the residency upsert accumulates — it would double-count the
        # chunk's bytes and inflate the figure the evictor sums against its deficit.
        in_flight = (object_id, int(object_version), int(part_number), int(chunk_index))
        if in_flight in self._promoting:
            return
        self._promoting.add(in_flight)
        claimed = False
        written = False
        try:
            meta = await self.fallback.get_meta(object_id, object_version, part_number)
            if meta is None:
                return
            # Claim the copy before writing it, so no disk write can outlive a lost claim. The
            # claim sits AFTER the meta read (a read, not a write) purely so the meta-missing
            # bail-out does not leave a row for a promotion that was never going to happen.
            #
            # Reported per CHUNK with the bytes about to be written, not the part's declared
            # total — a range GET promotes only the chunks it touches, so claiming the whole
            # part's size would inflate the number the evictor sums to decide it has freed
            # enough, stopping a pass early while it reports success.
            if self._on_promote is not None:
                if not await self._on_promote(object_id, object_version, part_number, len(data)):
                    _record_promotion_skipped("residency_failed")
                    return
                claimed = True
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
            written = True
        except (OSError, KeyError, TypeError, ValueError) as exc:
            logger.debug(
                "promotion to the local tier failed for %s v%s part %s chunk %s: %s",
                object_id,
                object_version,
                part_number,
                chunk_index,
                exc,
            )
            # Give back the claim, or the accumulating residency row keeps the bytes forever —
            # and gains them AGAIN on every retried promotion against the same failing disk.
            # Everything after the claim can raise into this handler (the meta int() coercions
            # included), so the flag, not the failing statement, decides whether to release.
            if claimed and self._on_promote_release is not None:
                await self._on_promote_release(object_id, object_version, part_number, len(data))
        finally:
            self._promoting.discard(in_flight)
        # Outside the try: the hook runs only for a copy that landed, and a hook fault must not
        # read as a failed write and give back a claim the copy is actually using.
        if written and self._on_promoted is not None:
            try:
                await self._on_promoted(object_id, int(object_version), int(part_number), int(chunk_index))
            except Exception as exc:  # noqa: BLE001 - a wakeup is best-effort; the read already has its bytes
                logger.debug(
                    "promoted-chunk hook failed for %s v%s part %s chunk %s: %s",
                    object_id,
                    object_version,
                    part_number,
                    chunk_index,
                    exc,
                )

    async def invalidate_local_chunk(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int
    ) -> bool:
        """Drop THIS node's copy of a chunk whose stored ciphertext would not authenticate.

        The local tier is read first, so a bad copy on this disk is a permanent, retry-immune
        failure for that object on this node — every retry re-reads the same bytes. Unlinking it
        makes the next read fall through to the peer or the pool, which is what turns a permanent
        failure into a transient one. Read-through promotion is what makes bad local bytes
        reachable in the first place, and no admission check covers torn writes or bit rot, so
        invalidation is the arm that covers every source uniformly.

        Only the PRIMARY is touched. The fallback is the authoritative copy and the fault may be
        in the DEK rather than in the bytes, so a pool chunk failing AEAD stays a genuine error.
        That is also why this method exists on the dual store alone: without a fallback dir the
        single store's root IS the shared pool, and the same call would delete the last copy.

        The pool-presence gate is a data-loss guard, not an optimisation. A freshly ingested part
        lives on SSD alone until the drain replicates it, and a DEK fault fails those chunks too —
        so an ungated unlink would destroy data over a fault that has nothing to do with the bytes.
        The gate is exact for a second reason: pool presence is meta-gated, and the drain persists
        the pool's meta.json LAST, after every chunk is copied and byte-verified (partdrain.rs), so
        anything this can unlink is already past the point where a drain in flight reads the source.
        A REDRIVEN part is the one case where pool presence lies — see the status check below.

        Removes one chunk file, never the part: `meta.json` is the readiness gate, and a part with
        meta and a hole is exactly the downloader's normal partial-fill state — the hole reads as a
        miss and falls through a tier while every sibling chunk still serves off flash.

        Returns whether a local copy was removed. A concurrent reader promoting the same chunk can
        re-write it immediately afterwards; that is self-limiting (the next failed read invalidates
        it again) and not worth serialising against the promotion path.
        """
        if not await self.fallback.chunk_exists(object_id, object_version, part_number, chunk_index):
            return False

        # Pool presence alone is not enough: a redrive flips the part's replication status back
        # to 'pending' while the pool still holds SUPERSEDED bytes that AEAD-verify under the
        # same DEK/AAD — meta-gated `chunk_exists` passes, and the retry would silently serve
        # stale plaintext. A FRESH status check (never the peer resolver's 30s memo) is the only
        # signal that window is open, so refuse both halves of the retry: keep the local copy —
        # during a redrive it may be the only good one — and return False, which the streamer
        # turns into the original decrypt failure. When the status IS 'replicated' nothing here
        # changes; the pre-announcement B-2 window behind that status is closed by the drain's
        # own redrive work (#403), not on the read path.
        if self._replication_suspect is not None and await self._replication_suspect(
            object_id, int(object_version), int(part_number)
        ):
            logger.warning(
                "refusing to invalidate a chunk that failed authentication: a redrive is in "
                "flight, so the pool copy is suspect and the local copy may be the only good "
                "one: %s v%s part %s chunk %s",
                object_id,
                object_version,
                part_number,
                chunk_index,
            )
            return False

        chunk_path = self._chunk_file(Path(self.part_path(object_id, object_version, part_number)), chunk_index)

        def _unlink() -> bool:
            try:
                chunk_path.unlink()
            except FileNotFoundError:
                return False  # the evictor or another reader got there first — same end state
            return True

        try:
            removed = await asyncio.to_thread(_unlink)
        except OSError as exc:
            # A read-only or failing mount must not replace the decrypt failure with an unrelated
            # errno: the caller re-raises the real fault, which is what the client should see.
            logger.error(
                "could not invalidate a chunk that failed authentication, so it stays poisoned on "
                "this node: %s v%s part %s chunk %s: %s",
                object_id,
                object_version,
                part_number,
                chunk_index,
                exc,
            )
            return False

        if removed:
            logger.warning(
                "invalidated a local chunk that failed authentication; the next read falls through "
                "to the pool: %s v%s part %s chunk %s",
                object_id,
                object_version,
                part_number,
                chunk_index,
            )
        return removed

    # These lookups walk local -> pool and stop; only `get_chunk` consults the peer. That is
    # deliberate. They gate whether the read path enqueues a repair, and the two ways of being
    # wrong are not symmetric: a false "missing" costs a redundant background fetch, because
    # `wait_for_chunk` calls `get_chunk` first and the peer still serves the bytes; a false
    # "present" suppresses the repair and stalls the stream when that peer does not have it.
    # Pinned by tests/unit/test_dual_store_lookup_tiers.py.

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
        # The read path decides cache-vs-pipeline from this batch check, so it must see the
        # fallback too. A part is routinely on the pool and not here: the drain retains its
        # copy on the INGEST node's flash, and this pod is usually not that node — and even
        # there, the drain-agent's evictor unlinks on a free-space policy while the pool copy
        # stays. Either way a part present only on the pool is durably available and must read
        # as cache rather than be re-fetched through the download pipeline. Only the primary
        # misses are re-checked, so the common all-present case stays a single primary pass.
        primary = await super().chunks_exist_batch(object_id, object_version, checks)
        missing = [check for check, present in zip(checks, primary, strict=False) if not present]
        if not missing:
            return primary
        fallback = await self.fallback.chunks_exist_batch(object_id, object_version, missing)
        found = {check for check, present in zip(missing, fallback, strict=False) if present}
        return [present or check in found for check, present in zip(checks, primary, strict=False)]

    async def read_local_chunk(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int
    ) -> Optional[bytes]:
        """The peer-serve read: this node's own copy, stamped as used.

        Same tier rule as the base method — the primary only, never the pool and never a peer —
        so a peer still answers from its flash or not at all. What the base cannot do is record
        the hit: a part that is read ONLY through peers (the owner's copy, served to the other
        nodes) never passed through `get_chunk` here and so never reached `_on_local_read`, and
        the evictor ranked the one copy that mattered most as this node's coldest.
        """
        result = await FileSystemPartsStore.get_chunk(self, object_id, object_version, part_number, chunk_index)
        if result is not None and self._on_local_read is not None:
            await self._on_local_read(object_id, int(object_version), int(part_number))
        return result
