"""Claims promoted parts in the drain's per-node SSD residency table.

A promoted copy lives on a node that did NOT ingest the part, so the drain-agent's
evictor — which is scoped to `cephor_ssd_residency.node_id` — cannot reclaim it unless
this node claims it. Without that row the copy sits on the disk forever with nothing able
to free it: `ssd_reclaim` skips `replicated` parts outright (they are the read tier now), so
a replicated part on disk with no residency row has NO owner in either process. It leaks
until some later read happens to promote the same chunk again. Nothing else collects it.

**The claim comes FIRST, before the bytes are written.** An earlier version of this module
argued the opposite — claim after the write, so a failed write leaves no row. That has it
backwards. Claiming second means a residency outage leaks one unreclaimable copy per promoted
chunk, for the whole outage, onto the disk whose filling makes the api answer 503 to every PUT.
Claiming first means the same outage disables promotion instead, and promotion is an
optimisation: the bytes are already served and the pool copy is authoritative, so a skipped
promotion costs a cache warm. Fail closed on the cheap side.

The cost of that ordering — a claim followed by a failed write, leaving a row for bytes never
put on disk — is small, because the evictor re-probes actual free space after each page rather
than trusting the accounted sum. An over-accounted part costs one wasted eviction candidate and
self-corrects on the next pass.

Writes happen once per PROMOTED CHUNK, not once per part, and each carries only the bytes
that chunk is about to write. A range GET promotes only the chunks it touches, so claiming the
whole part's declared size would inflate the number the evictor sums against its deficit and
stop an eviction pass early while it reported success.

Deliberately NOT memoised on "already recorded for this part". Such a memo lives in this
process while the evictor that DELETEs the row runs in another (drain-agent), so it cannot be
invalidated when the row disappears underneath it — a promote → evict → promote sequence
inside the memo window would then write chunks that no evictor can ever see. Duplicate
promotion of the same chunk is instead prevented at source by the in-flight guard in
`DualFileSystemPartsStore._promote_chunk`, which holds only in-flight keys and therefore
drains itself.
"""

from __future__ import annotations

import logging
from typing import Optional

import asyncpg


logger = logging.getLogger(__name__)


class ResidencyRecorder:
    """Claims promoted parts for this node so its evictor owns them."""

    def __init__(self, pool: asyncpg.Pool, node_id: str) -> None:
        self._pool = pool
        self._node_id = node_id

    async def __call__(self, object_id: str, object_version: int, part_number: int, size_bytes: int) -> bool:
        """Claim `size_bytes` of this part for this node. `False` means do not write the copy."""
        try:
            async with self._pool.acquire() as conn:
                # The conflict action ACCUMULATES, where the drain's `record_resident`
                # OVERWRITES. The two are writing different facts: the drain knows the whole
                # part's size at commit and states it; promotion learns the part one chunk at a
                # time and has to add. They cannot collide on a live (node, part) — the drain
                # records only on its own commit, a locally-resident part is served locally and
                # so is never promoted, and eviction removes the row and the directory
                # together, resetting both writers to the same empty starting point.
                #
                # The action itself is load-bearing and must never be dropped: `ON CONFLICT`
                # without one is a syntax error, every claim then fails into the except below,
                # and promoted chunks land on disk with NO residency row — unowned by the
                # node-scoped evictor and skipped by `ssd_reclaim` as read tier, i.e. a
                # permanent SSD leak. That is exactly the failure this recorder exists to
                # prevent, and it is invisible: reads still succeed.
                await conn.execute(
                    """
                    INSERT INTO cephor_ssd_residency (node_id, object_id, version, part_number, bytes)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (node_id, object_id, version, part_number)
                    DO UPDATE SET bytes = cephor_ssd_residency.bytes + EXCLUDED.bytes
                    """,
                    self._node_id,
                    str(object_id),
                    int(object_version),
                    int(part_number),
                    int(size_bytes),
                )
        except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError) as exc:
            # Never raises: the caller is on the read path with the bytes already in hand, so an
            # exception here would fail a request that has already succeeded. `InterfaceError` is
            # in the tuple because it is NOT a `PostgresError` — a closed or exhausted pool, which
            # is exactly the shape a residency outage takes, arrives as one.
            #
            # `False` is what makes this safe rather than merely quiet: the caller cancels the
            # promotion, so no copy lands that this node cannot later evict. WARNING, not debug —
            # sustained failures here mean the read tier has stopped warming, which nothing else
            # in the logs would say.
            logger.warning(
                "claiming promoted residency failed for %s v%s part %s, skipping promotion: %s",
                object_id,
                object_version,
                part_number,
                exc,
            )
            return False
        return True


def create_residency_recorder(pool: Optional[asyncpg.Pool], node_id: str) -> Optional[ResidencyRecorder]:
    """A recorder, or `None` when this process cannot safely claim residency.

    `None` disables promotion in `create_fs_store`, which is the intended outcome: without a
    node identity there is no way to say WHICH node holds the copy, and a promotion nobody
    claims is a copy nobody can evict.
    """
    if pool is None or not node_id:
        return None
    return ResidencyRecorder(pool, node_id)
