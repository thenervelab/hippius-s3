"""Records promoted parts in the drain's per-node SSD residency table.

A promoted copy lives on a node that did NOT ingest the part, so the drain-agent's
evictor — which is scoped to `cephor_ssd_residency.node_id` — cannot reclaim it unless
this node claims it. Without that row the copy sits on the disk forever with nothing able
to free it.

Writes happen once per PROMOTED CHUNK, not once per part, and each carries only the bytes
that chunk actually wrote. A range GET promotes only the chunks it touches, so claiming the
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

    async def __call__(self, object_id: str, object_version: int, part_number: int, size_bytes: int) -> None:
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO cephor_ssd_residency (node_id, object_id, version, part_number, bytes)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (node_id, object_id, version, part_number)
                    -- ACCUMULATES, where the drain's record_resident OVERWRITES. The two are
                    -- writing different facts: the drain knows the whole part's size at commit
                    -- and states it; promotion learns the part one chunk at a time and has to
                    -- add. They cannot collide on a live (node, part) — the drain records only
                    -- on its own commit, a locally-resident part is served locally and so is
                    -- never promoted, and eviction removes the row and the directory together,
                    -- which resets both writers to the same empty starting point.
                    """,
                    self._node_id,
                    str(object_id),
                    int(object_version),
                    int(part_number),
                    int(size_bytes),
                )
        except (asyncpg.PostgresError, OSError) as exc:
            # Best-effort, like the promotion it accompanies: the bytes are already served and
            # the pool copy is authoritative, so failing the read over a bookkeeping write would
            # trade a served request for nothing.
            #
            # Be clear about what this costs, because an earlier version of this comment claimed
            # the reclaimer's orphan sweep would collect the unclaimed copy and that is NOT true:
            # `ssd_reclaim` skips `replicated` parts outright (they are the read tier now), so a
            # replicated part on disk with no residency row has NO owner — the evictor is scoped
            # to the residency table and cannot see it either. It leaks until some later read
            # promotes the same chunk again and re-runs this upsert. Nothing else collects it.
            logger.debug(
                "recording promoted residency failed for %s v%s part %s: %s",
                object_id,
                object_version,
                part_number,
                exc,
            )
            return


def create_residency_recorder(pool: Optional[asyncpg.Pool], node_id: str) -> Optional[ResidencyRecorder]:
    """A recorder, or `None` when this process cannot safely claim residency.

    `None` disables promotion in `create_fs_store`, which is the intended outcome: without a
    node identity there is no way to say WHICH node holds the copy, and a promotion nobody
    claims is a copy nobody can evict.
    """
    if pool is None or not node_id:
        return None
    return ResidencyRecorder(pool, node_id)
