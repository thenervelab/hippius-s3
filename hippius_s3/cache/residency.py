"""Records promoted parts in the drain's per-node SSD residency table.

A promoted copy lives on a node that did NOT ingest the part, so the drain-agent's
evictor — which is scoped to `cephor_ssd_residency.node_id` — cannot reclaim it unless
this node claims it. Without that row the copy sits on the disk forever with nothing able
to free it.

Writes are deduplicated per part rather than per chunk: promotion runs once per chunk, but
residency is a per-part fact, so a 64-chunk part would otherwise issue 64 identical upserts
on the read path. The dedup set is bounded and self-clearing (see `_SEEN_LIMIT`).
"""

from __future__ import annotations

import logging
from typing import Optional

import asyncpg

from hippius_s3.cache.part_memo import PartMemo


logger = logging.getLogger(__name__)

# Cap on the "already recorded" memo. Promotion is idempotent, so over-evicting the memo costs
# a duplicate upsert, never correctness — the bound exists so a long-lived api pod serving a
# large working set cannot grow this without limit.
_SEEN_LIMIT = 100_000

# How long a recorded part stays remembered. Bounded rather than permanent because the drain's
# evictor can DELETE the residency row underneath us: a part promoted, evicted, then read and
# promoted again must be re-recorded, and a permanent memo would skip it forever, leaving a
# copy on disk that this node's evictor can never see — exactly the leak per-node residency
# exists to prevent.
_SEEN_TTL_SECONDS = 300.0


class ResidencyRecorder:
    """Claims promoted parts for this node so its evictor owns them."""

    def __init__(self, pool: asyncpg.Pool, node_id: str, ttl_seconds: float = _SEEN_TTL_SECONDS) -> None:
        self._pool = pool
        self._node_id = node_id
        self._seen: PartMemo[tuple[str, int, int], bool] = PartMemo(ttl_seconds, _SEEN_LIMIT)

    async def __call__(self, object_id: str, object_version: int, part_number: int, size_bytes: int) -> None:
        key = (object_id, int(object_version), int(part_number))
        if self._seen.get(key) is not None:
            return
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO cephor_ssd_residency (node_id, object_id, version, part_number, bytes)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (node_id, object_id, version, part_number)
                    DO UPDATE SET bytes = EXCLUDED.bytes
                    """,
                    self._node_id,
                    str(object_id),
                    int(object_version),
                    int(part_number),
                    int(size_bytes),
                )
        except (asyncpg.PostgresError, OSError) as exc:
            # Best-effort, like the promotion it accompanies: the bytes are already served and
            # the pool copy is authoritative. A failure here leaves an unclaimed copy that the
            # reclaimer's orphan sweep can still remove, so it degrades to wasted space rather
            # than a failed read. Not cached as seen, so the next promotion retries.
            logger.debug(
                "recording promoted residency failed for %s v%s part %s: %s",
                object_id,
                object_version,
                part_number,
                exc,
            )
            return
        self._seen.put(key, True)


def create_residency_recorder(pool: Optional[asyncpg.Pool], node_id: str) -> Optional[ResidencyRecorder]:
    """A recorder, or `None` when this process cannot safely claim residency.

    `None` disables promotion in `create_fs_store`, which is the intended outcome: without a
    node identity there is no way to say WHICH node holds the copy, and a promotion nobody
    claims is a copy nobody can evict.
    """
    if pool is None or not node_id:
        return None
    return ResidencyRecorder(pool, node_id)
