from __future__ import annotations

import logging
from typing import Optional

import asyncpg

from hippius_s3.cache.part_memo import PartMemo


logger = logging.getLogger(__name__)

# A part's ownership changes only when the drain commits it or the evictor drops it, so a short
# memo is safe and keeps this off the hot path for all but the first read of a part per window.
# Same shape and TTL as the peer resolver's memo, deliberately: both answer "where does this part
# live", and two different staleness windows for one question would be a trap.
_MEMO_TTL_SECONDS = 30.0
_MEMO_MAX_ENTRIES = 100_000


class LocalResidencyGate:
    """Answers whether THIS node may serve a part from its own flash.

    The node-local SSD is a CACHE of the pool, but `get_chunk` treated it as authoritative: it
    read the file and returned it without ever asking whether this node is recorded as holding
    that part. Nothing else notices, because the AAD binds (bucket, object, part, chunk) and NOT
    the upload attempt, so another attempt's bytes decrypt and authenticate perfectly in place.

    That gap is reachable. Two concurrent `UploadPart`s for the same part routed to different
    api pods each publish their own attempt to their own node's flash — correctly and atomically,
    per-node. Only one wins the `parts` row and gets residency and replication rows. The loser's
    bytes remain on the other node with NO record anywhere, and that node serves them for as long
    as the file exists. Promotion never corrects it: promotion fills a MISSING local copy and
    never overwrites one that is already there. Observed live on staging at 4 of 8 attempts.

    THE PREDICATE, and why it is not simply "is there a residency row".

    A freshly ingested part has no residency row yet — the drain writes one when it commits — and
    the pool does not have it either, because that is the same event. Gating on residency alone
    would refuse to serve the only copy that exists and break read-after-write on every upload.
    So the question is narrower:

        serve locally  unless  the part is REPLICATED and this node is not recorded as holding it

    Replicated means the pool has a durable, byte-verified copy, so falling through costs latency
    and nothing else. Not-replicated means this node's copy may be the only one and must be
    served. The orphan sits in the first case: replicated by its winning node, unrecorded here.

    FAILURE DIRECTION, deliberately split.

    A definitive "this node does not hold a replicated part" refuses the local read — that is the
    whole point. But an ERROR (DB unreachable, pool exhausted, tables absent) serves locally, as
    today. Failing closed on an outage would take every local read on the fleet to the pool at
    once, converting a database blip into a latency and load incident on the tier the drain is
    also writing to. An orphan is rare and bounded; that is not.
    """

    def __init__(self, pool: asyncpg.Pool, node_id: str) -> None:
        self._pool = pool
        self._node_id = node_id
        self._memo: PartMemo[tuple[str, int, int], bool] = PartMemo(_MEMO_TTL_SECONDS, _MEMO_MAX_ENTRIES)
        # Set once a query proves the drain's tables are absent, so a pre-drain deployment pays
        # one failed query rather than one per read forever.
        self._tables_absent = False

    async def may_serve_local(self, object_id: str, object_version: int, part_number: int) -> bool:
        if self._tables_absent:
            return True

        key = (str(object_id), int(object_version), int(part_number))
        cached = self._memo.get(key)
        if cached is not None:
            return cached

        allowed = await self._probe(key)
        # Only a definitive answer is memoised. Caching an error would extend one blip into a
        # 30s window of decisions taken on no information.
        if allowed is not None:
            self._memo.put(key, allowed)
            return allowed
        return True

    async def _probe(self, key: tuple[str, int, int]) -> Optional[bool]:
        object_id, object_version, part_number = key
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT
                        EXISTS (
                            SELECT 1 FROM cephor_ssd_residency r
                            WHERE r.node_id = $1 AND r.object_id = $2
                              AND r.version = $3 AND r.part_number = $4
                        ) AS resident,
                        (
                            SELECT c.status FROM cephor_replication_status c
                            WHERE c.object_id = $2 AND c.version = $3 AND c.part_number = $4
                        ) AS status
                    """,
                    self._node_id,
                    object_id,
                    int(object_version),
                    int(part_number),
                )
        except asyncpg.UndefinedTableError:
            # Pre-drain deployment: there is no ownership record to consult, and the local store
            # is simply the cache it has always been.
            self._tables_absent = True
            logger.info("drain tables absent; local-tier residency gate disabled")
            return None
        except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError) as exc:
            logger.debug("local residency probe failed for %s: %s", key, exc)
            return None

        if row is None:
            return True
        # A missing status row means the drain has never seen this part — it cannot be replicated,
        # so this node's copy is the only one there is.
        if row["status"] != "replicated":
            return True
        return bool(row["resident"])


def create_local_residency_gate(pool: Optional[asyncpg.Pool], node_id: str) -> Optional[LocalResidencyGate]:
    """The gate, or `None` where there is nothing to gate against.

    No pool (workers, scripts, tests) or no node identity means this process cannot know which
    node it is, and a gate that cannot identify itself would refuse every local read.
    """
    if pool is None or not node_id:
        return None
    return LocalResidencyGate(pool, node_id)
