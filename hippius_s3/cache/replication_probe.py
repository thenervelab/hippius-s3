from __future__ import annotations

import logging
from typing import Optional

import asyncpg


logger = logging.getLogger(__name__)


class ReplicationSuspectProbe:
    """Answers, freshly per call, whether the drain currently distrusts a part's pool copy.

    A redrive (`redrive_diverged_part` / `redrive_corrupt_parts` in the drain) flips a part's
    `cephor_replication_status` back to 'pending' while the pool still holds the SUPERSEDED
    bytes — which AEAD-verify under the same DEK/AAD, so nothing downstream can tell them from
    the real chunk. Any consumer about to trust the pool copy over a live local one therefore
    needs the status AT THAT MOMENT, not at some earlier resolve: this probe is deliberately
    unmemoised, which is affordable because its one call site is the AEAD-failure retry path,
    already a rare, request-fatal-if-wrong event.

    Suspect means the row EXISTS with a status other than 'replicated'. A missing row is not
    suspect: parts predating the drain, and pool copies the downloader wrote from Arion, have
    no row and their pool bytes are as trustworthy as they ever were.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        # Single-tier / pre-drain deployments (prod today) have no cephor tables at all.
        # Probed once and cached: the table's absence is a deployment fact, not a transient,
        # so re-raising UndefinedTableError into the log on every AEAD retry would be noise.
        self._table_missing = False

    async def __call__(self, object_id: str, object_version: int, part_number: int) -> bool:
        """True only when the pool copy is known-suspect. Never raises."""
        if self._table_missing:
            return False
        try:
            async with self._pool.acquire() as conn:
                status = await conn.fetchval(
                    """
                    SELECT status FROM cephor_replication_status
                    WHERE object_id = $1 AND version = $2 AND part_number = $3
                    """,
                    str(object_id),
                    int(object_version),
                    int(part_number),
                )
        except asyncpg.UndefinedTableError:
            self._table_missing = True
            logger.info("cephor_replication_status does not exist; pool copies are trusted as before")
            return False
        except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError) as exc:
            # Fail OPEN, degrading to the pre-probe behaviour: an unreachable DB is not evidence
            # of a redrive, and failing closed would let a DB blip turn every recoverable local
            # corruption into a failed read. The residual exposure — a redrive in flight during
            # the same blip — is exactly the window that existed before this probe.
            logger.warning(
                "replication status probe failed for %s v%s part %s, trusting the pool copy: %s",
                object_id,
                object_version,
                part_number,
                exc,
            )
            return False
        return status is not None and status != "replicated"


def create_replication_suspect_probe(pool: Optional[asyncpg.Pool]) -> Optional[ReplicationSuspectProbe]:
    """A probe, or `None` when this process has no DB access.

    `None` leaves `invalidate_local_chunk` on its pool-presence gate alone — the behaviour
    every worker (which never streams, so never invalidates) already has.
    """
    if pool is None:
        return None
    return ReplicationSuspectProbe(pool)
