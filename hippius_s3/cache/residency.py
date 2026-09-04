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
put on disk — is paid back by `release`: the promoter calls it when the disk write fails, and
it subtracts exactly what the claim added. Without it the over-count would not merely be stale,
it would GROW: the conflict action accumulates, so every retried promotion against a
persistently failing disk would add the same phantom bytes again, inflating node_cache_bytes
until DiskPressure and the allocator's weights steered on a figure the disk does not hold.

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


def _record_release_failure() -> None:
    """Count a decrement that failed. Never let observability break a read."""
    try:
        from hippius_s3.monitoring import get_metrics_collector

        collector = get_metrics_collector()
        if collector is not None:
            collector.record_residency_release_failure()
    except Exception:  # noqa: BLE001 - a metrics failure must not fail a read
        pass


def _record_drop_failure() -> None:
    """Count a version drop that failed. Never let observability break an abort."""
    try:
        from hippius_s3.monitoring import get_metrics_collector

        collector = get_metrics_collector()
        if collector is not None:
            collector.record_residency_drop_failure()
    except Exception:  # noqa: BLE001 - a metrics failure must not fail an abort
        pass


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

    async def release(self, object_id: str, object_version: int, part_number: int, size_bytes: int) -> None:
        """Give back a claim whose bytes never landed. Best-effort; never raises.

        Mirrors `__call__`'s shape and subtracts exactly what that claim added, so a promotion
        whose disk write fails leaves the accumulating row where it started. Floored at zero
        rather than allowed to go negative: the evictor SUMS this column against its deficit,
        and a negative row would corrupt that sum, while an under-count only costs one extra
        eviction candidate. The row is deliberately not deleted at zero — if the claim created
        it, the part dir may hold a meta.json from the failed promotion, and the zero-byte row
        is what keeps that residue owned by the evictor.
        """
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE cephor_ssd_residency
                    SET bytes = GREATEST(cephor_ssd_residency.bytes - $5, 0)
                    WHERE node_id = $1 AND object_id = $2 AND version = $3 AND part_number = $4
                    """,
                    self._node_id,
                    str(object_id),
                    int(object_version),
                    int(part_number),
                    int(size_bytes),
                )
        except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError) as exc:
            # Never raises: this runs on promotion's failure path, where an exception would turn
            # a read that already served its bytes into a failed one. The harm of swallowing is
            # bounded — reaching here at all takes the DB failing INSIDE the promotion whose
            # claim just succeeded on the same pool, and a fault that persists fails the claims
            # themselves, which cancels promotion outright. WARNING plus a counter because each
            # occurrence is phantom bytes the evictor will account against a disk not holding
            # them, and nothing else in the logs would say so.
            _record_release_failure()
            logger.warning(
                "releasing a residency claim failed for %s v%s part %s (%s bytes stay accounted): %s",
                object_id,
                object_version,
                part_number,
                size_bytes,
                exc,
            )

    async def drop_version(self, object_id: str, object_version: int) -> None:
        """Forget every part of a version whose directory this node just removed. Best-effort.

        Node-scoped, mirroring the drain's `drop_residency`: the caller unlinked THIS node's
        copy only, so only this node's rows describe bytes that are gone. A peer's row for the
        same version still names a real directory on the peer's disk, and deleting it would leave
        that copy with no owner — exactly the unreclaimable-leak this table exists to prevent.
        A lingering row is inert rather than dangerous: every reader of this table joins the
        replication row on `status = 'replicated'`, and the abort marks the version `failed`
        first. It is still wrong — the ledger names a directory the disk does not hold — and
        it would otherwise wait for the drain's failed-part reclaim to reach it after its grace.
        """
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    DELETE FROM cephor_ssd_residency
                    WHERE node_id = $1 AND object_id = $2 AND version = $3
                    """,
                    self._node_id,
                    str(object_id),
                    int(object_version),
                )
        except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError) as exc:
            # Never raises: the directory is already gone and the abort has already succeeded
            # from the client's point of view. A leftover row is bounded harm — the drain's
            # failed-part reclaim deletes the version's residency rows fleet-wide when it
            # reaches them — but a sustained rate here means this node's ledger is drifting
            # from its disk with nothing else in the logs to say so, so count it.
            _record_drop_failure()
            logger.warning(
                "dropping residency rows for %s v%s failed (rows stay accounted until reclaim): %s",
                object_id,
                object_version,
                exc,
            )


def create_residency_recorder(pool: Optional[asyncpg.Pool], node_id: str) -> Optional[ResidencyRecorder]:
    """A recorder, or `None` when this process cannot safely claim residency.

    `None` disables promotion in `create_fs_store`, which is the intended outcome: without a
    node identity there is no way to say WHICH node holds the copy, and a promotion nobody
    claims is a copy nobody can evict.
    """
    if pool is None or not node_id:
        return None
    return ResidencyRecorder(pool, node_id)
