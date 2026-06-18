"""Postgres-level view of the drain, for the staging e2e suite.

Coarse on purpose: it reports ``cephor_replication_status`` row counts by status
(``pending`` / ``draining`` / ``replicated`` / ``failed``), which is enough to assert
"the drain is committing replicated rows" without depending on the object->chunk_key
mapping — that mapping is the still-unresolved api<->drain contract (the drain keys on
its own content-addressed chunk_key, not the api's part identity). Tighten this to a
per-object assertion once that contract is fixed.
"""

import asyncio

import asyncpg  # type: ignore[import-untyped]


_STATUSES = ("pending", "draining", "replicated", "failed")


async def _counts(database_url: str) -> dict[str, int]:
    conn = await asyncpg.connect(database_url)
    try:
        rows = await conn.fetch("SELECT status, count(*) AS n FROM cephor_replication_status GROUP BY status")
    finally:
        await conn.close()
    counts = dict.fromkeys(_STATUSES, 0)
    for row in rows:
        counts[row["status"]] = row["n"]
    return counts


def replication_status_counts(database_url: str) -> dict[str, int]:
    """Return ``cephor_replication_status`` row counts keyed by status.

    Synchronous wrapper over a one-shot asyncpg connection so the pytest test stays
    sync. Raises if the table is absent (the migrations have not been applied) — that
    surfaces as a clear test error rather than a silent zero.
    """
    return asyncio.run(_counts(database_url))
