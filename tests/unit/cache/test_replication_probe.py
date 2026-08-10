"""The AEAD-retry path must not trust a pool copy the drain is currently rewriting.

A redrive flips `cephor_replication_status` back to 'pending' while the pool still holds
superseded bytes that AEAD-verify under the same DEK/AAD — indistinguishable from the real
chunk downstream. The probe is the fresh, per-failure status read that closes that window,
and it must degrade to "not suspect" everywhere the answer is unknowable (no row, no table,
DB down), because those are exactly the deployments and moments that predate the drain.
"""

from __future__ import annotations

import asyncpg
import pytest

from hippius_s3.cache.replication_probe import ReplicationSuspectProbe
from hippius_s3.cache.replication_probe import create_replication_suspect_probe


OBJ = "466916c0-d61b-4518-b81b-9576b574270a"


class FakeConn:
    def __init__(self, pool: "FakePool") -> None:
        self._pool = pool

    async def fetchval(self, sql: str, *args: object) -> object:
        self._pool.queries.append((sql, args))
        if self._pool.raises is not None:
            raise self._pool.raises
        return self._pool.status

    async def __aenter__(self) -> "FakeConn":
        return self

    async def __aexit__(self, *_a: object) -> None:
        return None


class FakePool:
    def __init__(self, status: object = None, raises: Exception | None = None) -> None:
        self.status = status
        self.raises = raises
        self.queries: list[tuple] = []

    def acquire(self) -> FakeConn:
        return FakeConn(self)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["pending", "draining", "failed", "corrupt"])
async def test_any_status_but_replicated_is_suspect(status: str) -> None:
    """The redrive resets to 'pending', but every non-replicated state means the same thing:
    the pool copy is not currently the drain's committed, verified output."""
    assert await ReplicationSuspectProbe(FakePool(status=status))(OBJ, 2, 3) is True


@pytest.mark.asyncio
async def test_a_replicated_part_is_not_suspect() -> None:
    pool = FakePool(status="replicated")
    assert await ReplicationSuspectProbe(pool)(OBJ, 2, 3) is False
    _sql, args = pool.queries[0]
    assert args == (OBJ, 2, 3), "the status is read for exactly this part"


@pytest.mark.asyncio
async def test_a_part_with_no_row_is_not_suspect() -> None:
    """Parts predating the drain and downloader-written pool copies have no row; their pool
    bytes are as trustworthy as they ever were."""
    assert await ReplicationSuspectProbe(FakePool(status=None))(OBJ, 2, 3) is False


@pytest.mark.asyncio
async def test_a_missing_table_degrades_once_and_is_never_probed_again() -> None:
    """Single-tier / pre-drain deployments have no cephor tables at all.

    The absence is a deployment fact, so it is cached: without that, every AEAD retry would
    pay a doomed round trip and log an error for a table that will not appear mid-process.
    """
    pool = FakePool(raises=asyncpg.UndefinedTableError("no cephor_replication_status"))
    probe = ReplicationSuspectProbe(pool)

    assert await probe(OBJ, 2, 3) is False
    assert await probe(OBJ, 2, 4) is False
    assert len(pool.queries) == 1, "the capability is probed once, not per failure"


@pytest.mark.asyncio
async def test_a_db_error_fails_open_to_the_pre_probe_behaviour() -> None:
    """An unreachable DB is not evidence of a redrive. Failing closed would let a DB blip turn
    every recoverable local corruption into a failed read; failing open only re-opens the
    window that existed before the probe, for the blip's duration."""
    pool = FakePool(raises=asyncpg.PostgresError("db down"))
    probe = ReplicationSuspectProbe(pool)

    assert await probe(OBJ, 2, 3) is False
    assert await probe(OBJ, 2, 3) is False
    assert len(pool.queries) == 2, "a transient error is not cached the way a missing table is"


def test_no_pool_means_no_probe() -> None:
    assert create_replication_suspect_probe(None) is None
    assert isinstance(create_replication_suspect_probe(FakePool()), ReplicationSuspectProbe)  # type: ignore[arg-type]
