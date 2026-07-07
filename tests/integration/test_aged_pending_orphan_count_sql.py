"""Truth table for the aged-pending-orphan gauge query, against real Postgres.

`count_aged_pending_orphans.sql` feeds the `janitor_aged_pending_orphans` gauge — the
A21-leak backlog the replicated-only soak gate is blind to. It counts the SAME population
`list_orphan_replication_versions.sql` sweeps (active + unservable + idle-past-grace), but
returns a single version count instead of a page to mark. This test pins that the count
tracks the sweep set exactly: every version the sweep would select is counted, and nothing
else is — so the gauge cannot silently under- or over-report the leak.

Uses TEMP tables shadowing `object_versions` / `cephor_replication_status` (no dependency on
the Rust drain migrations), mirroring test_orphan_replication_sweep_sql.py.
"""

from __future__ import annotations

import os
import uuid
from typing import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio

from hippius_s3.utils import get_query


pytestmark = pytest.mark.asyncio

_DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius?sslmode=disable")
_STALE = 3600  # a version counts once its newest part landed longer ago than this


@pytest_asyncio.fixture
async def conn() -> AsyncGenerator[asyncpg.Connection, None]:
    """A real PG connection with TEMP tables shadowing the two tables the query reads."""
    try:
        c = await asyncpg.connect(_DB_URL)
    except (OSError, asyncpg.PostgresError) as e:
        pytest.skip(f"integration Postgres unavailable ({e}); run `docker compose up -d postgres`")

    await c.execute(
        """
        CREATE TEMP TABLE object_versions (
            object_id      uuid    NOT NULL,
            object_version bigint  NOT NULL,
            address        text,
            size_bytes     bigint  NOT NULL DEFAULT 0,
            md5_hash       text,
            PRIMARY KEY (object_id, object_version)
        ) ON COMMIT PRESERVE ROWS;

        CREATE TEMP TABLE cephor_replication_status (
            object_id   text        NOT NULL,
            version     bigint      NOT NULL,
            part_number bigint      NOT NULL,
            status      text        NOT NULL,
            landed_at   timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (object_id, version, part_number)
        ) ON COMMIT PRESERVE ROWS;
        """
    )
    try:
        yield c
    finally:
        await c.close()


async def _seed_version(
    conn: asyncpg.Connection,
    object_id: str,
    version: int,
    *,
    address: str | None,
    size_bytes: int = 0,
    md5_hash: str | None = "",
) -> None:
    await conn.execute(
        "INSERT INTO object_versions (object_id, object_version, address, size_bytes, md5_hash) "
        "VALUES ($1::uuid, $2, $3, $4, $5)",
        object_id,
        version,
        address,
        size_bytes,
        md5_hash,
    )


async def _seed_status(
    conn: asyncpg.Connection,
    object_id: str,
    version: int,
    part_number: int,
    *,
    status: str,
    landed_age_seconds: int = 7200,
) -> None:
    # landed_age defaults to 2h (stale vs _STALE=3600) so the common case counts; pass a
    # small value to model a freshly-landed part that should spare its version.
    await conn.execute(
        "INSERT INTO cephor_replication_status (object_id, version, part_number, status, landed_at) "
        "VALUES ($1, $2, $3, $4, now() - make_interval(secs => $5))",
        object_id,
        version,
        part_number,
        status,
        landed_age_seconds,
    )


async def _count(conn: asyncpg.Connection, *, dlq_object_ids: list[str] | None = None) -> int:
    # $2 is the DLQ-protection array the janitor passes (the same set the reaper skips). Default
    # to an empty array — the healthy no-DLQ case — so every pre-existing case exercises "exclude
    # nothing" and only the DLQ test opts into a non-empty set.
    return int(
        await conn.fetchval(
            get_query("count_aged_pending_orphans"),
            _STALE,
            dlq_object_ids or [],
        )
        or 0
    )


def _oid() -> str:
    return str(uuid.uuid4())


# =========================================================== counted (the leak population)


async def test_aged_unservable_pending_orphan_is_counted(conn):
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, oid, 1, 1, status="pending")
    assert await _count(conn) == 1


async def test_draining_orphan_is_counted(conn):
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, oid, 1, 1, status="draining")
    assert await _count(conn) == 1


async def test_null_md5_is_unservable_and_counted(conn):
    # md5_hash IS NULL must count as unservable too (COALESCE in the predicate).
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=0, md5_hash=None)
    await _seed_status(conn, oid, 1, 1, status="pending")
    assert await _count(conn) == 1


async def test_multiple_parts_of_one_version_count_once(conn):
    # The subquery groups by (object_id, version), so a version with many leaked parts is
    # one unit of backlog — the count is a version count, matching the sweep's grain.
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, oid, 1, 1, status="pending")
    await _seed_status(conn, oid, 1, 2, status="draining")
    await _seed_status(conn, oid, 1, 3, status="pending")
    assert await _count(conn) == 1


# =========================================================== NOT counted


@pytest.mark.parametrize("status", ["replicated", "failed"])
async def test_terminal_status_is_not_counted(conn, status):
    # Terminal versions are not the leak: 'replicated' is done, 'failed' is already terminal.
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, oid, 1, 1, status=status)
    assert await _count(conn) == 0


@pytest.mark.parametrize(
    ("address", "size_bytes", "md5_hash"),
    [
        ("5Faddr", 0, ""),  # address written -> servable (finalized)
        (None, 4096, ""),  # size>0 -> servable (mid-finalize: size before address)
        (None, 0, "d41d8cd9"),  # md5 set -> servable (download filter satisfied)
    ],
)
async def test_servable_versions_are_never_counted(conn, address, size_bytes, md5_hash):
    # The load-bearing safety cases: a servable version with an active drain row must NEVER
    # count toward the leak — it is a live object, not an orphan. Each servable disjunct.
    oid = _oid()
    await _seed_version(conn, oid, 1, address=address, size_bytes=size_bytes, md5_hash=md5_hash)
    await _seed_status(conn, oid, 1, 1, status="pending")
    assert await _count(conn) == 0


async def test_freshly_landed_orphan_is_not_yet_counted(conn):
    # Unservable + active but its newest part landed inside the grace: indistinguishable from
    # a still-arriving upload, so it is spared until unambiguously idle.
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, oid, 1, 1, status="pending", landed_age_seconds=1)
    assert await _count(conn) == 0


async def test_a_fresh_part_keeps_a_stale_sibling_uncounted(conn):
    # MAX(landed_at) is the valve: one fresh part on the version means it is still active, so
    # even with an older sibling part the version is spared (mirrors the sweep's HAVING gate).
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, oid, 1, 1, status="pending", landed_age_seconds=7200)
    await _seed_status(conn, oid, 1, 2, status="pending", landed_age_seconds=1)
    assert await _count(conn) == 0


async def test_dlq_protected_orphan_is_not_counted(conn):
    # C2: a DLQ-parked orphan is one the sweep SKIPS (`if str(object_id) in dlq_object_ids:
    # continue`), so it is not a leak the sweep can clear — counting it would be a phantom
    # backlog that reads non-zero forever. The gauge must exclude the same set via $2.
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, oid, 1, 1, status="pending")
    assert await _count(conn) == 1  # without DLQ protection it is an ordinary aged orphan
    assert await _count(conn, dlq_object_ids=[oid]) == 0  # in the DLQ set -> excluded


async def test_dlq_set_only_excludes_its_own_members(conn):
    # A DLQ entry for one object must not spare a different aged orphan: exclusion is per
    # object_id, matching the reaper's membership test exactly.
    parked, leaked = _oid(), _oid()
    await _seed_version(conn, parked, 1, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, parked, 1, 1, status="pending")
    await _seed_version(conn, leaked, 1, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, leaked, 1, 1, status="pending")
    assert await _count(conn, dlq_object_ids=[parked]) == 1  # only `leaked` remains


async def test_version_with_no_object_versions_row_is_not_counted(conn):
    # A deleted-object orphan (no object_versions row) is the reclaim's job, not this gauge:
    # the INNER JOIN drops it, so it never inflates the pending-orphan backlog.
    oid = _oid()
    await _seed_status(conn, oid, 1, 1, status="pending")
    assert await _count(conn) == 0


async def test_mixed_population_counts_only_the_leak(conn):
    # An end-to-end tally: two genuine aged orphans among servable, fresh, terminal, and
    # deleted-object rows -> exactly 2.
    leak_a, leak_b = _oid(), _oid()
    await _seed_version(conn, leak_a, 1, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, leak_a, 1, 1, status="pending")
    await _seed_version(conn, leak_b, 1, address=None, size_bytes=0, md5_hash=None)
    await _seed_status(conn, leak_b, 1, 1, status="draining")

    servable, fresh, terminal, deleted = _oid(), _oid(), _oid(), _oid()
    await _seed_version(conn, servable, 1, address="5Flive", size_bytes=10, md5_hash="")
    await _seed_status(conn, servable, 1, 1, status="pending")
    await _seed_version(conn, fresh, 1, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, fresh, 1, 1, status="pending", landed_age_seconds=1)
    await _seed_version(conn, terminal, 1, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, terminal, 1, 1, status="replicated")
    await _seed_status(conn, deleted, 1, 1, status="pending")  # no object_versions row

    assert await _count(conn) == 2
