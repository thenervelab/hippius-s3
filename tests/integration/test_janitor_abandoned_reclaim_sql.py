"""Safety-critical truth table for the janitor's `is_terminally_abandoned` predicate.

This is the query that authorizes the janitor to DELETE a CephFS-pool part that is
NOT replicated to any backend. Getting it wrong means either a permanent storage leak
(too strict) or — far worse — deleting bytes a live GET could serve (too loose). The
unit suite mocks this predicate away, so the real SQL semantics (the `failed AND
unservable` AND, the `address IS NULL` + size/md5 download-servability filter, the
uuid cast, COALESCE on a NULL md5) are only ever exercised here, against a real
Postgres.

We run the real `janitor_part_terminally_abandoned.sql` against TEMP tables that
shadow `object_versions` and `cephor_replication_status` for this session — so the
exact production query is tested without depending on the full schema, FKs, or the
Rust drain migrations being applied to the test DB.

The single invariant under test: `is_terminally_abandoned` returns True **iff** the
part's drain row is `status='failed'` AND its version is unservable (`address IS NULL`
AND NOT(`size_bytes > 0 OR md5_hash <> ''`)). Every other combination must be False.
"""

from __future__ import annotations

import os
import uuid
from typing import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio

from workers import run_janitor_in_loop as janitor


pytestmark = pytest.mark.asyncio

_DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius?sslmode=disable")


@pytest_asyncio.fixture
async def conn() -> AsyncGenerator[asyncpg.Connection, None]:
    """A real PG connection with TEMP tables shadowing the two tables the predicate
    reads. TEMP tables live in the session's pg_temp schema, which precedes `public`
    in the search path, so the unqualified names in the query resolve to them. They
    vanish when the connection closes — no cleanup, no cross-test bleed."""
    try:
        c = await asyncpg.connect(_DB_URL)
    except (OSError, asyncpg.PostgresError) as e:
        pytest.skip(f"integration Postgres unavailable ({e}); run `docker compose up -d postgres`")

    # Column shapes mirror the production schema the query relies on:
    #   object_versions.object_id   uuid     (queried via $1::uuid)
    #   object_versions.size_bytes  bigint   (NOT NULL in prod; 0 = incomplete)
    #   object_versions.md5_hash    text     (nullable)
    #   object_versions.address     text     (nullable; NULL = never finalized)
    #   cephor_replication_status.object_id text, version/part_number bigint, status text
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
            object_id   text   NOT NULL,
            version     bigint NOT NULL,
            part_number bigint NOT NULL,
            status      text   NOT NULL,
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
    size_bytes: int,
    md5_hash: str | None,
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
) -> None:
    await conn.execute(
        "INSERT INTO cephor_replication_status (object_id, version, part_number, status) VALUES ($1, $2, $3, $4)",
        object_id,
        version,
        part_number,
        status,
    )


def _oid() -> str:
    return str(uuid.uuid4())


# ---- an UNSERVABLE version mirrors what InitiateMultipartUpload writes: no address,
# ---- size_bytes=0, md5_hash='' (or NULL). The reaper marks its parts 'failed'.
async def _seed_abandoned(conn, oid, *, version=1, part=1, md5="") -> None:
    await _seed_version(conn, oid, version, address=None, size_bytes=0, md5_hash=md5)
    await _seed_status(conn, oid, version, part, status="failed")


# ============================================================ the one TRUE case


async def test_failed_plus_unservable_empty_md5_is_abandoned(conn):
    oid = _oid()
    await _seed_abandoned(conn, oid, md5="")
    assert await janitor.is_terminally_abandoned(conn, oid, 1, 1) is True


async def test_failed_plus_unservable_null_md5_is_abandoned(conn):
    """md5_hash IS NULL must be treated as unservable too (COALESCE in the query)."""
    oid = _oid()
    await _seed_abandoned(conn, oid, md5=None)
    assert await janitor.is_terminally_abandoned(conn, oid, 1, 1) is True


async def test_multiple_parts_of_an_abandoned_version_all_qualify(conn):
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=0, md5_hash="")
    for part in (1, 2, 3):
        await _seed_status(conn, oid, 1, part, status="failed")
    for part in (1, 2, 3):
        assert await janitor.is_terminally_abandoned(conn, oid, 1, part) is True


# ===================================================== FALSE: servable versions
# These are the load-bearing safety cases — a 'failed' part whose version IS
# servable (the drain's corruption mark_failed can produce this) must be PROTECTED.


async def test_failed_but_servable_by_size_is_protected(conn):
    """failed + size_bytes>0 → the version passes the download filter → NEVER delete.
    This is the corruption-on-a-servable-simple-PUT case."""
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=7, md5_hash="")
    await _seed_status(conn, oid, 1, 1, status="failed")
    assert await janitor.is_terminally_abandoned(conn, oid, 1, 1) is False


async def test_failed_but_servable_by_md5_is_protected(conn):
    """failed + md5_hash non-empty → servable → NEVER delete."""
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=0, md5_hash="d41d8cd98f00b204e9800998ecf8427e")
    await _seed_status(conn, oid, 1, 1, status="failed")
    assert await janitor.is_terminally_abandoned(conn, oid, 1, 1) is False


async def test_failed_but_address_written_is_protected(conn):
    """failed + address present → the api finalized this version → NEVER delete,
    even if size/md5 look empty (belt-and-suspenders on the reaper's own predicate)."""
    oid = _oid()
    await _seed_version(conn, oid, 1, address="5Fabc...", size_bytes=0, md5_hash="")
    await _seed_status(conn, oid, 1, 1, status="failed")
    assert await janitor.is_terminally_abandoned(conn, oid, 1, 1) is False


# ===================================================== FALSE: non-failed statuses
# Only a terminal 'failed' row authorizes deletion. pending/draining/replicated do not.


@pytest.mark.parametrize("status", ["pending", "draining", "replicated"])
async def test_non_failed_status_is_never_abandoned(conn, status):
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, oid, 1, 1, status=status)
    assert await janitor.is_terminally_abandoned(conn, oid, 1, 1) is False


# ===================================================== FALSE: missing rows


async def test_no_cephor_row_is_never_abandoned(conn):
    """An unservable version with NO drain row at all (e.g. a part that never landed)
    must not be reclaimed via this path."""
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=0, md5_hash="")
    assert await janitor.is_terminally_abandoned(conn, oid, 1, 1) is False


async def test_failed_but_no_object_version_row_is_protected(conn):
    """A 'failed' drain row whose object_versions row is absent → cannot prove
    unservability → protect (the EXISTS(ov) clause is False)."""
    oid = _oid()
    await _seed_status(conn, oid, 1, 1, status="failed")
    assert await janitor.is_terminally_abandoned(conn, oid, 1, 1) is False


# ===================================================== part / version isolation


async def test_part_number_is_isolated(conn):
    """A 'failed' row for part 1 must not authorize deleting a different part."""
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, oid, 1, 1, status="failed")
    assert await janitor.is_terminally_abandoned(conn, oid, 1, 1) is True
    assert await janitor.is_terminally_abandoned(conn, oid, 1, 2) is False


async def test_version_isolation_overwrite_keeps_servable_v1(conn):
    """Key overwritten: v1 complete+servable, v2 an abandoned MPU. v2's failed parts
    qualify; v1 (servable, no failed row) never does — deleting v2's pool bytes is
    keyed to v2 only and cannot strand v1's live reads."""
    oid = _oid()
    # v1: a real, finalized, servable version (address + size + md5 all set).
    await _seed_version(conn, oid, 1, address="5Fowner", size_bytes=4096, md5_hash="abc123")
    await _seed_status(conn, oid, 1, 1, status="replicated")
    # v2: an abandoned MPU on the same key.
    await _seed_version(conn, oid, 2, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, oid, 2, 1, status="failed")

    assert await janitor.is_terminally_abandoned(conn, oid, 2, 1) is True, "v2 abandoned part is reclaimable"
    assert await janitor.is_terminally_abandoned(conn, oid, 1, 1) is False, "servable v1 must be protected"


# ===================================================== full mixed truth table


async def test_full_truth_table(conn):
    """One pass asserting the complete (status × servability) matrix in a single
    table, so a future change to the predicate that flips any cell is caught here."""
    # (status, address, size, md5) -> expected is_terminally_abandoned
    cases = [
        # the only TRUE rows: failed + unservable
        (("failed", None, 0, ""), True),
        (("failed", None, 0, None), True),
        # failed but servable -> protected
        (("failed", None, 1, ""), False),
        (("failed", None, 0, "x"), False),
        (("failed", "addr", 0, ""), False),
        (("failed", "addr", 10, "x"), False),
        # non-failed status, regardless of servability -> protected
        (("pending", None, 0, ""), False),
        (("draining", None, 0, ""), False),
        (("replicated", None, 0, ""), False),
        (("replicated", "addr", 10, "x"), False),
    ]
    for i, ((status, address, size, md5), expected) in enumerate(cases):
        oid = _oid()
        await _seed_version(conn, oid, 1, address=address, size_bytes=size, md5_hash=md5)
        await _seed_status(conn, oid, 1, 1, status=status)
        got = await janitor.is_terminally_abandoned(conn, oid, 1, 1)
        assert got is expected, f"case {i} {status=} {address=} {size=} {md5=}: expected {expected}, got {got}"
