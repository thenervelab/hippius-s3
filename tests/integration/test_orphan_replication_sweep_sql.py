"""Truth table for the A21 orphan sweep query, against real Postgres.

`list_orphan_replication_versions.sql` is the WI-20a backstop. Unlike the abandoned-MPU
reaper (`list_abandoned_versions`, which keys on `multipart_uploads`), this query keys
DIRECTLY on `cephor_replication_status`, so it catches versions whose MPU/parts header
rows are already GONE (an aborted upload deletes them) but whose drain replication rows
leaked — the exact A21 orphan that churns the drain forever. It selects a version to mark
`failed` iff ALL hold:

  * it still has an ACTIVE drain row (`status IN ('pending','draining')`),
  * its version is UNSERVABLE (`address IS NULL` AND size<=0 AND md5=''), the same
    download-servability predicate the janitor uses — so a servable / mid-finalize
    version is never marked, and
  * its most-recently-landed part is older than the grace window (`MAX(landed_at)` gate),
    the last-activity valve — a still-arriving upload keeps landing fresh parts and is
    spared, mirroring the reaper's per-upload activity gate but sourced purely from the
    drain rows (which survive the MPU-header delete).

The unit tests drive `sweep_orphan_replication_versions` with a fake db, so the SQL
predicate itself is only exercised here, against real Postgres via TEMP tables shadowing
`object_versions` and `cephor_replication_status` (no dependency on the Rust drain
migrations being applied to the test DB).
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
_STALE = 3600  # a version is sweepable once its newest part landed longer ago than this


@pytest_asyncio.fixture
async def conn() -> AsyncGenerator[asyncpg.Connection, None]:
    """A real PG connection with TEMP tables shadowing the two tables the sweep reads.
    TEMP tables live in pg_temp (ahead of `public` in the search path), so the query's
    unqualified names resolve to them; they vanish on close — no cleanup, no bleed."""
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
            object_id         text        NOT NULL,
            version           bigint      NOT NULL,
            part_number       bigint      NOT NULL,
            status            text        NOT NULL,
            landed_at         timestamptz NOT NULL DEFAULT now(),
            -- Mirrors drain migration 0012; the sweep's Tier-2 arm reads it, so a mirror
            -- without it makes every case here fail on an undefined column.
            upload_enqueued_at timestamptz,
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
    upload_enqueued: bool = False,
) -> None:
    # landed_age defaults to 2h (stale vs _STALE=3600) so the common case sweeps; pass a
    # small value to model a freshly-landed part that should protect its version.
    # upload_enqueued only matters for status='replicated': set means the backend upload was
    # published, which is what makes such a part legitimately done rather than a Tier-2 orphan.
    await conn.execute(
        "INSERT INTO cephor_replication_status "
        "(object_id, version, part_number, status, landed_at, upload_enqueued_at) "
        "VALUES ($1, $2, $3, $4, now() - make_interval(secs => $5), CASE WHEN $6 THEN now() END)",
        object_id,
        version,
        part_number,
        status,
        landed_age_seconds,
        upload_enqueued,
    )


async def _swept(conn: asyncpg.Connection) -> set[tuple[str, int]]:
    rows = await conn.fetch(get_query("list_orphan_replication_versions"), _STALE)
    return {(r["object_id"], r["version"]) for r in rows}


def _oid() -> str:
    return str(uuid.uuid4())


# =========================================================== the TRUE cases


async def test_pending_unservable_aged_orphan_is_swept(conn):
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, oid, 1, 1, status="pending")
    assert (oid, 1) in await _swept(conn)


async def test_draining_orphan_is_swept(conn):
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, oid, 1, 1, status="draining")
    assert (oid, 1) in await _swept(conn)


async def test_null_md5_is_unservable_and_swept(conn):
    # md5_hash IS NULL must count as unservable too (COALESCE in the predicate).
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=0, md5_hash=None)
    await _seed_status(conn, oid, 1, 1, status="pending")
    assert (oid, 1) in await _swept(conn)


async def test_orphan_with_no_multipart_upload_row_is_swept(conn):
    # The whole reason the sweep exists: an aborted upload has already deleted its
    # multipart_uploads/parts rows, so the abandoned-MPU reaper can never see it. The
    # sweep keys only on cephor + object_versions, so it self-heals such an orphan — this
    # is the "3 live legacy orphans self-heal on first run" case, at the query level.
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, oid, 1, 1, status="pending")
    # No multipart_uploads/parts tables exist in this fixture at all — proof the sweep
    # does not depend on them.
    assert (oid, 1) in await _swept(conn)


# =========================================================== FALSE: terminal status


@pytest.mark.parametrize(
    ("status", "upload_enqueued"),
    [
        # 'failed' is already terminal — re-marking is a no-op but selecting it would churn.
        ("failed", False),
        # A 'replicated' part is only done once its backend upload was ENQUEUED: that proves
        # the address existed, so the object completed.
        ("replicated", True),
    ],
)
async def test_terminal_status_is_not_swept(conn, status, upload_enqueued):
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, oid, 1, 1, status=status, upload_enqueued=upload_enqueued)
    assert await _swept(conn) == set()


async def test_replicated_but_unenqueued_is_swept(conn):
    # The Tier-2 decoupled-commit orphan: the part reached the pool before its object's address
    # was written, so it committed 'replicated' with the enqueue deferred to the sweep. The
    # object was then abandoned, so the address never landed and the enqueue never happens —
    # leaving no chunk_backend rows and a pool copy the janitor's replication gate pins
    # forever. Marking it terminal is what lets that copy be reclaimed.
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, oid, 1, 1, status="replicated", upload_enqueued=False)
    assert await _swept(conn) == {(oid, 1)}


# =========================================================== FALSE: servable versions
# The load-bearing safety cases — the drain's corruption mark can leave a 'pending'/'draining'
# row on a SERVABLE version; the sweep must never mark such a version and strand a live GET.


async def test_servable_by_size_is_not_swept(conn):
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=7, md5_hash="")
    await _seed_status(conn, oid, 1, 1, status="pending")
    assert await _swept(conn) == set()


async def test_servable_by_md5_is_not_swept(conn):
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=0, md5_hash="d41d8cd98f00b204e9800998ecf8427e")
    await _seed_status(conn, oid, 1, 1, status="pending")
    assert await _swept(conn) == set()


async def test_address_written_is_not_swept(conn):
    oid = _oid()
    await _seed_version(conn, oid, 1, address="5Faddr", size_bytes=0, md5_hash="")
    await _seed_status(conn, oid, 1, 1, status="pending")
    assert await _swept(conn) == set()


async def test_mid_finalize_window_is_not_swept(conn):
    # address=NULL BUT size>0 AND md5 set — a version between the size/md5 UPDATE and the
    # set_object_version_address call is GET-servable; the size/md5 guard protects it.
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=4096, md5_hash="9a0364b9e99bb480dd25e1f0284c8555")
    await _seed_status(conn, oid, 1, 1, status="pending")
    assert await _swept(conn) == set()


# =========================================================== FALSE: the activity gate


async def test_freshly_landed_orphan_is_not_swept(conn):
    # Unservable + active, but its part landed inside the grace window → the upload may
    # still be in flight; do not sweep yet.
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, oid, 1, 1, status="pending", landed_age_seconds=60)
    assert await _swept(conn) == set()


async def test_one_recently_landed_part_protects_the_version(conn):
    # MAX(landed_at) is the gate: a single fresh part (a still-arriving upload adding part
    # N) protects the whole version even though an earlier part is stale — without this a
    # slow/in-flight MPU would be swept mid-upload.
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, oid, 1, 1, status="pending", landed_age_seconds=7200)  # stale
    await _seed_status(conn, oid, 1, 2, status="pending", landed_age_seconds=60)  # just landed
    assert await _swept(conn) == set()


# =========================================================== FALSE: missing rows


async def test_no_object_version_row_is_not_swept(conn):
    # A drain row whose object_versions row is absent → unservability cannot be proven →
    # protect (the JOIN yields no row), mirroring the janitor's EXISTS(ov) guard.
    oid = _oid()
    await _seed_status(conn, oid, 1, 1, status="pending")
    assert await _swept(conn) == set()


# =========================================================== dedup / isolation / age


async def test_multiple_active_parts_dedup_to_one_row(conn):
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=0, md5_hash="")
    for part in (1, 2, 3):
        await _seed_status(conn, oid, 1, part, status="pending")
    rows = await conn.fetch(get_query("list_orphan_replication_versions"), _STALE)
    assert [(r["object_id"], r["version"]) for r in rows] == [(oid, 1)], "one row per (object_id, version)"


async def test_version_isolation_keeps_servable_v1(conn):
    # Key overwritten: v1 complete + servable, v2 an abandoned MPU. Only v2 is swept.
    oid = _oid()
    await _seed_version(conn, oid, 1, address="5Fowner", size_bytes=4096, md5_hash="abc123")
    await _seed_status(conn, oid, 1, 1, status="replicated")
    await _seed_version(conn, oid, 2, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, oid, 2, 1, status="pending")
    assert await _swept(conn) == {(oid, 2)}


async def test_age_seconds_reports_oldest_part(conn):
    oid = _oid()
    await _seed_version(conn, oid, 1, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, oid, 1, 1, status="pending", landed_age_seconds=10000)
    await _seed_status(conn, oid, 1, 2, status="pending", landed_age_seconds=5000)
    rows = await conn.fetch(get_query("list_orphan_replication_versions"), _STALE)
    assert len(rows) == 1
    # age is measured from the OLDEST part (MIN landed_at) — the version's true lag.
    assert rows[0]["age_seconds"] >= 10000, "age reflects the oldest part, not the newest"


async def test_full_truth_table(conn):
    """One interleaved pass over the full (status × servability × age) matrix — a future
    predicate edit that flips any cell is caught here."""
    swept_oid = _oid()  # pending + unservable + aged  → SWEPT
    draining_oid = _oid()  # draining + unservable + aged → SWEPT
    servable_oid = _oid()  # pending + servable          → protected
    fresh_oid = _oid()  # pending + unservable + fresh → protected
    replicated_oid = _oid()  # replicated + ENQUEUED       → protected

    await _seed_version(conn, swept_oid, 1, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, swept_oid, 1, 1, status="pending")

    await _seed_version(conn, draining_oid, 1, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, draining_oid, 1, 1, status="draining")

    await _seed_version(conn, servable_oid, 1, address=None, size_bytes=9, md5_hash="")
    await _seed_status(conn, servable_oid, 1, 1, status="pending")

    await _seed_version(conn, fresh_oid, 1, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, fresh_oid, 1, 1, status="pending", landed_age_seconds=30)

    await _seed_version(conn, replicated_oid, 1, address=None, size_bytes=0, md5_hash="")
    await _seed_status(conn, replicated_oid, 1, 1, status="replicated", upload_enqueued=True)

    assert await _swept(conn) == {(swept_oid, 1), (draining_oid, 1)}
