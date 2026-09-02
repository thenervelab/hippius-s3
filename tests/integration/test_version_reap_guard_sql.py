"""Truth table for the version reaper's readiness gate.

`reap_deleted_version_parts.sql` is what authorizes the janitor to DELETE the `parts` rows of a
soft-deleted object version — which cascades `part_chunks` -> `chunk_backend`, destroying the
backend-identifier trail. Getting it wrong in the loose direction is the worst failure this feature
has: a version whose upload has not landed yet has ZERO `chunk_backend` rows, so a naive
"nothing live, therefore done" reading calls it ready. Reap it, and when the uploader finally lands
`insert_chunk_backend` does `INSERT ... SELECT pc.id FROM part_chunks WHERE part_id = $1` against a
row that no longer exists — inserting nothing, silently, after the bytes are already on Arion. That
is a permanent backend orphan with no DB record, i.e. exactly the leak this whole change exists to
close.

The unit suite mocks the janitor's SQL away, so the real semantics (the version-scoped NOT EXISTS,
the 1h grace, and the 24h relaxation for the zero-rows case) are only exercised here.

We run the production queries against TEMP tables shadowing the four tables they read, following
`test_janitor_abandoned_reclaim_sql.py` — the exact shipped SQL, no dependency on the full schema.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio

from hippius_s3.utils import get_query


pytestmark = pytest.mark.asyncio

_DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius?sslmode=disable")

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_NIL_UUID = "00000000-0000-0000-0000-000000000000"


@pytest_asyncio.fixture
async def conn() -> AsyncGenerator[asyncpg.Connection, None]:
    """TEMP tables shadowing the tables the reap queries read. pg_temp precedes `public` in the
    search path, so the unqualified names in the shipped SQL resolve to these; they vanish with the
    connection, so there is no cleanup and no cross-test bleed."""
    try:
        c = await asyncpg.connect(_DB_URL)
    except (OSError, asyncpg.PostgresError) as e:
        pytest.skip(f"integration Postgres unavailable ({e}); run `docker compose up -d postgres`")

    await c.execute("""
        CREATE TEMP TABLE object_versions(
            object_id uuid, object_version bigint, deleted_at timestamptz,
            -- Object Lock Tier 2: the reap queries now carry the lock predicate, so the shadow
            -- table needs the columns. Defaults mirror the real migration (unlocked), which keeps
            -- every pre-lock test seeding exactly the unlocked behaviour it always asserted.
            object_lock_mode text, object_lock_retain_until timestamptz,
            object_lock_legal_hold boolean NOT NULL DEFAULT false
        ) ON COMMIT PRESERVE ROWS;
        CREATE TEMP TABLE parts(
            part_id uuid PRIMARY KEY, object_id uuid, object_version bigint
        ) ON COMMIT PRESERVE ROWS;
        CREATE TEMP TABLE part_chunks(
            id uuid PRIMARY KEY, part_id uuid
        ) ON COMMIT PRESERVE ROWS;
        CREATE TEMP TABLE chunk_backend(
            chunk_id uuid, backend text, deleted bool
        ) ON COMMIT PRESERVE ROWS;
    """)
    try:
        yield c
    finally:
        await c.close()


async def _seed(
    conn: asyncpg.Connection,
    *,
    deleted_hours_ago: float,
    backend_rows: list[bool] | None,
) -> tuple[uuid.UUID, int]:
    """One soft-deleted version with a part. `backend_rows` is the `deleted` flag per
    chunk_backend row; None means the chunk exists but has no backend row at all (the
    upload-still-in-flight / CopyObject-destination shape)."""
    object_id = uuid.uuid4()
    part_id = uuid.uuid4()
    chunk_id = uuid.uuid4()
    version = 3
    deleted_at = datetime.now(timezone.utc) - timedelta(hours=deleted_hours_ago)

    await conn.execute(
        "INSERT INTO object_versions(object_id, object_version, deleted_at) VALUES($1,$2,$3)",
        object_id,
        version,
        deleted_at,
    )
    await conn.execute(
        "INSERT INTO parts(part_id, object_id, object_version) VALUES($1,$2,$3)",
        part_id,
        object_id,
        version,
    )
    await conn.execute("INSERT INTO part_chunks(id, part_id) VALUES($1,$2)", chunk_id, part_id)
    for is_deleted in backend_rows or []:
        await conn.execute(
            "INSERT INTO chunk_backend(chunk_id, backend, deleted) VALUES($1,'arion',$2)",
            chunk_id,
            is_deleted,
        )
    return object_id, version


async def _is_ready(conn: asyncpg.Connection, object_id: uuid.UUID) -> bool:
    rows = await conn.fetch(get_query("find_versions_ready_for_reap"), 100, _EPOCH, _NIL_UUID, 0)
    matching = [r for r in rows if r["object_id"] == object_id]
    assert matching, "the seeded version should always be a CANDIDATE, ready or not"
    return bool(matching[0]["ready"])


async def _reap(conn: asyncpg.Connection, object_id: uuid.UUID, version: int) -> bool:
    tag = await conn.execute(get_query("reap_deleted_version_parts"), object_id, version)
    return tag != "DELETE 0"


async def _parts_remaining(conn: asyncpg.Connection, object_id: uuid.UUID) -> int:
    return int(await conn.fetchval("SELECT count(*) FROM parts WHERE object_id = $1", object_id) or 0)


async def test_unpinned_version_is_reaped(conn: asyncpg.Connection) -> None:
    """The normal case: the version was replicated and its backend copies are confirmed gone."""
    object_id, version = await _seed(conn, deleted_hours_ago=2, backend_rows=[True])

    assert await _is_ready(conn, object_id) is True
    assert await _reap(conn, object_id, version) is True
    assert await _parts_remaining(conn, object_id) == 0


async def test_live_backend_copy_blocks_the_reap(conn: asyncpg.Connection) -> None:
    """A live chunk_backend row means the unpin has not completed — never reap."""
    object_id, version = await _seed(conn, deleted_hours_ago=48, backend_rows=[False])

    assert await _is_ready(conn, object_id) is False
    # Even if the finder were wrong, the guarded delete must refuse on its own.
    assert await _reap(conn, object_id, version) is False
    assert await _parts_remaining(conn, object_id) == 1


async def test_in_flight_upload_survives_the_short_grace(conn: asyncpg.Connection) -> None:
    """THE regression guard: zero chunk_backend rows is ambiguous, so 1h is not enough.

    A version deleted minutes after its PUT, while the drain/uploader is backlogged, has no
    chunk_backend rows yet. Reaping it here destroys the parts rows the pending upload needs and
    orphans the bytes on Arion permanently.
    """
    object_id, version = await _seed(conn, deleted_hours_ago=2, backend_rows=None)

    assert await _is_ready(conn, object_id) is False
    assert await _reap(conn, object_id, version) is False
    assert await _parts_remaining(conn, object_id) == 1


async def test_never_replicated_version_is_reaped_after_the_aged_relaxation(
    conn: asyncpg.Connection,
) -> None:
    """Past 24h the zero-rows state is accepted as "never replicated" (a CopyObject destination),
    otherwise those versions would be immortal."""
    object_id, version = await _seed(conn, deleted_hours_ago=25, backend_rows=None)

    assert await _is_ready(conn, object_id) is True
    assert await _reap(conn, object_id, version) is True
    assert await _parts_remaining(conn, object_id) == 0


async def test_grace_period_excludes_a_fresh_delete(conn: asyncpg.Connection) -> None:
    """Inside the 1h grace the version is not even a candidate."""
    object_id, version = await _seed(conn, deleted_hours_ago=0.1, backend_rows=[True])

    rows = await conn.fetch(get_query("find_versions_ready_for_reap"), 100, _EPOCH, _NIL_UUID, 0)
    assert [r for r in rows if r["object_id"] == object_id] == []
    assert await _reap(conn, object_id, version) is False


async def test_reap_leaves_the_version_row_as_a_tombstone(conn: asyncpg.Connection) -> None:
    """Version numbers must stay monotonic.

    upsert_object_basic allocates GREATEST(current, MAX(object_version)) + 1. A versioned DELETE
    repoints current DOWN, so if the reaper also removed the object_versions row, MAX would drop
    and the next PUT would RE-MINT a version number that already existed — colliding with stale FS
    cache under `v<version>/` and letting a queued unpin target live data. The tombstone keeps MAX
    correct; it is removed later when the whole object is hard-deleted.
    """
    object_id, version = await _seed(conn, deleted_hours_ago=2, backend_rows=[True])

    assert await _reap(conn, object_id, version) is True
    assert await _parts_remaining(conn, object_id) == 0
    surviving = await conn.fetchval(
        "SELECT count(*) FROM object_versions WHERE object_id = $1 AND object_version = $2",
        object_id,
        version,
    )
    assert int(surviving) == 1, "the object_versions row must survive as a tombstone"


async def _is_candidate(conn: asyncpg.Connection, object_id: uuid.UUID) -> bool:
    """Whether the finder returns this version AT ALL, ready or not — distinct from `_is_ready`,
    which asserts candidacy and reports only the readiness flag."""
    rows = await conn.fetch(get_query("find_versions_ready_for_reap"), 100, _EPOCH, _NIL_UUID, 0)
    return any(r["object_id"] == object_id for r in rows)


async def test_an_already_reaped_tombstone_stops_being_a_candidate(conn: asyncpg.Connection) -> None:
    """The `EXISTS (parts)` predicate, verified by RE-RUNNING the finder after a reap.

    Nothing marks a reaped version as reaped — the object_versions tombstone is kept on purpose, so
    the only thing distinguishing "already reclaimed" from "still to do" is whether any `parts` rows
    remain. Without the predicate every tombstone the system has ever produced re-qualifies as
    `ready` on every lap, the guarded DELETE runs again for a guaranteed `DELETE 0`, and the ring
    grows monotonically with every versioned DELETE ever performed. Prod holds one object with
    646,993 versions, so that tail pushes reclamation of NEWLY deleted versions out by a full lap.

    The existing coverage stops at the first reap; this is the second lap.
    """
    object_id, version = await _seed(conn, deleted_hours_ago=2, backend_rows=[True])

    assert await _is_candidate(conn, object_id) is True, "precondition: it starts out reapable"
    assert await _reap(conn, object_id, version) is True
    assert await _parts_remaining(conn, object_id) == 0

    assert await _is_candidate(conn, object_id) is False, (
        "an already-reaped tombstone must drop out of the finder, or the ring never stops growing"
    )


async def test_a_version_with_no_parts_is_never_a_candidate(conn: asyncpg.Connection) -> None:
    """The same predicate seen from the delete-marker side: a marker is a zero-size version with no
    parts and nothing to reclaim, so it must never enter the ring in the first place."""
    object_id = uuid.uuid4()
    await conn.execute(
        "INSERT INTO object_versions(object_id, object_version, deleted_at) VALUES($1,$2,$3)",
        object_id,
        1,
        datetime.now(timezone.utc) - timedelta(hours=2),
    )

    assert await _is_candidate(conn, object_id) is False
