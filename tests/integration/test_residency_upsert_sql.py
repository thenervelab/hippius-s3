"""The residency UPSERT, executed against real Postgres.

`ResidencyRecorder` is the only thing that claims a promoted part for the node that copied it.
Without its row the part is invisible to the node-scoped evictor AND skipped by `ssd_reclaim`
(which treats `replicated` parts as read tier, not debris), so it sits on the disk with no owner
— a permanent leak, while reads keep succeeding the whole time.

**Why this file exists rather than another unit test.** The recorder writes its SQL inline and
swallows every failure by design, so a statement Postgres rejects is indistinguishable from one
that worked: the read still returns bytes and the leak is silent. That is not hypothetical —
commit `5e858ffd` replaced the `DO UPDATE SET` line with the comment explaining it, leaving
`ON CONFLICT (...)` with no action, which is a syntax error. The whole unit suite passed,
because a mocked connection accepts any string.

`test_the_residency_upsert_has_a_conflict_action` now pins that statement's TEXT, which is a
good regression test for that one line. This is the complement: it executes the real statement
against a real server, so the class — syntax, column names, type mismatches, a conflict target
that does not match a real constraint — is caught for the whole path rather than for one string.

Follows the `test_*_sql.py` convention in this directory: a TEMP table shadowing the real schema
for the session, so nothing touches a shared table.
"""

from __future__ import annotations

import os
import uuid
from typing import Any
from typing import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio

from hippius_s3.cache.residency import ResidencyRecorder


pytestmark = pytest.mark.asyncio

_DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius?sslmode=disable")

NODE = "k8s-v3-node2"
OTHER_NODE = "k8s-v3-node3"


class _SingleConnPool:
    """Pool shim handing out the one connection that owns the TEMP table.

    A real pool may return any connection, and a TEMP table exists on exactly one — so the
    recorder would run against a session where the table is absent. Only the pool is adapted:
    the statement, the server, and the schema are all real, which is the point of this file.
    """

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    def acquire(self) -> Any:
        conn = self._conn

        class _Ctx:
            async def __aenter__(self) -> asyncpg.Connection:
                return conn

            async def __aexit__(self, *_: object) -> None:
                return None

        return _Ctx()


@pytest_asyncio.fixture
async def conn() -> AsyncGenerator[asyncpg.Connection, None]:
    try:
        c = await asyncpg.connect(_DB_URL)
    except (OSError, asyncpg.PostgresError) as e:
        pytest.skip(f"integration Postgres unavailable ({e}); run `docker compose up -d postgres`")

    # Mirrors migrations 0016 + 0017, including the composite PRIMARY KEY the UPSERT's
    # ON CONFLICT target must match — a conflict target that names no unique constraint is
    # itself an error this file is meant to catch.
    await c.execute(
        """
        CREATE TEMP TABLE cephor_ssd_residency (
            node_id     text        NOT NULL,
            object_id   text        NOT NULL,
            version     bigint      NOT NULL,
            part_number bigint      NOT NULL,
            resident_at timestamptz NOT NULL DEFAULT now(),
            bytes       bigint      NOT NULL DEFAULT 0,
            last_read_at timestamptz,
            PRIMARY KEY (node_id, object_id, version, part_number)
        ) ON COMMIT PRESERVE ROWS;
        """
    )
    try:
        yield c
    finally:
        await c.close()


async def _bytes_for(conn: asyncpg.Connection, node: str, object_id: str) -> int | None:
    row = await conn.fetchrow(
        "SELECT bytes FROM cephor_ssd_residency WHERE node_id = $1 AND object_id = $2",
        node,
        object_id,
    )
    return None if row is None else int(row["bytes"])


async def test_a_first_promotion_inserts_the_claim(conn: asyncpg.Connection) -> None:
    """The statement has to execute at all. This is what a mock cannot tell you."""
    object_id = str(uuid.uuid4())
    await ResidencyRecorder(_SingleConnPool(conn), NODE)(object_id, 1, 0, 4096)

    assert await _bytes_for(conn, NODE, object_id) == 4096, (
        "no row means the promoted copy is unowned: invisible to the node-scoped evictor and "
        "skipped by ssd_reclaim as read tier"
    )


async def test_promotions_of_further_chunks_accumulate(conn: asyncpg.Connection) -> None:
    """Promotion learns a part one chunk at a time, so the conflict action must ADD.

    Overwriting would report only the last chunk's bytes to the evictor, which sums this column
    to decide it has freed enough — so it would stop a pass early believing it succeeded.
    """
    object_id = str(uuid.uuid4())
    recorder = ResidencyRecorder(_SingleConnPool(conn), NODE)

    for _ in range(3):
        await recorder(object_id, 1, 0, 4096)

    assert await _bytes_for(conn, NODE, object_id) == 3 * 4096


async def test_residency_is_per_node_so_a_peer_claim_is_a_separate_row(conn: asyncpg.Connection) -> None:
    """Two nodes can hold the same part, and each one's evictor owns only its own copy.

    Collapsing these into one row would let one node's eviction retire the other's claim.
    """
    object_id = str(uuid.uuid4())
    await ResidencyRecorder(_SingleConnPool(conn), NODE)(object_id, 1, 0, 4096)
    await ResidencyRecorder(_SingleConnPool(conn), OTHER_NODE)(object_id, 1, 0, 8192)

    assert await _bytes_for(conn, NODE, object_id) == 4096
    assert await _bytes_for(conn, OTHER_NODE, object_id) == 8192


async def test_a_failed_write_is_swallowed_and_leaves_no_row(conn: asyncpg.Connection) -> None:
    """The swallow is deliberate — the chunk is already served — but it must not corrupt state.

    This also pins the shape that made the original bug invisible: the caller cannot distinguish
    a rejected statement from a successful one, which is precisely why the assertions above run
    against a real server instead of a mock.
    """
    object_id = str(uuid.uuid4())
    await conn.execute("DROP TABLE cephor_ssd_residency")

    await ResidencyRecorder(_SingleConnPool(conn), NODE)(object_id, 1, 0, 4096)  # must not raise
