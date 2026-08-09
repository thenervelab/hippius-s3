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
from hypothesis import HealthCheck
from hypothesis import example
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

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
    assert await ResidencyRecorder(_SingleConnPool(conn), NODE)(object_id, 1, 0, 4096) is True, (
        "a claim that landed must report success, or the caller cancels the promotion it allows"
    )

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


async def test_a_failed_claim_is_reported_as_false_rather_than_raising(conn: asyncpg.Connection) -> None:
    """The swallow is deliberate — the chunk is already served — but the caller must be told.

    Returning `False` against a real rejection is what makes promotion fail closed: `False`
    cancels the copy, so a residency outage stops the tier warming instead of leaking one
    unreclaimable copy per promoted chunk. Asserted against a real server because a mock accepts
    any statement, and a statement Postgres rejects is exactly what this has to catch.
    """
    object_id = str(uuid.uuid4())
    await conn.execute("DROP TABLE cephor_ssd_residency")

    assert await ResidencyRecorder(_SingleConnPool(conn), NODE)(object_id, 1, 0, 4096) is False


async def test_a_release_returns_the_row_to_its_pre_claim_bytes(conn: asyncpg.Connection) -> None:
    """The claim's compensation, against the real statement. A claim whose disk write failed
    must be subtracted back or — because the conflict action ADDS — every retried promotion
    against a failing disk grows the row's phantom bytes without bound.
    """
    object_id = str(uuid.uuid4())
    recorder = ResidencyRecorder(_SingleConnPool(conn), NODE)
    await recorder(object_id, 1, 0, 4096)
    await recorder(object_id, 1, 0, 8192)

    await recorder.release(object_id, 1, 0, 8192)

    assert await _bytes_for(conn, NODE, object_id) == 4096, "the release must undo exactly the failed claim"


async def test_a_release_floors_at_zero_rather_than_going_negative(conn: asyncpg.Connection) -> None:
    """The evictor SUMS this column against its deficit, so a negative row corrupts the sum,
    while an under-count only costs one extra eviction candidate."""
    object_id = str(uuid.uuid4())
    recorder = ResidencyRecorder(_SingleConnPool(conn), NODE)
    await recorder(object_id, 1, 0, 100)

    await recorder.release(object_id, 1, 0, 4096)

    assert await _bytes_for(conn, NODE, object_id) == 0


async def test_a_release_for_a_row_the_evictor_already_removed_is_a_no_op(conn: asyncpg.Connection) -> None:
    """The evictor deletes rows from another process, so a release can always lose that race.
    It must neither raise nor resurrect the row — a recreated row would claim a part dir the
    evictor just unlinked."""
    object_id = str(uuid.uuid4())

    await ResidencyRecorder(_SingleConnPool(conn), NODE).release(object_id, 1, 0, 4096)

    assert await _bytes_for(conn, NODE, object_id) is None


# The evictor's own statement, verbatim from `SsdEvictStore::clear_residency`
# (crates/hippius-drain-core/src/store.rs). It runs in the drain-agent, so the interleaving below
# is the only place the two writers of this column meet.
_EVICT_SQL = """
    DELETE FROM cephor_ssd_residency
    WHERE node_id = $1
      AND (object_id, version, part_number) IN
          (SELECT * FROM UNNEST($2::text[], $3::bigint[], $4::bigint[]))
"""

_PARTS = (0, 1, 2)

# Small and repetitive on purpose: a claim of 1 followed by a release of 4096 is what drives the
# row toward negative, and the interesting interleavings all need several ops on the SAME part.
_OPERATIONS = st.lists(
    st.tuples(
        st.sampled_from(["claim", "release", "evict"]),
        st.sampled_from(_PARTS),
        st.sampled_from([1, 4096, 8192]),
    ),
    min_size=1,
    max_size=12,
)


async def _evict(conn: asyncpg.Connection, node: str, object_id: str, part_number: int) -> None:
    await conn.execute(_EVICT_SQL, node, [object_id], [1], [part_number])


@given(operations=_OPERATIONS)
# Pinned rather than left to generation. Both are needle-in-haystack shapes at 25 examples — an
# over-release needs two specific ops on the same part in the right order — and both are the exact
# interleavings the fold's order-dependence turns on, so a run that happens to miss them proves
# nothing. Generation still explores everything around them.
@example(operations=[("claim", 2, 1), ("release", 2, 8192)])
@example(operations=[("claim", 0, 1), ("release", 0, 4096), ("claim", 0, 4096)])
@example(operations=[("claim", 1, 4096), ("evict", 1, 0), ("release", 1, 4096)])
@settings(
    # Every example executes ~12 statements against a real server, so the count is modest; the
    # op space (3 kinds x 3 parts x 3 sizes) is small enough to be covered densely at 25.
    max_examples=25,
    deadline=None,
    # `conn` is function-scoped and therefore shared across examples. Each example uses a fresh
    # object_id, so no example can observe another's rows.
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
async def test_the_ledger_folds_claims_and_releases_and_never_goes_negative(
    conn: asyncpg.Connection, operations: list[tuple[str, int, int]]
) -> None:
    """`bytes` is the column DiskPressure and the allocator's water-fill steer on, so drift here
    misroutes the whole drain — and both processes writing it are blind to each other.

    The example tests above each pin one op in isolation. This applies generated interleavings of
    all three (this process claims and releases, the drain-agent evicts) and compares the real
    rows against a fold, which is what pins the ORDER-dependence: the clamp is applied per
    release, not to the total. `claim 1, release 4096, claim 4096` leaves 4096, where a total of
    `sum(claims) - sum(releases)` clamped once at the end would say 1024 short of it. Over-release
    is absorbed at the moment it happens and never carried forward as a debt.
    """
    object_id = str(uuid.uuid4())
    recorder = ResidencyRecorder(_SingleConnPool(conn), NODE)
    # (row exists, bytes) per part — the row's existence is load-bearing separately from its
    # value: a release must not create one, and a release to zero must not delete one.
    expected: dict[int, tuple[bool, int]] = dict.fromkeys(_PARTS, (False, 0))

    for kind, part, size in operations:
        exists, value = expected[part]
        if kind == "claim":
            assert await recorder(object_id, 1, part, size) is True
            expected[part] = (True, (value if exists else 0) + size)
        elif kind == "release":
            await recorder.release(object_id, 1, part, size)
            expected[part] = (exists, max(value - size, 0) if exists else value)
        else:
            await _evict(conn, NODE, object_id, part)
            expected[part] = (False, 0)

    for part in _PARTS:
        row = await conn.fetchrow(
            "SELECT bytes FROM cephor_ssd_residency "
            "WHERE node_id = $1 AND object_id = $2 AND version = $3 AND part_number = $4",
            NODE,
            object_id,
            1,
            part,
        )
        exists, value = expected[part]
        assert (row is not None) is exists, (
            f"part {part}: a release must not create a row, and a release to zero must not delete "
            "one — the zero-byte row is what keeps a failed promotion's meta.json residue owned"
        )
        if row is not None:
            # Non-negativity first: it is the invariant the other writer depends on, and naming it
            # separately means an over-release reports the harm rather than an arithmetic mismatch.
            assert int(row["bytes"]) >= 0, (
                f"part {part}: the evictor SUMS this column against its deficit, so a negative row "
                f"({int(row['bytes'])}) under-reports the whole node and stops an eviction pass early"
            )
            assert int(row["bytes"]) == value, f"part {part}: the fold and the real rows disagree"


# Same shape as `_OPERATIONS`, but the axis is the NODE rather than the part: every op lands on one
# part, which is the configuration promotion actually creates — several nodes holding the same part,
# each one's copy owned by its own evictor.
_NODE_OPERATIONS = st.lists(
    st.tuples(
        st.sampled_from(["claim", "release", "evict"]),
        st.sampled_from([NODE, OTHER_NODE]),
        st.sampled_from([1, 4096, 8192]),
    ),
    min_size=1,
    max_size=12,
)

_SHARED_PART = 0


@given(operations=_NODE_OPERATIONS)
# The shapes that discriminate, pinned rather than generated: each needs the two nodes to act on the
# same part in a specific order, which 25 examples over a 3x2x3 space will not reliably produce.
# The first is the one a release that forgot `node_id` gets wrong; the second is the same for evict.
@example(operations=[("claim", NODE, 4096), ("claim", OTHER_NODE, 8192), ("release", OTHER_NODE, 8192)])
@example(operations=[("claim", NODE, 4096), ("claim", OTHER_NODE, 8192), ("evict", OTHER_NODE, 0)])
@settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
async def test_one_nodes_claims_and_releases_never_move_another_nodes_row(
    conn: asyncpg.Connection, operations: list[tuple[str, str, int]]
) -> None:
    """Per-node ownership, over interleavings rather than the two-claim example above.

    `test_residency_is_per_node_so_a_peer_claim_is_a_separate_row` pins this for CLAIMS, where the
    `ON CONFLICT` target keeps the rows apart. Nothing pinned it for the other two statements, and
    both carry `node_id` in a WHERE clause instead — where losing it is a one-token edit that no
    single-node test can see. Dropping `node_id` from the release's WHERE passes the entire suite.

    The harm is not an arithmetic slip. Promotion is what puts one part on several nodes, and each
    node's evictor reclaims only what its own row claims. A release that matched every node would
    let one node's failed promotion decrement a peer's live claim, so the peer would keep the bytes
    on disk while accounting for fewer of them — under-reporting `node_cache_bytes` for a disk that
    is genuinely filling, on the node least able to afford it.
    """
    object_id = str(uuid.uuid4())
    recorders = {node: ResidencyRecorder(_SingleConnPool(conn), node) for node in (NODE, OTHER_NODE)}
    expected: dict[str, tuple[bool, int]] = dict.fromkeys((NODE, OTHER_NODE), (False, 0))

    for kind, node, size in operations:
        exists, value = expected[node]
        if kind == "claim":
            assert await recorders[node](object_id, 1, _SHARED_PART, size) is True
            expected[node] = (True, (value if exists else 0) + size)
        elif kind == "release":
            await recorders[node].release(object_id, 1, _SHARED_PART, size)
            expected[node] = (exists, max(value - size, 0) if exists else value)
        else:
            await _evict(conn, node, object_id, _SHARED_PART)
            expected[node] = (False, 0)

    for node in (NODE, OTHER_NODE):
        row = await conn.fetchrow(
            "SELECT bytes FROM cephor_ssd_residency "
            "WHERE node_id = $1 AND object_id = $2 AND version = $3 AND part_number = $4",
            node,
            object_id,
            1,
            _SHARED_PART,
        )
        exists, value = expected[node]
        assert (row is not None) is exists, f"{node}: another node's operation created or removed this row"
        if row is not None:
            assert int(row["bytes"]) == value, f"{node}: another node's operation moved this row's bytes"
