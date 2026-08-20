"""The peer-resolution query, executed against real Postgres.

`PeerChunkFetcher._resolve_part` answers two questions in one statement: which node holds this
part on flash, and what the exact ciphertext size of each of its chunks is. The second is what
lets a peer's 200 be checked before its body is served and promoted onto local flash.

**Why this file exists rather than another unit test.** The unit suite drives the fetcher through
a fake connection that returns a canned row and never looks at the SQL, so a statement Postgres
rejects passes every one of those tests. It would then fail in production the quietest way
available: `_resolve_part` raises, `DualFileSystemPartsStore._fetch_from_peer` swallows it, and
every read falls through to the pool. Reads keep succeeding, the peer tier is simply gone, and
nothing says so. That is not hypothetical for this directory — the sibling
`test_residency_upsert_sql.py` exists because commit `5e858ffd` shipped an `ON CONFLICT` with no
action past a fully green unit suite.

This statement carries more of that risk than most: a CTE, a LATERAL join, `array_agg`, a
text-to-uuid cast on a parameter shared between a text column and a uuid one, and two `cephor_*`
tables owned by the Rust drain rather than by these migrations.

Follows the `test_*_sql.py` convention in this directory: TEMP tables shadowing the real schema
for the session, so nothing touches a shared table.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime
from datetime import timezone
from typing import Any
from typing import AsyncGenerator
from typing import Optional

import asyncpg
import pytest
import pytest_asyncio

from hippius_s3.cache.peers import PeerChunkFetcher


pytestmark = pytest.mark.asyncio

_DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius?sslmode=disable")

NODE = "k8s-v3-node2"
PEER = "k8s-v3-node3"
# A second holder of the same part — what a read-through promotion leaves behind. Named so it
# sorts BEFORE `PEER`: the residency primary key leads with node_id, so a plan that reads the
# index would otherwise hand back the long-held copy by luck of the alphabet.
PROMOTED_PEER = "k8s-v3-node1"


class _SingleConnPool:
    """Pool shim handing out the one connection that owns the TEMP tables.

    A real pool may return any connection, and a TEMP table exists on exactly one — so the
    query would run against a session where the tables are absent. Only the pool is adapted:
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


def _fetcher(conn: asyncpg.Connection) -> PeerChunkFetcher:
    """A fetcher wired to nothing but the database — `_resolve_part` touches neither."""
    return PeerChunkFetcher(
        _SingleConnPool(conn),  # type: ignore[arg-type]
        registry=None,  # type: ignore[arg-type]
        node_name=NODE,
        client=None,  # type: ignore[arg-type]
        auth_secret="unused-by-resolve",
    )


@pytest_asyncio.fixture
async def conn() -> AsyncGenerator[asyncpg.Connection, None]:
    try:
        c = await asyncpg.connect(_DB_URL)
    except (OSError, asyncpg.PostgresError) as e:
        pytest.skip(f"integration Postgres unavailable ({e}); run `docker compose up -d postgres`")

    # `cephor_*` mirror drain migrations 0016/0017; `parts` and `part_chunks` mirror this repo's.
    # The column TYPES are the point: object_id is text on the drain side and uuid on ours, which
    # is why the query casts one shared parameter, and a cast that does not hold is exactly the
    # class of failure this file catches.
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

        CREATE TEMP TABLE cephor_replication_status (
            object_id   text   NOT NULL,
            version     bigint NOT NULL,
            part_number bigint NOT NULL,
            status      text   NOT NULL,
            node_id     text,
            claimed_at  timestamptz
        ) ON COMMIT PRESERVE ROWS;

        CREATE TEMP TABLE parts (
            part_id        uuid PRIMARY KEY,
            object_id      uuid    NOT NULL,
            object_version integer NOT NULL,
            part_number    integer NOT NULL
        ) ON COMMIT PRESERVE ROWS;

        CREATE TEMP TABLE part_chunks (
            part_id           uuid   NOT NULL,
            chunk_index       integer NOT NULL,
            cipher_size_bytes bigint NOT NULL,
            UNIQUE (part_id, chunk_index)
        ) ON COMMIT PRESERVE ROWS;
        """
    )
    try:
        yield c
    finally:
        await c.close()


async def _add_residency(
    conn: asyncpg.Connection,
    *,
    object_id: str,
    node: str,
    resident_at: Optional[datetime] = None,
    version: int = 1,
    part_number: int = 3,
) -> None:
    """One more node holding an already-seeded part, at an explicit residency time."""
    await conn.execute(
        "INSERT INTO cephor_ssd_residency (node_id, object_id, version, part_number, resident_at) "
        "VALUES ($1, $2, $3, $4, COALESCE($5, now()))",
        node,
        object_id,
        version,
        part_number,
        resident_at,
    )


async def _seed(
    conn: asyncpg.Connection,
    *,
    object_id: str,
    sizes: list[int],
    resident_on: Optional[str] = PEER,
    status: str = "replicated",
    part_number: int = 3,
    version: int = 1,
    resident_at: Optional[datetime] = None,
    claimed_by: Optional[str] = None,
    claimed_at: Optional[datetime] = None,
) -> None:
    """One part with `sizes` as its chunk sizes; resident on `resident_on` unless None.

    `claimed_by` is the drain claimant recorded on the replication-status row — for a part
    still in 'pending'/'draining' that names the INGEST node, the only holder of the bytes.
    """
    if resident_on is not None:
        await _add_residency(
            conn,
            object_id=object_id,
            node=resident_on,
            resident_at=resident_at,
            version=version,
            part_number=part_number,
        )
    await conn.execute(
        "INSERT INTO cephor_replication_status (object_id, version, part_number, status, node_id, claimed_at) "
        "VALUES ($1, $2, $3, $4, $5, $6)",
        object_id,
        version,
        part_number,
        status,
        claimed_by,
        claimed_at,
    )
    part_id = str(uuid.uuid4())
    await conn.execute(
        "INSERT INTO parts (part_id, object_id, object_version, part_number) VALUES ($1, $2, $3, $4)",
        part_id,
        object_id,
        version,
        part_number,
    )
    for index, size in enumerate(sizes):
        await conn.execute(
            "INSERT INTO part_chunks (part_id, chunk_index, cipher_size_bytes) VALUES ($1, $2, $3)",
            part_id,
            index,
            size,
        )


async def test_the_owner_and_every_chunk_size_come_back_from_one_statement(conn: asyncpg.Connection) -> None:
    """The statement has to execute at all, and yield both facts. A mock cannot tell you that.

    The sizes are what the peer-body check is made of, so a query that runs but returns them
    empty would silently turn every peer fetch into an `unknown_size` shed — the peer tier off,
    with reads still succeeding from the pool.
    """
    object_id = str(uuid.uuid4())
    await _seed(conn, object_id=object_id, sizes=[4194332, 4194332, 1048604])

    owner, sizes = await _fetcher(conn)._resolve_part(object_id, 1, 3)

    assert owner == PEER
    assert sizes == {0: 4194332, 1: 4194332, 2: 1048604}, "the exact per-chunk sizes, keyed by chunk index"


async def test_a_short_final_chunk_keeps_its_own_size(conn: asyncpg.Connection) -> None:
    """The whole point of per-chunk sizes: the last chunk of a part is legitimately shorter.

    If these collapsed to one number per part, a correct final chunk and a truncated middle one
    would be indistinguishable, which is the failure the size check exists to catch.
    """
    object_id = str(uuid.uuid4())
    await _seed(conn, object_id=object_id, sizes=[100, 100, 100, 40])

    _, sizes = await _fetcher(conn)._resolve_part(object_id, 1, 3)

    assert sizes[3] == 40
    assert sizes[1] == 100


async def test_a_part_no_other_node_holds_resolves_to_no_peer(conn: asyncpg.Connection) -> None:
    """No owner means no row at all, so the sizes are never even computed."""
    object_id = str(uuid.uuid4())
    await _seed(conn, object_id=object_id, sizes=[100], resident_on=NODE)

    assert await _fetcher(conn)._resolve_part(object_id, 1, 3) == (None, {})


async def test_an_unreplicated_part_with_no_claimant_resolves_to_no_peer(conn: asyncpg.Connection) -> None:
    """Before the drain claims a fresh part (a ~1s window), nothing records who holds it.

    Residency rows exist only for the read tier (replicated or promoted copies), and the
    status row's node_id is NULL until the ingest node's drain agent claims it — so there is
    genuinely no answer to give, and the read falls through the tiers exactly as before.
    """
    object_id = str(uuid.uuid4())
    await _seed(conn, object_id=object_id, sizes=[100], status="draining")

    assert await _fetcher(conn)._resolve_part(object_id, 1, 3) == (None, {})


async def test_a_fresh_part_resolves_to_its_drain_claimant(conn: asyncpg.Connection) -> None:
    """The fresh-object window: from PUT until replication completes, the ingest node's SSD is
    the ONLY copy anywhere — and the drain gate makes it undeletable until replicated.

    Before this branch existed, a wrong-node read of a just-written part could not resolve a
    peer, fell through every tier, and ended up tailing the drain's ceph copy at replication
    speed (~45 MB/s measured on staging) while the ingest node would have served at 540+.
    The residency row for `resident_on=None` here mirrors reality: none exists yet.
    """
    object_id = str(uuid.uuid4())
    await _seed(conn, object_id=object_id, sizes=[100, 40], resident_on=None, status="pending", claimed_by=PEER)

    owner, sizes = await _fetcher(conn)._resolve_part(object_id, 1, 3)

    assert owner == PEER
    assert sizes == {0: 100, 1: 40}, "the size check applies to fresh parts exactly as to replicated ones"


async def test_a_fresh_part_claimed_by_this_node_resolves_to_no_peer(conn: asyncpg.Connection) -> None:
    """The reader on the ingest node itself serves locally; offering it itself as a peer would
    turn one local read into a loopback HTTP fetch."""
    object_id = str(uuid.uuid4())
    await _seed(conn, object_id=object_id, sizes=[100], resident_on=None, status="pending", claimed_by=NODE)

    assert await _fetcher(conn)._resolve_part(object_id, 1, 3) == (None, {})


async def test_a_redriven_part_resolves_to_the_claimant_not_the_stale_residency(
    conn: asyncpg.Connection,
) -> None:
    """The redrive shape: residency rows survive while the status flips back to 'pending'.

    The two owner branches are mutually exclusive by status — one status row per part, so a
    'pending' part can never yield a replicated-residency candidate. What CAN coexist with
    'pending' is a residency row left from before the redrive, and the branch structure makes
    it unreachable — correctly so: during a redrive the claimant's SSD copy is the SOURCE the
    drain re-copies from (current bytes by definition), while a promoted copy elsewhere is of
    unknown vintage. Before this branch existed, a redriven part resolved to NO peer and reads
    fell to the pool — the exact copy the redrive marked suspect.
    """
    object_id = str(uuid.uuid4())
    await _seed(
        conn,
        object_id=object_id,
        sizes=[100],
        resident_on=PROMOTED_PEER,
        status="pending",
        claimed_by=PEER,
        claimed_at=datetime(2026, 8, 1, 9, 0, tzinfo=timezone.utc),
    )

    owner, _ = await _fetcher(conn)._resolve_part(object_id, 1, 3)

    assert owner == PEER, "the claimant's SSD copy is the source during a redrive; stale residency must not win"


async def test_a_part_with_no_recorded_chunks_resolves_to_an_owner_but_no_sizes(conn: asyncpg.Connection) -> None:
    """An owner with no `part_chunks` rows comes back as empty sizes, not as a crash.

    `array_agg` over no rows returns NULL rather than an empty array, so the two columns arrive
    as `None` and iterating them directly raises `TypeError` — on the read path, inside the
    broad `except` in `_fetch_from_peer`, which would swallow it into a silent pool fallback.
    Empty sizes instead route the chunk to the counted `unknown_size` shed.

    Note what this does NOT pin: the join being LEFT. An aggregate with no GROUP BY yields one
    row regardless, so the owner survives an inner join too — the LEFT is defensive, not
    load-bearing, and swapping it changes nothing here.
    """
    object_id = str(uuid.uuid4())
    await _seed(conn, object_id=object_id, sizes=[])

    owner, sizes = await _fetcher(conn)._resolve_part(object_id, 1, 3)

    assert owner == PEER
    assert sizes == {}


async def test_the_longest_resident_copy_wins_when_several_nodes_hold_the_part(
    conn: asyncpg.Connection,
) -> None:
    """Read-through promotion means a part can be resident on several nodes at once.

    `LIMIT 1` with nothing to order it takes whichever row the plan happens to emit first, so
    the answer for one part is not merely arbitrary but UNSTABLE — it can change between
    replans. The `PartMemo` pins one answer for 30 s and hides that locally, while fleet-wide
    the peer traffic ends up distributed by the planner rather than by anything that accounts
    for it, working against the serve-side fanout cap that is supposed to protect a peer.

    Ordering on `resident_at` makes it deterministic, and picks the copy that has already
    survived eviction passes over one a read created moments ago.

    The seeding is adversarial on purpose. The promoted copy is inserted FIRST and sits on the
    alphabetically-first node, so both orders an unordered `LIMIT 1` can fall back on — physical
    row order, and the residency primary key, which leads with node_id — pick it. Only an
    explicit ordering on `resident_at` returns the other one.
    """
    promoted_at = datetime(2026, 8, 6, 12, 0, tzinfo=timezone.utc)
    ingested_at = datetime(2026, 8, 1, 9, 0, tzinfo=timezone.utc)

    object_id = str(uuid.uuid4())
    await _seed(conn, object_id=object_id, sizes=[100], resident_on=PROMOTED_PEER, resident_at=promoted_at)
    await _add_residency(conn, object_id=object_id, node=PEER, resident_at=ingested_at)

    owner, sizes = await _fetcher(conn)._resolve_part(object_id, 1, 3)

    assert owner == PEER, "a copy promoted five days later won over the long-held one"
    assert sizes == {0: 100}, "and the sizes still come back alongside the owner"


async def test_another_parts_chunk_sizes_are_not_mixed_in(conn: asyncpg.Connection) -> None:
    """Sizes are per (object, version, part). Bleeding across parts would check chunks against
    the wrong lengths — rejecting good bodies and, worse, admitting bad ones."""
    object_id = str(uuid.uuid4())
    await _seed(conn, object_id=object_id, sizes=[100, 100], part_number=3)
    await _seed(conn, object_id=object_id, sizes=[7], part_number=4)

    _, part_three = await _fetcher(conn)._resolve_part(object_id, 1, 3)
    _, part_four = await _fetcher(conn)._resolve_part(object_id, 1, 4)

    assert part_three == {0: 100, 1: 100}
    assert part_four == {0: 7}
