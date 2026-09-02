"""Object Lock Tier 2: the SQL gates are the durability guarantee, so they run for real here.

Two queries decide whether locked bytes can be destroyed:

- `get_chunk_backend_identifiers` — everything the unpinner deletes from Arion and every backup
  backend flows through it, INCLUDING the `object_version IS NULL` ("all versions of this object")
  form that a versionId-less DELETE enqueues, where the API cannot know what it is about to
  destroy.
- `find_objects_ready_for_hard_delete` — the janitor ring that drops the object row and cascades
  its versions, parts and chunk metadata.

Gating in the API alone would leave the ops scripts (nuke_user, purge_buckets,
delete_legacy_object_versions) and any future caller able to walk straight past it, so these are
the real boundary and they are tested against real Postgres rather than mocked.

The last test pins the SQL predicate against the Python one in object_lock_enforcement: they
encode the same rule in two languages, and a divergence is silent in both directions — either
locked data becomes deletable, or unlocked data becomes permanently undeletable.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from typing import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio

from hippius_s3.api.s3.object_lock_enforcement import is_version_locked
from hippius_s3.utils import get_query


pytestmark = pytest.mark.asyncio


class Ctx:
    def __init__(self, conn: asyncpg.Connection, bucket_id: uuid.UUID, account: str) -> None:
        self.conn = conn
        self.bucket_id = bucket_id
        self.account = account


@pytest_asyncio.fixture
async def ctx(pg_conn: asyncpg.Connection) -> AsyncGenerator[Ctx, None]:
    c = pg_conn
    bucket_id = uuid.uuid4()
    account = f"objlock-{bucket_id}"
    await c.execute("INSERT INTO users (main_account_id) VALUES ($1) ON CONFLICT DO NOTHING", account)
    await c.execute(
        "INSERT INTO buckets (bucket_id, bucket_name, created_at, main_account_id) VALUES ($1, $2, now(), $3)",
        bucket_id,
        f"objlock-{bucket_id}",
        account,
    )
    try:
        yield Ctx(c, bucket_id, account)
    finally:
        await c.execute("DELETE FROM buckets WHERE bucket_id = $1", bucket_id)
        await c.execute("DELETE FROM users WHERE main_account_id = $1", account)


async def _make_object_with_versions(
    ctx: Ctx,
    *,
    key: str,
    versions: list[dict[str, Any]],
    soft_deleted: bool = True,
) -> uuid.UUID:
    """Build an object whose versions each own one part/chunk with a distinct backend identifier.

    Soft-deleted by default: that is the state in which everything is unpin-eligible, i.e. the
    state where the lock is the ONLY thing standing between the bytes and deletion.
    """
    object_id = uuid.uuid4()
    upload_id = uuid.uuid4()
    # One transaction: objects.current_object_version FKs to object_versions and vice versa, so the
    # pair is only consistent at commit. The constraint is DEFERRABLE INITIALLY DEFERRED precisely
    # for this, but deferral only applies inside a transaction.
    async with ctx.conn.transaction():
        await _seed(ctx, object_id, upload_id, key, versions, soft_deleted)
    return object_id


async def _seed(
    ctx: Ctx,
    object_id: uuid.UUID,
    upload_id: uuid.UUID,
    key: str,
    versions: list[dict[str, Any]],
    soft_deleted: bool,
) -> None:
    await ctx.conn.execute(
        "INSERT INTO objects (object_id, bucket_id, object_key, created_at, current_object_version, deleted_at) "
        "VALUES ($1, $2, $3, now(), $4, $5)",
        object_id,
        ctx.bucket_id,
        key,
        max(v["version"] for v in versions),
        datetime.now(timezone.utc) - timedelta(hours=2) if soft_deleted else None,
    )
    await ctx.conn.execute(
        "INSERT INTO multipart_uploads (upload_id, bucket_id, object_key, initiated_at) VALUES ($1, $2, $3, now())",
        upload_id,
        ctx.bucket_id,
        key,
    )
    for v in versions:
        await ctx.conn.execute(
            "INSERT INTO object_versions (object_id, object_version, storage_version, size_bytes, content_type, "
            "object_lock_mode, object_lock_retain_until, object_lock_legal_hold) "
            "VALUES ($1, $2, 5, 10, 'text/plain', $3, $4, $5)",
            object_id,
            v["version"],
            v.get("mode"),
            v.get("retain_until"),
            v.get("legal_hold", False),
        )
        part_id = uuid.uuid4()
        await ctx.conn.execute(
            "INSERT INTO parts (part_id, upload_id, object_id, object_version, part_number, size_bytes, etag, "
            "uploaded_at) VALUES ($1, $2, $3, $4, 1, 10, 'e', now())",
            part_id,
            upload_id,
            object_id,
            v["version"],
        )
        chunk_id = await ctx.conn.fetchval(
            "INSERT INTO part_chunks (part_id, chunk_index, cipher_size_bytes) VALUES ($1, 0, 10) RETURNING id",
            part_id,
        )
        await ctx.conn.execute(
            "INSERT INTO chunk_backend (chunk_id, backend, backend_identifier, deleted) VALUES ($1, 'arion', $2, false)",
            chunk_id,
            f"backend-id-v{v['version']}",
        )


async def _unpin_targets(ctx: Ctx, object_id: uuid.UUID, version: int | None) -> list[str]:
    rows = await ctx.conn.fetch(get_query("get_chunk_backend_identifiers"), "arion", object_id, version)
    return sorted(r["backend_identifier"] for r in rows)


FUTURE = timedelta(days=365)
PAST = timedelta(days=-1)


class TestUnpinnerGate:
    async def test_locked_version_is_never_handed_to_the_unpinner(self, ctx: Ctx) -> None:
        """The core promise. A DELETE with no versionId enqueues version=NULL, so this query is the
        only thing that decides which versions' backend bytes get destroyed."""
        oid = await _make_object_with_versions(
            ctx,
            key="k1",
            versions=[
                {"version": 1, "mode": "COMPLIANCE", "retain_until": datetime.now(timezone.utc) + FUTURE},
                {"version": 2},
            ],
        )
        assert await _unpin_targets(ctx, oid, None) == ["backend-id-v2"]

    async def test_legal_hold_alone_protects(self, ctx: Ctx) -> None:
        """No retention at all — the hold is the only protection, and it must be enough."""
        oid = await _make_object_with_versions(
            ctx, key="k2", versions=[{"version": 1, "legal_hold": True}, {"version": 2}]
        )
        assert await _unpin_targets(ctx, oid, None) == ["backend-id-v2"]

    async def test_expired_retention_with_live_hold_still_protects(self, ctx: Ctx) -> None:
        """The combination that a single 'locked' flag would get wrong."""
        oid = await _make_object_with_versions(
            ctx,
            key="k3",
            versions=[
                {
                    "version": 1,
                    "mode": "GOVERNANCE",
                    "retain_until": datetime.now(timezone.utc) + PAST,
                    "legal_hold": True,
                }
            ],
        )
        assert await _unpin_targets(ctx, oid, None) == []

    async def test_expired_retention_becomes_deletable(self, ctx: Ctx) -> None:
        """A lock must LAPSE, or every locked object is undeletable forever and the bucket can
        never be cleaned up."""
        oid = await _make_object_with_versions(
            ctx,
            key="k4",
            versions=[{"version": 1, "mode": "COMPLIANCE", "retain_until": datetime.now(timezone.utc) + PAST}],
        )
        assert await _unpin_targets(ctx, oid, None) == ["backend-id-v1"]

    async def test_explicitly_targeting_a_locked_version_is_still_refused(self, ctx: Ctx) -> None:
        """Passing the version explicitly must not be a way around the gate."""
        oid = await _make_object_with_versions(
            ctx,
            key="k5",
            versions=[{"version": 1, "mode": "COMPLIANCE", "retain_until": datetime.now(timezone.utc) + FUTURE}],
        )
        assert await _unpin_targets(ctx, oid, 1) == []

    async def test_lock_expiring_mid_flight_is_evaluated_at_query_time(self, ctx: Ctx) -> None:
        """`now()` is evaluated per query, so a lock that lapses between two calls changes the
        answer — the guarantee is 'locked right now', not 'locked when the delete was enqueued'."""
        oid = await _make_object_with_versions(
            ctx,
            key="k6",
            versions=[
                {"version": 1, "mode": "GOVERNANCE", "retain_until": datetime.now(timezone.utc) + timedelta(seconds=1)}
            ],
        )
        assert await _unpin_targets(ctx, oid, None) == []
        await ctx.conn.execute(
            "UPDATE object_versions SET object_lock_retain_until = now() - interval '1 second' "
            "WHERE object_id = $1 AND object_version = 1",
            oid,
        )
        assert await _unpin_targets(ctx, oid, None) == ["backend-id-v1"]


class TestHardDeleteGate:
    async def _ready_ids(self, ctx: Ctx) -> set[uuid.UUID]:
        rows = await ctx.conn.fetch(
            get_query("find_objects_ready_for_hard_delete"),
            1000,
            datetime(1970, 1, 1, tzinfo=timezone.utc),
            uuid.UUID(int=0),
        )
        return {r["object_id"] for r in rows}

    async def test_object_with_a_locked_version_is_not_a_candidate(self, ctx: Ctx) -> None:
        """Hard delete is object-granular and cascades, so one locked version protects the row."""
        locked = await _make_object_with_versions(
            ctx,
            key="hd1",
            versions=[
                {"version": 1, "mode": "COMPLIANCE", "retain_until": datetime.now(timezone.utc) + FUTURE},
                {"version": 2},
            ],
        )
        assert locked not in await self._ready_ids(ctx)

    async def test_object_with_only_expired_locks_is_a_candidate_again(self, ctx: Ctx) -> None:
        unlocked = await _make_object_with_versions(
            ctx,
            key="hd2",
            versions=[{"version": 1, "mode": "GOVERNANCE", "retain_until": datetime.now(timezone.utc) + PAST}],
        )
        # chunk_backend rows must be deleted for the ring to consider it ready at all
        await ctx.conn.execute(
            "UPDATE chunk_backend SET deleted = true WHERE chunk_id IN ("
            "  SELECT pc.id FROM part_chunks pc JOIN parts p ON p.part_id = pc.part_id WHERE p.object_id = $1)",
            unlocked,
        )
        assert unlocked in await self._ready_ids(ctx)

    async def test_legal_hold_keeps_it_out_of_the_ring(self, ctx: Ctx) -> None:
        held = await _make_object_with_versions(ctx, key="hd3", versions=[{"version": 1, "legal_hold": True}])
        await ctx.conn.execute(
            "UPDATE chunk_backend SET deleted = true WHERE chunk_id IN ("
            "  SELECT pc.id FROM part_chunks pc JOIN parts p ON p.part_id = pc.part_id WHERE p.object_id = $1)",
            held,
        )
        assert held not in await self._ready_ids(ctx)


class TestSqlAndPythonAgree:
    """The same rule lives in SQL (workers) and Python (API). A divergence is silent both ways."""

    @pytest.mark.parametrize(
        "mode,delta,hold",
        [
            (None, None, False),
            ("COMPLIANCE", FUTURE, False),
            ("GOVERNANCE", FUTURE, False),
            ("COMPLIANCE", PAST, False),
            ("GOVERNANCE", PAST, False),
            (None, None, True),
            ("COMPLIANCE", PAST, True),
            ("GOVERNANCE", FUTURE, True),
        ],
    )
    async def test_same_verdict(self, ctx: Ctx, mode: str | None, delta: timedelta | None, hold: bool) -> None:
        retain_until = datetime.now(timezone.utc) + delta if delta is not None else None
        oid = await _make_object_with_versions(
            ctx,
            key=f"agree-{mode}-{delta}-{hold}",
            versions=[{"version": 1, "mode": mode, "retain_until": retain_until, "legal_hold": hold}],
        )
        row = await ctx.conn.fetchrow(get_query("get_object_version_lock"), oid, 1)
        python_says_locked = is_version_locked(row)
        sql_says_locked = await _unpin_targets(ctx, oid, None) == []
        assert python_says_locked == sql_says_locked, (
            f"SQL and Python disagree for mode={mode} delta={delta} hold={hold}: "
            f"python={python_says_locked} sql={sql_says_locked}"
        )


class TestOpsScriptGates:
    """The ops scripts issue RAW SQL, so they bypass both query gates entirely.

    A WORM guarantee that holds everywhere except the scripts an operator reaches for during an
    incident is not a guarantee — and the team decision on COMPLIANCE mode names these paths
    explicitly. These run the scripts' actual delete statements against real rows.
    """

    async def test_purge_source_versions_skips_a_locked_version(self, ctx: Ctx) -> None:
        oid = await _make_object_with_versions(
            ctx,
            key="ops1",
            versions=[{"version": 1, "mode": "COMPLIANCE", "retain_until": datetime.now(timezone.utc) + FUTURE}],
        )
        await ctx.conn.execute(
            """
            WITH locked AS (
                SELECT 1 FROM object_versions
                WHERE object_id = $1 AND object_version = $2
                  AND (object_lock_legal_hold
                       OR (object_lock_retain_until IS NOT NULL AND object_lock_retain_until > now()))
            ), del_parts AS (
                DELETE FROM parts WHERE object_id = $1 AND object_version = $2
                  AND NOT EXISTS (SELECT 1 FROM locked) RETURNING 1
            ), del_ver AS (
                DELETE FROM object_versions WHERE object_id = $1 AND object_version = $2
                  AND NOT EXISTS (SELECT 1 FROM locked) RETURNING 1
            )
            SELECT 1
            """,
            oid,
            1,
        )
        survived = await ctx.conn.fetchval(
            "SELECT count(*) FROM object_versions WHERE object_id = $1 AND object_version = 1", oid
        )
        assert survived == 1, "purge_source_versions destroyed a COMPLIANCE-locked version"

    async def test_delete_legacy_versions_skips_a_locked_version(self, ctx: Ctx) -> None:
        oid = await _make_object_with_versions(ctx, key="ops2", versions=[{"version": 1, "legal_hold": True}])
        await ctx.conn.execute(
            """
            DELETE FROM object_versions
             WHERE object_id = $1::uuid AND object_version = $2::bigint
               AND NOT (object_lock_legal_hold
                        OR (object_lock_retain_until IS NOT NULL AND object_lock_retain_until > now()))
            """,
            oid,
            1,
        )
        survived = await ctx.conn.fetchval(
            "SELECT count(*) FROM object_versions WHERE object_id = $1 AND object_version = 1", oid
        )
        assert survived == 1, "delete_legacy_object_versions destroyed a legal-held version"

    async def test_bulk_object_delete_is_blocked_by_one_locked_version(self, ctx: Ctx) -> None:
        """Cascades to every version/part/chunk row, so a single locked version protects the object."""
        oid = await _make_object_with_versions(
            ctx,
            key="ops3",
            versions=[
                {"version": 1, "mode": "GOVERNANCE", "retain_until": datetime.now(timezone.utc) + FUTURE},
                {"version": 2},
            ],
        )
        await ctx.conn.execute(
            """
            DELETE FROM objects
             WHERE object_id = ANY($1::uuid[])
               AND NOT EXISTS (
                   SELECT 1 FROM object_versions ov
                   WHERE ov.object_id = objects.object_id
                     AND (ov.object_lock_legal_hold
                          OR (ov.object_lock_retain_until IS NOT NULL AND ov.object_lock_retain_until > now()))
               )
            """,
            [oid],
        )
        assert await ctx.conn.fetchval("SELECT count(*) FROM objects WHERE object_id = $1", oid) == 1

    async def test_version_reaper_skips_locked_versions(self, ctx: Ctx) -> None:
        oid = await _make_object_with_versions(
            ctx,
            key="ops4",
            versions=[{"version": 1, "mode": "COMPLIANCE", "retain_until": datetime.now(timezone.utc) + FUTURE}],
        )
        await ctx.conn.execute(
            "UPDATE object_versions SET deleted_at = now() - interval '2 hours' WHERE object_id = $1", oid
        )
        rows = await ctx.conn.fetch(
            get_query("find_versions_ready_for_reap"),
            1000,
            datetime(1970, 1, 1, tzinfo=timezone.utc),
            uuid.UUID(int=0),
            0,
        )
        assert oid not in {r["object_id"] for r in rows}, "the reaper offered up a locked version"
