"""Abort must not hand the next upload a version number that already existed.

The bug this pins: `abort_cleanup_orphan_version.sql` used to DELETE the aborted MPU's reserved
`object_versions` row *and* repoint `objects.current_object_version` down to MAX(survivors). Every
version allocator computes `GREATEST(current_object_version, MAX(object_version)) + 1`, so after an
abort of version N both inputs read N-1 and the very next upload on that key was handed N again.

`cephor_replication_status` has no FK to `object_versions`, so the aborted attempt's rows — which
the abort marks terminal 'failed' a few statements earlier — survive the delete. The reused
version's parts then land on those terminal rows, and nothing re-drives a 'failed' row: the
reconciler skips it, `claim_part` never claims it, and the R4 re-drive worker reads only 'corrupt'.
The result is a completed, servable version with no pool copy, no backend upload and no
`chunk_backend` rows — readable only from the single ingest node still holding the SSD copy.

Retaining the reserved row keeps MAX(object_version) = N, so the number can never be reissued.

These run the REAL queries against real Postgres so the allocator, the FK
(`objects_current_version_fk`, DEFERRABLE INITIALLY DEFERRED) and the read-path filters are
exercised as deployed, not mocked.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import datetime
from datetime import timezone
from typing import Any
from typing import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio

from hippius_s3.utils import get_query


pytestmark = pytest.mark.asyncio


class Ctx:
    """Connection plus the bucket everything in a test hangs off (asyncpg conns use __slots__)."""

    def __init__(self, conn: asyncpg.Connection, bucket_id: uuid.UUID, bucket_name: str) -> None:
        self.conn = conn
        self.bucket_id = bucket_id
        self.bucket_name = bucket_name


@pytest_asyncio.fixture
async def ctx(pg_conn: asyncpg.Connection) -> AsyncGenerator[Ctx, None]:
    # Committed writes, not pg_tx: the locking test needs a SECOND connection to observe this one's
    # rows, which a rolled-back transaction would hide. Everything hangs off one bucket row, so
    # cleanup is a single cascading delete.
    c = pg_conn
    bucket_id = uuid.uuid4()
    account = f"abort-reuse-{bucket_id}"
    name = f"abort-reuse-{bucket_id}"
    await c.execute("INSERT INTO users (main_account_id) VALUES ($1) ON CONFLICT DO NOTHING", account)
    await c.execute(
        "INSERT INTO buckets (bucket_id, bucket_name, created_at, main_account_id) VALUES ($1, $2, now(), $3)",
        bucket_id,
        name,
        account,
    )
    try:
        yield Ctx(c, bucket_id, name)
    finally:
        await c.execute("DELETE FROM buckets WHERE bucket_id = $1", bucket_id)
        await c.execute("DELETE FROM users WHERE main_account_id = $1", account)


async def _initiate_mpu(ctx: Ctx, object_id: uuid.UUID, key: str) -> int:
    """Reserve a version through the REAL multipart allocator; returns the version handed out."""
    row = await ctx.conn.fetchrow(
        get_query("upsert_object_multipart"),
        object_id,
        ctx.bucket_id,
        key,
        "application/octet-stream",
        json.dumps({}),
        "",  # md5 empty until complete
        0,  # size 0 until complete
        datetime.now(timezone.utc),
        5,
        ["arion"],
    )
    assert row is not None
    return int(row["current_object_version"])


async def _simple_put(ctx: Ctx, object_id: uuid.UUID, key: str) -> int:
    """Reserve a version through the REAL simple-PUT allocator; returns the version handed out."""
    row = await ctx.conn.fetchrow(
        get_query("upsert_object_basic"),
        object_id,
        ctx.bucket_id,
        key,
        "application/octet-stream",
        json.dumps({}),
        "",
        0,
        datetime.now(timezone.utc),
        5,
        ["arion"],
    )
    assert row is not None
    return int(row["current_object_version"])


async def _complete(ctx: Ctx, object_id: uuid.UUID, version: int) -> None:
    """Make a reserved version look completed (serveable): real size, md5 and address."""
    await ctx.conn.execute(
        "UPDATE object_versions SET size_bytes = 11, md5_hash = $3, address = 'addr' "
        "WHERE object_id = $1 AND object_version = $2",
        object_id,
        version,
        "5eb63bbbe01eeed093cb22bb8f5acdc3",
    )


async def _abort_cleanup(ctx: Ctx, object_id: uuid.UUID, version: int) -> Any:
    return await ctx.conn.fetchrow(get_query("abort_cleanup_orphan_version"), object_id, version)


async def _version_exists(ctx: Ctx, object_id: uuid.UUID, version: int) -> bool:
    return bool(
        await ctx.conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM object_versions WHERE object_id = $1 AND object_version = $2)",
            object_id,
            version,
        )
    )


async def _current(ctx: Ctx, object_id: uuid.UUID) -> int:
    return int(await ctx.conn.fetchval("SELECT current_object_version FROM objects WHERE object_id = $1", object_id))


# ---------------------------------------------------------------------------
# The regression: a reissued version number
# ---------------------------------------------------------------------------


async def test_next_multipart_initiate_does_not_reuse_the_aborted_version(ctx: Ctx) -> None:
    """v1 completed, v2 initiated then aborted -> the next MPU must get v3, never v2 again."""
    object_id, key = uuid.uuid4(), "k"
    v1 = await _initiate_mpu(ctx, object_id, key)
    await _complete(ctx, object_id, v1)
    v2 = await _initiate_mpu(ctx, object_id, key)
    assert (v1, v2) == (1, 2)

    await _abort_cleanup(ctx, object_id, v2)

    v_next = await _initiate_mpu(ctx, object_id, key)
    assert v_next == 3, f"abort reissued version {v_next}; the aborted attempt's drain rows live under it"


async def test_next_simple_put_does_not_reuse_the_aborted_version(ctx: Ctx) -> None:
    """The poison is not multipart-specific: upsert_object_basic shares the allocator."""
    object_id, key = uuid.uuid4(), "k"
    v1 = await _initiate_mpu(ctx, object_id, key)
    await _complete(ctx, object_id, v1)
    v2 = await _initiate_mpu(ctx, object_id, key)
    await _abort_cleanup(ctx, object_id, v2)

    assert await _simple_put(ctx, object_id, key) == 3


async def test_aborted_version_row_is_retained(ctx: Ctx) -> None:
    """The retained row IS the monotonicity guarantee — MAX(object_version) must not drop."""
    object_id, key = uuid.uuid4(), "k"
    v1 = await _initiate_mpu(ctx, object_id, key)
    await _complete(ctx, object_id, v1)
    v2 = await _initiate_mpu(ctx, object_id, key)

    await _abort_cleanup(ctx, object_id, v2)

    assert await _version_exists(ctx, object_id, v2), "the reserved row must survive as a tombstone"
    assert await _current(ctx, object_id) == v1, "current must still be repointed off the aborted version"
    assert (
        await ctx.conn.fetchval("SELECT max(object_version) FROM object_versions WHERE object_id = $1", object_id)
    ) == v2


async def test_repeated_aborts_keep_climbing(ctx: Ctx) -> None:
    """A client that aborts in a loop (the shape that produced this incident) must never collide."""
    object_id, key = uuid.uuid4(), "k"
    await _complete(ctx, object_id, await _initiate_mpu(ctx, object_id, key))

    seen = set()
    for _ in range(25):
        v = await _initiate_mpu(ctx, object_id, key)
        assert v not in seen, f"version {v} was handed out twice"
        seen.add(v)
        await _abort_cleanup(ctx, object_id, v)

    assert await _current(ctx, object_id) == 1


# ---------------------------------------------------------------------------
# Guards that must NOT fire
# ---------------------------------------------------------------------------


async def test_sole_version_abort_is_a_noop(ctx: Ctx) -> None:
    """No fallback target: current_object_version is NOT NULL and FK-bound, so nothing may move."""
    object_id, key = uuid.uuid4(), "k"
    v1 = await _initiate_mpu(ctx, object_id, key)

    assert await _abort_cleanup(ctx, object_id, v1) is None
    assert await _version_exists(ctx, object_id, v1)
    assert await _current(ctx, object_id) == v1
    # Still monotonic: MAX is unchanged, so the next upload cannot collide either.
    assert await _initiate_mpu(ctx, object_id, key) == 2


async def test_repoint_skipped_when_current_already_advanced(ctx: Ctx) -> None:
    """A later upload owns the pointer; a late abort of an older version must not drag it back."""
    object_id, key = uuid.uuid4(), "k"
    await _complete(ctx, object_id, await _initiate_mpu(ctx, object_id, key))
    v2 = await _initiate_mpu(ctx, object_id, key)
    v3 = await _initiate_mpu(ctx, object_id, key)
    assert await _current(ctx, object_id) == v3

    assert await _abort_cleanup(ctx, object_id, v2) is None
    assert await _current(ctx, object_id) == v3


async def test_completed_version_is_not_repointed_off(ctx: Ctx) -> None:
    """Aborting an upload whose version has since COMPLETED must not hide it behind an older
    pointer. This predicate used to guard the DELETE; it guards the repoint now."""
    object_id, key = uuid.uuid4(), "k"
    v1 = await _initiate_mpu(ctx, object_id, key)
    await _complete(ctx, object_id, v1)
    v2 = await _initiate_mpu(ctx, object_id, key)
    await _complete(ctx, object_id, v2)

    assert await _abort_cleanup(ctx, object_id, v2) is None
    assert await _current(ctx, object_id) == v2, "current must still point at the completed version"


async def test_abort_cleanup_is_idempotent(ctx: Ctx) -> None:
    """The caller runs this best-effort and the reaper can re-run it; a second pass must no-op."""
    object_id, key = uuid.uuid4(), "k"
    v1 = await _initiate_mpu(ctx, object_id, key)
    await _complete(ctx, object_id, v1)
    v2 = await _initiate_mpu(ctx, object_id, key)

    assert await _abort_cleanup(ctx, object_id, v2) is not None
    assert await _abort_cleanup(ctx, object_id, v2) is None
    assert await _current(ctx, object_id) == v1
    assert await _version_exists(ctx, object_id, v2)


async def test_abort_of_unknown_version_is_a_noop(ctx: Ctx) -> None:
    """A version that never existed (legacy/NULL-version parts) must not move the pointer."""
    object_id, key = uuid.uuid4(), "k"
    v1 = await _initiate_mpu(ctx, object_id, key)
    await _complete(ctx, object_id, v1)

    assert await _abort_cleanup(ctx, object_id, 999) is None
    assert await _current(ctx, object_id) == v1


# ---------------------------------------------------------------------------
# The retained row must stay invisible to reads
# ---------------------------------------------------------------------------


async def test_tombstone_is_invisible_to_the_unversioned_download_query(ctx: Ctx) -> None:
    """A plain GET must resolve to the last completed version, not the retained reserved row."""
    object_id, key = uuid.uuid4(), "k"
    v1 = await _initiate_mpu(ctx, object_id, key)
    await _complete(ctx, object_id, v1)
    v2 = await _initiate_mpu(ctx, object_id, key)
    await _abort_cleanup(ctx, object_id, v2)
    bucket = ctx.bucket_name

    row = await ctx.conn.fetchrow(get_query("get_object_for_download_with_permissions"), bucket, key)

    assert row is not None, "the completed version must still resolve"
    assert int(row["object_version"]) == v1


async def test_tombstone_is_invisible_to_the_by_version_download_query(ctx: Ctx) -> None:
    """`GET ?versionId=<aborted>` must be NoSuchVersion, not a 0-byte body. Without the serveable
    filter the retained row joins cleanly and the endpoint serves an empty object."""
    object_id, key = uuid.uuid4(), "k"
    v1 = await _initiate_mpu(ctx, object_id, key)
    await _complete(ctx, object_id, v1)
    v2 = await _initiate_mpu(ctx, object_id, key)
    await _abort_cleanup(ctx, object_id, v2)
    bucket = ctx.bucket_name

    row = await ctx.conn.fetchrow(get_query("get_object_for_download_with_permissions_by_version"), bucket, key, v2)

    assert row is None, "a reserved/aborted version must not be fetchable by explicit versionId"


async def test_in_flight_mpu_version_is_not_fetchable_by_version(ctx: Ctx) -> None:
    """The same filter must hide a live, still-uploading MPU's reserved row — pre-existing hole."""
    object_id, key = uuid.uuid4(), "k"
    v1 = await _initiate_mpu(ctx, object_id, key)
    await _complete(ctx, object_id, v1)
    v2 = await _initiate_mpu(ctx, object_id, key)
    bucket = ctx.bucket_name

    row = await ctx.conn.fetchrow(get_query("get_object_for_download_with_permissions_by_version"), bucket, key, v2)

    assert row is None


async def test_zero_byte_object_is_still_fetchable_by_version(ctx: Ctx) -> None:
    """A legitimately empty object is a real S3 object. It has size 0 but a real md5, so the
    disjunct must admit it — otherwise the new filter breaks empty uploads."""
    object_id, key = uuid.uuid4(), "k"
    v1 = await _initiate_mpu(ctx, object_id, key)
    await ctx.conn.execute(
        "UPDATE object_versions SET size_bytes = 0, md5_hash = $3, address = 'addr' "
        "WHERE object_id = $1 AND object_version = $2",
        object_id,
        v1,
        "d41d8cd98f00b204e9800998ecf8427e",
    )
    bucket = ctx.bucket_name

    row = await ctx.conn.fetchrow(get_query("get_object_for_download_with_permissions_by_version"), bucket, key, v1)

    assert row is not None, "a 0-byte object with a real md5 must remain fetchable"
    assert int(row["object_version"]) == v1


# ---------------------------------------------------------------------------
# Schema-level invariants
# ---------------------------------------------------------------------------


async def test_current_version_fk_holds_after_abort(ctx: Ctx) -> None:
    """Regression guard, not a live hazard: with the DELETE gone this query only repoints onto a
    row it just read, so it cannot violate objects_current_version_fk. The FK is DEFERRABLE, so a
    violation would surface at commit — force the check rather than trust the statement."""
    object_id, key = uuid.uuid4(), "k"
    v1 = await _initiate_mpu(ctx, object_id, key)
    await _complete(ctx, object_id, v1)
    v2 = await _initiate_mpu(ctx, object_id, key)

    async with ctx.conn.transaction():
        await _abort_cleanup(ctx, object_id, v2)
        await ctx.conn.execute("SET CONSTRAINTS ALL IMMEDIATE")

    assert await _current(ctx, object_id) == v1


async def test_all_version_allocators_share_one_expression() -> None:
    """Three queries mint versions. The fix relies on MAX(object_version) being an input to every
    one of them; a future edit that diverges silently reopens the reuse hole in that path only."""
    allocator = (
        "GREATEST(\n      objects.current_object_version,\n"
        "      (SELECT COALESCE(MAX(ov.object_version), 0) FROM object_versions ov "
        "WHERE ov.object_id = objects.object_id)\n    ) + 1"
    )
    for name in ("upsert_object_basic", "upsert_object_multipart", "upsert_object_with_cid"):
        assert allocator in get_query(name), f"{name} no longer shares the monotonic version allocator"


async def test_repoint_skips_a_concurrent_in_flight_reserved_version(ctx: Ctx) -> None:
    """v1 completed, v2 still uploading, v3 aborted. current must land on v1 — repointing onto v2's
    reserved row would put a 0-byte placeholder in front of every query that joins current."""
    object_id, key = uuid.uuid4(), "k"
    v1 = await _initiate_mpu(ctx, object_id, key)
    await _complete(ctx, object_id, v1)
    v2 = await _initiate_mpu(ctx, object_id, key)
    v3 = await _initiate_mpu(ctx, object_id, key)

    await _abort_cleanup(ctx, object_id, v3)

    assert await _current(ctx, object_id) == v1
    assert await _version_exists(ctx, object_id, v2), "the in-flight upload's row must be untouched"


async def test_repoints_to_the_highest_remaining_version_when_none_are_complete(ctx: Ctx) -> None:
    """Two MPUs open on a new key at once: at abort time BOTH versions are still reserved, so there
    is no completed row to point at. The pointer must still move off the aborted version — leaving
    it stranded there is permanent, because CompleteMultipartUpload writes only object_versions and
    assumes initiate already set the pointer. A stranded pointer makes DELETE resolve to a version
    with no chunk_backend rows, so the real version's chunks are never unpinned."""
    object_id, key = uuid.uuid4(), "k"
    v1 = await _initiate_mpu(ctx, object_id, key)
    v2 = await _initiate_mpu(ctx, object_id, key)

    assert await _abort_cleanup(ctx, object_id, v2) is not None
    assert await _current(ctx, object_id) == v1, "pointer stranded on the aborted version"

    # The surviving upload completes; the pointer must already be on it.
    await _complete(ctx, object_id, v1)
    assert await _current(ctx, object_id) == v1
    assert await _initiate_mpu(ctx, object_id, key) == 3, "monotonicity still holds"


async def test_fallback_never_reaches_above_the_aborted_version(ctx: Ctx) -> None:
    """The migrator finalizes a version ABOVE current before its CAS promotes it. An unbounded MAX
    would promote it early and break that CAS, so both fallback arms are bounded below $2."""
    object_id, key = uuid.uuid4(), "k"
    v1 = await _initiate_mpu(ctx, object_id, key)
    await _complete(ctx, object_id, v1)
    v2 = await _initiate_mpu(ctx, object_id, key)
    v3 = await _initiate_mpu(ctx, object_id, key)
    await _complete(ctx, object_id, v3)
    # current still points at v2 (v3 is finalized but unpromoted).
    await ctx.conn.execute("UPDATE objects SET current_object_version = $2 WHERE object_id = $1", object_id, v2)

    await _abort_cleanup(ctx, object_id, v2)

    assert await _current(ctx, object_id) == v1, "the pointer jumped forward onto an unpromoted version"


async def test_abort_racing_complete_does_not_hide_the_completed_version(ctx: Ctx) -> None:
    """A client can race AbortMultipartUpload against CompleteMultipartUpload on the same upload —
    the abort handler never checks is_completed. Here the completion COMMITS FIRST, so the abort
    sees it and must decline. (The harder ordering — completion still uncommitted when the abort
    runs — is what `test_abort_blocks_on_an_uncommitted_completion_and_then_declines` covers; only
    that one exercises the row lock.)"""
    object_id, key = uuid.uuid4(), "k"
    v1 = await _initiate_mpu(ctx, object_id, key)
    await _complete(ctx, object_id, v1)
    v2 = await _initiate_mpu(ctx, object_id, key)

    other = await asyncpg.connect(os.environ["DATABASE_URL"])
    try:
        async with ctx.conn.transaction():
            # Establish the aborting statement's snapshot BEFORE the concurrent completion lands.
            await ctx.conn.fetchval("SELECT count(*) FROM object_versions WHERE object_id = $1", object_id)
            await other.execute(
                "UPDATE object_versions SET size_bytes = 11, md5_hash = $3, address = 'addr' "
                "WHERE object_id = $1 AND object_version = $2",
                object_id,
                v2,
                "5eb63bbbe01eeed093cb22bb8f5acdc3",
            )
            await _abort_cleanup(ctx, object_id, v2)
    finally:
        await other.close()

    assert await _current(ctx, object_id) == v2, "the pointer was rewound off a completed version"


async def test_abort_blocks_on_an_uncommitted_completion_and_then_declines(ctx: Ctx) -> None:
    """The reserved-check must be a locked CAS, not a snapshot read.

    T2 completes v2 inside an open transaction; T1's abort must BLOCK on that row rather than act
    on its own (stale) snapshot. When T2 commits, T1 re-reads the latest row, sees a completed
    version, and declines to repoint. Without `FOR UPDATE` T1 never blocks: it repoints to v1 on
    the stale snapshot and hides the just-completed v2 from every read.
    """
    object_id, key = uuid.uuid4(), "k"
    v1 = await _initiate_mpu(ctx, object_id, key)
    await _complete(ctx, object_id, v1)
    v2 = await _initiate_mpu(ctx, object_id, key)

    other = await asyncpg.connect(os.environ["DATABASE_URL"])
    try:
        tx = other.transaction()
        await tx.start()
        await other.execute(
            "UPDATE object_versions SET size_bytes = 11, md5_hash = $3, address = 'addr' "
            "WHERE object_id = $1 AND object_version = $2",
            object_id,
            v2,
            "5eb63bbbe01eeed093cb22bb8f5acdc3",
        )

        abort = asyncio.create_task(_abort_cleanup(ctx, object_id, v2))
        await asyncio.sleep(0.3)
        assert not abort.done(), "abort did not block on the uncommitted completion — the CAS is not locking"

        await tx.commit()
        assert await abort is None, "abort repointed off a version that had just completed"
    finally:
        await other.close()

    assert await _current(ctx, object_id) == v2


async def test_repoint_lands_on_the_NEWEST_completed_version(ctx: Ctx) -> None:
    """MAX, not MIN. With two completed versions below the aborted one, picking the older would
    silently roll the object back to ancient content on every abort."""
    object_id, key = uuid.uuid4(), "k"
    v1 = await _initiate_mpu(ctx, object_id, key)
    await _complete(ctx, object_id, v1)
    v2 = await _initiate_mpu(ctx, object_id, key)
    await _complete(ctx, object_id, v2)
    v3 = await _initiate_mpu(ctx, object_id, key)

    await _abort_cleanup(ctx, object_id, v3)

    assert await _current(ctx, object_id) == v2


async def test_abort_of_a_completed_zero_byte_version_is_a_noop(ctx: Ctx) -> None:
    """A 0-byte object is a real object: size 0 but a real md5. The reserved-check must require
    BOTH halves, or aborting a completed empty object rewinds the pointer off live data."""
    object_id, key = uuid.uuid4(), "k"
    v1 = await _initiate_mpu(ctx, object_id, key)
    await _complete(ctx, object_id, v1)
    v2 = await _initiate_mpu(ctx, object_id, key)
    await ctx.conn.execute(
        "UPDATE object_versions SET size_bytes = 0, md5_hash = $3, address = 'addr' "
        "WHERE object_id = $1 AND object_version = $2",
        object_id,
        v2,
        "d41d8cd98f00b204e9800998ecf8427e",
    )

    assert await _abort_cleanup(ctx, object_id, v2) is None
    assert await _current(ctx, object_id) == v2


async def test_reserved_row_with_null_md5_is_still_recognised(ctx: Ctx) -> None:
    """InitiateMultipartUpload can leave md5_hash NULL rather than ''. Both spellings of "never
    completed" must satisfy the guard, or the pointer is stranded on the aborted version."""
    object_id, key = uuid.uuid4(), "k"
    v1 = await _initiate_mpu(ctx, object_id, key)
    await _complete(ctx, object_id, v1)
    v2 = await _initiate_mpu(ctx, object_id, key)
    await ctx.conn.execute(
        "UPDATE object_versions SET md5_hash = NULL WHERE object_id = $1 AND object_version = $2",
        object_id,
        v2,
    )

    assert await _abort_cleanup(ctx, object_id, v2) is not None
    assert await _current(ctx, object_id) == v1
