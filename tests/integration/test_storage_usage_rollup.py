"""The maintained rollup must equal the canonical storage count, byte for byte, after every write.

`bucket_storage_usage` is what a plan customer is billed against, and it is maintained by Postgres
triggers rather than computed. Nothing about that is checkable by reading the trigger bodies: the
only question that matters is whether, after each real production statement, the counter agrees with
get_account_storage_bytes.sql -- THE definition of "storage used" in this codebase.

So every case here drives the REAL query files (upsert_object_basic, insert_delete_marker,
soft_delete_object_version, hard_delete_object, ...), compacts the ledger, and asserts the rollup
against that oracle. Hand-rolled INSERTs would test a trigger set against a fiction; a statement
shape is exactly the thing that can break this design (see the four data-modifying-CTE upserts).

Seeded via `pg_tx` (auto-rolled-back) except where a test needs real concurrency, which needs real
commits -- those use `committed_pool` and clean up after themselves. Skips if no Postgres is
reachable on DATABASE_URL.
"""

import asyncio
import contextlib
import datetime
import json
import os
import uuid
from typing import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio

from hippius_s3.db_retry import retry_on_object_version_conflict
from hippius_s3.services import storage_rollup_service
from hippius_s3.utils import get_query


CT = "application/octet-stream"


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


# --------------------------------------------------------------------------------------------
# Seeding, always through the production statements.
# --------------------------------------------------------------------------------------------


async def _seed_account(conn: asyncpg.Connection) -> str:
    acct = f"5ROLLUP{uuid.uuid4().hex[:16]}"
    await conn.execute("INSERT INTO users(main_account_id) VALUES($1) ON CONFLICT DO NOTHING", acct)
    return acct


async def _seed_bucket(conn: asyncpg.Connection, acct: str, *, deleted: bool = False) -> uuid.UUID:
    bucket_id = uuid.uuid4()
    await conn.execute(
        "INSERT INTO buckets(bucket_id, bucket_name, created_at, main_account_id, deleted_at)"
        " VALUES($1, $2, now(), $3, CASE WHEN $4 THEN now() ELSE NULL END)",
        bucket_id,
        f"rollup-test-{bucket_id}",
        acct,
        deleted,
    )
    return bucket_id


async def _reserve(
    conn: asyncpg.Connection,
    bucket_id: uuid.UUID,
    key: str,
    *,
    size: int = 0,
    multipart: bool = False,
) -> asyncpg.Record:
    """The reserve half of a PUT / MPU initiate: one statement, objects + object_versions.

    Wrapped in retry_on_object_version_conflict because EVERY production reserve is -- object_writer,
    multipart, repositories/objects and delete_object_endpoint all go through it. Calling the raw
    query here made this helper unfaithful to the shipped path, and the difference is observable:
    the upsert allocates the next version as GREATEST(current_object_version, MAX(object_version))+1,
    whose MAX() floor is snapshot-stale under READ COMMITTED, so a concurrent statement that moves
    versions (abort_cleanup_orphan_version repointing current DOWN) can hand back a colliding
    version and raise object_versions_pkey. Production retries that one constraint and converges;
    the unwrapped helper surfaced it as a test failure roughly one run in five.
    """
    query = "upsert_object_multipart" if multipart else "upsert_object_basic"

    async def _reserve_once() -> asyncpg.Record:
        return await conn.fetchrow(
            get_query(query),
            uuid.uuid4(),
            bucket_id,
            key,
            CT,
            json.dumps({}),
            None,
            size,
            _now(),
            5,
            ["arion"],
        )

    return await retry_on_object_version_conflict(_reserve_once)


async def _finalize(conn: asyncpg.Connection, object_id: uuid.UUID, version: int, size: int) -> None:
    """The tail half of a PUT: the UPDATE that makes the version serveable."""
    await conn.execute(
        get_query("update_object_version_metadata"),
        size,
        "d41d8cd98f00b204e9800998ecf8427e",
        CT,
        json.dumps({}),
        _now(),
        object_id,
        version,
        None,
    )


async def _put(conn: asyncpg.Connection, bucket_id: uuid.UUID, key: str, size: int) -> uuid.UUID:
    """A full PUT, exactly as object_writer does it: reserve at size 0, then set the real size."""
    row = await _reserve(conn, bucket_id, key)
    await _finalize(conn, row["object_id"], row["current_object_version"], size)
    return row["object_id"]


async def _mpu(conn: asyncpg.Connection, bucket_id: uuid.UUID, key: str, size: int) -> uuid.UUID:
    """MPU initiate + complete. Complete writes only object_versions -- it never repoints."""
    row = await _reserve(conn, bucket_id, key, multipart=True)
    # Mirrors object_writer.mpu_complete: absolute size on the already-current version.
    await conn.execute(
        "UPDATE object_versions SET md5_hash = $1, size_bytes = $2, last_modified = NOW(),"
        " status = 'publishing' WHERE object_id = $3 AND object_version = $4",
        "etag-1",
        size,
        row["object_id"],
        row["current_object_version"],
    )
    return row["object_id"]


# --------------------------------------------------------------------------------------------
# Assertions.
# --------------------------------------------------------------------------------------------


async def _oracle(conn: asyncpg.Connection, acct: str) -> int:
    """get_account_storage_bytes.sql: the canonical definition, and the only thing worth comparing to."""
    return int(await conn.fetchval(get_query("get_account_storage_bytes"), acct))


async def _compact(conn: asyncpg.Connection, batch: int = 10_000) -> int:
    return (await storage_rollup_service.compact_until_drained(conn, batch)).rows_claimed


async def _rollup(conn: asyncpg.Connection, acct: str) -> int:
    row = await conn.fetchrow(get_query("get_account_storage_bytes_rollup"), acct)
    return int(row["bytes_used"])


async def _raw_rollup(conn: asyncpg.Connection, acct: str) -> int:
    """Unclamped per-bucket sum, so a negative counter is visible rather than floored."""
    return int(
        await conn.fetchval(
            "SELECT COALESCE(SUM(bsu.bytes_used), 0)::bigint FROM buckets b"
            " JOIN bucket_storage_usage bsu ON bsu.bucket_id = b.bucket_id"
            " WHERE b.main_account_id = $1 AND b.deleted_at IS NULL",
            acct,
        )
    )


async def _assert_matches_oracle(conn: asyncpg.Connection, acct: str) -> int:
    """THE core invariant. Compacts first, then demands byte-for-byte equality."""
    await _compact(conn)
    oracle = await _oracle(conn, acct)
    assert await _raw_rollup(conn, acct) == oracle, "rollup disagrees with get_account_storage_bytes.sql"
    assert await _rollup(conn, acct) == oracle
    return oracle


async def _ledger_rows(conn: asyncpg.Connection, bucket_id: uuid.UUID) -> list[int]:
    rows = await conn.fetch(
        "SELECT delta_bytes FROM storage_delta_ledger WHERE bucket_id = $1 ORDER BY ledger_id",
        bucket_id,
    )
    return [int(r["delta_bytes"]) for r in rows]


pytestmark = pytest.mark.asyncio


# --------------------------------------------------------------------------------------------
# The trigger set. Pinned, because "adding the obviously missing one" is a real regression.
# --------------------------------------------------------------------------------------------

# tgtype bits, from PostgreSQL's include/catalog/pg_trigger.h.
_ROW, _BEFORE, _INSERT, _DELETE, _UPDATE = 1, 2, 4, 8, 16

_EXPECTED_TRIGGERS = {
    # An objects INSERT and its first object_versions row arrive in ONE statement, so only an AFTER
    # ROW trigger -- which fires at end-of-statement -- can see the version.
    ("objects", "objects_storage_delta_ins"): _ROW | _INSERT,
    ("objects", "objects_storage_delta_upd"): _ROW | _UPDATE,
    # BEFORE, uniquely: object_versions cascades from an objects DELETE via an
    # RI_ConstraintTrigger_* AFTER trigger, and AFTER ROW triggers fire in NAME order, so 'RI_...'
    # would win and every version row would already be gone by the time we looked.
    ("objects", "objects_storage_delta_del"): _ROW | _BEFORE | _DELETE,
    ("object_versions", "object_versions_storage_delta_upd"): _ROW | _UPDATE,
    ("object_versions", "object_versions_storage_delta_del"): _ROW | _DELETE,
    # Not ours. Listed so this test fails loudly if someone else adds a trigger to these tables
    # without thinking about how it interacts with byte accounting.
    ("objects", "objects_reject_duplicate_live_name"): _ROW | _BEFORE | _INSERT | _UPDATE,
}


async def test_trigger_set_is_exactly_as_designed(pg_conn: asyncpg.Connection) -> None:
    """THERE IS DELIBERATELY NO INSERT TRIGGER ON object_versions.

    upsert_object_basic is a single statement whose CTEs both upsert `objects` and insert
    `object_versions`. The objects trigger already counts the new version, so a version INSERT
    trigger would count it twice and every PUT would bill double. A version inserted WITHOUT
    becoming current (create_migration_version) contributes nothing and needs no trigger.

    This test exists so that adding the missing-looking trigger turns the build red instead of
    turning up on an invoice.
    """
    rows = await pg_conn.fetch(
        "SELECT c.relname, t.tgname, t.tgtype FROM pg_trigger t"
        " JOIN pg_class c ON c.oid = t.tgrelid"
        " WHERE NOT t.tgisinternal AND c.relname IN ('objects', 'object_versions')"
    )

    actual = {(r["relname"], r["tgname"]): int(r["tgtype"]) for r in rows}
    assert actual == _EXPECTED_TRIGGERS

    # And nothing on `buckets`: liveness and ownership are applied at read time, so a bucket being
    # soft-deleted or transferred needs no counter maintenance at all.
    bucket_triggers = await pg_conn.fetch(
        "SELECT t.tgname FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid"
        " WHERE NOT t.tgisinternal AND c.relname = 'buckets'"
    )
    assert [r["tgname"] for r in bucket_triggers] == []


# --------------------------------------------------------------------------------------------
# The one place the design depends on Postgres trigger-timing semantics.
# --------------------------------------------------------------------------------------------


async def test_reserve_in_one_statement_is_seen_by_the_trigger(pg_tx: asyncpg.Connection) -> None:
    """AFTER ROW triggers queue to end-of-statement, so they see the statement's own CTE inserts.

    upsert_object_basic upserts `objects` and inserts `object_versions` in ONE statement, and the
    objects trigger has to read that brand-new version row to know how many bytes it is. If AFTER
    ROW triggers fired mid-statement (or if this were a BEFORE trigger), the lookup would find
    nothing and every PUT would be counted as zero bytes.

    Reserving at a NON-zero size makes that visible: a zero-size reserve is indistinguishable from
    "the trigger saw nothing".
    """
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    await _reserve(pg_tx, bucket_id, "k", size=4096)

    assert await _ledger_rows(pg_tx, bucket_id) == [4096]
    await _assert_matches_oracle(pg_tx, acct)


async def test_reserve_at_zero_bytes_emits_nothing(pg_tx: asyncpg.Connection) -> None:
    """The real reserve inserts size_bytes = 0 and bumps current_object_version in one statement.

    Both the objects trigger (new current version) and, if one existed, a version INSERT trigger
    would fire on it. Zero rows in the ledger is the direct evidence that nothing is counted twice
    -- one emitter of 0 and one emitter of 0 also sums to 0, so this is asserted on the ROW COUNT.
    """
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    await _reserve(pg_tx, bucket_id, "k", size=0)

    assert await _ledger_rows(pg_tx, bucket_id) == []
    assert await _assert_matches_oracle(pg_tx, acct) == 0


# --------------------------------------------------------------------------------------------
# Write paths.
# --------------------------------------------------------------------------------------------


async def test_put_new_key(pg_tx: asyncpg.Connection) -> None:
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    await _put(pg_tx, bucket_id, "a", 1000)

    assert await _assert_matches_oracle(pg_tx, acct) == 1000


async def test_put_overwrite_nets_new_minus_old(pg_tx: asyncpg.Connection) -> None:
    """The single most likely bug: an overwrite must move the counter by new - old, not by new.

    Note the shape it actually takes. The reserve repoints current_object_version at a 0-byte
    version, so the counter first drops to 0 (which is also what the canonical query says about a
    key mid-overwrite), and the finalize brings it to the new size.
    """
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    await _put(pg_tx, bucket_id, "a", 1000)
    assert await _assert_matches_oracle(pg_tx, acct) == 1000

    row = await _reserve(pg_tx, bucket_id, "a", size=0)
    assert await _assert_matches_oracle(pg_tx, acct) == 0

    await _finalize(pg_tx, row["object_id"], row["current_object_version"], 250)
    assert await _assert_matches_oracle(pg_tx, acct) == 250


async def test_put_overwrite_with_size_at_reserve(pg_tx: asyncpg.Connection) -> None:
    """Same as above but with the size present on the reserve, as upsert_object_with_cid does.

    This is the case that would double-count if there were both an objects UPDATE trigger and an
    object_versions INSERT trigger, because both fire on the same statement.
    """
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    await _put(pg_tx, bucket_id, "a", 1000)
    await _reserve(pg_tx, bucket_id, "a", size=250)

    assert await _assert_matches_oracle(pg_tx, acct) == 250


async def test_put_revives_a_soft_deleted_key(pg_tx: asyncpg.Connection) -> None:
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    await _put(pg_tx, bucket_id, "a", 1000)
    await pg_tx.execute(get_query("soft_delete_object"), bucket_id, "a")
    assert await _assert_matches_oracle(pg_tx, acct) == 0

    await _put(pg_tx, bucket_id, "a", 700)
    assert await _assert_matches_oracle(pg_tx, acct) == 700


async def test_mpu_complete(pg_tx: asyncpg.Connection) -> None:
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    await _mpu(pg_tx, bucket_id, "big", 5_242_880)

    assert await _assert_matches_oracle(pg_tx, acct) == 5_242_880


async def test_mpu_complete_over_an_existing_key(pg_tx: asyncpg.Connection) -> None:
    """Initiate repoints to an empty version, so the key reads as 0 bytes until Complete lands.

    That is not the rollup being wrong -- it is what the canonical query says too, because Complete
    writes object_versions and never advances current_object_version.
    """
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    await _put(pg_tx, bucket_id, "k", 900)
    row = await _reserve(pg_tx, bucket_id, "k", multipart=True)
    assert await _assert_matches_oracle(pg_tx, acct) == 0

    await pg_tx.execute(
        "UPDATE object_versions SET size_bytes = $1 WHERE object_id = $2 AND object_version = $3",
        4000,
        row["object_id"],
        row["current_object_version"],
    )
    assert await _assert_matches_oracle(pg_tx, acct) == 4000


async def test_s4_append_is_a_relative_size_update(pg_tx: asyncpg.Connection) -> None:
    """`SET size_bytes = size_bytes + $3`. The trigger reads OLD/NEW, so relative works unchanged."""
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    object_id = await _put(pg_tx, bucket_id, "log", 100)
    version = await pg_tx.fetchval("SELECT current_object_version FROM objects WHERE object_id = $1", object_id)

    for _ in range(3):
        await pg_tx.execute(
            "UPDATE object_versions SET size_bytes = size_bytes + $1, append_version = append_version + 1"
            " WHERE object_id = $2 AND object_version = $3 AND deleted_at IS NULL",
            50,
            object_id,
            version,
        )

    assert await _assert_matches_oracle(pg_tx, acct) == 250


async def test_cross_bucket_copy_counts_in_the_destination(pg_tx: asyncpg.Connection) -> None:
    """Both the fast path and the streaming path land as a new object in the destination bucket."""
    acct = await _seed_account(pg_tx)
    src = await _seed_bucket(pg_tx, acct)
    dst = await _seed_bucket(pg_tx, acct)

    await _put(pg_tx, src, "a", 1000)
    await _put(pg_tx, dst, "a-copy", 1000)

    assert await _assert_matches_oracle(pg_tx, acct) == 2000
    assert await pg_tx.fetchval("SELECT bytes_used FROM bucket_storage_usage WHERE bucket_id = $1", src) == 1000
    assert await pg_tx.fetchval("SELECT bytes_used FROM bucket_storage_usage WHERE bucket_id = $1", dst) == 1000


async def test_same_bucket_alias_copy_moves_exactly_zero(pg_tx: asyncpg.Connection) -> None:
    """A same-bucket copy only writes `object_names`, so it must emit NO ledger rows at all.

    Asserted on the row count, not the total: a +N and a -N would also net to zero while proving
    the trigger set had started reacting to a statement that moves no bytes.
    """
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    object_id = await _put(pg_tx, bucket_id, "a", 1000)
    await _compact(pg_tx)

    await pg_tx.execute(get_query("insert_object_name"), bucket_id, "a-alias", object_id)

    assert await _ledger_rows(pg_tx, bucket_id) == []
    assert await _assert_matches_oracle(pg_tx, acct) == 1000

    # And the rename half: promote_object_name UPDATEs objects.object_key, which the trigger's WHEN
    # clause must not react to.
    await pg_tx.fetchval(get_query("promote_object_name"), bucket_id, "a")
    assert await _ledger_rows(pg_tx, bucket_id) == []
    assert await _assert_matches_oracle(pg_tx, acct) == 1000


async def test_delete_object_unversioned(pg_tx: asyncpg.Connection) -> None:
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    await _put(pg_tx, bucket_id, "a", 1000)
    await _put(pg_tx, bucket_id, "b", 500)
    await pg_tx.execute(get_query("soft_delete_object"), bucket_id, "a")

    assert await _assert_matches_oracle(pg_tx, acct) == 500


async def test_delete_marker(pg_tx: asyncpg.Connection) -> None:
    """A delete marker becomes current, so the key stops counting while its data version survives."""
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    await _put(pg_tx, bucket_id, "a", 1000)
    await pg_tx.fetchrow(get_query("insert_delete_marker"), bucket_id, "a")

    assert await _assert_matches_oracle(pg_tx, acct) == 0


async def test_delete_marker_then_overwrite(pg_tx: asyncpg.Connection) -> None:
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    await _put(pg_tx, bucket_id, "a", 1000)
    await pg_tx.fetchrow(get_query("insert_delete_marker"), bucket_id, "a")
    await _put(pg_tx, bucket_id, "a", 333)

    assert await _assert_matches_oracle(pg_tx, acct) == 333


async def test_versioned_delete_of_the_current_version_repoints(pg_tx: asyncpg.Connection) -> None:
    """Two statements: soft-delete the version, then repoint. Each must move the counter its own way.

    This is the pair that would break if it were ever collapsed into one statement -- both AFTER ROW
    triggers would fire at the same end-of-statement and each would see the other's finished work.
    """
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    await _put(pg_tx, bucket_id, "a", 1000)
    object_id = await _reserve(pg_tx, bucket_id, "a")
    await _finalize(pg_tx, object_id["object_id"], object_id["current_object_version"], 400)
    oid, current = object_id["object_id"], object_id["current_object_version"]
    assert await _assert_matches_oracle(pg_tx, acct) == 400

    await pg_tx.fetchrow(get_query("soft_delete_object_version"), oid, current)
    assert await _assert_matches_oracle(pg_tx, acct) == 0

    repointed = await pg_tx.fetchval(get_query("repoint_current_version_after_delete"), oid, current)
    assert repointed == 1
    assert await _assert_matches_oracle(pg_tx, acct) == 1000


async def test_versioned_delete_of_a_non_current_version_moves_nothing(pg_tx: asyncpg.Connection) -> None:
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    await _put(pg_tx, bucket_id, "a", 1000)
    row = await _reserve(pg_tx, bucket_id, "a")
    await _finalize(pg_tx, row["object_id"], row["current_object_version"], 400)
    await _compact(pg_tx)

    await pg_tx.fetchrow(get_query("soft_delete_object_version"), row["object_id"], 1)

    assert await _ledger_rows(pg_tx, bucket_id) == []
    assert await _assert_matches_oracle(pg_tx, acct) == 400


async def test_bulk_delete_objects(pg_tx: asyncpg.Connection) -> None:
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    for index in range(10):
        await _put(pg_tx, bucket_id, f"k{index}", 100 + index)
    total = sum(100 + index for index in range(10))
    assert await _assert_matches_oracle(pg_tx, acct) == total

    for index in range(0, 10, 2):
        await pg_tx.execute(get_query("soft_delete_object"), bucket_id, f"k{index}")

    assert await _assert_matches_oracle(pg_tx, acct) == sum(100 + index for index in range(1, 10, 2))


async def test_purge_batch_soft_deletes_a_whole_bucket(pg_tx: asyncpg.Connection) -> None:
    """purge_soft_delete_objects_batch soft-deletes many objects in ONE statement."""
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    for index in range(7):
        await _put(pg_tx, bucket_id, f"k{index}", 1000)
    assert await _assert_matches_oracle(pg_tx, acct) == 7000

    await pg_tx.fetch(get_query("purge_soft_delete_objects_batch"), bucket_id, 500)

    assert await _assert_matches_oracle(pg_tx, acct) == 0


async def test_janitor_hard_delete_of_a_soft_deleted_object(pg_tx: asyncpg.Connection) -> None:
    """The bytes left when the object was soft-deleted; the hard delete must not decrement twice."""
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    object_id = await _put(pg_tx, bucket_id, "a", 1000)
    await _put(pg_tx, bucket_id, "b", 500)
    await pg_tx.execute(get_query("soft_delete_object"), bucket_id, "a")
    assert await _assert_matches_oracle(pg_tx, acct) == 500
    await _compact(pg_tx)

    # hard_delete_object re-checks a 1h grace window under its own row lock, so age the row.
    await pg_tx.execute("UPDATE objects SET deleted_at = now() - INTERVAL '48 hours' WHERE object_id = $1", object_id)
    assert await pg_tx.execute(get_query("hard_delete_object"), object_id) == "DELETE 1"

    assert await _ledger_rows(pg_tx, bucket_id) == []
    assert await _assert_matches_oracle(pg_tx, acct) == 500


async def test_hard_delete_of_a_live_object_decrements(pg_tx: asyncpg.Connection) -> None:
    """The case a BEFORE DELETE trigger exists for.

    An AFTER DELETE trigger on `objects` would run after the ON DELETE CASCADE had already removed
    every object_versions row (RI_ConstraintTrigger_* sorts before any lowercase trigger name), find
    0 bytes, and leave the object billed forever. This is the shape nuke_user.py and
    delete_legacy_object_versions.py take.
    """
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    object_id = await _put(pg_tx, bucket_id, "a", 1000)
    await _put(pg_tx, bucket_id, "b", 500)
    await _compact(pg_tx)

    await pg_tx.execute("DELETE FROM objects WHERE object_id = $1", object_id)

    assert await _ledger_rows(pg_tx, bucket_id) == [-1000]
    assert await _assert_matches_oracle(pg_tx, acct) == 500


async def test_bucket_hard_delete_cascade_nets_out(pg_tx: asyncpg.Connection) -> None:
    """purge_buckets.py's shape: DELETE FROM buckets -> cascade objects -> cascade object_versions.

    The rollup row cascades away with the bucket and the deltas are for a bucket that no longer
    exists, so the compactor discards them. What must NOT happen is the compactor failing on the FK
    and wedging, or the other bucket's counter moving.
    """
    acct = await _seed_account(pg_tx)
    doomed = await _seed_bucket(pg_tx, acct)
    kept = await _seed_bucket(pg_tx, acct)

    await _put(pg_tx, doomed, "a", 1000)
    await _put(pg_tx, kept, "b", 500)
    assert await _assert_matches_oracle(pg_tx, acct) == 1500

    await pg_tx.execute("DELETE FROM buckets WHERE bucket_id = $1", doomed)

    assert await _assert_matches_oracle(pg_tx, acct) == 500
    assert await pg_tx.fetchval("SELECT count(*) FROM bucket_storage_usage WHERE bucket_id = $1", doomed) == 0
    assert await pg_tx.fetchval("SELECT count(*) FROM storage_delta_ledger WHERE bucket_id = $1", doomed) == 0


async def test_bucket_soft_delete_needs_no_trigger(pg_tx: asyncpg.Connection) -> None:
    """Liveness is applied at READ time, so the per-bucket counter is deliberately left alone."""
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    await _put(pg_tx, bucket_id, "a", 1000)
    assert await _assert_matches_oracle(pg_tx, acct) == 1000

    await pg_tx.execute(get_query("soft_delete_bucket"), bucket_id)

    assert await _assert_matches_oracle(pg_tx, acct) == 0
    assert await pg_tx.fetchval("SELECT bytes_used FROM bucket_storage_usage WHERE bucket_id = $1", bucket_id) == 1000


async def test_bucket_transfer_needs_no_trigger(pg_tx: asyncpg.Connection) -> None:
    """Ownership is applied at read time too, so the bytes follow the bucket with no delta at all."""
    giver = await _seed_account(pg_tx)
    taker = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, giver)

    await _put(pg_tx, bucket_id, "a", 1000)
    await _compact(pg_tx)

    await pg_tx.execute("UPDATE buckets SET main_account_id = $1 WHERE bucket_id = $2", taker, bucket_id)

    assert await _ledger_rows(pg_tx, bucket_id) == []
    assert await _assert_matches_oracle(pg_tx, giver) == 0
    assert await _assert_matches_oracle(pg_tx, taker) == 1000


async def test_migration_version_is_not_counted_until_promoted(pg_tx: asyncpg.Connection) -> None:
    """create_migration_version inserts a version ABOVE current without bumping the pointer.

    It must contribute nothing until swap_current_version_cas promotes it -- which is the v4 -> v5
    migrator's shape, and the reason there is no object_versions INSERT trigger to get this wrong.
    """
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    object_id = await _put(pg_tx, bucket_id, "a", 1000)
    await _compact(pg_tx)

    new_version = await pg_tx.fetchval(
        get_query("create_migration_version"), str(object_id), CT, json.dumps({}), 5, ["arion"]
    )
    await pg_tx.execute(
        "UPDATE object_versions SET size_bytes = $1 WHERE object_id = $2 AND object_version = $3",
        1000,
        object_id,
        new_version,
    )
    assert await _ledger_rows(pg_tx, bucket_id) == []
    assert await _assert_matches_oracle(pg_tx, acct) == 1000

    await pg_tx.fetchrow(get_query("swap_current_version_cas"), object_id, 1, new_version)
    assert await _assert_matches_oracle(pg_tx, acct) == 1000


async def test_migration_version_of_a_different_size_moves_the_counter(pg_tx: asyncpg.Connection) -> None:
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    object_id = await _put(pg_tx, bucket_id, "a", 1000)
    new_version = await pg_tx.fetchval(
        get_query("create_migration_version"), str(object_id), CT, json.dumps({}), 5, ["arion"]
    )
    await pg_tx.execute(
        "UPDATE object_versions SET size_bytes = $1 WHERE object_id = $2 AND object_version = $3",
        1500,
        object_id,
        new_version,
    )
    await pg_tx.fetchrow(get_query("swap_current_version_cas"), object_id, 1, new_version)

    assert await _assert_matches_oracle(pg_tx, acct) == 1500


async def test_abort_multipart_repoints_off_the_reserved_version(pg_tx: asyncpg.Connection) -> None:
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    await _put(pg_tx, bucket_id, "k", 900)
    row = await _reserve(pg_tx, bucket_id, "k", multipart=True)
    assert await _assert_matches_oracle(pg_tx, acct) == 0

    await pg_tx.fetchrow(get_query("abort_cleanup_orphan_version"), row["object_id"], row["current_object_version"])

    assert await _assert_matches_oracle(pg_tx, acct) == 900


async def test_janitor_version_reap_of_a_dead_version(pg_tx: asyncpg.Connection) -> None:
    """Hard-deleting an already-soft-deleted version must move nothing; it was decremented already."""
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    await _put(pg_tx, bucket_id, "a", 1000)
    row = await _reserve(pg_tx, bucket_id, "a")
    await _finalize(pg_tx, row["object_id"], row["current_object_version"], 400)
    await pg_tx.fetchrow(get_query("soft_delete_object_version"), row["object_id"], 1)
    await _compact(pg_tx)

    await pg_tx.fetchrow(get_query("delete_version_and_parts"), row["object_id"], 1)

    assert await _ledger_rows(pg_tx, bucket_id) == []
    assert await _assert_matches_oracle(pg_tx, acct) == 400


async def test_aborted_put_leaves_an_under_report_never_an_over_report(pg_tx: asyncpg.Connection) -> None:
    """A reserved-but-never-finalized version is the broken-v5 orphan shape.

    The version is current at size 0, so the key contributes nothing -- to the rollup AND to the
    canonical query, which is the point: the error direction is UNDER-reporting a customer's usage,
    which costs us money rather than charging them for bytes they do not have.
    """
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    await _put(pg_tx, bucket_id, "a", 1000)
    await _reserve(pg_tx, bucket_id, "a", size=0)

    total = await _assert_matches_oracle(pg_tx, acct)
    assert total == 0

    # And the rollback the PUT handler performs on the same row (size_bytes = 0) is a no-op.
    await _compact(pg_tx)
    await pg_tx.execute(
        "UPDATE object_versions SET size_bytes = 0, md5_hash = ''"
        " WHERE object_id IN (SELECT object_id FROM objects WHERE bucket_id = $1)",
        bucket_id,
    )
    assert await _ledger_rows(pg_tx, bucket_id) == []
    assert await _assert_matches_oracle(pg_tx, acct) == 0


async def test_untracked_column_updates_emit_nothing(pg_tx: asyncpg.Connection) -> None:
    """The WHEN clauses must keep the hottest UPDATEs in the schema off the ledger entirely."""
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    object_id = await _put(pg_tx, bucket_id, "a", 1000)
    version = await pg_tx.fetchval("SELECT current_object_version FROM objects WHERE object_id = $1", object_id)
    await _compact(pg_tx)

    await pg_tx.execute(get_query("set_object_version_address"), object_id, version, acct)
    await pg_tx.execute(
        "UPDATE object_versions SET status = 'uploaded', last_modified = now()"
        " WHERE object_id = $1 AND object_version = $2",
        object_id,
        version,
    )
    await pg_tx.execute("UPDATE objects SET object_key = 'renamed' WHERE object_id = $1", object_id)

    assert await _ledger_rows(pg_tx, bucket_id) == []
    assert await _assert_matches_oracle(pg_tx, acct) == 1000


# --------------------------------------------------------------------------------------------
# Compaction and recompute mechanics.
# --------------------------------------------------------------------------------------------


async def test_compaction_is_exactly_once_across_repeated_calls(pg_tx: asyncpg.Connection) -> None:
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    await _put(pg_tx, bucket_id, "a", 1000)
    assert await _compact(pg_tx) > 0

    # The ledger is empty now, so every further pass must be a no-op rather than a re-apply.
    for _ in range(3):
        assert await _compact(pg_tx) == 0
    assert await _assert_matches_oracle(pg_tx, acct) == 1000


async def test_compaction_batches_smaller_than_the_ledger(pg_tx: asyncpg.Connection) -> None:
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    # Drain first: other tests in this tier commit real writes, and the ledger is global. Rolled
    # back with the rest of the transaction.
    await _compact(pg_tx)

    for index in range(20):
        await _put(pg_tx, bucket_id, f"k{index}", 100)

    result = await storage_rollup_service.compact_until_drained(pg_tx, batch_size=1)
    assert result.rows_claimed == 20
    assert await pg_tx.fetchval("SELECT count(*) FROM storage_delta_ledger") == 0
    assert await _oracle(pg_tx, acct) == 2000
    assert await _raw_rollup(pg_tx, acct) == 2000


async def test_recompute_is_idempotent(pg_tx: asyncpg.Connection) -> None:
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    await _put(pg_tx, bucket_id, "a", 1000)
    await _put(pg_tx, bucket_id, "b", 250)

    first = await storage_rollup_service.recompute_bucket(pg_tx, bucket_id, 60.0)
    assert first.bytes_after == 1250

    for _ in range(3):
        again = await storage_rollup_service.recompute_bucket(pg_tx, bucket_id, 60.0)
        assert again.bytes_before == 1250
        assert again.bytes_after == 1250
        assert again.drift_bytes == 0


async def test_recompute_discards_the_deltas_it_supersedes(pg_tx: asyncpg.Connection) -> None:
    """A recompute must not leave pending deltas behind, or the next fold applies them twice."""
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    await _put(pg_tx, bucket_id, "a", 1000)
    assert await _ledger_rows(pg_tx, bucket_id) != []

    result = await storage_rollup_service.recompute_bucket(pg_tx, bucket_id, 60.0)
    assert result.bytes_after == 1000
    assert await _ledger_rows(pg_tx, bucket_id) == []

    await _compact(pg_tx)
    assert await _raw_rollup(pg_tx, acct) == 1000


async def test_recompute_repairs_injected_drift_and_reports_it(pg_tx: asyncpg.Connection) -> None:
    """The reconciler's whole job. Drift is injected by hand because nothing else can produce it."""
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    await _put(pg_tx, bucket_id, "a", 1000)
    await _compact(pg_tx)
    await pg_tx.execute("UPDATE bucket_storage_usage SET bytes_used = 12345 WHERE bucket_id = $1", bucket_id)

    result = await storage_rollup_service.recompute_bucket(pg_tx, bucket_id, 60.0)

    assert result.bytes_before == 12345
    assert result.bytes_after == 1000
    assert result.drift_bytes == -11345
    assert await _raw_rollup(pg_tx, acct) == 1000


async def test_recompute_repairs_a_negative_counter(pg_tx: asyncpg.Connection) -> None:
    """A negative counter is only reachable via drift, and it is NOT clamped where it is stored.

    The clamp lives on the read path. Clamping the counter would make every later increment start
    from zero instead of from the true value, pinning it permanently high.
    """
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    await _put(pg_tx, bucket_id, "a", 1000)
    await _compact(pg_tx)
    await pg_tx.execute("UPDATE bucket_storage_usage SET bytes_used = -500 WHERE bucket_id = $1", bucket_id)

    row = await pg_tx.fetchrow(get_query("get_account_storage_bytes_rollup"), acct)
    assert row["negative_buckets"] == 1
    assert row["bytes_used"] == 0
    assert await _raw_rollup(pg_tx, acct) == -500

    await storage_rollup_service.recompute_bucket(pg_tx, bucket_id, 60.0)
    assert await _raw_rollup(pg_tx, acct) == 1000


async def test_decrements_survive_the_upsert_arms(pg_tx: asyncpg.Connection) -> None:
    """A decrement folded into a bucket with NO rollup row yet must still land as a decrement.

    This is the shape that a `GREATEST(0, ...)` on the INSERT arm of the fold would destroy:
    EXCLUDED.bytes_used would carry the clamped value into the DO UPDATE arm and floor every
    decrement at zero. There is no clamp in compact_storage_delta_ledger.sql, and this is the test
    that says so.
    """
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    await _put(pg_tx, bucket_id, "a", 1000)
    await _compact(pg_tx)
    # Drop the row so the next fold takes the INSERT arm with a negative delta.
    await pg_tx.execute("DELETE FROM bucket_storage_usage WHERE bucket_id = $1", bucket_id)
    await pg_tx.execute(get_query("soft_delete_object"), bucket_id, "a")

    await _compact(pg_tx)

    assert await pg_tx.fetchval("SELECT bytes_used FROM bucket_storage_usage WHERE bucket_id = $1", bucket_id) == -1000


async def test_reconcile_walks_the_least_recently_recomputed_buckets(pg_tx: asyncpg.Connection) -> None:
    acct = await _seed_account(pg_tx)
    buckets = [await _seed_bucket(pg_tx, acct) for _ in range(3)]
    for index, bucket_id in enumerate(buckets):
        await _put(pg_tx, bucket_id, "a", 100 * (index + 1))

    results = await storage_rollup_service.reconcile_buckets(pg_tx, limit=500, timeout=60.0)

    reconciled = {r.bucket_id for r in results}
    assert set(buckets) <= reconciled
    assert await _raw_rollup(pg_tx, acct) == 600

    # Soft-deleted buckets are deliberately skipped: nobody reads their total.
    dead = await _seed_bucket(pg_tx, acct, deleted=True)
    await _put(pg_tx, dead, "a", 999)
    again = await storage_rollup_service.reconcile_buckets(pg_tx, limit=500, timeout=60.0)
    assert dead not in {r.bucket_id for r in again}


# --------------------------------------------------------------------------------------------
# Concurrency. Needs real commits, so these bypass pg_tx and clean up after themselves.
# --------------------------------------------------------------------------------------------


class CommittedPool:
    """A pool for tests that must COMMIT, tracking what to tear down.

    pg_tx cannot be used for concurrency: two connections cannot share one uncommitted transaction,
    and lost-update bugs only exist between committed transactions.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool
        self.buckets: list[uuid.UUID] = []

    def acquire(self) -> object:
        return self.pool.acquire()

    async def new_bucket(self, acct: str) -> uuid.UUID:
        async with self.pool.acquire() as conn:
            bucket_id = await _seed_bucket(conn, acct)
        self.buckets.append(bucket_id)
        return bucket_id

    async def new_account(self) -> str:
        async with self.pool.acquire() as conn:
            return await _seed_account(conn)


@pytest_asyncio.fixture
async def committed_pool(pg_conn: asyncpg.Connection) -> AsyncGenerator[CommittedPool, None]:
    """Teardown hard-deletes every bucket the test made, which cascades objects and versions.

    That cascade emits its own deltas, so the ledger is swept for those bucket ids too -- otherwise
    a committed test would leave rows behind for the next one to compact.
    """
    dsn = os.environ["DATABASE_URL"]
    pool = await asyncpg.create_pool(dsn, min_size=2, max_size=8)
    wrapper = CommittedPool(pool)

    try:
        yield wrapper
    finally:
        async with pool.acquire() as conn:
            for bucket_id in wrapper.buckets:
                await conn.execute("DELETE FROM buckets WHERE bucket_id = $1", bucket_id)
                await conn.execute("DELETE FROM storage_delta_ledger WHERE bucket_id = $1", bucket_id)
        await pool.close()


async def test_parallel_puts_into_one_bucket_converge(committed_pool: CommittedPool) -> None:
    """No lost updates: the counter is a sum of independent INSERTs, not a read-modify-write."""
    acct = await committed_pool.new_account()
    bucket_id = await committed_pool.new_bucket(acct)

    async def put(index: int) -> None:
        async with committed_pool.acquire() as conn:
            await _put(conn, bucket_id, f"k{index}", 100 + index)

    await asyncio.gather(*(put(i) for i in range(24)))

    async with committed_pool.acquire() as conn:
        await _compact(conn)
        oracle = await _oracle(conn, acct)
        assert oracle == sum(100 + i for i in range(24))
        assert await _raw_rollup(conn, acct) == oracle


async def test_parallel_overwrites_of_the_same_key_converge(committed_pool: CommittedPool) -> None:
    """The hardest case: every writer repoints the SAME objects row, so their deltas interleave.

    Only the winner's size may end up counted. Which writer wins is a race and is not asserted;
    that the counter equals the canonical query afterwards is.
    """
    acct = await committed_pool.new_account()
    bucket_id = await committed_pool.new_bucket(acct)

    async with committed_pool.acquire() as conn:
        await _put(conn, bucket_id, "hot", 1)

    async def overwrite(size: int) -> None:
        async with committed_pool.acquire() as conn:
            async with conn.transaction():
                row = await _reserve(conn, bucket_id, "hot")
                await _finalize(conn, row["object_id"], row["current_object_version"], size)

    await asyncio.gather(*(overwrite(1000 + i) for i in range(16)))

    async with committed_pool.acquire() as conn:
        await _compact(conn)
        oracle = await _oracle(conn, acct)
        assert await _raw_rollup(conn, acct) == oracle
        assert 1000 <= oracle <= 1015


async def test_interleaved_put_and_delete_of_the_same_key_converge(committed_pool: CommittedPool) -> None:
    acct = await committed_pool.new_account()
    bucket_id = await committed_pool.new_bucket(acct)

    async def put(index: int) -> None:
        async with committed_pool.acquire() as conn:
            async with conn.transaction():
                await _put(conn, bucket_id, "churn", 500 + index)

    async def delete() -> None:
        async with committed_pool.acquire() as conn:
            await conn.execute(get_query("soft_delete_object"), bucket_id, "churn")

    async with committed_pool.acquire() as conn:
        await _put(conn, bucket_id, "churn", 10)

    await asyncio.gather(*[put(i) for i in range(8)], *[delete() for _ in range(8)])

    async with committed_pool.acquire() as conn:
        await _compact(conn)
        assert await _raw_rollup(conn, acct) == await _oracle(conn, acct)


async def test_compaction_interleaved_with_live_writes_converges(committed_pool: CommittedPool) -> None:
    """Fold while writes are landing. The claim is transactional, so nothing may be lost or doubled."""
    acct = await committed_pool.new_account()
    bucket_id = await committed_pool.new_bucket(acct)

    async def writer() -> None:
        for index in range(20):
            async with committed_pool.acquire() as conn:
                await _put(conn, bucket_id, f"k{index}", 100)
            await asyncio.sleep(0)

    async def compactor() -> None:
        for _ in range(40):
            async with committed_pool.acquire() as conn:
                await storage_rollup_service.compact_once(conn, batch_size=3)
            await asyncio.sleep(0)

    await asyncio.gather(writer(), compactor())

    async with committed_pool.acquire() as conn:
        await _compact(conn)
        assert await _raw_rollup(conn, acct) == await _oracle(conn, acct) == 2000


async def test_recompute_interleaved_with_live_writes_converges(committed_pool: CommittedPool) -> None:
    """The backfill's real operating condition: recompute a bucket while it is being written.

    recompute SETS the counter, which is only safe because it discards the ledger rows it supersedes
    in the SAME snapshot it aggregates in, and holds an advisory lock that keeps the compactor out.
    Split those apart and this test loses bytes.
    """
    acct = await committed_pool.new_account()
    bucket_id = await committed_pool.new_bucket(acct)

    async def writer() -> None:
        for index in range(20):
            async with committed_pool.acquire() as conn:
                await _put(conn, bucket_id, f"k{index}", 100)
            await asyncio.sleep(0)

    async def recomputer() -> None:
        for _ in range(10):
            async with committed_pool.acquire() as conn:
                await storage_rollup_service.recompute_bucket(conn, bucket_id, 60.0)
            await asyncio.sleep(0)

    async def compactor() -> None:
        for _ in range(20):
            async with committed_pool.acquire() as conn:
                await storage_rollup_service.compact_once(conn, batch_size=5)
            await asyncio.sleep(0)

    await asyncio.gather(writer(), recomputer(), compactor())

    async with committed_pool.acquire() as conn:
        await _compact(conn)
        assert await _raw_rollup(conn, acct) == await _oracle(conn, acct) == 2000


async def test_backfill_run_twice_does_not_double_count(committed_pool: CommittedPool) -> None:
    acct = await committed_pool.new_account()
    bucket_id = await committed_pool.new_bucket(acct)

    async with committed_pool.acquire() as conn:
        for index in range(5):
            await _put(conn, bucket_id, f"k{index}", 200)

        for _ in range(2):
            await storage_rollup_service.recompute_bucket(conn, bucket_id, 60.0)
            await _compact(conn)

        assert await _raw_rollup(conn, acct) == await _oracle(conn, acct) == 1000


async def test_recompute_keeps_a_delta_it_could_not_see(committed_pool: CommittedPool) -> None:
    """The recompute/compactor race, made deterministic instead of hoped about.

    A write that has not COMMITTED when the recompute takes its snapshot is invisible to both halves
    of the recompute's single statement: the aggregate misses its bytes AND the DELETE misses its
    ledger row. The row must therefore survive and be folded in afterwards. If the recompute
    deleted rows it had not accounted for -- which is what happens the moment the DELETE and the
    aggregate get separate snapshots -- those bytes would be gone for good.
    """
    acct = await committed_pool.new_account()
    bucket_id = await committed_pool.new_bucket(acct)

    async with committed_pool.acquire() as seeder:
        await _put(seeder, bucket_id, "settled", 1000)
        await _compact(seeder)

    async with committed_pool.acquire() as writer:
        tx = writer.transaction()
        await tx.start()
        await _put(writer, bucket_id, "in-flight", 250)

        # Recompute on another connection while the write is still uncommitted.
        async with committed_pool.acquire() as other:
            result = await storage_rollup_service.recompute_bucket(other, bucket_id, 60.0)
            assert result.bytes_after == 1000

        await tx.commit()

    async with committed_pool.acquire() as conn:
        await _compact(conn)
        assert await _raw_rollup(conn, acct) == await _oracle(conn, acct) == 1250


async def test_recompute_absorbs_a_delta_it_can_see(committed_pool: CommittedPool) -> None:
    """The mirror case: a COMMITTED write is in the aggregate, so its pending ledger row must go.

    Leaving it would apply the same bytes a second time on the next fold.
    """
    acct = await committed_pool.new_account()
    bucket_id = await committed_pool.new_bucket(acct)

    async with committed_pool.acquire() as conn:
        await _put(conn, bucket_id, "a", 1000)
        assert await _ledger_rows(conn, bucket_id) != []

    async with committed_pool.acquire() as other:
        assert (await storage_rollup_service.recompute_bucket(other, bucket_id, 60.0)).bytes_after == 1000

    async with committed_pool.acquire() as conn:
        assert await _ledger_rows(conn, bucket_id) == []
        await _compact(conn)
        assert await _raw_rollup(conn, acct) == await _oracle(conn, acct) == 1000


async def test_compactor_yields_to_a_running_recompute(committed_pool: CommittedPool) -> None:
    """The advisory lock: a compactor must SKIP rather than fold underneath a recompute.

    Folding concurrently is what lets a recompute's SET discard a delta the compactor has already
    consumed -- silently, permanently, and in a way that would keep the reconciler's drift metric
    non-zero forever.
    """
    acct = await committed_pool.new_account()
    bucket_id = await committed_pool.new_bucket(acct)

    async with committed_pool.acquire() as holder:
        tx = holder.transaction()
        await tx.start()
        await holder.fetchval("SELECT pg_advisory_xact_lock(storage_usage_rollup_lock_key(), 0)")

        async with committed_pool.acquire() as writer:
            await _put(writer, bucket_id, "a", 1000)

        async with committed_pool.acquire() as compactor:
            skipped = await storage_rollup_service.compact_once(compactor, batch_size=100)
            assert skipped.rows_claimed == 0
            assert await _ledger_rows(compactor, bucket_id) != []

        await tx.rollback()

    async with committed_pool.acquire() as conn:
        await _compact(conn)
        assert await _raw_rollup(conn, acct) == await _oracle(conn, acct) == 1000


async def test_recompute_drain_and_aggregate_share_one_snapshot(committed_pool: CommittedPool) -> None:
    """The single-statement recompute, proven by widening the window it exists to close.

    recompute_bucket_storage_usage drains the bucket's pending ledger rows and aggregates the truth
    in ONE statement, therefore in ONE snapshot. Split into two statements -- which is the obvious,
    readable way to write it -- each gets its own snapshot under READ COMMITTED, and a write that
    COMMITS BETWEEN THEM is counted twice: invisible to the DELETE so its ledger row survives, but
    visible to the aggregate so its bytes are already in the value that gets SET.

    That window is microseconds wide, so no amount of hammering finds it (measured: 300 writers
    against 300 recomputes, three runs, zero hits). This test forces it open by row-locking the
    ledger so the recompute stalls where the gap would be, committing a PUT into the stall, then
    releasing. Against a two-statement recompute this over-counts by exactly that PUT.
    """
    acct = await committed_pool.new_account()
    bucket_id = await committed_pool.new_bucket(acct)

    async with committed_pool.acquire() as conn:
        await _put(conn, bucket_id, "settled", 1000)
        assert await _ledger_rows(conn, bucket_id) != [], "the blocker needs a ledger row to lock"

    blocker = await committed_pool.pool.acquire()
    recomputer = await committed_pool.pool.acquire()
    try:
        blocking_tx = blocker.transaction()
        await blocking_tx.start()
        await blocker.fetch("SELECT ledger_id FROM storage_delta_ledger WHERE bucket_id = $1 FOR UPDATE", bucket_id)

        task = asyncio.create_task(storage_rollup_service.recompute_bucket(recomputer, bucket_id, 60.0))
        # Long enough for the recompute to reach the lock and stop there.
        await asyncio.sleep(0.4)
        assert not task.done()

        async with committed_pool.acquire() as writer:
            await _put(writer, bucket_id, "mid-gap", 250)

        await blocking_tx.rollback()
        result = await task

        # The recompute's snapshot predates the mid-gap PUT, so it must report only the settled
        # bytes -- the point being that it therefore must not have eaten the mid-gap ledger row.
        assert result.bytes_after == 1000
    finally:
        await committed_pool.pool.release(recomputer)
        await committed_pool.pool.release(blocker)

    async with committed_pool.acquire() as conn:
        await _compact(conn)
        assert await _raw_rollup(conn, acct) == await _oracle(conn, acct) == 1250


@pytest.mark.asyncio
async def test_a_pathological_size_cannot_abort_a_customer_write(pg_tx: asyncpg.Connection) -> None:
    """No delta arithmetic may overflow, because an overflow inside a trigger fails the user's PUT.

    This is not a reachable state through the application -- sizes are byte counts, and the largest
    object is orders of magnitude below the bigint range. It is pinned because the guarantee is only
    worth having unconditionally: before storage_usage_narrow existed, an overwrite between the
    bigint extremes raised `bigint out of range` from inside
    storage_usage_objects_update_trigger, and the affected row then became UNDELETABLE because the
    delete trigger raised on the same negation. Verified against this schema, not theorised.

    `object_versions.size_bytes` has no non-negative CHECK, so the schema does permit these values.
    """
    acct = await _seed_account(pg_tx)
    bucket_id = await _seed_bucket(pg_tx, acct)

    row = await _reserve(pg_tx, bucket_id, "extremes")
    object_id, version = row["object_id"], row["current_object_version"]

    # Outgoing version at the bottom of the range, incoming at the top: the subtraction that
    # storage_usage_apply performs is the widest possible.
    await pg_tx.execute(
        "UPDATE object_versions SET size_bytes = $1 WHERE object_id = $2 AND object_version = $3",
        -(2**63),
        object_id,
        version,
    )
    await pg_tx.execute(
        "UPDATE object_versions SET size_bytes = $1 WHERE object_id = $2 AND object_version = $3",
        2**63 - 1,
        object_id,
        version,
    )

    # And the negation path: deleting the object while it holds the minimum.
    await pg_tx.execute(
        "UPDATE object_versions SET size_bytes = $1 WHERE object_id = $2 AND object_version = $3",
        -(2**63),
        object_id,
        version,
    )
    await pg_tx.execute("DELETE FROM objects WHERE object_id = $1", object_id)

    # Out-of-range deltas are dropped rather than written, so nothing corrupt reaches the ledger.
    for delta in await _ledger_rows(pg_tx, bucket_id):
        assert -(2**63) <= delta <= 2**63 - 1


# --------------------------------------------------------------------------------------------
# The two-step PUT under concurrency. THE bug this rollup shipped with.
#
# A PUT is TWO transactions (hippius_s3/writer/object_writer.py): the reserve points
# current_object_version at a version whose size_bytes is 0, then -- after streaming the whole
# body, which can take minutes -- a separate transaction sets the real size. The objects trigger
# computed its decrement by looking up the OUTGOING version's size, and between another writer's
# reserve and its finalize that size is still 0. So an overwrite subtracted 0, while the outgoing
# version's own finalize had already added its full size (it was still current when it ran). Every
# version ever written was added exactly once and never subtracted: on a three-way race of
# 1000/2000/3000 bytes the ledger read 6000 on every run against a truth of whichever version won.
#
# The concurrency tests above did not catch it for one reason: they wrap reserve and finalize in ONE
# transaction, which holds the objects row across both and serialises the writers by accident. The
# production path does not. Everything in this section runs the un-wrapped, two-transaction shape.
#
# See 20260911090000_storage_usage_lock_outgoing_version.sql.
# --------------------------------------------------------------------------------------------


@pytest.mark.parametrize("concurrency", [1, 4, 16, 32])
async def test_same_key_concurrent_two_step_puts_do_not_over_count(
    committed_pool: CommittedPool,
    concurrency: int,
) -> None:
    """The regression test. Un-wrapped two-step PUTs of ONE key, at four concurrencies.

    SIZES MUST VARY. An equal-size overwrite nets to zero and emits no ledger row at all (the
    zero-delta guard returns early), so a same-key test at constant size passes against the broken
    trigger and proves nothing at all.

    Asserted against get_account_storage_bytes.sql, never against a constant: which writer wins is a
    race, and "the counter agrees with the canonical query" is the entire claim.
    """
    acct = await committed_pool.new_account()
    bucket_id = await committed_pool.new_bucket(acct)

    sizes = [1024 * (i + 1) for i in range(concurrency)]

    async def put(size: int) -> None:
        # NO conn.transaction(): reserve and finalize commit separately, as production does.
        async with committed_pool.acquire() as conn:
            await _put(conn, bucket_id, "hot", size)

    for _ in range(4):
        await asyncio.gather(*(put(s) for s in sizes))

    async with committed_pool.acquire() as conn:
        await _assert_matches_oracle(conn, acct)


async def test_a_reserve_waits_for_an_uncommitted_finalize_of_the_outgoing_version(
    committed_pool: CommittedPool,
) -> None:
    """The mechanism, forced open so there is no race to lose.

    The finalize of v1 is held UNCOMMITTED. Its trigger has already added v1's bytes -- v1 was still
    current when it ran. A reserve then arrives to point the key at v2, and to be right it must
    subtract v1's real size. It can only know that size by waiting: without the lock it reads the
    pre-finalize 0, subtracts nothing, and v1's bytes stay on the bill forever.

    Two assertions, because either alone is weak: the reserve BLOCKS, and the total is right after.
    """
    acct = await committed_pool.new_account()
    bucket_id = await committed_pool.new_bucket(acct)

    async with committed_pool.acquire() as conn:
        row = await _reserve(conn, bucket_id, "hot")
        object_id, version = row["object_id"], row["current_object_version"]

    finalizer = await committed_pool.pool.acquire()
    reserver = await committed_pool.pool.acquire()
    try:
        tx = finalizer.transaction()
        await tx.start()
        await _finalize(finalizer, object_id, version, 1000)

        task = asyncio.create_task(_reserve(reserver, bucket_id, "hot"))
        # Long enough for the reserve to reach the lock and stop there.
        await asyncio.sleep(0.4)
        assert not task.done(), "the reserve did not wait for the finalize; the size it subtracts is stale"

        await tx.commit()
        await task
    finally:
        await committed_pool.pool.release(reserver)
        await committed_pool.pool.release(finalizer)

    async with committed_pool.acquire() as conn:
        # v2 is current and empty, so the truth is 0 -- and the counter must say 0, not 1000.
        assert await _assert_matches_oracle(conn, acct) == 0


async def test_a_repoint_onto_an_existing_version_waits_for_its_uncommitted_finalize(
    committed_pool: CommittedPool,
) -> None:
    """The mirror of the above, on the INCOMING side, which under-counts rather than over-counts.

    The objects trigger reads TWO version sizes: the outgoing one to subtract and the incoming one
    to add. Locking only the outgoing one leaves this: a statement that repoints current onto an
    ALREADY-EXISTING version whose size is being written right now adds the pre-write size.

    abort_cleanup_orphan_version is that statement -- it repoints DOWN onto an existing lower
    version -- and its own comment documents the collision ("a CompleteMultipartUpload of a LOWER
    in-flight version committing after this statement's snapshot is invisible").

    Measured without the incoming lock: truth 1000, ledger 0.
    """
    acct = await committed_pool.new_account()
    bucket_id = await committed_pool.new_bucket(acct)

    async with committed_pool.acquire() as conn:
        first = await _reserve(conn, bucket_id, "k")
        object_id, low_version = first["object_id"], first["current_object_version"]
        second = await _reserve(conn, bucket_id, "k")
        reserved_version = second["current_object_version"]

    finalizer = await committed_pool.pool.acquire()
    aborter = await committed_pool.pool.acquire()
    try:
        tx = finalizer.transaction()
        await tx.start()
        # The lower version completes while it is NOT current, so its own trigger emits nothing --
        # the repoint is the only thing that can ever count these bytes.
        await _finalize(finalizer, object_id, low_version, 1000)

        task = asyncio.create_task(
            aborter.fetchrow(get_query("abort_cleanup_orphan_version"), object_id, reserved_version)
        )
        await asyncio.sleep(0.4)
        assert not task.done(), "the repoint did not wait; the size it adds for the incoming version is stale"

        await tx.commit()
        await task
    finally:
        await committed_pool.pool.release(aborter)
        await committed_pool.pool.release(finalizer)

    async with committed_pool.acquire() as conn:
        assert (
            await conn.fetchval("SELECT current_object_version FROM objects WHERE object_id = $1", object_id)
            == low_version
        )
        assert await _assert_matches_oracle(conn, acct) == 1000


async def test_an_append_racing_a_repoint_is_counted_at_its_appended_size(
    committed_pool: CommittedPool,
) -> None:
    """S4 append is the OTHER path that sets a size after the fact, and it had the same exposure.

    `SET size_bytes = size_bytes + $N` on the already-current version. Forced open rather than
    raced: the append is held uncommitted while the reserve arrives. Without the lock the reserve
    subtracts the pre-append size and the appended bytes are billed forever.
    """
    acct = await committed_pool.new_account()
    bucket_id = await committed_pool.new_bucket(acct)

    async with committed_pool.acquire() as conn:
        object_id = await _put(conn, bucket_id, "log", 1000)
        version = await conn.fetchval("SELECT current_object_version FROM objects WHERE object_id = $1", object_id)

    appender = await committed_pool.pool.acquire()
    reserver = await committed_pool.pool.acquire()
    try:
        tx = appender.transaction()
        await tx.start()
        await appender.execute(
            "UPDATE object_versions SET size_bytes = size_bytes + $1, append_version = append_version + 1"
            " WHERE object_id = $2 AND object_version = $3 AND deleted_at IS NULL",
            5000,
            object_id,
            version,
        )

        task = asyncio.create_task(_reserve(reserver, bucket_id, "log"))
        await asyncio.sleep(0.4)
        assert not task.done(), "the reserve did not wait for the append; it will subtract a stale size"

        await tx.commit()
        await task
    finally:
        await committed_pool.pool.release(reserver)
        await committed_pool.pool.release(appender)

    async with committed_pool.acquire() as conn:
        assert await _assert_matches_oracle(conn, acct) == 0


async def test_concurrent_appends_alone_are_exact(committed_pool: CommittedPool) -> None:
    """The other half of the append claim, and the reason the append path needs no change of its own.

    Appends that race only each other contend on ONE version row, so they serialise themselves, and
    the trigger computes NEW.size_bytes - OLD.size_bytes from its own tuple -- there is no cross-row
    read of a size to go stale. Measured clean at concurrency 4/16/32 both before and after the fix.
    """
    acct = await committed_pool.new_account()
    bucket_id = await committed_pool.new_bucket(acct)

    async with committed_pool.acquire() as conn:
        object_id = await _put(conn, bucket_id, "log", 100)
        version = await conn.fetchval("SELECT current_object_version FROM objects WHERE object_id = $1", object_id)

    deltas = [10 * (i + 1) for i in range(16)]

    async def append(size: int) -> None:
        async with committed_pool.acquire() as conn:
            await conn.execute(
                "UPDATE object_versions SET size_bytes = size_bytes + $1, append_version = append_version + 1"
                " WHERE object_id = $2 AND object_version = $3 AND deleted_at IS NULL",
                size,
                object_id,
                version,
            )

    await asyncio.gather(*(append(d) for d in deltas))

    async with committed_pool.acquire() as conn:
        assert await _assert_matches_oracle(conn, acct) == 100 + sum(deltas)


@pytest.mark.parametrize("concurrency", [4, 16])
async def test_mpu_completion_alone_under_concurrency_converges(
    committed_pool: CommittedPool,
    concurrency: int,
) -> None:
    """MPU initiate is a repoint onto a fresh zero-size version; Complete sets the size after.

    So the MPU pair has the two-step shape on its own, with no simple PUT involved -- and it drifted
    on its own, badly (measured +13.7M at concurrency 32 against the shipped triggers).
    """
    acct = await committed_pool.new_account()
    bucket_id = await committed_pool.new_bucket(acct)

    async def mpu(size: int) -> None:
        async with committed_pool.acquire() as conn:
            await _mpu(conn, bucket_id, "big", size)

    for _ in range(4):
        await asyncio.gather(*(mpu(4096 * (i + 1)) for i in range(concurrency)))

    async with committed_pool.acquire() as conn:
        await _assert_matches_oracle(conn, acct)


@pytest.mark.parametrize("concurrency", [4, 16])
async def test_two_step_puts_racing_a_soft_delete_converge(
    committed_pool: CommittedPool,
    concurrency: int,
) -> None:
    """Overwrites and whole-key soft-deletes of ONE key, interleaved, production shape.

    The objects trigger also fires on a deleted_at change, and there OLD and NEW name the SAME
    version -- which a concurrent finalize can be resizing right now. That side needs the lock too.
    """
    acct = await committed_pool.new_account()
    bucket_id = await committed_pool.new_bucket(acct)

    async def put(size: int) -> None:
        async with committed_pool.acquire() as conn:
            await _put(conn, bucket_id, "churn", size)

    async def soft_delete() -> None:
        async with committed_pool.acquire() as conn:
            await conn.execute(get_query("soft_delete_object"), bucket_id, "churn")

    for _ in range(4):
        await asyncio.gather(*(put(1024 * (i + 1)) if i % 3 else soft_delete() for i in range(concurrency)))

    async with committed_pool.acquire() as conn:
        await _assert_matches_oracle(conn, acct)


@pytest.mark.parametrize("concurrency", [4, 16])
async def test_two_step_puts_racing_a_delete_marker_converge(
    committed_pool: CommittedPool,
    concurrency: int,
) -> None:
    """A delete marker is a new current version carrying is_delete_marker, so it repoints too."""
    acct = await committed_pool.new_account()
    bucket_id = await committed_pool.new_bucket(acct)

    async def put(size: int) -> None:
        async with committed_pool.acquire() as conn:
            await _put(conn, bucket_id, "marked", size)

    async def marker() -> None:
        async with committed_pool.acquire() as conn:
            await conn.execute(get_query("insert_delete_marker"), bucket_id, "marked")

    for _ in range(4):
        await asyncio.gather(*(put(1024 * (i + 1)) if i % 4 else marker() for i in range(concurrency)))

    async with committed_pool.acquire() as conn:
        await _assert_matches_oracle(conn, acct)


@pytest.mark.parametrize("concurrency", [4, 16])
async def test_aborted_puts_racing_overwrites_converge(
    committed_pool: CommittedPool,
    concurrency: int,
) -> None:
    """A PUT that dies mid-stream leaves current_object_version on a row stranded at size 0.

    That orphan must be worth exactly nothing -- including when it is the version an overwrite is
    reading to compute its decrement, which is the case that broke.
    """
    acct = await committed_pool.new_account()
    bucket_id = await committed_pool.new_bucket(acct)

    async def aborted() -> None:
        async with committed_pool.acquire() as conn:
            await _reserve(conn, bucket_id, "orphan")

    async def put(size: int) -> None:
        async with committed_pool.acquire() as conn:
            await _put(conn, bucket_id, "orphan", size)

    for _ in range(4):
        await asyncio.gather(*(aborted() if i % 3 == 0 else put(1024 * (i + 1)) for i in range(concurrency)))

    async with committed_pool.acquire() as conn:
        await _assert_matches_oracle(conn, acct)


# --------------------------------------------------------------------------------------------
# Lock order: objects BEFORE object_versions, everywhere, or deadlock.
# --------------------------------------------------------------------------------------------


async def test_taking_the_version_row_before_the_objects_row_deadlocks(
    committed_pool: CommittedPool,
) -> None:
    """WHY the lock-order invariant exists, demonstrated rather than asserted in a comment.

    The objects trigger runs while its statement holds the objects row and now locks the version
    rows, so its order is objects -> object_versions. A transaction taking them the other way round
    closes a cycle, and Postgres resolves a cycle by failing somebody's request.

    Pinned as a test because the invariant is invisible from either end: nothing about
    `UPDATE object_versions ...; UPDATE objects ...` looks like it touches billing.
    """
    acct = await committed_pool.new_account()
    bucket_id = await committed_pool.new_bucket(acct)

    async with committed_pool.acquire() as conn:
        object_id = await _put(conn, bucket_id, "k", 1000)
        version = await conn.fetchval("SELECT current_object_version FROM objects WHERE object_id = $1", object_id)

    offender = await committed_pool.pool.acquire()
    overwriter = await committed_pool.pool.acquire()
    try:
        await offender.execute("BEGIN")
        # The forbidden order: version row first.
        await offender.fetch(
            "SELECT 1 FROM object_versions WHERE object_id = $1 AND object_version = $2 FOR UPDATE",
            object_id,
            version,
        )

        # A plain overwrite: locks the objects row, then its trigger wants the version row.
        overwrite = asyncio.create_task(_reserve(overwriter, bucket_id, "k"))
        await asyncio.sleep(0.4)
        assert not overwrite.done()

        # And now the offender wants the objects row the overwrite is holding.
        offending = asyncio.create_task(
            offender.execute("UPDATE objects SET current_object_version = $2 WHERE object_id = $1", object_id, version)
        )

        deadlocked = False
        for task in (offending, overwrite):
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=15)
            except asyncpg.DeadlockDetectedError:
                deadlocked = True
            except asyncio.TimeoutError:
                pass

        assert deadlocked, "expected Postgres to break the cycle; the ordering rule may have stopped mattering"
    finally:
        for conn_ in (offender, overwriter):
            with contextlib.suppress(Exception):
                await conn_.execute("ROLLBACK")
            await committed_pool.pool.release(conn_)


async def test_abort_cleanup_locks_the_objects_row_before_the_version_row(
    committed_pool: CommittedPool,
) -> None:
    """abort_cleanup_orphan_version is ONE statement, so a per-transaction audit cannot see its order.

    Its CAS locks the reserved version row FOR UPDATE and then updates `objects`. Written in the
    natural order that is version -> objects, the forbidden one, and it deadlocks against a
    concurrent overwrite of the same key (verified: 40P01). It now takes the objects row in a
    leading CTE, joined by a data dependency so the planner cannot reorder it -- an unreferenced
    plain CTE is not evaluated at all.

    Proven by holding the objects row and asking whether the blocked statement has ALREADY taken the
    version row. If it has, it is holding a version while waiting for objects, which is the cycle.
    """
    acct = await committed_pool.new_account()
    bucket_id = await committed_pool.new_bucket(acct)

    async with committed_pool.acquire() as conn:
        first = await _reserve(conn, bucket_id, "k")
        object_id = first["object_id"]
        second = await _reserve(conn, bucket_id, "k")
        reserved_version = second["current_object_version"]

    holder = await committed_pool.pool.acquire()
    aborter = await committed_pool.pool.acquire()
    prober = await committed_pool.pool.acquire()
    try:
        await holder.execute("BEGIN")
        await holder.fetch("SELECT 1 FROM objects WHERE object_id = $1 FOR NO KEY UPDATE", object_id)

        await aborter.execute("BEGIN")
        abort = asyncio.create_task(
            aborter.fetchrow(get_query("abort_cleanup_orphan_version"), object_id, reserved_version)
        )
        await asyncio.sleep(0.4)
        assert not abort.done(), "the abort did not block on the objects row, so this proves nothing"

        await prober.execute("BEGIN")
        version_row_is_free = True
        try:
            await prober.fetch(
                "SELECT 1 FROM object_versions WHERE object_id = $1 AND object_version = $2 FOR UPDATE NOWAIT",
                object_id,
                reserved_version,
            )
        except asyncpg.LockNotAvailableError:
            version_row_is_free = False
        await prober.execute("ROLLBACK")

        assert version_row_is_free, (
            "abort_cleanup_orphan_version holds the version row while waiting for the objects row -- "
            "the forbidden order, which deadlocks against a concurrent overwrite"
        )

        await holder.execute("ROLLBACK")
        with contextlib.suppress(asyncpg.PostgresError):
            await abort
    finally:
        for conn_ in (holder, aborter, prober):
            with contextlib.suppress(Exception):
                await conn_.execute("ROLLBACK")
            await committed_pool.pool.release(conn_)


@pytest.mark.parametrize("concurrency", [4, 16])
async def test_abort_multipart_racing_an_overwrite_does_not_deadlock(
    committed_pool: CommittedPool,
    concurrency: int,
) -> None:
    """The real abort statement against real overwrites: no exception, and the counter still agrees.

    A deadlock here is a 500 on a customer request, so the assertion is on the exceptions as much as
    on the bytes.
    """
    acct = await committed_pool.new_account()
    bucket_id = await committed_pool.new_bucket(acct)

    async def put(size: int) -> None:
        async with committed_pool.acquire() as conn:
            await _put(conn, bucket_id, "aborty", size)

    async def abort() -> None:
        async with committed_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT object_id, current_object_version FROM objects"
                " WHERE bucket_id = $1 AND object_key = $2 AND deleted_at IS NULL",
                bucket_id,
                "aborty",
            )
            if row:
                await conn.fetchrow(
                    get_query("abort_cleanup_orphan_version"), row["object_id"], row["current_object_version"]
                )

    for _ in range(6):
        work = [abort() if i % 3 == 0 else put(1024 * (i + 1)) for i in range(concurrency)]
        outcomes = await asyncio.gather(*work, return_exceptions=True)
        raised = [o for o in outcomes if isinstance(o, BaseException)]
        assert not raised, f"concurrent abort and overwrite raised {raised}"

    async with committed_pool.acquire() as conn:
        await _assert_matches_oracle(conn, acct)


async def test_the_locking_read_helper_is_not_stable(pg_conn: asyncpg.Connection) -> None:
    """storage_usage_version_bytes_locked must stay VOLATILE.

    It takes a row lock, so it is not repeatable and the planner must not be free to fold, cache or
    elide the call. Marking it STABLE would also re-pin it to the calling snapshot, which is the
    exact shape the bug had. `v` is VOLATILE, `s` STABLE, `i` IMMUTABLE in pg_proc.
    """
    # provolatile is Postgres's internal "char" type, which asyncpg hands back as bytes.
    volatility = await pg_conn.fetchval(
        "SELECT provolatile::text FROM pg_proc WHERE proname = 'storage_usage_version_bytes_locked'"
    )
    assert volatility == "v"


# --------------------------------------------------------------------------------------------
# The real PUT tail transaction. Everything above this line used a reserve+finalize helper that
# does NOT insert the parts / multipart_uploads rows -- and that omission is why an adversarial
# probe over 2,880 operations reported zero deadlocks while the hot path deadlocked 24% of the
# time at concurrency 32. These tests drive the statement set object_writer actually issues.
# --------------------------------------------------------------------------------------------


async def _real_put_tail(
    conn: asyncpg.Connection,
    bucket_id: uuid.UUID,
    key: str,
    object_id: uuid.UUID,
    version: int,
    size: int,
    *,
    lock_objects_first: bool,
) -> None:
    """object_writer's tail transaction, statement for statement.

    The two INSERTs carry object_id FKs, which Postgres services with an implicit
    `objects ... FOR KEY SHARE` at the point of the INSERT -- i.e. AFTER the UPDATE below has
    already taken the object_versions row. Without the leading objects lock the transaction's
    order is object_versions -> objects, which closes a cycle against a concurrent reserve.
    """
    async with conn.transaction():
        if lock_objects_first:
            await conn.execute(get_query("lock_object_row_by_id"), object_id)
        await _finalize(conn, object_id, version, size)
        upload_id = uuid.uuid4()
        await conn.execute(
            "INSERT INTO multipart_uploads(upload_id, bucket_id, object_key, content_type,"
            " metadata, initiated_at, object_id, is_completed)"
            " VALUES($1, $2, $3, $4, $5, NOW(), $6, false)",
            upload_id,
            bucket_id,
            key,
            CT,
            json.dumps({}),
            object_id,
        )
        await conn.execute(
            "INSERT INTO parts(part_id, upload_id, part_number, size_bytes, etag, uploaded_at,"
            " object_id, chunk_size_bytes, object_version)"
            " VALUES($1, $2, 1, $3, 'e', NOW(), $4, 4194304, $5)",
            uuid.uuid4(),
            upload_id,
            size,
            object_id,
            version,
        )


@pytest.mark.parametrize("concurrency", [8, 32])
async def test_concurrent_same_key_puts_with_the_real_statement_set_do_not_deadlock(
    committed_pool: CommittedPool, concurrency: int
) -> None:
    """Zero deadlocks AND zero drift. Both, because fixing either alone is easy and wrong.

    Measured without the leading objects lock: 4/48 deadlocks at c=8 and 46/192 at c=32. Each one
    is a 500 returned after the whole request body was received and staged, and db_retry only
    retries object_versions_pkey, so nothing absorbs it.
    """
    acct = await committed_pool.new_account()
    bucket_id = await committed_pool.new_bucket(acct)
    sizes = [1024, 4096, 16384, 65536]

    async def one_put(index: int) -> None:
        async with committed_pool.acquire() as conn:
            row = await _reserve(conn, bucket_id, "hot")
        async with committed_pool.acquire() as conn:
            await _real_put_tail(
                conn,
                bucket_id,
                "hot",
                row["object_id"],
                row["current_object_version"],
                sizes[index % len(sizes)],
                lock_objects_first=True,
            )

    results = await asyncio.gather(*(one_put(i) for i in range(concurrency)), return_exceptions=True)

    deadlocks = [r for r in results if isinstance(r, asyncpg.DeadlockDetectedError)]
    assert not deadlocks, (
        f"{len(deadlocks)}/{concurrency} concurrent same-key PUTs deadlocked. The tail transaction "
        f"must take the objects row BEFORE object_versions -- the parts/multipart_uploads INSERTs "
        f"reach objects through their FKs. See lock_object_row_by_id.sql."
    )
    other = [r for r in results if isinstance(r, BaseException)]
    assert not other, f"unexpected failures: {other}"

    async with committed_pool.acquire() as conn:
        await _compact(conn)
        await _assert_matches_oracle(conn, acct)


async def test_the_objects_lock_is_taken_before_the_version_row_in_the_put_tail(
    committed_pool: CommittedPool,
) -> None:
    """Deterministic companion to the stress test above: prove the ORDER, not just the outcome.

    A stress test that happens to pass proves nothing about why. This pins the mechanism: with the
    leading lock the tail transaction blocks on a concurrent reserve's objects row instead of
    acquiring its version row first and deadlocking.
    """
    acct = await committed_pool.new_account()
    bucket_id = await committed_pool.new_bucket(acct)

    async with committed_pool.acquire() as conn:
        row = await _reserve(conn, bucket_id, "hot")
    object_id, version = row["object_id"], row["current_object_version"]

    async with committed_pool.acquire() as holder:
        async with holder.transaction():
            # Hold the objects row the way a concurrent reserve does.
            await holder.execute("SELECT 1 FROM objects WHERE object_id = $1 FOR UPDATE", object_id)

            async def tail() -> None:
                async with committed_pool.acquire() as conn:
                    await _real_put_tail(conn, bucket_id, "hot", object_id, version, 4096, lock_objects_first=True)

            task = asyncio.create_task(tail())
            await asyncio.sleep(0.6)
            blocked = not task.done()

        await task

    assert blocked, (
        "the tail transaction did not wait on the objects row, so it is not taking that lock first "
        "-- which is exactly the ordering that deadlocks under concurrency"
    )

    async with committed_pool.acquire() as conn:
        await _compact(conn)
        assert await _assert_matches_oracle(conn, acct) == 4096


# --------------------------------------------------------------------------------------------
# Known-issue fixes. Each of these was a real defect found in review; the test states the defect
# so a future change that reintroduces it fails with the reason rather than a bare mismatch.
# --------------------------------------------------------------------------------------------


async def test_a_negative_bucket_does_not_net_against_a_positive_one(pg_tx: asyncpg.Connection) -> None:
    """The clamp must be PER BUCKET, not on the account total.

    With GREATEST(0, SUM(...)) at the account level, one bucket at -3 GB and another at +10 GB
    reports 7 GB: the account is silently UNDER-billed and the clamp never fires, so nothing
    indicates anything is wrong. Clamping each bucket first fails in the safe direction (over-
    reporting the broken bucket as 0) while `negative_buckets` still flags it for the reconciler.
    """
    acct = await _seed_account(pg_tx)
    good = await _seed_bucket(pg_tx, acct)
    bad = await _seed_bucket(pg_tx, acct)

    await pg_tx.execute(
        "INSERT INTO bucket_storage_usage(bucket_id, bytes_used, updated_at) VALUES($1, $2, now())",
        good,
        10_000,
    )
    await pg_tx.execute(
        "INSERT INTO bucket_storage_usage(bucket_id, bytes_used, updated_at) VALUES($1, $2, now())",
        bad,
        -3_000,
    )

    row = await pg_tx.fetchrow(get_query("get_account_storage_bytes_rollup"), acct)

    assert row["bytes_used"] == 10_000, (
        f"got {row['bytes_used']}: the negative bucket netted against the positive one. Clamp per "
        f"bucket -- SUM(GREATEST(0, bsu.bytes_used)) -- not on the total."
    )
    assert row["negative_buckets"] == 1, "the negative bucket must still be reported"


async def test_a_negative_counter_on_a_soft_deleted_bucket_is_not_reported(
    pg_tx: asyncpg.Connection,
) -> None:
    """Otherwise it alarms forever and nothing can ever clear it.

    get_storage_delta_ledger_stats counted `bytes_used < 0` across ALL of bucket_storage_usage with
    no liveness filter, while list_buckets_for_usage_reconcile only recomputes LIVE buckets. So a
    negative counter on a soft-deleted bucket is unreachable by the repair path while being counted
    by the alarm -- STORAGE_ROLLUP_NEGATIVE at ERROR on every cycle, permanently, which is precisely
    how you teach an operator to ignore the one alert this design depends on.

    A soft-deleted bucket's total is read by nobody (every read path joins `deleted_at IS NULL`), so
    its counter being wrong has no billing consequence and must not alarm.
    """
    acct = await _seed_account(pg_tx)
    live = await _seed_bucket(pg_tx, acct)
    dead = await _seed_bucket(pg_tx, acct, deleted=True)

    for bucket_id, value in ((live, 5_000), (dead, -9_000)):
        await pg_tx.execute(
            "INSERT INTO bucket_storage_usage(bucket_id, bytes_used, updated_at) VALUES($1, $2, now())",
            bucket_id,
            value,
        )

    stats = await storage_rollup_service.ledger_stats(pg_tx)

    assert stats.negative_buckets == 0, (
        "a negative counter on a soft-deleted bucket was reported. The reconciler only sweeps live "
        "buckets, so this alarm can never be cleared -- filter the stat on buckets.deleted_at IS NULL."
    )

    # And a LIVE one must still be caught, or the fix has traded a false positive for a false negative.
    await pg_tx.execute("UPDATE bucket_storage_usage SET bytes_used = -1 WHERE bucket_id = $1", live)
    assert (await storage_rollup_service.ledger_stats(pg_tx)).negative_buckets == 1


async def test_the_two_rollup_tables_carry_queue_shaped_storage_parameters(
    pg_conn: asyncpg.Connection,
) -> None:
    """A queue and a hot-updated counter both need non-default autovacuum settings.

    `storage_delta_ledger` is append-at-the-tail, bulk-DELETE-the-head: the default
    autovacuum_vacuum_scale_factor of 0.2 is proportional to a table that is *supposed* to stay
    near-empty, so it triggers constantly on a tiny absolute number of dead tuples -- and production
    runs autovacuum_max_workers = 3 against 168M-row tables, so a small table grabbing a worker slot
    every few seconds is stolen capacity from where it matters. A flat threshold is the right shape
    for a queue.

    `bucket_storage_usage` takes one UPDATE per active bucket per fold. At fillfactor 100 every one
    of those needs a new page once the page is full, so the table and its PK index bloat and HOT
    updates are impossible. Leaving headroom keeps the new row tuple on the same page.

    Neither matters at prod's current ~1-3 ledger rows/s, which is why this is cheap insurance
    rather than a fix -- but both get worse with load, and neither can be changed under load
    without an ACCESS EXCLUSIVE moment.
    """
    rows = await pg_conn.fetch(
        "SELECT relname, COALESCE(reloptions, '{}') AS opts FROM pg_class"
        " WHERE relname IN ('storage_delta_ledger', 'bucket_storage_usage')"
    )
    opts = {r["relname"]: " ".join(r["opts"]) for r in rows}

    assert set(opts) == {"storage_delta_ledger", "bucket_storage_usage"}, f"tables missing: {opts}"

    assert "autovacuum_vacuum_threshold" in opts["storage_delta_ledger"], (
        f"storage_delta_ledger has no flat autovacuum threshold: {opts['storage_delta_ledger']!r}"
    )
    assert "autovacuum_vacuum_scale_factor=0" in opts["storage_delta_ledger"].replace(" ", ""), (
        "the proportional scale factor must be disabled on a queue table, or the flat threshold "
        f"never governs: {opts['storage_delta_ledger']!r}"
    )
    assert "fillfactor" in opts["bucket_storage_usage"], (
        f"bucket_storage_usage needs page headroom for HOT updates: {opts['bucket_storage_usage']!r}"
    )
