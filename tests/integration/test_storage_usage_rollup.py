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
import datetime
import json
import os
import uuid
from typing import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio

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
    """The reserve half of a PUT / MPU initiate: one statement, objects + object_versions."""
    query = "upsert_object_multipart" if multipart else "upsert_object_basic"
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
    await pg_tx.execute(
        "UPDATE objects SET deleted_at = now() - INTERVAL '48 hours' WHERE object_id = $1", object_id
    )
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
