"""The trigger-maintained storage rollup, against a real Postgres.

None of this can be unit tested: the counter lives entirely in database triggers, so mocks would
test nothing. Every test asserts the same invariant --

    bucket_storage_usage.bytes_used == the ground-truth SUM

-- after some sequence of writes. If that ever diverges, the quota gate is billing customers on a
number that does not match what they can see, and the reconciler is papering over a real defect.

Read the header of migrations/20260908120000_bucket_storage_usage.sql first; it explains which
trigger owns which transition and why there is deliberately no trigger on object_versions INSERT.
"""

import uuid

import asyncpg
import pytest


GB = 1_000_000_000
pytestmark = pytest.mark.asyncio


# --------------------------------------------------------------------------- helpers


async def make_bucket(db: asyncpg.Connection, account: str) -> uuid.UUID:
    bucket_id = uuid.uuid4()
    # buckets.main_account_id has an FK to users; seed the owner first.
    await db.execute(
        "INSERT INTO users (main_account_id, created_at) VALUES ($1, now()) ON CONFLICT DO NOTHING",
        account,
    )
    await db.execute(
        "INSERT INTO buckets (bucket_id, bucket_name, created_at, main_account_id) VALUES ($1, $2, now(), $3)",
        bucket_id,
        f"test-{bucket_id}",
        account,
    )
    return bucket_id


async def put_object(db: asyncpg.Connection, bucket_id: uuid.UUID, key: str, size: int) -> tuple[uuid.UUID, int]:
    """Reserve-then-finalize, the same two steps object_writer takes for a simple PUT."""
    object_id, version = await reserve(db, bucket_id, key)
    await finalize(db, object_id, version, size)
    return object_id, version


async def reserve(db: asyncpg.Connection, bucket_id: uuid.UUID, key: str) -> tuple[uuid.UUID, int]:
    """The shape of upsert_object_basic: bump objects and insert the version at size 0, in ONE
    statement, so the end-of-statement AFTER-trigger ordering is exercised for real."""
    row = await db.fetchrow(
        """
        WITH upserted AS (
            INSERT INTO objects (object_id, bucket_id, object_key, created_at, current_object_version)
            VALUES ($1, $2, $3, now(), 1)
            ON CONFLICT (bucket_id, object_key) DO UPDATE
              SET deleted_at = NULL,
                  current_object_version = GREATEST(
                      objects.current_object_version,
                      (SELECT COALESCE(MAX(ov.object_version), 0)
                       FROM object_versions ov WHERE ov.object_id = objects.object_id)
                  ) + 1
            RETURNING object_id, current_object_version
        ), ins AS (
            INSERT INTO object_versions
                (object_id, object_version, version_type, storage_version, size_bytes, content_type)
            SELECT u.object_id, u.current_object_version, 'user', 5, 0, 'application/octet-stream'
            FROM upserted u
            RETURNING object_id, object_version
        )
        SELECT u.object_id, u.current_object_version FROM upserted u
        """,
        uuid.uuid4(),
        bucket_id,
        key,
    )
    return row["object_id"], row["current_object_version"]


async def finalize(db: asyncpg.Connection, object_id: uuid.UUID, version: int, size: int) -> None:
    await db.execute(
        "UPDATE object_versions SET size_bytes = $1 WHERE object_id = $2 AND object_version = $3",
        size,
        object_id,
        version,
    )


async def rollup(db: asyncpg.Connection, bucket_id: uuid.UUID) -> tuple[int, int]:
    row = await db.fetchrow(
        "SELECT bytes_used, objects_count FROM bucket_storage_usage WHERE bucket_id = $1", bucket_id
    )
    return (int(row["bytes_used"]), int(row["objects_count"])) if row else (0, 0)


async def ground_truth(db: asyncpg.Connection, bucket_id: uuid.UUID) -> int:
    row = await db.fetchrow(
        """
        SELECT COALESCE(SUM(ov.size_bytes), 0)::bigint AS bytes
        FROM buckets b
        JOIN objects o ON o.bucket_id = b.bucket_id AND o.deleted_at IS NULL
        JOIN object_versions ov ON ov.object_id = o.object_id
                               AND ov.object_version = o.current_object_version
                               AND ov.deleted_at IS NULL
                               AND NOT ov.is_delete_marker
        WHERE b.bucket_id = $1 AND b.deleted_at IS NULL
        """,
        bucket_id,
    )
    return int(row["bytes"])


async def assert_consistent(db: asyncpg.Connection, bucket_id: uuid.UUID, expected: int) -> None:
    counter, _ = await rollup(db, bucket_id)
    truth = await ground_truth(db, bucket_id)
    assert truth == expected, f"ground truth {truth} != expected {expected}"
    assert counter == truth, f"counter {counter} drifted from ground truth {truth}"


@pytest.fixture
def account() -> str:
    return f"5Test{uuid.uuid4().hex[:20]}"


# --------------------------------------------------------------------------- the trigger set


async def test_the_trigger_set_is_exactly_as_expected(pg_conn: asyncpg.Connection) -> None:
    """Pins the negative invariant that lives only in a comment otherwise.

    Adding an AFTER INSERT trigger on object_versions is the "obviously missing" change a future
    dev will reach for -- and it would double-count every new object, because the `objects` triggers
    already own the transition that makes a version current. Fail the build instead.
    """
    rows = await pg_conn.fetch(
        """
        SELECT c.relname AS table_name, t.tgname AS trigger_name
        FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        WHERE NOT t.tgisinternal AND t.tgname LIKE 'trg_usage%'
        ORDER BY c.relname, t.tgname
        """
    )
    found = {(r["table_name"], r["trigger_name"]) for r in rows}

    assert found == {
        ("object_versions", "trg_usage_object_versions_update"),
        ("objects", "trg_usage_objects_delete"),
        ("objects", "trg_usage_objects_insert"),
        ("objects", "trg_usage_objects_update"),
    }


# --------------------------------------------------------------------------- writes


async def test_a_simple_put_counts_once(pg_tx: asyncpg.Connection, account: str) -> None:
    bucket = await make_bucket(pg_tx, account)
    await put_object(pg_tx, bucket, "a", 5 * GB)

    await assert_consistent(pg_tx, bucket, 5 * GB)
    assert (await rollup(pg_tx, bucket))[1] == 1


async def test_reserve_and_finalize_is_a_single_net_delta(pg_tx: asyncpg.Connection, account: str) -> None:
    """The Postgres-semantics assumption the whole design rests on.

    upsert_object_basic is ONE statement whose CTEs update `objects` and insert into
    `object_versions`. AFTER ROW triggers queue to end-of-statement, so the objects trigger must see
    the version row the ins CTE created. If that were not true the reserve would charge a phantom
    delta and every PUT would drift.
    """
    bucket = await make_bucket(pg_tx, account)

    object_id, version = await reserve(pg_tx, bucket, "a")
    assert await rollup(pg_tx, bucket) == (0, 1), "the reserve must contribute 0 bytes, 1 object"

    await finalize(pg_tx, object_id, version, 3 * GB)
    await assert_consistent(pg_tx, bucket, 3 * GB)


async def test_a_put_overwrite_is_new_minus_old(pg_tx: asyncpg.Connection, account: str) -> None:
    """The single highest-risk arithmetic in the feature. Getting it wrong inflates usage
    monotonically until every account is refused."""
    bucket = await make_bucket(pg_tx, account)

    await put_object(pg_tx, bucket, "a", 10 * GB)
    await put_object(pg_tx, bucket, "a", 1 * GB)

    await assert_consistent(pg_tx, bucket, 1 * GB)
    assert (await rollup(pg_tx, bucket))[1] == 1, "an overwrite must not create a second object"


async def test_repeated_overwrites_do_not_accumulate(pg_tx: asyncpg.Connection, account: str) -> None:
    bucket = await make_bucket(pg_tx, account)
    for size in (1 * GB, 2 * GB, 3 * GB, 1 * GB, 7 * GB):
        await put_object(pg_tx, bucket, "a", size)

    await assert_consistent(pg_tx, bucket, 7 * GB)


async def test_an_append_adds_only_the_delta(pg_tx: asyncpg.Connection, account: str) -> None:
    """S4 append works with zero application changes: it is a size_bytes UPDATE like any other."""
    bucket = await make_bucket(pg_tx, account)
    object_id, version = await put_object(pg_tx, bucket, "a", 1 * GB)

    await pg_tx.execute(
        "UPDATE object_versions SET size_bytes = size_bytes + $1 WHERE object_id = $2 AND object_version = $3",
        500_000_000,
        object_id,
        version,
    )

    await assert_consistent(pg_tx, bucket, 1_500_000_000)


async def test_a_migration_version_inserted_above_current_is_not_counted(
    pg_tx: asyncpg.Connection, account: str
) -> None:
    """create_migration_version inserts a version WITHOUT bumping current_object_version."""
    bucket = await make_bucket(pg_tx, account)
    object_id, version = await put_object(pg_tx, bucket, "a", 1 * GB)

    await pg_tx.execute(
        "INSERT INTO object_versions (object_id, object_version, version_type, storage_version, "
        "size_bytes, content_type) VALUES ($1, $2, 'migration', 5, $3, 'application/octet-stream')",
        object_id,
        version + 1,
        99 * GB,
    )

    await assert_consistent(pg_tx, bucket, 1 * GB)


async def test_a_version_swap_moves_the_counter_to_the_new_current(pg_tx: asyncpg.Connection, account: str) -> None:
    bucket = await make_bucket(pg_tx, account)
    object_id, version = await put_object(pg_tx, bucket, "a", 1 * GB)
    await pg_tx.execute(
        "INSERT INTO object_versions (object_id, object_version, version_type, storage_version, "
        "size_bytes, content_type) VALUES ($1, $2, 'migration', 5, $3, 'application/octet-stream')",
        object_id,
        version + 1,
        4 * GB,
    )

    await pg_tx.execute("UPDATE objects SET current_object_version = $1 WHERE object_id = $2", version + 1, object_id)

    await assert_consistent(pg_tx, bucket, 4 * GB)


# --------------------------------------------------------------------------- deletes


async def test_a_soft_delete_releases_the_bytes(pg_tx: asyncpg.Connection, account: str) -> None:
    bucket = await make_bucket(pg_tx, account)
    await put_object(pg_tx, bucket, "a", 4 * GB)

    await pg_tx.execute("UPDATE objects SET deleted_at = now() WHERE bucket_id = $1", bucket)

    await assert_consistent(pg_tx, bucket, 0)
    assert (await rollup(pg_tx, bucket))[1] == 0


async def test_a_repeated_soft_delete_is_idempotent(pg_tx: asyncpg.Connection, account: str) -> None:
    bucket = await make_bucket(pg_tx, account)
    await put_object(pg_tx, bucket, "a", 4 * GB)

    for _ in range(3):
        await pg_tx.execute("UPDATE objects SET deleted_at = now() WHERE bucket_id = $1 AND deleted_at IS NULL", bucket)

    await assert_consistent(pg_tx, bucket, 0)


async def test_reviving_a_soft_deleted_key_restores_the_count(pg_tx: asyncpg.Connection, account: str) -> None:
    bucket = await make_bucket(pg_tx, account)
    await put_object(pg_tx, bucket, "a", 4 * GB)
    await pg_tx.execute("UPDATE objects SET deleted_at = now() WHERE bucket_id = $1", bucket)

    await put_object(pg_tx, bucket, "a", 2 * GB)

    await assert_consistent(pg_tx, bucket, 2 * GB)
    assert (await rollup(pg_tx, bucket))[1] == 1


async def test_a_delete_marker_zeroes_the_key(pg_tx: asyncpg.Connection, account: str) -> None:
    bucket = await make_bucket(pg_tx, account)
    object_id, version = await put_object(pg_tx, bucket, "a", 6 * GB)

    await pg_tx.execute(
        """
        WITH ins AS (
            INSERT INTO object_versions (object_id, object_version, version_type, storage_version,
                                         size_bytes, content_type, is_delete_marker)
            VALUES ($1, $2, 'user', 5, 0, 'application/octet-stream', true)
            RETURNING object_id
        )
        UPDATE objects SET current_object_version = $2 WHERE object_id = $1
        """,
        object_id,
        version + 1,
    )

    await assert_consistent(pg_tx, bucket, 0)


async def test_deleting_the_current_version_repoints_to_the_successor(pg_tx: asyncpg.Connection, account: str) -> None:
    """Two triggers cooperate: the version soft-delete charges -old, the repoint adds +successor."""
    bucket = await make_bucket(pg_tx, account)
    await put_object(pg_tx, bucket, "a", 3 * GB)
    object_id, v2 = await put_object(pg_tx, bucket, "a", 8 * GB)

    await pg_tx.execute(
        "UPDATE object_versions SET deleted_at = now() WHERE object_id = $1 AND object_version = $2",
        object_id,
        v2,
    )
    await pg_tx.execute(
        """
        UPDATE objects o SET current_object_version = (
            SELECT max(ov.object_version) FROM object_versions ov
            WHERE ov.object_id = o.object_id AND ov.deleted_at IS NULL AND ov.object_version < $2
        ) WHERE o.object_id = $1
        """,
        object_id,
        v2,
    )

    await assert_consistent(pg_tx, bucket, 3 * GB)


async def test_deleting_a_superseded_version_changes_nothing(pg_tx: asyncpg.Connection, account: str) -> None:
    """Superseded versions were never counted, so reaping one must be a no-op."""
    bucket = await make_bucket(pg_tx, account)
    object_id, v1 = await put_object(pg_tx, bucket, "a", 3 * GB)
    await put_object(pg_tx, bucket, "a", 8 * GB)

    await pg_tx.execute(
        "UPDATE object_versions SET deleted_at = now() WHERE object_id = $1 AND object_version = $2",
        object_id,
        v1,
    )

    await assert_consistent(pg_tx, bucket, 8 * GB)


async def test_hard_deleting_a_soft_deleted_object_does_not_double_decrement(
    pg_tx: asyncpg.Connection, account: str
) -> None:
    """The janitor's normal path. The bytes were released at soft-delete; the cascade must add 0."""
    bucket = await make_bucket(pg_tx, account)
    await put_object(pg_tx, bucket, "a", 4 * GB)
    await put_object(pg_tx, bucket, "b", 1 * GB)
    await pg_tx.execute("UPDATE objects SET deleted_at = now() WHERE bucket_id = $1 AND object_key = 'a'", bucket)
    assert (await rollup(pg_tx, bucket))[0] == 1 * GB

    await pg_tx.execute("DELETE FROM objects WHERE bucket_id = $1 AND object_key = 'a'", bucket)

    await assert_consistent(pg_tx, bucket, 1 * GB)


async def test_hard_deleting_a_LIVE_object_is_charged(pg_tx: asyncpg.Connection, account: str) -> None:
    """nuke_user.py / purge_buckets.py delete live rows out of band. The cascade catches them --
    this is the case an application-side funnel could never cover."""
    bucket = await make_bucket(pg_tx, account)
    await put_object(pg_tx, bucket, "a", 4 * GB)
    await put_object(pg_tx, bucket, "b", 1 * GB)

    await pg_tx.execute("DELETE FROM objects WHERE bucket_id = $1 AND object_key = 'a'", bucket)

    await assert_consistent(pg_tx, bucket, 1 * GB)


async def test_bulk_deleting_a_whole_bucket_out_of_band_zeroes_it(pg_tx: asyncpg.Connection, account: str) -> None:
    bucket = await make_bucket(pg_tx, account)
    for i in range(20):
        await put_object(pg_tx, bucket, f"key-{i}", GB)

    await pg_tx.execute("DELETE FROM objects WHERE bucket_id = $1", bucket)

    await assert_consistent(pg_tx, bucket, 0)
    assert (await rollup(pg_tx, bucket))[1] == 0


async def test_the_counter_never_goes_negative(pg_tx: asyncpg.Connection, account: str) -> None:
    """A double decrement must floor at 0 on the UPDATE arm."""
    bucket = await make_bucket(pg_tx, account)
    await put_object(pg_tx, bucket, "a", GB)

    await pg_tx.execute("SELECT usage_apply($1, $2, $3)", bucket, -(99 * GB), -50)

    counter, objects = await rollup(pg_tx, bucket)
    assert counter == 0
    assert objects == 0


async def test_a_negative_delta_on_a_bucket_with_no_row_yet_cannot_insert_a_negative(
    pg_tx: asyncpg.Connection, account: str
) -> None:
    """The INSERT arm needs the same clamp as the UPDATE arm, and only this case reaches it.

    The triggers are created before the backfill runs, so on deploy every pre-existing bucket has no
    rollup row. The first delete or overwrite there is a negative delta with nothing to conflict
    against. An unclamped INSERT would store it, and because the account total SUMs across buckets
    that one negative row would subtract from the account's usage and inflate its headroom.
    """
    bucket = await make_bucket(pg_tx, account)
    assert await rollup(pg_tx, bucket) == (0, 0), "no row yet"

    await pg_tx.execute("SELECT usage_apply($1, $2, $3)", bucket, -(5 * GB), -1)

    counter, objects = await rollup(pg_tx, bucket)
    assert counter == 0
    assert objects == 0

    row = await pg_tx.fetchrow(
        "SELECT COALESCE(SUM(bytes_used), 0)::bigint AS bytes FROM bucket_storage_usage WHERE main_account_id = $1",
        account,
    )
    assert int(row["bytes"]) >= 0, "a negative bucket row would inflate the account's headroom"


async def test_decrements_still_work_after_the_insert_arm_is_clamped(
    pg_tx: asyncpg.Connection, account: str
) -> None:
    """Guards the obvious wrong fix.

    Clamping the INSERT arm's SELECT and leaving DO UPDATE on EXCLUDED.* would make EXCLUDED carry
    the clamped value, flooring every decrement at 0 -- the counter could then only ever grow, which
    is worse than the bug being fixed. The DO UPDATE arm adds the raw parameters for this reason.
    """
    bucket = await make_bucket(pg_tx, account)
    await put_object(pg_tx, bucket, "a", 10 * GB)
    assert (await rollup(pg_tx, bucket))[0] == 10 * GB

    await pg_tx.execute("SELECT usage_apply($1, $2, $3)", bucket, -(4 * GB), 0)

    assert (await rollup(pg_tx, bucket))[0] == 6 * GB, "a decrement on an existing row must subtract"


# --------------------------------------------------------------------------- account totals + repair


async def test_the_account_total_sums_across_buckets(pg_tx: asyncpg.Connection, account: str) -> None:
    b1 = await make_bucket(pg_tx, account)
    b2 = await make_bucket(pg_tx, account)
    await put_object(pg_tx, b1, "a", 2 * GB)
    await put_object(pg_tx, b2, "b", 3 * GB)

    row = await pg_tx.fetchrow(
        "SELECT COALESCE(SUM(u.bytes_used),0)::bigint AS bytes FROM bucket_storage_usage u "
        "JOIN buckets b ON b.bucket_id = u.bucket_id AND b.deleted_at IS NULL "
        "WHERE u.main_account_id = $1",
        account,
    )
    assert int(row["bytes"]) == 5 * GB


async def test_a_soft_deleted_bucket_drops_out_of_the_account_total(pg_tx: asyncpg.Connection, account: str) -> None:
    b1 = await make_bucket(pg_tx, account)
    b2 = await make_bucket(pg_tx, account)
    await put_object(pg_tx, b1, "a", 2 * GB)
    await put_object(pg_tx, b2, "b", 3 * GB)

    await pg_tx.execute("UPDATE buckets SET deleted_at = now() WHERE bucket_id = $1", b2)

    row = await pg_tx.fetchrow(
        "SELECT COALESCE(SUM(u.bytes_used),0)::bigint AS bytes FROM bucket_storage_usage u "
        "JOIN buckets b ON b.bucket_id = u.bucket_id AND b.deleted_at IS NULL "
        "WHERE u.main_account_id = $1",
        account,
    )
    assert int(row["bytes"]) == 2 * GB


async def test_recompute_restores_a_deliberately_corrupted_counter(pg_tx: asyncpg.Connection, account: str) -> None:
    """The property that makes the whole design safe: the counter is a cache of a computable truth,
    so drift is always repairable and never permanent corruption."""
    bucket = await make_bucket(pg_tx, account)
    await put_object(pg_tx, bucket, "a", 7 * GB)

    await pg_tx.execute("UPDATE bucket_storage_usage SET bytes_used = 999999 WHERE bucket_id = $1", bucket)

    from hippius_s3.utils import get_query

    row = await pg_tx.fetchrow(get_query("recompute_bucket_storage_usage"), bucket)

    assert int(row["bytes_used"]) == 7 * GB
    assert int(row["previous_bytes_used"]) == 999999, "drift must be reported, not silently repaired"
    await assert_consistent(pg_tx, bucket, 7 * GB)


async def test_recompute_is_idempotent(pg_tx: asyncpg.Connection, account: str) -> None:
    """The backfill SETs rather than ADDs, so re-running it cannot inflate anything."""
    from hippius_s3.utils import get_query

    bucket = await make_bucket(pg_tx, account)
    await put_object(pg_tx, bucket, "a", 7 * GB)

    for _ in range(3):
        row = await pg_tx.fetchrow(get_query("recompute_bucket_storage_usage"), bucket)
        assert int(row["bytes_used"]) == 7 * GB


async def test_recompute_agrees_with_the_triggers_on_a_messy_bucket(pg_tx: asyncpg.Connection, account: str) -> None:
    """Everything at once: overwrites, appends, deletes, markers, a superseded version and a
    revival. Whatever the triggers computed must equal what a from-scratch recompute computes."""
    from hippius_s3.utils import get_query

    bucket = await make_bucket(pg_tx, account)

    await put_object(pg_tx, bucket, "keep", 3 * GB)
    await put_object(pg_tx, bucket, "overwritten", 10 * GB)
    await put_object(pg_tx, bucket, "overwritten", 2 * GB)
    oid, ver = await put_object(pg_tx, bucket, "appended", 1 * GB)
    await pg_tx.execute(
        "UPDATE object_versions SET size_bytes = size_bytes + $1 WHERE object_id = $2 AND object_version = $3",
        GB,
        oid,
        ver,
    )
    await put_object(pg_tx, bucket, "deleted", 5 * GB)
    await pg_tx.execute("UPDATE objects SET deleted_at = now() WHERE bucket_id = $1 AND object_key = 'deleted'", bucket)
    await put_object(pg_tx, bucket, "revived", 4 * GB)
    await pg_tx.execute("UPDATE objects SET deleted_at = now() WHERE bucket_id = $1 AND object_key = 'revived'", bucket)
    await put_object(pg_tx, bucket, "revived", 6 * GB)

    from_triggers, _ = await rollup(pg_tx, bucket)
    row = await pg_tx.fetchrow(get_query("recompute_bucket_storage_usage"), bucket)

    assert from_triggers == int(row["bytes_used"])
    assert int(row["previous_bytes_used"]) == from_triggers, "the triggers must have zero drift"
    # 3 keep + 2 overwritten + 2 appended + 6 revived
    assert from_triggers == 13 * GB
