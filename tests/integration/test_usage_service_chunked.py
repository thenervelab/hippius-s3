"""The chunked storage count must equal the single-statement one, byte for byte.

usage_service walks each bucket in keyset pages instead of running one aggregate, because the
single statement cannot finish for a 7.83M-object bucket inside either 30s ceiling (our asyncpg
timeout, and the replica's max_standby_streaming_delay). Chunking is a performance change ONLY --
if it also changes the number, it changes what customers are billed.

So every case here asserts the chunked walk against get_account_storage_bytes.sql, the canonical
definition, on identical seeded data. Page sizes of 1 and 2 force multi-page walks over tiny
fixtures, which is what exercises the cursor arithmetic that a real 500k page size never would.

Seeded via `pg_tx` (auto-rolled-back). Skips if no Postgres is reachable on DATABASE_URL.
"""

import uuid

import asyncpg
import pytest

from hippius_s3.services import usage_service
from hippius_s3.utils import get_query


async def _seed_account(conn: asyncpg.Connection) -> str:
    acct = f"5USAGE{uuid.uuid4().hex[:16]}"
    await conn.execute("INSERT INTO users(main_account_id) VALUES($1) ON CONFLICT DO NOTHING", acct)
    return acct


async def _seed_bucket(conn: asyncpg.Connection, acct: str, *, deleted: bool = False) -> uuid.UUID:
    bucket_id = uuid.uuid4()
    await conn.execute(
        "INSERT INTO buckets(bucket_id, bucket_name, created_at, main_account_id, deleted_at)"
        " VALUES($1, $2, now(), $3, CASE WHEN $4 THEN now() ELSE NULL END)",
        bucket_id,
        f"usage-test-{bucket_id}",
        acct,
        deleted,
    )
    return bucket_id


async def _seed_object(
    conn: asyncpg.Connection,
    bucket_id: uuid.UUID,
    key: str,
    size: int,
    *,
    object_deleted: bool = False,
    version_deleted: bool = False,
    delete_marker: bool = False,
    superseded_size: int | None = None,
) -> uuid.UUID:
    """One object. `superseded_size` adds an older v1 that must NOT be counted (current is v2)."""
    oid = uuid.uuid4()
    current = 2 if superseded_size is not None else 1

    await conn.execute(
        "INSERT INTO objects(object_id, bucket_id, object_key, created_at, current_object_version, deleted_at)"
        " VALUES($1, $2, $3, now(), $4, CASE WHEN $5 THEN now() ELSE NULL END)",
        oid,
        bucket_id,
        key,
        current,
        object_deleted,
    )
    if superseded_size is not None:
        await conn.execute(
            "INSERT INTO object_versions(object_id, object_version, storage_version, size_bytes, content_type)"
            " VALUES($1, 1, 5, $2, 'application/octet-stream')",
            oid,
            superseded_size,
        )
    await conn.execute(
        "INSERT INTO object_versions"
        "(object_id, object_version, storage_version, size_bytes, content_type, deleted_at, is_delete_marker)"
        " VALUES($1, $2, 5, $3, 'application/octet-stream',"
        "        CASE WHEN $4 THEN now() ELSE NULL END, $5)",
        oid,
        current,
        size,
        version_deleted,
        delete_marker,
    )
    return oid


async def _canonical(conn: asyncpg.Connection, acct: str) -> int:
    row = await conn.fetchrow(get_query("get_account_storage_bytes"), acct)
    return int(row["bytes_used"]) if row else 0


async def _chunked(conn: asyncpg.Connection, acct: str, page_size: int) -> int:
    return await usage_service.get_account_storage_bytes(conn, acct, timeout=30.0, page_size=page_size)


async def _assert_agrees(conn: asyncpg.Connection, acct: str, expected: int) -> None:
    canonical = await _canonical(conn, acct)
    assert canonical == expected, "the fixture itself is wrong -- canonical query disagrees"
    for page_size in (1, 2, 3, 1000):
        got = await _chunked(conn, acct, page_size)
        assert got == canonical, f"chunked walk disagreed at page_size={page_size}"


@pytest.mark.asyncio
async def test_an_account_with_nothing_counts_zero(pg_tx: asyncpg.Connection) -> None:
    acct = await _seed_account(pg_tx)
    await _assert_agrees(pg_tx, acct, 0)


@pytest.mark.asyncio
async def test_live_objects_across_several_buckets(pg_tx: asyncpg.Connection) -> None:
    acct = await _seed_account(pg_tx)
    b1 = await _seed_bucket(pg_tx, acct)
    b2 = await _seed_bucket(pg_tx, acct)

    for i in range(5):
        await _seed_object(pg_tx, b1, f"a-{i:03d}", 100)
    for i in range(3):
        await _seed_object(pg_tx, b2, f"b-{i:03d}", 1000)

    await _assert_agrees(pg_tx, acct, 5 * 100 + 3 * 1000)


@pytest.mark.asyncio
async def test_the_four_kinds_of_excluded_row(pg_tx: asyncpg.Connection) -> None:
    """Each exclusion is a separate WHERE clause in the canonical query, and each one is a way the
    chunked walk could silently over-count if its page query drifted."""
    acct = await _seed_account(pg_tx)
    bucket = await _seed_bucket(pg_tx, acct)

    await _seed_object(pg_tx, bucket, "live", 700)
    await _seed_object(pg_tx, bucket, "soft-deleted-object", 1, object_deleted=True)
    await _seed_object(pg_tx, bucket, "soft-deleted-version", 2, version_deleted=True)
    await _seed_object(pg_tx, bucket, "delete-marker", 4, delete_marker=True)

    dead_bucket = await _seed_bucket(pg_tx, acct, deleted=True)
    await _seed_object(pg_tx, dead_bucket, "in-a-deleted-bucket", 8)

    await _assert_agrees(pg_tx, acct, 700)


@pytest.mark.asyncio
async def test_only_the_current_version_is_counted(pg_tx: asyncpg.Connection) -> None:
    """An overwrite leaves the old version row in place. Counting both would bill the customer for
    storage the console does not show them."""
    acct = await _seed_account(pg_tx)
    bucket = await _seed_bucket(pg_tx, acct)

    await _seed_object(pg_tx, bucket, "overwritten", 50, superseded_size=9999)

    await _assert_agrees(pg_tx, acct, 50)


@pytest.mark.asyncio
async def test_another_accounts_buckets_are_never_counted(pg_tx: asyncpg.Connection) -> None:
    acct = await _seed_account(pg_tx)
    other = await _seed_account(pg_tx)
    mine = await _seed_bucket(pg_tx, acct)
    theirs = await _seed_bucket(pg_tx, other)

    await _seed_object(pg_tx, mine, "mine", 11)
    await _seed_object(pg_tx, theirs, "theirs", 22222)

    await _assert_agrees(pg_tx, acct, 11)


@pytest.mark.asyncio
async def test_a_page_boundary_landing_exactly_on_the_last_object(pg_tx: asyncpg.Connection) -> None:
    """A full final page is indistinguishable from "there may be more", so the walk probes once more.
    Off-by-one here drops a bucket's tail, and page_size=2 over 4 objects is the tightest version of
    that case."""
    acct = await _seed_account(pg_tx)
    bucket = await _seed_bucket(pg_tx, acct)
    for i in range(4):
        await _seed_object(pg_tx, bucket, f"k-{i:03d}", 25)

    assert await _chunked(pg_tx, acct, 2) == 100
    assert await _chunked(pg_tx, acct, 4) == 100
    await _assert_agrees(pg_tx, acct, 100)


@pytest.mark.asyncio
async def test_a_whole_page_of_zero_byte_rows_does_not_end_the_walk(pg_tx: asyncpg.Connection) -> None:
    """Delete markers and soft-deleted versions occupy PAGE rows but contribute no bytes. If the walk
    stopped on a zero-sum page it would under-count everything ordered after them -- which is why
    rows_seen counts page rows rather than joined rows."""
    acct = await _seed_account(pg_tx)
    bucket = await _seed_bucket(pg_tx, acct)

    # Keys chosen so the excluded rows sort FIRST and fill the opening pages entirely.
    for i in range(4):
        await _seed_object(pg_tx, bucket, f"aaa-{i:03d}", 5, delete_marker=True)
    await _seed_object(pg_tx, bucket, "zzz-real", 640)

    assert await _chunked(pg_tx, acct, 2) == 640
    await _assert_agrees(pg_tx, acct, 640)


@pytest.mark.asyncio
async def test_keys_that_stress_the_keyset_ordering(pg_tx: asyncpg.Connection) -> None:
    """The cursor is `object_key > $2` under the column's collation, and ORDER BY uses the same one,
    so they agree by construction -- but keys with punctuation, spaces, unicode and shared prefixes
    are where a hand-rolled cursor would go wrong. Page size 1 forces one round trip per key."""
    acct = await _seed_account(pg_tx)
    bucket = await _seed_bucket(pg_tx, acct)

    keys = ["a", "a/b", "a/b/c", "a b", "a-b", "a_b", "a.b", "Z", "z", "0", "~", "é", "日本", "a" * 200]
    for key in keys:
        await _seed_object(pg_tx, bucket, key, 3)

    await _assert_agrees(pg_tx, acct, 3 * len(keys))
    assert await _chunked(pg_tx, acct, 1) == 3 * len(keys), "one key per page must visit every key once"


@pytest.mark.asyncio
async def test_sizes_that_exceed_a_32_bit_int(pg_tx: asyncpg.Connection) -> None:
    """Quotas are in TiB. Summing in Python across pages must not lose precision or overflow where
    the single-statement bigint SUM would not."""
    acct = await _seed_account(pg_tx)
    bucket = await _seed_bucket(pg_tx, acct)

    big = 5 * (1 << 40)  # 5 TiB
    for i in range(3):
        await _seed_object(pg_tx, bucket, f"big-{i}", big)

    await _assert_agrees(pg_tx, acct, 3 * big)


@pytest.mark.asyncio
async def test_the_page_predicate_can_be_served_by_a_bucket_key_index(pg_tx: asyncpg.Connection) -> None:
    """Bounded per-statement cost depends on the page predicate matching an index on
    (bucket_id, object_key) WHERE deleted_at IS NULL. On a big table it must never seq-scan.

    Asserted with seqscan disabled rather than by reading the default plan: a test database holds a
    handful of rows, where a Seq Scan is genuinely the cheaper plan and the planner is right to pick
    it. What is stable across table sizes -- and what actually breaks if someone reorders the
    predicate or drops the partial index -- is whether an index path EXISTS at all.
    """
    acct = await _seed_account(pg_tx)
    bucket = await _seed_bucket(pg_tx, acct)
    await _seed_object(pg_tx, bucket, "k", 1)

    await pg_tx.execute("SET LOCAL enable_seqscan = off")
    plan = "\n".join(
        r["QUERY PLAN"]
        for r in await pg_tx.fetch(
            "EXPLAIN " + get_query("get_bucket_storage_bytes_page"),
            bucket,
            "",
            500,
        )
    )

    assert "idx_objects_bucket_prefix" in plan, plan
    assert "object_versions_pkey" in plan, "the version lookup must stay a PK probe, not a join\n" + plan
