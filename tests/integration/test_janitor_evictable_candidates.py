"""Truth table for the janitor's evictable-candidate prefilter, against real Postgres.

`janitor_evictable_candidates.sql` is `find_underreplicated_live_chunks` with its coverage
predicate FLIPPED: it returns the FULLY-replicated, aged, cache-resident parts the SQL
discovery phase may consider for eviction (the worker still re-runs the authoritative
per-part gate before deleting). Getting the flip wrong is a data-loss risk in the making —
an under-replicated part must never surface here — so the required-set / coverage /
expected-chunks / age / keyset semantics are pinned against real Postgres.

Seed data is written via the `pg_tx` fixture (always rolled back); the tables are the real
migrated schema, so the EXPLAIN test can assert the production indexes are used. Each test
wipes `fs_cache_inventory` inside its transaction first: because every candidate must be
present in inventory, that clean slate makes the returned set exactly the rows the test
seeded, so assertions can be equality rather than membership.
"""

from __future__ import annotations

import datetime
import json
import uuid
from typing import Any

import asyncpg
import pytest

from hippius_s3.utils import get_query


pytestmark = pytest.mark.asyncio

_UTC = datetime.timezone.utc
_DEFAULT_UPLOAD = ["arion"]  # config.upload_backends fallback for legacy (NULL) rows
_MAX_AGE = 900  # age gate window used by the tests
_AGED = 3600  # parts.uploaded_at this far in the past => past the age gate
_YOUNG = 0  # uploaded just now => inside the age gate
# Keyset start sentinel: strictly below every real row (cursor columns are NOT NULL, so the
# caller must never pass NULL — a NULL element would make the row comparison NULL and drop all).
_CURSOR_START = (datetime.datetime(1970, 1, 1, tzinfo=_UTC), "", 0, 0)


async def _seed_bucket(conn: asyncpg.Connection) -> uuid.UUID:
    acct = f"5HDTEST{uuid.uuid4().hex[:12]}"
    await conn.execute("INSERT INTO users(main_account_id) VALUES($1) ON CONFLICT DO NOTHING", acct)
    bucket_id = uuid.uuid4()
    await conn.execute(
        "INSERT INTO buckets(bucket_id, bucket_name, created_at, main_account_id) VALUES($1, $2, now(), $3)",
        bucket_id,
        f"evict-test-{bucket_id}",
        acct,
    )
    return bucket_id


async def _seed_candidate(
    conn: asyncpg.Connection,
    bucket_id: uuid.UUID,
    *,
    per_chunk_live: list[list[str]],
    per_chunk_deleted: list[list[str]] | None = None,
    expected_chunks: int | None = None,
    version_type: str = "user",
    upload_backends: list[str] | None = None,
    uploaded_age_secs: int = _AGED,
    in_inventory: bool = True,
    cached_at: datetime.datetime | None = None,
) -> tuple[str, int, int]:
    """Seed one object -> version -> part -> chunks -> chunk_backend rows (+ optional inventory row).

    per_chunk_live[i] is the list of live (deleted=false) backends for chunk i; its length is
    the number of part_chunks created. expected_chunks overrides the size/chunk_size-derived
    expected count (default: the number of chunks) to exercise the mid-materialisation guard.
    Returns the (object_id, object_version, part_number) inventory key.
    """
    n_chunks = len(per_chunk_live)
    expected = expected_chunks if expected_chunks is not None else n_chunks
    per_chunk_deleted = per_chunk_deleted or [[] for _ in per_chunk_live]

    oid = uuid.uuid4()
    key = f"evict-test-{oid}"
    await conn.execute(
        "INSERT INTO objects(object_id, bucket_id, object_key, created_at, current_object_version, deleted_at)"
        " VALUES($1, $2, $3, now(), 1, NULL)",
        oid,
        bucket_id,
        key,
    )
    await conn.execute(
        "INSERT INTO object_versions(object_id, object_version, storage_version, size_bytes, content_type,"
        " version_type, upload_backends)"
        " VALUES($1, 1, 5, 100, 'application/octet-stream', $2::version_type, $3)",
        oid,
        version_type,
        upload_backends,
    )
    upload_id = uuid.uuid4()
    await conn.execute(
        "INSERT INTO multipart_uploads(upload_id, bucket_id, object_key, initiated_at) VALUES($1, $2, $3, now())",
        upload_id,
        bucket_id,
        key,
    )
    part_id = uuid.uuid4()
    # size_bytes / chunk_size_bytes chosen so CEIL(size/chunk) == expected (100 B chunks).
    await conn.execute(
        "INSERT INTO parts(part_id, upload_id, part_number, size_bytes, chunk_size_bytes, etag, uploaded_at,"
        " object_id, object_version)"
        " VALUES($1, $2, 1, $3, 100, 'etag', now() - make_interval(secs => $4), $5, 1)",
        part_id,
        upload_id,
        max(expected, 0) * 100,
        uploaded_age_secs,
        oid,
    )
    for i, live in enumerate(per_chunk_live):
        chunk_pk = await conn.fetchval(
            "INSERT INTO part_chunks(part_id, chunk_index, cipher_size_bytes) VALUES($1, $2, 100) RETURNING id",
            part_id,
            i,
        )
        for backend in live:
            await conn.execute(
                "INSERT INTO chunk_backend(chunk_id, backend, deleted) VALUES($1, $2, false)", chunk_pk, backend
            )
        for backend in per_chunk_deleted[i]:
            await conn.execute(
                "INSERT INTO chunk_backend(chunk_id, backend, deleted) VALUES($1, $2, true)", chunk_pk, backend
            )
    if in_inventory:
        await conn.execute(
            "INSERT INTO fs_cache_inventory(object_id, object_version, part_number, cached_at)"
            " VALUES($1, 1, 1, COALESCE($2, now()))",
            str(oid),
            cached_at,
        )
    return str(oid), 1, 1


async def _candidates(
    conn: asyncpg.Connection,
    *,
    backup: list[str],
    default_upload: list[str] | None = None,
    limit: int = 1000,
    max_age: int = _MAX_AGE,
    ignore_age: bool = False,
    cursor: tuple[datetime.datetime, str, int, int] = _CURSOR_START,
) -> list[asyncpg.Record]:
    return await conn.fetch(
        get_query("janitor_evictable_candidates"),
        backup,
        default_upload if default_upload is not None else _DEFAULT_UPLOAD,
        limit,
        max_age,
        ignore_age,
        cursor[0],
        cursor[1],
        cursor[2],
        cursor[3],
    )


def _keys(rows: list[asyncpg.Record]) -> set[tuple[str, int, int]]:
    return {(r["object_id"], r["object_version"], r["part_number"]) for r in rows}


async def _fresh_inventory(conn: asyncpg.Connection) -> None:
    await conn.execute("DELETE FROM fs_cache_inventory")


# ===================================================== returned (safe to evict)


async def test_fully_replicated_aged_in_inventory_is_returned(pg_tx: asyncpg.Connection) -> None:
    await _fresh_inventory(pg_tx)
    bucket = await _seed_bucket(pg_tx)
    cand = await _seed_candidate(pg_tx, bucket, per_chunk_live=[["arion"], ["arion"]], upload_backends=["arion"])
    assert _keys(await _candidates(pg_tx, backup=[])) == {cand}


# ===================================================== NOT returned: coverage gaps


async def test_one_chunk_missing_one_required_backend_is_not_returned(pg_tx: asyncpg.Connection) -> None:
    await _fresh_inventory(pg_tx)
    bucket = await _seed_bucket(pg_tx)
    # chunk 0 covered, chunk 1 has no arion row -> the part is under-replicated.
    await _seed_candidate(pg_tx, bucket, per_chunk_live=[["arion"], []], upload_backends=["arion"])
    assert _keys(await _candidates(pg_tx, backup=[])) == set()


async def test_zero_chunk_backend_rows_is_not_returned(pg_tx: asyncpg.Connection) -> None:
    await _fresh_inventory(pg_tx)
    bucket = await _seed_bucket(pg_tx)
    # The CopyObject destination population: part + chunk exist, zero chunk_backend rows.
    await _seed_candidate(pg_tx, bucket, per_chunk_live=[[]], upload_backends=["arion"])
    assert _keys(await _candidates(pg_tx, backup=[])) == set()


async def test_soft_deleted_backend_row_does_not_count_as_coverage(pg_tx: asyncpg.Connection) -> None:
    await _fresh_inventory(pg_tx)
    bucket = await _seed_bucket(pg_tx)
    # arion was unpinned (deleted=true) -> no live coverage -> not evictable.
    await _seed_candidate(pg_tx, bucket, per_chunk_live=[[]], per_chunk_deleted=[["arion"]], upload_backends=["arion"])
    assert _keys(await _candidates(pg_tx, backup=[])) == set()


async def test_sibling_parts_are_scored_independently(pg_tx: asyncpg.Connection) -> None:
    # Two parts under ONE object: part 1 fully covered, part 2 missing arion. The coverage anti-join
    # scopes chunks by pc.part_id = p.part_id, so each part is scored on its OWN chunks — part 1 is
    # returned despite its under-replicated sibling, and part 2 is excluded despite its covered sibling.
    # The shared _seed_candidate helper is one-part-per-object, so this shared-object shape is inlined.
    await _fresh_inventory(pg_tx)
    bucket = await _seed_bucket(pg_tx)
    oid = uuid.uuid4()
    key = f"evict-test-{oid}"
    await pg_tx.execute(
        "INSERT INTO objects(object_id, bucket_id, object_key, created_at, current_object_version, deleted_at)"
        " VALUES($1, $2, $3, now(), 1, NULL)",
        oid,
        bucket,
        key,
    )
    await pg_tx.execute(
        "INSERT INTO object_versions(object_id, object_version, storage_version, size_bytes, content_type,"
        " version_type, upload_backends)"
        " VALUES($1, 1, 5, 100, 'application/octet-stream', 'user'::version_type, $2)",
        oid,
        ["arion"],
    )
    upload_id = uuid.uuid4()
    await pg_tx.execute(
        "INSERT INTO multipart_uploads(upload_id, bucket_id, object_key, initiated_at) VALUES($1, $2, $3, now())",
        upload_id,
        bucket,
        key,
    )
    for part_number, live in ((1, ["arion"]), (2, [])):
        part_id = uuid.uuid4()
        await pg_tx.execute(
            "INSERT INTO parts(part_id, upload_id, part_number, size_bytes, chunk_size_bytes, etag, uploaded_at,"
            " object_id, object_version)"
            " VALUES($1, $2, $3, 100, 100, 'etag', now() - make_interval(secs => $4), $5, 1)",
            part_id,
            upload_id,
            part_number,
            _AGED,
            oid,
        )
        chunk_pk = await pg_tx.fetchval(
            "INSERT INTO part_chunks(part_id, chunk_index, cipher_size_bytes) VALUES($1, 0, 100) RETURNING id",
            part_id,
        )
        for backend in live:
            await pg_tx.execute(
                "INSERT INTO chunk_backend(chunk_id, backend, deleted) VALUES($1, $2, false)", chunk_pk, backend
            )
        await pg_tx.execute(
            "INSERT INTO fs_cache_inventory(object_id, object_version, part_number, cached_at) VALUES($1, 1, $2, now())",
            str(oid),
            part_number,
        )

    result = _keys(await _candidates(pg_tx, backup=[]))
    assert (str(oid), 1, 1) in result, "the fully-covered part is evictable"
    assert (str(oid), 1, 2) not in result, "the under-replicated sibling part is not"


# ===================================================== NOT returned: mid-materialisation


async def test_part_chunks_below_expected_is_not_returned(pg_tx: asyncpg.Connection) -> None:
    await _fresh_inventory(pg_tx)
    bucket = await _seed_bucket(pg_tx)
    # size says 3 chunks; only 2 materialised (both covered) -> still filling, keep it.
    await _seed_candidate(
        pg_tx, bucket, per_chunk_live=[["arion"], ["arion"]], expected_chunks=3, upload_backends=["arion"]
    )
    assert _keys(await _candidates(pg_tx, backup=[])) == set()


async def test_zero_part_chunks_is_not_returned(pg_tx: asyncpg.Connection) -> None:
    await _fresh_inventory(pg_tx)
    bucket = await _seed_bucket(pg_tx)
    # No part_chunks rows at all -> the >0 guard excludes it.
    await _seed_candidate(pg_tx, bucket, per_chunk_live=[], expected_chunks=0, upload_backends=["arion"])
    assert _keys(await _candidates(pg_tx, backup=[])) == set()


# ===================================================== NOT returned: not in inventory


async def test_aged_replicated_but_not_in_inventory_is_not_returned(pg_tx: asyncpg.Connection) -> None:
    await _fresh_inventory(pg_tx)
    bucket = await _seed_bucket(pg_tx)
    await _seed_candidate(pg_tx, bucket, per_chunk_live=[["arion"]], upload_backends=["arion"], in_inventory=False)
    assert _keys(await _candidates(pg_tx, backup=[])) == set()


# ===================================================== age gate / pressure override


async def test_young_part_respects_age_gate_unless_pressure(pg_tx: asyncpg.Connection) -> None:
    await _fresh_inventory(pg_tx)
    bucket = await _seed_bucket(pg_tx)
    cand = await _seed_candidate(
        pg_tx, bucket, per_chunk_live=[["arion"]], upload_backends=["arion"], uploaded_age_secs=_YOUNG
    )
    # Membership/absence rather than global equality: the age toggle is orthogonal to which other
    # rows happen to be candidates, and concurrent e2e traffic on the shared DB could commit inventory
    # rows into our READ COMMITTED snapshot between the two fetches. We only assert OUR key's presence.
    assert cand not in _keys(await _candidates(pg_tx, backup=[], ignore_age=False)), "age gate hides young part"
    assert cand in _keys(await _candidates(pg_tx, backup=[], ignore_age=True)), "pressure override includes it"


# ===================================================== per-version required set


async def test_migration_version_requires_only_ipfs(pg_tx: asyncpg.Connection) -> None:
    await _fresh_inventory(pg_tx)
    bucket = await _seed_bucket(pg_tx)
    # ipfs-only coverage is enough for a migration version even though $2 default is ['arion'].
    covered = await _seed_candidate(
        pg_tx, bucket, per_chunk_live=[["ipfs"]], version_type="migration", upload_backends=["arion"]
    )
    # A migration version with only arion (no ipfs) is NOT fully covered.
    await _seed_candidate(
        pg_tx, bucket, per_chunk_live=[["arion"]], version_type="migration", upload_backends=["arion"]
    )
    assert _keys(await _candidates(pg_tx, backup=[], default_upload=["arion"])) == {covered}


async def test_backup_backend_union_gates_until_backup_rows_exist(pg_tx: asyncpg.Connection) -> None:
    await _fresh_inventory(pg_tx)
    bucket = await _seed_bucket(pg_tx)
    upload_only = await _seed_candidate(pg_tx, bucket, per_chunk_live=[["arion"]], upload_backends=["arion"])
    both = await _seed_candidate(pg_tx, bucket, per_chunk_live=[["arion", "ovh"]], upload_backends=["arion"])

    # backup=[] -> arion coverage suffices; both parts qualify.
    assert _keys(await _candidates(pg_tx, backup=[])) == {upload_only, both}
    # backup=['ovh'] -> only the part that actually carries an ovh row qualifies.
    assert _keys(await _candidates(pg_tx, backup=["ovh"])) == {both}


# ===================================================== keyset paging


async def test_keyset_pagination_walks_all_candidates_disjoint_and_ordered(pg_tx: asyncpg.Connection) -> None:
    await _fresh_inventory(pg_tx)
    bucket = await _seed_bucket(pg_tx)
    base = datetime.datetime(2026, 1, 1, tzinfo=_UTC)
    seeded: set[tuple[str, int, int]] = set()
    for i in range(5):
        cand = await _seed_candidate(
            pg_tx,
            bucket,
            per_chunk_live=[["arion"]],
            upload_backends=["arion"],
            cached_at=base + datetime.timedelta(seconds=i),
        )
        seeded.add(cand)

    collected: list[tuple[str, int, int]] = []
    cursor = _CURSOR_START
    pages = 0
    while True:
        rows = await _candidates(pg_tx, backup=[], limit=2, cursor=cursor)
        pages += 1
        if not rows:
            break
        for r in rows:
            collected.append((r["object_id"], r["object_version"], r["part_number"]))
        last = rows[-1]
        cursor = (last["cached_at"], last["object_id"], last["object_version"], last["part_number"])
        if len(rows) < 2:
            break

    # Ordered by cached_at, disjoint (no dupes), and complete.
    ordered_cached_at = []
    for oid, ov, pn in collected:
        ordered_cached_at.append(
            await pg_tx.fetchval(
                "SELECT cached_at FROM fs_cache_inventory WHERE object_id = $1 AND object_version = $2"
                " AND part_number = $3",
                oid,
                ov,
                pn,
            )
        )
    assert ordered_cached_at == sorted(ordered_cached_at), "pages walk oldest-first"
    assert len(collected) == len(set(collected)) == 5, "every candidate seen exactly once"
    assert set(collected) == seeded
    assert pages == 3, "5 rows at LIMIT 2 => pages of 2, 2, 1 (the short last page terminates)"


async def test_keyset_short_final_page_terminates(pg_tx: asyncpg.Connection) -> None:
    await _fresh_inventory(pg_tx)
    bucket = await _seed_bucket(pg_tx)
    base = datetime.datetime(2026, 2, 1, tzinfo=_UTC)
    for i in range(2):
        await _seed_candidate(
            pg_tx,
            bucket,
            per_chunk_live=[["arion"]],
            upload_backends=["arion"],
            cached_at=base + datetime.timedelta(seconds=i),
        )
    page1 = await _candidates(pg_tx, backup=[], limit=5)
    assert len(page1) == 2  # short page (< limit) => end of ring, no second page needed
    last = page1[-1]
    cursor = (last["cached_at"], last["object_id"], last["object_version"], last["part_number"])
    assert await _candidates(pg_tx, backup=[], limit=5, cursor=cursor) == []


# ===================================================== plan shape (index usage)


def _walk_plan(node: dict[str, Any]) -> list[dict[str, Any]]:
    nodes = [node]
    for child in node.get("Plans", []):
        nodes.extend(_walk_plan(child))
    return nodes


async def test_explain_uses_inventory_index_and_keeps_parts_indexed(pg_tx: asyncpg.Connection) -> None:
    await _fresh_inventory(pg_tx)
    bucket = await _seed_bucket(pg_tx)
    for _ in range(3):
        await _seed_candidate(pg_tx, bucket, per_chunk_live=[["arion"]], upload_backends=["arion"])

    # The test DB is tiny, so without this the planner would seq-scan everything regardless of
    # index shape. Forcing seqscan off proves the query CAN be served by indexes: fs_cache_inventory
    # via its cached_at index, and parts via idx_parts_object_id — the latter only reachable because
    # the ::uuid cast is on the inventory side, not on parts.object_id.
    await pg_tx.execute("SET LOCAL enable_seqscan = off")
    rows = await pg_tx.fetch(
        "EXPLAIN (FORMAT JSON) " + get_query("janitor_evictable_candidates"),
        [],
        _DEFAULT_UPLOAD,
        1000,
        _MAX_AGE,
        False,
        _CURSOR_START[0],
        _CURSOR_START[1],
        _CURSOR_START[2],
        _CURSOR_START[3],
    )
    raw = rows[0][0]
    plan = json.loads(raw) if isinstance(raw, str) else raw
    nodes = _walk_plan(plan[0]["Plan"])

    seq_scans = {n.get("Relation Name") for n in nodes if n.get("Node Type") == "Seq Scan"}
    assert "fs_cache_inventory" not in seq_scans, f"inventory seq-scanned: {nodes}"
    assert "parts" not in seq_scans, f"parts seq-scanned (cast defeated its index): {nodes}"

    # fs_cache_inventory is reached through its cached_at index (a Bitmap Index Scan carries the
    # Index Name; its parent Bitmap Heap Scan carries the Relation Name — either node form counts).
    used_indexes = {n.get("Index Name") for n in nodes if n.get("Index Name")}
    assert "fs_cache_inventory_cached_at" in used_indexes, f"cached_at index not used: {nodes}"
