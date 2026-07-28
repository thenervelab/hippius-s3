"""Truth table for the janitor's evictable-candidate FILTER, against real Postgres.

`janitor_evictable_candidates.sql` is `find_underreplicated_live_chunks` with its coverage
predicate FLIPPED: it returns the FULLY-replicated, aged parts that are safe to evict. Since the
slice-then-filter split, it no longer scans fs_cache_inventory — it filters a GIVEN set of inventory
tuples (the page produced by `janitor_inventory_slice.sql`, whose own keyset walk is covered in
test_janitor_inventory_slice.py) passed in as three parallel arrays. Getting the flip wrong is a
data-loss risk in the making — an under-replicated part must never surface here — so the required-set
/ coverage / expected-chunks / age semantics are pinned against real Postgres.

Seed data is written via the `pg_tx` fixture (always rolled back); the tables are the real
migrated schema, so the EXPLAIN test can assert the parts index survives the ::uuid cast. Each test
wipes `fs_cache_inventory` inside its transaction first: the `_candidates` helper builds the filter's
tuple arrays from whatever inventory is resident, so a clean slate makes the input set exactly the
rows the test seeded and the returned set an equality (not membership) assertion.
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
# Keyset start sentinel for the slice read the helper does before filtering (strictly below every
# real row; cursor columns are NOT NULL, so a NULL element would drop every row).
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


async def _resident_tuples(conn: asyncpg.Connection) -> tuple[list[str], list[int], list[int]]:
    """Read every resident inventory tuple (the slice the worker would feed the filter). Tests wipe
    inventory first, so this is exactly the seeded set — as three parallel arrays for the filter."""
    rows = await conn.fetch(
        get_query("janitor_inventory_slice"),
        100000,
        _CURSOR_START[0],
        _CURSOR_START[1],
        _CURSOR_START[2],
        _CURSOR_START[3],
    )
    return (
        [r["object_id"] for r in rows],
        [r["object_version"] for r in rows],
        [r["part_number"] for r in rows],
    )


async def _candidates(
    conn: asyncpg.Connection,
    *,
    backup: list[str],
    default_upload: list[str] | None = None,
    max_age: int = _MAX_AGE,
    ignore_age: bool = False,
) -> list[asyncpg.Record]:
    object_ids, versions, part_numbers = await _resident_tuples(conn)
    return await conn.fetch(
        get_query("janitor_evictable_candidates"),
        object_ids,
        versions,
        part_numbers,
        backup,
        default_upload if default_upload is not None else _DEFAULT_UPLOAD,
        max_age,
        ignore_age,
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


# ===================================================== plan shape (index usage)


def _walk_plan(node: dict[str, Any]) -> list[dict[str, Any]]:
    nodes = [node]
    for child in node.get("Plans", []):
        nodes.extend(_walk_plan(child))
    return nodes


async def test_explain_keeps_parts_indexed_off_the_slice_arrays(pg_tx: asyncpg.Connection) -> None:
    await _fresh_inventory(pg_tx)
    bucket = await _seed_bucket(pg_tx)
    for _ in range(3):
        await _seed_candidate(pg_tx, bucket, per_chunk_live=[["arion"]], upload_backends=["arion"])
    object_ids, versions, part_numbers = await _resident_tuples(pg_tx)

    # The filter only re-touches fs_cache_inventory via a PK LEFT JOIN for last_access_at (index-only);
    # the load-bearing property is that the join into parts still drives off an object_id-leading index
    # rather than a seq scan. The test DB is tiny, so force seqscan off to prove the query CAN be served by an index
    # — reachable only because the ::uuid cast is on the slice side (s.oid::uuid), keeping
    # parts.object_id a bare indexed column. Casting parts.object_id::text would defeat every parts
    # index and force a seq scan.
    await pg_tx.execute("SET LOCAL enable_seqscan = off")
    rows = await pg_tx.fetch(
        "EXPLAIN (FORMAT JSON) " + get_query("janitor_evictable_candidates"),
        object_ids,
        versions,
        part_numbers,
        [],
        _DEFAULT_UPLOAD,
        _MAX_AGE,
        False,
    )
    raw = rows[0][0]
    plan = json.loads(raw) if isinstance(raw, str) else raw
    nodes = _walk_plan(plan[0]["Plan"])

    seq_scans = {n.get("Relation Name") for n in nodes if n.get("Node Type") == "Seq Scan"}
    assert "parts" not in seq_scans, f"parts seq-scanned (cast defeated its index): {nodes}"

    # parts is reached by an Index Scan whose Index Cond carries the slice-side cast — proof the cast
    # direction kept parts.object_id indexable (the planner picks whichever object_id-leading index,
    # e.g. idx_parts_object_id / idx_parts_object_version; the name is not load-bearing, the cast is).
    parts_scans = [n for n in nodes if n.get("Relation Name") == "parts"]
    assert parts_scans, f"parts not scanned at all: {nodes}"
    assert all(n.get("Node Type") == "Index Scan" for n in parts_scans), f"parts not index-scanned: {nodes}"
    # Deliberately NOT asserting the cast text inside Index Cond: its rendering is plan-state
    # dependent (alias/CTE-inlining variations flake the string match). A wrong cast direction
    # (parts-side ::text) cannot produce an Index Scan on parts at all — the two assertions above
    # are the real regression guard.
