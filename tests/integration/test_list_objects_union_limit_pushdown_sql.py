"""The per-arm LIMIT in the listing queries must not change a single returned row.

`list_objects.sql` and `list_object_versions.sql` used to union `objects` with `object_names` and
apply one ORDER BY/LIMIT above the union. An outer limit above an Append cannot stop either arm
early, so every page materialised the bucket's whole remaining key range. Both queries now limit
each arm separately and merge the two already-limited streams.

That rewrite is only sound because each arm applies EVERY row-eliminating predicate before its own
LIMIT. Limiting the key sources first — the obvious version of the same idea — silently drops keys:
if the objects arm's first N keys include one hidden by a delete marker, the merged page comes back
short and the endpoint reads a short page as "the bucket has no more keys". `test_unsafe_variant_*`
below is that mutant, pinned as failing.

The oracle is the pre-change SQL, kept verbatim: deliberately naive, obviously correct, and slow.
"""

from __future__ import annotations

import datetime
import os
import uuid
from typing import Any
from typing import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio

from hippius_s3.utils import get_query


_DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius?sslmode=disable")

# The shipped query before the per-arm limit landed. Kept as a semantic oracle, not as a suggestion.
REFERENCE_LIST_OBJECTS = """
SELECT o.object_id, o.object_key, ov.size_bytes, ov.content_type, o.created_at, ov.md5_hash,
       ov.status, ov.multipart, ov.body_blake3
FROM (
    SELECT o.object_id, o.object_key, o.created_at, o.current_object_version, o.bucket_id
    FROM objects o
    WHERE o.bucket_id = $1
      AND o.deleted_at IS NULL
      AND ($2::text IS NULL OR o.object_key LIKE $2::text || '%')
      AND ($3::text IS NULL OR o.object_key >= $3::text)
      AND ($5::text IS NULL OR o.object_key < $5::text COLLATE "C")
    UNION ALL
    SELECT o.object_id, n.object_key, n.created_at, o.current_object_version, n.bucket_id
    FROM object_names n
    JOIN objects o ON o.object_id = n.object_id AND o.deleted_at IS NULL
    WHERE n.bucket_id = $1
      AND ($2::text IS NULL OR n.object_key LIKE $2::text || '%')
      AND ($3::text IS NULL OR n.object_key >= $3::text)
      AND ($5::text IS NULL OR n.object_key < $5::text COLLATE "C")
) o,
     LATERAL (
         SELECT v.size_bytes, v.content_type, v.md5_hash, v.status, v.multipart, v.is_delete_marker,
                v.body_blake3
         FROM object_versions v
         WHERE v.object_id = o.object_id
           AND v.object_version <= o.current_object_version
           AND v.deleted_at IS NULL
           AND (v.is_delete_marker OR v.size_bytes > 0 OR (v.md5_hash IS NOT NULL AND v.md5_hash != ''))
         ORDER BY v.object_version DESC
         LIMIT 1
     ) ov
WHERE o.bucket_id = $1
  AND ($2::text IS NULL OR o.object_key LIKE $2::text || '%')
  AND ($3::text IS NULL OR o.object_key >= $3::text)
  AND ($5::text IS NULL OR o.object_key < $5::text COLLATE "C")
  AND NOT ov.is_delete_marker
ORDER BY o.object_key
LIMIT $4::int
"""

# The tempting-but-wrong rewrite: each arm limits its KEYS, and the version lookup that decides
# whether a key is listed at all runs above the union. Pinned as a mutant, never as an alternative.
UNSAFE_LIST_OBJECTS = """
SELECT o.object_id, o.object_key, ov.size_bytes, ov.content_type, o.created_at, ov.md5_hash,
       ov.status, ov.multipart, ov.body_blake3
FROM (
    (SELECT o.object_id, o.object_key, o.created_at, o.current_object_version
     FROM objects o
     WHERE o.bucket_id = $1 AND o.deleted_at IS NULL
       AND ($2::text IS NULL OR o.object_key LIKE $2::text || '%')
       AND ($3::text IS NULL OR o.object_key >= $3::text)
       AND ($5::text IS NULL OR o.object_key < $5::text COLLATE "C")
     ORDER BY o.object_key LIMIT $4::int)
    UNION ALL
    (SELECT o.object_id, n.object_key, n.created_at, o.current_object_version
     FROM object_names n
     JOIN objects o ON o.object_id = n.object_id AND o.deleted_at IS NULL
     WHERE n.bucket_id = $1
       AND ($2::text IS NULL OR n.object_key LIKE $2::text || '%')
       AND ($3::text IS NULL OR n.object_key >= $3::text)
       AND ($5::text IS NULL OR n.object_key < $5::text COLLATE "C")
     ORDER BY n.object_key LIMIT $4::int)
) o,
     LATERAL (
         SELECT v.size_bytes, v.content_type, v.md5_hash, v.status, v.multipart, v.is_delete_marker,
                v.body_blake3
         FROM object_versions v
         WHERE v.object_id = o.object_id
           AND v.object_version <= o.current_object_version
           AND v.deleted_at IS NULL
           AND (v.is_delete_marker OR v.size_bytes > 0 OR (v.md5_hash IS NOT NULL AND v.md5_hash != ''))
         ORDER BY v.object_version DESC
         LIMIT 1
     ) ov
WHERE NOT ov.is_delete_marker
ORDER BY o.object_key
LIMIT $4::int
"""

REFERENCE_LIST_OBJECT_VERSIONS = """
SELECT o.object_key, ov.object_version, ov.is_delete_marker, ov.size_bytes, ov.md5_hash,
       ov.body_blake3, COALESCE(ov.last_modified, ov.created_at) AS last_modified,
       o.current_object_version
FROM (
    SELECT o.object_id, o.object_key, o.current_object_version, o.bucket_id
    FROM objects o
    WHERE o.bucket_id = $1
      AND o.deleted_at IS NULL
      AND ($2::text IS NULL OR o.object_key LIKE $2::text || '%')
      AND ($3::text IS NULL OR o.object_key >= $3::text)
      AND ($6::text IS NULL OR o.object_key < $6::text COLLATE "C")
    UNION ALL
    SELECT o.object_id, n.object_key, o.current_object_version, n.bucket_id
    FROM object_names n
    JOIN objects o ON o.object_id = n.object_id AND o.deleted_at IS NULL
    WHERE n.bucket_id = $1
      AND ($2::text IS NULL OR n.object_key LIKE $2::text || '%')
      AND ($3::text IS NULL OR n.object_key >= $3::text)
      AND ($6::text IS NULL OR n.object_key < $6::text COLLATE "C")
) o
JOIN object_versions ov ON ov.object_id = o.object_id
WHERE o.bucket_id = $1
  AND ov.deleted_at IS NULL
  AND ov.object_version <= o.current_object_version
  AND ($2::text IS NULL OR o.object_key LIKE $2::text || '%')
  AND ($6::text IS NULL OR o.object_key < $6::text COLLATE "C")
  AND ($3::text IS NULL OR o.object_key >= $3::text)
  AND (
        $3::text IS NULL
        OR o.object_key > $3::text
        OR (o.object_key = $3::text AND $4::bigint IS NOT NULL AND ov.object_version <= $4::bigint)
      )
  AND (ov.is_delete_marker OR ov.size_bytes > 0 OR (ov.md5_hash IS NOT NULL AND ov.md5_hash != ''))
  AND (NOT $7::boolean OR ov.object_version = o.current_object_version)
ORDER BY o.object_key, ov.object_version DESC
LIMIT $5::int
"""

# (key, [(status, size, md5, is_delete_marker), ...], soft_deleted)
POPULATION = [
    ("a/one", [("uploaded", 128, "aa", False)], False),
    ("a/two", [("uploaded", 128, "bb", False), ("uploaded", 256, "cc", False)], False),
    # newest version is a delete marker: the key and every alias of it drop out of ListObjects
    ("a/three", [("uploaded", 128, "dd", False), ("uploaded", 0, "", True)], False),
    # reserved-only version (InitiateMultipartUpload without Complete): no serveable version at all
    ("b/one", [("publishing", 0, "", False)], False),
    ("b/two", [("uploaded", 128, "ee", False)], True),
    ("c/one", [("uploaded", 128, "ff", False)], False),
    ("m/zeta", [("uploaded", 128, "gg", False), ("uploaded", 512, "hh", False), ("uploaded", 64, "ii", False)], False),
    ("z/last", [("uploaded", 128, "jj", False)], False),
]

# Aliases interleave with the primary keys on purpose: a per-arm limit is only interesting when the
# merged page has to take rows from both arms.
ALIASES = [
    ("a/alias-of-c", "c/one"),
    ("b/alias-of-m", "m/zeta"),
    ("n/alias-of-a-three", "a/three"),
    ("zz/alias-of-z", "z/last"),
]

VISIBLE_KEYS = [
    "a/alias-of-c",
    "a/one",
    "a/two",
    "b/alias-of-m",
    "c/one",
    "m/zeta",
    "z/last",
    "zz/alias-of-z",
]

PREFIXES = [None, "a/", "b/", "zzz/", "a"]
CURSORS = [None, "a/alias-of-c", "a/one\x01", "b/", "m/zeta\x01", "zzzzzz"]
KEY_MARKERS = [None, "a/alias-of-c", "a/one", "b/", "m/zeta", "zzzzzz"]
# 0 (degenerate probe), 1, either side of the visible count, and comfortably past the end.
LIMITS = [0, 1, 2, 3, 5, 7, 8, 9, 20]


def _prefix_upper(prefix: str | None) -> str | None:
    """The exclusive upper bound the endpoints compute (`_prefix_resume`)."""
    if not prefix:
        return None
    for i in range(len(prefix) - 1, -1, -1):
        if ord(prefix[i]) < 0x10FFFF:
            bumped = prefix[:i] + chr(ord(prefix[i]) + 1)
            return None if bumped == prefix else bumped
    return None


@pytest_asyncio.fixture
async def seeded() -> AsyncGenerator[tuple[asyncpg.Connection, uuid.UUID], None]:
    try:
        conn = await asyncpg.connect(_DB_URL)
    except OSError as exc:  # only an unreachable server is a legitimate skip
        pytest.skip(f"postgres unavailable: {exc}")

    tx = conn.transaction()
    await tx.start()

    account = f"5T{uuid.uuid4().hex[:12]}"
    bucket_id = uuid.uuid4()
    await conn.execute("INSERT INTO users (main_account_id) VALUES ($1)", account)
    await conn.execute(
        "INSERT INTO buckets (bucket_id, bucket_name, main_account_id, created_at) VALUES ($1,$2,$3, now())",
        bucket_id,
        f"listopt-{uuid.uuid4().hex[:10]}",
        account,
    )

    object_ids: dict[str, uuid.UUID] = {}
    for key, versions, soft_deleted in POPULATION:
        object_id = uuid.uuid4()
        object_ids[key] = object_id
        await conn.execute(
            "INSERT INTO objects (object_id, bucket_id, object_key, current_object_version, created_at, deleted_at)"
            " VALUES ($1,$2,$3,$4, now(), $5)",
            object_id,
            bucket_id,
            key,
            len(versions),
            datetime.datetime.now(datetime.timezone.utc) if soft_deleted else None,
        )
        for number, (status, size, md5, marker) in enumerate(versions, start=1):
            await conn.execute(
                "INSERT INTO object_versions (object_id, object_version, storage_version, size_bytes, md5_hash,"
                " content_type, status, is_delete_marker, body_blake3)"
                " VALUES ($1,$2,5,$3,$4,'application/octet-stream',$5,$6,$7)",
                object_id,
                number,
                size,
                md5,
                status,
                marker,
                f"blake3-{key}-{number}",
            )

    for alias, target in ALIASES:
        await conn.execute(
            "INSERT INTO object_names (bucket_id, object_key, object_id) VALUES ($1,$2,$3)",
            bucket_id,
            alias,
            object_ids[target],
        )

    try:
        yield conn, bucket_id
    finally:
        await tx.rollback()
        await conn.close()


def _rows(records: list[asyncpg.Record]) -> list[tuple[Any, ...]]:
    return [tuple(record) for record in records]


@pytest.mark.asyncio
@pytest.mark.parametrize("prefix", PREFIXES)
@pytest.mark.parametrize("cursor", CURSORS)
async def test_list_objects_matches_the_unlimited_union(
    seeded: tuple[asyncpg.Connection, uuid.UUID],
    prefix: str | None,
    cursor: str | None,
) -> None:
    conn, bucket_id = seeded
    upper = _prefix_upper(prefix)
    for limit in LIMITS:
        args = (bucket_id, prefix, cursor, limit, upper)
        expected = _rows(await conn.fetch(REFERENCE_LIST_OBJECTS, *args))
        actual = _rows(await conn.fetch(get_query("list_objects"), *args))
        assert actual == expected, f"prefix={prefix!r} cursor={cursor!r} limit={limit}"


@pytest.mark.asyncio
@pytest.mark.parametrize("prefix", PREFIXES)
@pytest.mark.parametrize("key_marker", KEY_MARKERS)
async def test_list_object_versions_matches_the_unlimited_union(
    seeded: tuple[asyncpg.Connection, uuid.UUID],
    prefix: str | None,
    key_marker: str | None,
) -> None:
    conn, bucket_id = seeded
    upper = _prefix_upper(prefix)
    for version_marker in (None, 1, 2, 3):
        for current_only in (False, True):
            for limit in LIMITS:
                args = (bucket_id, prefix, key_marker, version_marker, limit, upper, current_only)
                expected = _rows(await conn.fetch(REFERENCE_LIST_OBJECT_VERSIONS, *args))
                actual = _rows(await conn.fetch(get_query("list_object_versions"), *args))
                assert actual == expected, (
                    f"prefix={prefix!r} key_marker={key_marker!r} version_marker={version_marker} "
                    f"limit={limit} current_only={current_only}"
                )


@pytest.mark.asyncio
async def test_the_visible_key_set_is_what_the_semantics_say(
    seeded: tuple[asyncpg.Connection, uuid.UUID],
) -> None:
    """Pins what the differential is differencing against — an oracle that agrees on nothing is cheap."""
    conn, bucket_id = seeded
    rows = await conn.fetch(get_query("list_objects"), bucket_id, None, None, 100, None)
    assert [row["object_key"] for row in rows] == VISIBLE_KEYS

    # a/three's newest version is a delete marker, and the marker lives on the shared object_id,
    # so the alias name for the same object disappears with it.
    assert "a/three" not in VISIBLE_KEYS
    assert "n/alias-of-a-three" not in VISIBLE_KEYS


@pytest.mark.asyncio
@pytest.mark.parametrize("page_size", [1, 2, 3, 5])
async def test_paging_at_any_page_size_walks_every_key_exactly_once(
    seeded: tuple[asyncpg.Connection, uuid.UUID],
    page_size: int,
) -> None:
    """The endpoint stops on a short page, so a per-arm limit that truncates early loses keys."""
    conn, bucket_id = seeded
    cursor: str | None = None
    walked: list[str] = []
    while True:
        batch = await conn.fetch(get_query("list_objects"), bucket_id, None, cursor, page_size, None)
        walked.extend(row["object_key"] for row in batch)
        if len(batch) < page_size:
            break
        cursor = batch[-1]["object_key"] + "\x01"
    assert walked == VISIBLE_KEYS


@pytest.mark.asyncio
async def test_unsafe_variant_limiting_keys_before_the_version_lookup_loses_rows(
    seeded: tuple[asyncpg.Connection, uuid.UUID],
) -> None:
    """The mutant, pinned. If this ever stops differing, the fixture stopped exercising the hazard."""
    conn, bucket_id = seeded
    args = (bucket_id, None, "a/one\x01", 1, None)
    reference = [row["object_key"] for row in await conn.fetch(REFERENCE_LIST_OBJECTS, *args)]
    unsafe = [row["object_key"] for row in await conn.fetch(UNSAFE_LIST_OBJECTS, *args)]
    assert reference == ["a/two"]
    assert unsafe != reference, "limiting the key arms above the delete-marker filter must lose a key"

    actual = [row["object_key"] for row in await conn.fetch(get_query("list_objects"), *args)]
    assert actual == reference


@pytest.mark.parametrize("query_name", ["list_objects", "list_object_versions"])
def test_both_arms_still_carry_their_own_limit(query_name: str) -> None:
    """Collapsing back to one outer LIMIT is the regression: it costs the whole remaining keyspace."""
    sql = get_query(query_name)
    limit_param = "$4::int" if query_name == "list_objects" else "$5::int"
    assert sql.count(f"LIMIT {limit_param}") == 3, (
        f"{query_name}.sql must LIMIT each union arm as well as the merge, or the outer limit sits "
        "above an Append and neither arm can stop early"
    )


@pytest.mark.parametrize("query_name", ["list_objects", "list_object_versions"])
def test_both_arms_stay_bounded_on_the_upper_end(query_name: str) -> None:
    """LS-2. This one changes no rows, so only the SQL text can pin it — hence a text assertion."""
    sql = get_query(query_name)
    upper = "$5::text" if query_name == "list_objects" else "$6::text"
    assert sql.count(f"{upper} IS NULL OR") == 2, (
        f"{query_name}.sql must carry the exclusive upper bound in BOTH arms, or that arm's "
        "(bucket_id, object_key) index range runs to the end of the bucket"
    )
