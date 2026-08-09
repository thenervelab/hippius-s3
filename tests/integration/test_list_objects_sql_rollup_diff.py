"""LS-1 differential: the SQL delimiter rollup must equal the Python collapse, byte-for-byte.

Full-crawls BOTH `_collect_page` (the current Python skip-scan) and `_collect_page_sql` (the
recursive-CTE query) through their pagination across randomized prefix/delimiter/max-keys/start-after
sequences, asserting the emitted items, truncation, and continuation cursors are identical on every
page. This is the byte-identical proof that gates the `HIPPIUS_LIST_OBJECTS_SQL_ROLLUP` flag.

The rollup's prefix-successor skip-scan is only correct under C (byte-order) collation — the same
assumption `list_objects.sql` documents for prod. So the test provisions its OWN throwaway
C-collation database with the two tables the queries touch, independent of whatever collation the
ambient DB uses (the e2e Postgres is en_US, which would spuriously diverge).

Needs a live Postgres (HIPPIUS_E2E_DB_DSN / DATABASE_URL); skips otherwise.
"""

from __future__ import annotations

import os
import uuid
from typing import Any
from typing import Callable

import asyncpg
import pytest

from hippius_s3.api.s3.buckets.list_objects_endpoint import _collect_page
from hippius_s3.api.s3.buckets.list_objects_endpoint import _collect_page_sql
from hippius_s3.api.s3.buckets.list_objects_endpoint import _content_resume


_KEYS = [
    "data/01.txt", "data/02.txt",
    "images/a.png", "images/b.png", "images/c.png",
    "logs/2023/old.txt", "logs/2024/a.txt", "logs/2024/b.txt", "logs/2024/c.txt", "logs/2025/new.txt",
    "root-a.txt", "root-b.txt", "zzz-last.txt",
]
_PREFIXES = [None, "logs/", "logs/2024/", "log", "images/", "nope/"]
_DELIMITERS = [None, "/"]
_MAX_KEYS = [1, 2, 3, 5, 100]
_START_AFTER = [None, "logs/", "logs/2024/a.txt", "root-a.txt", "images/b.png"]

_SCHEMA = """
CREATE TABLE objects(
    object_id uuid PRIMARY KEY, bucket_id uuid, object_key text,
    current_object_version bigint, created_at timestamptz DEFAULT now(), deleted_at timestamptz
);
CREATE TABLE object_versions(
    object_id uuid, object_version bigint, size_bytes bigint, md5_hash text,
    content_type text DEFAULT 'application/octet-stream', multipart bool DEFAULT false, status text DEFAULT 'published'
);
CREATE INDEX idx_obj_bucket_key ON objects(bucket_id, object_key);
"""


def _admin_dsn() -> str:
    dsn = os.environ.get("HIPPIUS_E2E_DB_DSN") or os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("HIPPIUS_E2E_DB_DSN / DATABASE_URL not set; skipping LS-1 differential")
    return dsn


async def _provision_c_collation_db() -> tuple[str, str]:
    """Create a throwaway C-collation DB; return (its dsn, its name). Skips if Postgres is unreachable."""
    admin = _admin_dsn()
    name = f"ls1_ctest_{uuid.uuid4().hex[:12]}"
    try:
        conn = await asyncpg.connect(dsn=admin, database="postgres")
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"Postgres unreachable: {exc}")
    try:
        await conn.execute(f'CREATE DATABASE "{name}" LC_COLLATE \'C\' LC_CTYPE \'C\' TEMPLATE template0')
    finally:
        await conn.close()
    # Rebuild the DSN with the new database name.
    base = admin.split("?")[0]
    tail = base.rsplit("/", 1)[0]
    return f"{tail}/{name}", name


async def _drop_db(name: str) -> None:
    conn = await asyncpg.connect(dsn=_admin_dsn(), database="postgres")
    try:
        await conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
    finally:
        await conn.close()


def _normalize(items: list[tuple[str, Any]]) -> list[tuple[str, str]]:
    return [(kind, payload if kind == "prefix" else payload["object_key"]) for kind, payload in items]


async def _crawl(
    collect: Callable[..., Any],
    pool: asyncpg.Pool,
    bucket_id: uuid.UUID,
    *,
    prefix: str | None,
    delimiter: str | None,
    max_keys: int,
    start_after: str | None,
) -> list[tuple[list[tuple[str, str]], bool, str | None]]:
    """Follow the endpoint's real pagination: cp_floor applies only on the first page."""
    pages: list[tuple[list[tuple[str, str]], bool, str | None]] = []
    cursor = _content_resume(start_after) if start_after else None
    cp_floor: str | None = start_after
    for _ in range(60):  # generous page cap; the keyspace is tiny
        items, truncated, next_cursor = await collect(
            pool, bucket_id,
            prefix=prefix, delimiter=delimiter, cursor=cursor, target=max_keys, cp_floor=cp_floor,
        )
        pages.append((_normalize(items), truncated, next_cursor))
        if not truncated:
            break
        cursor = next_cursor
        cp_floor = None
    return pages


@pytest.mark.asyncio
async def test_sql_rollup_matches_python_across_params() -> None:
    dsn, name = await _provision_c_collation_db()
    try:
        # Both collectors take a pool, not a connection: since LS-45 they hold one pooled
        # connection for the whole skip-scan and call pool.acquire() themselves.
        pool = await asyncpg.create_pool(dsn=dsn)
        assert pool is not None
        try:
            await pool.execute(_SCHEMA)
            bucket_id = uuid.uuid4()
            for key in _KEYS:
                oid = uuid.uuid4()
                await pool.execute(
                    "INSERT INTO objects(object_id, bucket_id, object_key, current_object_version) VALUES($1,$2,$3,1)",
                    oid, bucket_id, key,
                )
                await pool.execute(
                    "INSERT INTO object_versions(object_id, object_version, size_bytes, md5_hash) VALUES($1,1,$2,$3)",
                    oid, len(key), uuid.uuid4().hex,
                )

            combos = 0
            for prefix in _PREFIXES:
                for delimiter in _DELIMITERS:
                    for max_keys in _MAX_KEYS:
                        for start_after in _START_AFTER:
                            py = await _crawl(
                                _collect_page, pool, bucket_id,
                                prefix=prefix, delimiter=delimiter, max_keys=max_keys, start_after=start_after,
                            )
                            sql = await _crawl(
                                _collect_page_sql, pool, bucket_id,
                                prefix=prefix, delimiter=delimiter, max_keys=max_keys, start_after=start_after,
                            )
                            assert sql == py, (
                                f"LS-1 divergence for prefix={prefix!r} delimiter={delimiter!r} "
                                f"max_keys={max_keys} start_after={start_after!r}\npython={py}\nsql={sql}"
                            )
                            combos += 1
            assert combos == len(_PREFIXES) * len(_DELIMITERS) * len(_MAX_KEYS) * len(_START_AFTER)
        finally:
            await pool.close()
    finally:
        await _drop_db(name)
