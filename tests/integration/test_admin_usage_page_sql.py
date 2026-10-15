from __future__ import annotations

import os
import uuid
from typing import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio

from hippius_s3.utils import get_query


pytestmark = pytest.mark.asyncio

_DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius?sslmode=disable")
_SQL = get_query("list_admin_usage_accounts")


@pytest_asyncio.fixture
async def conn() -> AsyncGenerator[asyncpg.Connection, None]:
    try:
        connection = await asyncpg.connect(_DB_URL)
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"integration Postgres unavailable ({exc}); run `docker compose up -d postgres`")

    await connection.execute(
        """
        CREATE TEMP TABLE buckets (
            bucket_id uuid PRIMARY KEY,
            main_account_id text NOT NULL,
            deleted_at timestamptz
        ) ON COMMIT PRESERVE ROWS;
        CREATE INDEX idx_buckets_main_account ON buckets (main_account_id);
        CREATE TEMP TABLE bucket_storage_usage (
            bucket_id uuid PRIMARY KEY,
            bytes_used bigint NOT NULL
        ) ON COMMIT PRESERVE ROWS;
        CREATE TEMP TABLE storage_usage_rollup_state (
            singleton boolean PRIMARY KEY,
            backfilled_at timestamptz
        ) ON COMMIT PRESERVE ROWS;
        """
    )
    try:
        yield connection
    finally:
        await connection.close()


async def _bucket(
    conn: asyncpg.Connection,
    account_id: str,
    bytes_used: int,
    *,
    deleted: bool = False,
    rollup: bool = True,
) -> None:
    bucket_id = uuid.uuid4()
    await conn.execute(
        """
        INSERT INTO buckets (bucket_id, main_account_id, deleted_at)
        VALUES ($1, $2, CASE WHEN $3 THEN now() ELSE NULL END)
        """,
        bucket_id,
        account_id,
        deleted,
    )
    if rollup:
        await conn.execute(
            "INSERT INTO bucket_storage_usage (bucket_id, bytes_used) VALUES ($1, $2)",
            bucket_id,
            bytes_used,
        )


async def _ready(conn: asyncpg.Connection) -> None:
    await conn.execute("INSERT INTO storage_usage_rollup_state (singleton, backfilled_at) VALUES (true, now())")


async def test_per_bucket_clamp_and_deleted_buckets(conn: asyncpg.Connection) -> None:
    await _ready(conn)
    await _bucket(conn, "acct", -3)
    await _bucket(conn, "acct", 10)
    await _bucket(conn, "acct", 100, deleted=True)
    await _bucket(conn, "other", 4)

    rows = await conn.fetch(_SQL, None, 10)
    by_account = {row["account_id"]: row for row in rows}
    assert by_account["acct"]["buckets"] == 2
    assert by_account["acct"]["bytes"] == 10
    assert by_account["other"]["bytes"] == 4
    assert all(row["ready"] for row in rows)


async def test_bypass_owner_is_absent(conn: asyncpg.Connection) -> None:
    await _ready(conn)
    await _bucket(conn, "bypass", 50)
    await _bucket(conn, "acct", 4)

    rows = await conn.fetch(_SQL, None, 10)
    assert [row["account_id"] for row in rows] == ["acct"]
    assert rows[0]["bytes"] == 4


async def test_live_bucket_without_rollup_row_is_zero_bytes(conn: asyncpg.Connection) -> None:
    await _ready(conn)
    await _bucket(conn, "acct", 0, rollup=False)

    rows = await conn.fetch(_SQL, None, 10)
    assert len(rows) == 1
    assert rows[0]["buckets"] == 1
    assert rows[0]["bytes"] == 0


async def test_cursor_does_not_repeat_and_orders_by_byte_value(conn: asyncpg.Connection) -> None:
    await _ready(conn)
    for account_id in ("b", "A", "c"):
        await _bucket(conn, account_id, 1)

    first = await conn.fetch(_SQL, None, 2)
    assert [row["account_id"] for row in first] == ["A", "b"]

    rest = await conn.fetch(_SQL, first[-1]["account_id"], 10)
    assert [row["account_id"] for row in rest] == ["c"]


async def test_not_ready_is_visible_on_an_empty_page_and_with_rows(conn: asyncpg.Connection) -> None:
    empty = await conn.fetch(_SQL, None, 10)
    assert len(empty) == 1
    assert empty[0]["ready"] is False
    assert empty[0]["account_id"] is None

    await conn.execute("INSERT INTO storage_usage_rollup_state (singleton, backfilled_at) VALUES (true, NULL)")
    await _bucket(conn, "acct", 10)
    page = await conn.fetch(_SQL, None, 10)
    assert page[0]["ready"] is False
    assert page[0]["account_id"] == "acct"
    assert page[0]["bytes"] == 10


async def test_plan_uses_account_index_and_usage_pk(conn: asyncpg.Connection) -> None:
    await conn.execute("SET enable_seqscan = off")
    await conn.execute("SET enable_hashjoin = off")
    await conn.execute("SET enable_mergejoin = off")
    plan = "\n".join(row["QUERY PLAN"] for row in await conn.fetch("EXPLAIN (COSTS OFF) " + _SQL, "A", 10))
    assert "storage_usage_rollup_state" in plan
    assert "idx_buckets_main_account" in plan
    assert "bucket_storage_usage_pkey" in plan
    assert 'COLLATE "C"' in plan
