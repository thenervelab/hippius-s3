"""Schema-level checks for the recent_uploads SQL.

These tests PREPARE the query against a live Postgres so column / table
typos are caught at test time instead of in production. They skip
silently if no Postgres is reachable on DATABASE_URL.
"""

import os

import asyncpg
import pytest

from hippius_s3.utils import get_query


async def _connect_or_skip() -> asyncpg.Connection:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set; skipping live-schema check")
    try:
        return await asyncpg.connect(dsn=dsn)
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"Postgres unreachable on DATABASE_URL: {exc}")


@pytest.mark.asyncio
async def test_recent_uploads_query_prepares_against_live_schema() -> None:
    conn = await _connect_or_skip()
    try:
        sql = get_query("get_recent_uploads_for_account")
        await conn.prepare(sql)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_recent_uploads_index_present() -> None:
    conn = await _connect_or_skip()
    try:
        row = await conn.fetchrow(
            "SELECT 1 FROM pg_indexes "
            "WHERE schemaname = 'public' "
            "  AND indexname = 'idx_object_versions_last_modified_desc'"
        )
        assert row is not None, (
            "idx_object_versions_last_modified_desc is missing — did the 20260427000000 migration run?"
        )
    finally:
        await conn.close()
