"""Tests for the post-evacuation reconcile script (hippius_s3/scripts/reconcile_lost_uploads.py)."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from hippius_s3.scripts.reconcile_lost_uploads import CANDIDATES_SQL
from hippius_s3.scripts.reconcile_lost_uploads import find_candidates
from hippius_s3.scripts.reconcile_lost_uploads import mark_failed


def _row(object_id: str, version: int = 1) -> dict:
    return {"object_id": object_id, "object_version": version, "status": "publishing", "chunk_count": 3}


@pytest.mark.asyncio
async def test_find_candidates_passes_min_age():
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=[_row("a")])
    rows = await find_candidates(conn, 45)
    conn.fetch.assert_awaited_once_with(CANDIDATES_SQL, 45)
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_mark_failed_counts_only_actual_updates():
    """The status re-check in MARK_FAILED_SQL means a concurrently-recovered version updates 0
    rows — it must not be counted as marked."""
    conn = AsyncMock()
    conn.execute = AsyncMock(side_effect=["UPDATE 1", "UPDATE 0", "UPDATE 1"])
    marked = await mark_failed(conn, [_row("a"), _row("b"), _row("c")])
    assert marked == 2
    assert conn.execute.await_count == 3


def test_candidates_sql_guards():
    """The query must only target pre-upload statuses and require zero LIVE backend rows."""
    assert "'publishing', 'pinning'" in CANDIDATES_SQL
    assert "NOT EXISTS" in CANDIDATES_SQL
    assert "NOT cb.deleted" in CANDIDATES_SQL, "soft-deleted backend rows must not count as replicated"
    assert "make_interval" in CANDIDATES_SQL, "min-age guard must be parameterized"
