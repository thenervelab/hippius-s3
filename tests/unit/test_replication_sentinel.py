"""Unit tests for the janitor's G2 replication-gate sentinel orchestration.

The SQL predicate itself is pinned in tests/integration/test_replication_sentinel_sql.py
against real Postgres. Here we drive `check_replication_sentinel` with a fake pool to
verify the orchestration: it publishes the gauge, returns the violation count, escalates
to ERROR only under critical disk pressure, and stays silent on a clean scan.
"""

from __future__ import annotations

import logging
from unittest.mock import patch

import pytest

from workers import run_janitor_in_loop as janitor


class _FakeConn:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows
        self.fetched_args: tuple | None = None

    async def fetch(self, query: str, *args: object) -> list[dict]:
        self.fetched_args = args
        return self._rows


class _FakeAcquire:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _FakePool:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self._conn)


def _row(chunk_id: int) -> dict:
    return {"object_id": "obj", "object_version": 1, "chunk_id": chunk_id}


@pytest.mark.asyncio
async def test_clean_scan_reports_zero_and_logs_nothing(caplog):
    pool = _FakePool(_FakeConn([]))
    with (
        patch.object(janitor.config, "upload_backends", ["arion"]),
        patch.object(janitor.config, "backup_backends", [], create=True),
    ):
        with caplog.at_level(logging.WARNING, logger=janitor.logger.name):
            result = await janitor.check_replication_sentinel(pool, pressure=0)
    assert result == 0
    assert janitor._replication_sentinel_violations == 0
    assert not caplog.records, "a clean scan is silent"


@pytest.mark.asyncio
async def test_violations_publish_the_gauge_and_warn_below_critical(caplog):
    pool = _FakePool(_FakeConn([_row(1), _row(2)]))
    with (
        patch.object(janitor.config, "upload_backends", ["arion"]),
        patch.object(janitor.config, "backup_backends", ["ipfs"], create=True),
    ):
        with caplog.at_level(logging.WARNING, logger=janitor.logger.name):
            result = await janitor.check_replication_sentinel(pool, pressure=1)
    assert result == 2
    assert janitor._replication_sentinel_violations == 2
    assert any(r.levelno == logging.WARNING for r in caplog.records), "a gap below critical pressure warns"
    assert not any(r.levelno == logging.ERROR for r in caplog.records)


@pytest.mark.asyncio
async def test_violations_under_critical_pressure_escalate_to_error(caplog):
    pool = _FakePool(_FakeConn([_row(1)]))
    with (
        patch.object(janitor.config, "upload_backends", ["arion"]),
        patch.object(janitor.config, "backup_backends", [], create=True),
    ):
        with caplog.at_level(logging.WARNING, logger=janitor.logger.name):
            result = await janitor.check_replication_sentinel(pool, pressure=2)
    assert result == 1
    assert any(r.levelno == logging.ERROR for r in caplog.records), "a gap at critical pressure pages (ERROR)"


@pytest.mark.asyncio
async def test_the_query_is_bound_with_backup_upload_and_the_scan_limit():
    conn = _FakeConn([])
    pool = _FakePool(conn)
    with (
        patch.object(janitor.config, "upload_backends", ["arion"]),
        patch.object(janitor.config, "backup_backends", ["ipfs"], create=True),
    ):
        await janitor.check_replication_sentinel(pool, pressure=0)
    assert conn.fetched_args == (
        ["ipfs"],
        ["arion"],
        janitor.SENTINEL_SCAN_LIMIT,
        janitor.config.replication_sla_seconds,
    ), "bound as (backup_backends, config.upload_backends, scan_limit, replication_sla_seconds)"
