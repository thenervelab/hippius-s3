"""Unit tests for the abandoned-multipart-upload reaper and the terminal-mark helper.

These drive the public functions in `hippius_s3.services.mpu_cleanup` with a fake db
connection (no real Postgres): the SQL is loaded via `get_query`, so asserting on the
executed query text + args verifies the orchestration — which version is marked
terminal, that the multipart_uploads row is deleted, and that DLQ-protected objects are
spared. The central path is DB-only by design (node-local SSD is unreachable from a
central caller), so there is no filesystem fake here.
"""

from __future__ import annotations

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from hippius_s3.services import mpu_cleanup


class FakeDb:
    """A minimal asyncpg-connection stand-in: fetch returns canned rows; execute records."""

    def __init__(self, fetch_rows: list[dict] | None = None) -> None:
        self._fetch_rows = fetch_rows or []
        self.executed: list[tuple[str, tuple]] = []

    async def fetch(self, query: str, *args: object) -> list[dict]:
        return self._fetch_rows

    async def execute(self, query: str, *args: object) -> str:
        self.executed.append((query, args))
        return "UPDATE 1"


class _FakeAcquire:
    def __init__(self, db: FakeDb) -> None:
        self._db = db

    async def __aenter__(self) -> FakeDb:
        return self._db

    async def __aexit__(self, *exc: object) -> bool:
        return False


class FakePool:
    """asyncpg.Pool stand-in: `acquire()` yields the same FakeDb."""

    def __init__(self, db: FakeDb) -> None:
        self._db = db

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self._db)


def _fake_redis(entries: list[str] | None = None) -> MagicMock:
    client = MagicMock()
    client.lrange = AsyncMock(return_value=entries or [])
    return client


@pytest.mark.asyncio
async def test_fail_version_replication_marks_the_rows_failed() -> None:
    db = FakeDb()
    await mpu_cleanup.fail_version_replication(db, object_id="obj-1", object_version=3)
    assert len(db.executed) == 1
    query, args = db.executed[0]
    assert "cephor_replication_status" in query and "'failed'" in query, "marks the drain rows terminal"
    assert args == ("obj-1", 3), "object_id is stringified and the version bound as int"


@pytest.mark.asyncio
async def test_reaper_marks_each_abandoned_version_and_deletes_its_mpu_row() -> None:
    rows = [
        {"upload_id": "u1", "object_id": "obj-1", "object_version": 1, "age_seconds": 90000.0},
        {"upload_id": "u2", "object_id": "obj-2", "object_version": 5, "age_seconds": 172800.0},
    ]
    db = FakeDb(rows)
    result = await mpu_cleanup.reap_abandoned_uploads(db, stale_seconds=86400, dlq_object_ids=set())
    assert result.count == 2
    assert result.oldest_reaped_age_seconds == 172800.0, "reports the age of the oldest reaped upload"
    failed = sum(1 for query, _ in db.executed if "'failed'" in query)
    deleted_mpu_rows = sum(1 for query, _ in db.executed if "DELETE FROM multipart_uploads" in query)
    assert failed == 2, "each abandoned version's replication rows are marked terminal"
    assert deleted_mpu_rows == 2, "each abandoned upload's header row is removed so it is not reaped again"


@pytest.mark.asyncio
async def test_reaper_spares_dlq_protected_objects() -> None:
    # An object with an in-flight DLQ operation must never be reaped, mirroring the
    # janitor's DLQ gate — its data may still be needed.
    rows = [{"upload_id": "u1", "object_id": "obj-1", "object_version": 1, "age_seconds": 90000.0}]
    db = FakeDb(rows)
    result = await mpu_cleanup.reap_abandoned_uploads(db, stale_seconds=86400, dlq_object_ids={"obj-1"})
    assert result.count == 0
    assert result.oldest_reaped_age_seconds is None, "nothing reaped → no lag reported"
    assert db.executed == [], "a DLQ-protected object's rows are left intact"


@pytest.mark.asyncio
async def test_reaper_handles_rows_without_age_seconds() -> None:
    # asyncpg Records / dict rows lacking the column must not blow up (row.get(...) → None).
    rows = [{"upload_id": "u1", "object_id": "obj-1", "object_version": 1}]
    db = FakeDb(rows)
    result = await mpu_cleanup.reap_abandoned_uploads(db, stale_seconds=86400, dlq_object_ids=set())
    assert result.count == 1
    assert result.oldest_reaped_age_seconds is None


# ---------------------------------------------------------------- cycle metrics


@pytest.mark.asyncio
async def test_run_reaper_cycle_records_a_successful_cycle() -> None:
    rows = [{"upload_id": "u1", "object_id": "obj-1", "object_version": 1, "age_seconds": 90000.0}]
    pool = FakePool(FakeDb(rows))
    collector = MagicMock()

    with patch.object(mpu_cleanup, "get_metrics_collector", return_value=collector):
        await mpu_cleanup.run_reaper_cycle(pool, _fake_redis(), stale_seconds=86400, upload_backends=["arion"])

    collector.record_mpu_reaper_cycle.assert_called_once()
    _, kwargs = collector.record_mpu_reaper_cycle.call_args
    assert kwargs["success"] is True
    assert kwargs["reaped"] == 1
    assert kwargs["oldest_reaped_age"] == 90000.0
    assert kwargs["duration"] >= 0.0


@pytest.mark.asyncio
async def test_run_reaper_cycle_records_a_failure_without_raising() -> None:
    # A DB/Redis fault in a cycle must be swallowed (loop keeps running) and recorded
    # as success=false so a stalled reaper is visible on the dashboard.
    pool = MagicMock()
    pool.acquire = MagicMock(side_effect=RuntimeError("db down"))
    collector = MagicMock()

    with patch.object(mpu_cleanup, "get_metrics_collector", return_value=collector):
        await mpu_cleanup.run_reaper_cycle(pool, _fake_redis(), stale_seconds=86400, upload_backends=["arion"])

    collector.record_mpu_reaper_cycle.assert_called_once()
    _, kwargs = collector.record_mpu_reaper_cycle.call_args
    assert kwargs["success"] is False
    assert kwargs["reaped"] == 0
