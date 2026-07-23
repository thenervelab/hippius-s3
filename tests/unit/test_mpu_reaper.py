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


def statement_of(query: str) -> str:
    """The query with its leading comments stripped.

    These tests route on the SQL text, and query files routinely NAME other tables in
    their comments — list_abandoned_versions.sql explains its plan in terms of the drain's
    cephor_replication_status. Matching the raw text then silently misroutes: the reaper's
    query looks like the sweep's, so the fake feeds it the wrong rows and the grace-window
    assertion reads the wrong call. Route on the statement instead.
    """
    return "\n".join(line for line in query.splitlines() if not line.lstrip().startswith("--"))


class FakeDb:
    """A minimal asyncpg-connection stand-in: fetch returns canned rows; execute records.

    `fetch` dispatches on the query text so one FakeDb can back both reaper paths in a
    single cycle: the abandoned-MPU query selects from ``multipart_uploads`` while the
    orphan sweep's query selects from ``cephor_replication_status`` — they return
    differently-shaped rows and must not cross-feed. ``raise_mark_for`` forces the
    terminal-mark ``execute`` to throw for one object_id, modelling a poison row the
    sweep must skip without aborting the rest of the pass.
    """

    def __init__(
        self,
        fetch_rows: list[dict] | None = None,
        *,
        sweep_rows: list[dict] | None = None,
        raise_mark_for: str | None = None,
    ) -> None:
        self._fetch_rows = fetch_rows or []
        self._sweep_rows = sweep_rows or []
        self._raise_mark_for = raise_mark_for
        self.executed: list[tuple[str, tuple]] = []
        self.fetched: list[tuple[str, tuple]] = []

    async def fetch(self, query: str, *args: object) -> list[dict]:
        self.fetched.append((query, args))
        if "cephor_replication_status" in statement_of(query):
            return self._sweep_rows
        return self._fetch_rows

    async def execute(self, query: str, *args: object) -> str:
        if self._raise_mark_for is not None and args and args[0] == self._raise_mark_for:
            raise RuntimeError(f"forced mark failure for {self._raise_mark_for}")
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
async def test_fail_version_replication_passes_null_version_through() -> None:
    # Legacy parts can carry a NULL object_version; the helper must bind NULL (not crash
    # on int(None)) so the query fails every version of the abandoned object.
    db = FakeDb()
    await mpu_cleanup.fail_version_replication(db, object_id="obj-1", object_version=None)
    assert len(db.executed) == 1
    _, args = db.executed[0]
    assert args == ("obj-1", None), "a NULL version is bound through, not int()-cast"


@pytest.mark.asyncio
async def test_reaper_handles_abandoned_row_with_null_object_version() -> None:
    # The regression that wedged the staging reaper: an abandoned row whose object_version
    # is NULL must still be reaped, not raise TypeError on int(None).
    rows = [{"upload_id": "u1", "object_id": "obj-1", "object_version": None, "age_seconds": 90000.0}]
    db = FakeDb(rows)
    result = await mpu_cleanup.reap_abandoned_uploads(db, stale_seconds=86400, dlq_object_ids=set())
    assert result.count == 1
    fail_args = [args for query, args in db.executed if "'failed'" in query]
    assert fail_args == [("obj-1", None)], "the NULL-version row is marked terminal without crashing"


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
        await mpu_cleanup.run_reaper_cycle(
            pool, _fake_redis(), stale_seconds=86400, sweep_grace_seconds=86400, upload_backends=["arion"]
        )

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
        await mpu_cleanup.run_reaper_cycle(
            pool, _fake_redis(), stale_seconds=86400, sweep_grace_seconds=86400, upload_backends=["arion"]
        )

    collector.record_mpu_reaper_cycle.assert_called_once()
    _, kwargs = collector.record_mpu_reaper_cycle.call_args
    assert kwargs["success"] is False
    assert kwargs["reaped"] == 0


# ---------------------------------------------------------------- orphan sweep (WI-20a/A21)
# The abandoned-MPU reaper above keys on multipart_uploads rows. An aborted upload deletes
# that header row, so its leaked cephor_replication_status rows are invisible to it. The
# sweep is the real A21 backstop: it keys DIRECTLY on cephor_replication_status, so it
# catches orphans whose MPU/parts rows are already gone. It only ever MARKS 'failed' (never
# deletes), so unlike the reaper it can tolerate a per-version mark failure and press on.


@pytest.mark.asyncio
async def test_sweep_marks_each_orphan_version_failed() -> None:
    rows = [
        {"object_id": "obj-1", "version": 1, "age_seconds": 90000.0},
        {"object_id": "obj-2", "version": 5, "age_seconds": 172800.0},
    ]
    db = FakeDb(sweep_rows=rows)
    result = await mpu_cleanup.sweep_orphan_replication_versions(db, stale_seconds=86400, dlq_object_ids=set())
    assert result.count == 2
    assert result.oldest_reaped_age_seconds == 172800.0, "reports the age of the oldest swept orphan"
    marked = [args for query, args in db.executed if "'failed'" in query]
    assert marked == [("obj-1", 1), ("obj-2", 5)], "every orphan version's active rows are marked terminal"
    assert not any("DELETE" in query for query, _ in db.executed), "the sweep never deletes — only marks"


@pytest.mark.asyncio
async def test_sweep_spares_dlq_protected_objects() -> None:
    rows = [{"object_id": "obj-1", "version": 1, "age_seconds": 90000.0}]
    db = FakeDb(sweep_rows=rows)
    result = await mpu_cleanup.sweep_orphan_replication_versions(db, stale_seconds=86400, dlq_object_ids={"obj-1"})
    assert result.count == 0
    assert result.oldest_reaped_age_seconds is None
    assert db.executed == [], "an in-flight DLQ object is never swept"


@pytest.mark.asyncio
async def test_sweep_presses_on_when_one_versions_mark_throws() -> None:
    # A poison version (e.g. transient row-lock) must not starve the rest of the pass: the
    # sweep is mark-only and idempotent, so it logs the failure, counts only successes, and
    # the failed one is retried on the next cycle (it stays active in cephor).
    rows = [
        {"object_id": "obj-boom", "version": 1, "age_seconds": 90000.0},
        {"object_id": "obj-ok", "version": 2, "age_seconds": 100000.0},
    ]
    db = FakeDb(sweep_rows=rows, raise_mark_for="obj-boom")
    result = await mpu_cleanup.sweep_orphan_replication_versions(db, stale_seconds=86400, dlq_object_ids=set())
    assert result.count == 1, "only the successfully-marked version is counted"
    marked = [args for query, args in db.executed if "'failed'" in query]
    assert marked == [("obj-ok", 2)], "the healthy version is still swept despite the earlier failure"
    assert result.oldest_reaped_age_seconds == 100000.0, "age is reported from swept versions only"


@pytest.mark.asyncio
async def test_sweep_reports_none_age_when_nothing_to_sweep() -> None:
    db = FakeDb(sweep_rows=[])
    result = await mpu_cleanup.sweep_orphan_replication_versions(db, stale_seconds=86400, dlq_object_ids=set())
    assert result.count == 0
    assert result.oldest_reaped_age_seconds is None


@pytest.mark.asyncio
async def test_run_reaper_cycle_runs_the_sweep_and_records_its_count() -> None:
    # One cycle drains BOTH paths: the abandoned-MPU reaper and the cephor-orphan sweep.
    mpu_rows = [{"upload_id": "u1", "object_id": "obj-1", "object_version": 1, "age_seconds": 90000.0}]
    sweep_rows = [
        {"object_id": "orphan-a", "version": 1, "age_seconds": 200000.0},
        {"object_id": "orphan-b", "version": 2, "age_seconds": 50000.0},
    ]
    pool = FakePool(FakeDb(mpu_rows, sweep_rows=sweep_rows))
    collector = MagicMock()

    with patch.object(mpu_cleanup, "get_metrics_collector", return_value=collector):
        await mpu_cleanup.run_reaper_cycle(
            pool, _fake_redis(), stale_seconds=86400, sweep_grace_seconds=86400, upload_backends=["arion"]
        )

    collector.record_mpu_reaper_cycle.assert_called_once()
    _, kwargs = collector.record_mpu_reaper_cycle.call_args
    assert kwargs["success"] is True
    assert kwargs["reaped"] == 1, "the abandoned-MPU reaper still runs"
    assert kwargs["swept"] == 2, "the cephor-orphan sweep count is recorded separately"


@pytest.mark.asyncio
async def test_run_reaper_cycle_threads_distinct_grace_windows() -> None:
    # The reaper and the orphan sweep take SEPARATE grace windows: the abandoned-MPU query
    # gets stale_seconds, the cephor-orphan sweep gets sweep_grace_seconds. The sweep grace
    # must be the value the janitor's aged-pending-orphan gauge also uses, so it is critical
    # that run_reaper_cycle threads it to the sweep query and NOT the reaper's stale_seconds.
    db = FakeDb([], sweep_rows=[])
    pool = FakePool(db)
    collector = MagicMock()

    with patch.object(mpu_cleanup, "get_metrics_collector", return_value=collector):
        await mpu_cleanup.run_reaper_cycle(
            pool, _fake_redis(), stale_seconds=86400, sweep_grace_seconds=999, upload_backends=["arion"]
        )

    reaper_grace = next(args[0] for query, args in db.fetched if "multipart_uploads" in statement_of(query))
    sweep_grace = next(args[0] for query, args in db.fetched if "cephor_replication_status" in statement_of(query))
    assert reaper_grace == 86400, "the abandoned-MPU reaper uses stale_seconds"
    assert sweep_grace == 999, "the orphan sweep uses the distinct sweep_grace_seconds"
