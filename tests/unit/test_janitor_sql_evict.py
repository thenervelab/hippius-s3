"""Unit tests for the janitor's SQL-driven eviction phase (evict_from_inventory).

The phase keyset-pages fs_cache_inventory (candidate PREFILTER), stats only the candidates, and
re-runs the ABSOLUTE per-part replication gate before every delete. These tests pin the safety
seams the plan calls out: a stale inventory row self-heals instead of being trusted; hot and
under-replicated candidates are never deleted; the durable cursor advances only after a page is
fully drained and wraps the ring on exhaustion; the kill switch issues no queries at all.

Everything below the SQL is faked (the query file ships in a parallel worktree): get_query and
conn.fetch are patched, so these tests exercise the Python control flow, not the SQL.
"""

from __future__ import annotations

import asyncio
import logging
import types
from datetime import datetime
from datetime import timezone
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from workers import run_janitor_in_loop as janitor


OID_A = "aaaaaaaa-0000-0000-0000-000000000001"
OID_B = "bbbbbbbb-0000-0000-0000-000000000002"
COLD = 1.0  # atime far in the past → never hot
HOT = None  # sentinel replaced with time.time() at build → always hot under normal pressure


def _row(oid: str, ov: int = 1, pn: int = 1, cached_at: datetime | None = None) -> dict:
    return {
        "object_id": oid,
        "object_version": ov,
        "part_number": pn,
        "cached_at": cached_at or datetime(2020, 1, 1, tzinfo=timezone.utc),
    }


def _stat(atime: float) -> types.SimpleNamespace:
    return types.SimpleNamespace(st_atime=atime)


class _FakeFs:
    """fs_store double: stat_part answers from a map, delete_part records the tuple."""

    def __init__(self, stat_map: dict | None = None) -> None:
        self.stat_map = stat_map or {}
        self.deleted: list[tuple[str, int, int]] = []

    def stat_part(self, oid: str, ov: int, pn: int) -> types.SimpleNamespace | None:
        return self.stat_map.get((oid, ov, pn))

    async def delete_part(self, oid: str, ov: int, pn: int) -> None:
        self.deleted.append((oid, ov, pn))


class _Pool:
    """Hands out one shared conn for every acquire() — the phase's fetches and the worker pool's
    per-item acquires all see it; the DB funcs the workers call are module-patched, so the conn
    identity only matters for conn.fetch (the candidate pages)."""

    def __init__(self, conn: object) -> None:
        self._conn = conn

    def acquire(self):  # type: ignore[no-untyped-def]
        conn = self._conn

        class _Ctx:
            async def __aenter__(self_):  # type: ignore[no-untyped-def]
                return conn

            async def __aexit__(self_, *exc):  # type: ignore[no-untyped-def]
                return False

        return _Ctx()


@pytest.fixture(autouse=True)
def _config(monkeypatch: pytest.MonkeyPatch) -> None:
    janitor.config.janitor_sql_max_deletes_per_cycle = 50000
    janitor.config.janitor_sql_page_size = 1000
    janitor.config.janitor_concurrency = 4
    janitor.config.fs_cache_gc_max_age_seconds = 86400
    janitor.config.fs_cache_hot_retention_seconds = 10800
    janitor.config.upload_backends = ["arion"]
    janitor.config.backup_backends = []
    # No SQL file in this worktree; the flow is what's under test, not the query.
    monkeypatch.setattr(janitor, "get_query", lambda name: "SELECT 1")
    monkeypatch.setattr(janitor, "get_janitor_state", AsyncMock(return_value=None))
    monkeypatch.setattr(janitor, "set_janitor_state", AsyncMock())
    monkeypatch.setattr(janitor, "clear_cached", AsyncMock())
    monkeypatch.setattr(janitor, "get_all_dlq_object_ids", AsyncMock(return_value=set()))
    monkeypatch.setattr(janitor, "_janitor_deleted_counter", MagicMock())


def _conn_with_pages(*pages: list[dict]) -> AsyncMock:
    conn = AsyncMock()
    conn.fetch = AsyncMock(side_effect=list(pages))
    return conn


async def _evict(conn: AsyncMock, fs: _FakeFs, *, pressure: int = 0) -> int:
    return await janitor.evict_from_inventory(_Pool(conn), fs, MagicMock(), pressure=pressure)


# ------------------------------------------------------------------- (a) stale row


@pytest.mark.asyncio
async def test_stat_miss_clears_row_and_does_not_delete(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    fs = _FakeFs(stat_map={})  # stat_part → None (dir gone)
    conn = _conn_with_pages([_row(OID_A)], [])

    deleted = await _evict(conn, fs)

    assert deleted == 0
    assert fs.deleted == []
    janitor.clear_cached.assert_awaited_once()  # stale row self-healed
    assert janitor.clear_cached.await_args.args[1:] == (OID_A, 1, 1)


# ------------------------------------------------------------------- (b) hot


@pytest.mark.asyncio
async def test_hot_candidate_is_kept(monkeypatch: pytest.MonkeyPatch) -> None:
    import time

    repl = AsyncMock(return_value=True)
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", repl)
    fs = _FakeFs(stat_map={(OID_A, 1, 1): _stat(time.time())})  # read just now → hot
    conn = _conn_with_pages([_row(OID_A)], [])

    deleted = await _evict(conn, fs, pressure=0)

    assert deleted == 0
    assert fs.deleted == []
    repl.assert_not_awaited()  # never reaches the gate — hot short-circuits first
    janitor.clear_cached.assert_not_awaited()


# ------------------------------------------------------------------- (c) gate False


@pytest.mark.asyncio
async def test_under_replicated_candidate_is_kept(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=False))
    fs = _FakeFs(stat_map={(OID_A, 1, 1): _stat(COLD)})
    conn = _conn_with_pages([_row(OID_A)], [])

    deleted = await _evict(conn, fs)

    assert deleted == 0
    assert fs.deleted == []
    janitor.clear_cached.assert_not_awaited()  # prefilter/gate divergence is survivable — row kept


# ------------------------------------------------------------------- (d) delete


@pytest.mark.asyncio
async def test_replicated_cold_candidate_is_deleted_cleared_and_counted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    fs = _FakeFs(stat_map={(OID_A, 1, 1): _stat(COLD)})
    conn = _conn_with_pages([_row(OID_A)], [])

    deleted = await _evict(conn, fs)

    assert deleted == 1
    assert fs.deleted == [(OID_A, 1, 1)]
    janitor.clear_cached.assert_awaited_once()
    janitor._janitor_deleted_counter.add.assert_called_once_with(1, attributes={"reason": "sql_evict"})


@pytest.mark.asyncio
async def test_delete_still_counted_when_clear_cached_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    # The narrow try around clear_cached: the delete already happened, so it must still count and
    # the row self-heals on the next stat-miss rather than the delete being lost.
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    monkeypatch.setattr(janitor, "clear_cached", AsyncMock(side_effect=RuntimeError("db blip")))
    fs = _FakeFs(stat_map={(OID_A, 1, 1): _stat(COLD)})
    conn = _conn_with_pages([_row(OID_A)], [])

    deleted = await _evict(conn, fs)

    assert deleted == 1
    assert fs.deleted == [(OID_A, 1, 1)]
    janitor._janitor_deleted_counter.add.assert_called_once_with(1, attributes={"reason": "sql_evict"})


# ------------------------------------------------------------------- (e) DLQ


@pytest.mark.asyncio
async def test_dlq_parked_object_is_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    monkeypatch.setattr(janitor, "get_all_dlq_object_ids", AsyncMock(return_value={OID_A}))
    fs = _FakeFs(stat_map={(OID_A, 1, 1): _stat(COLD)})
    conn = _conn_with_pages([_row(OID_A)], [])

    deleted = await _evict(conn, fs)

    assert deleted == 0
    assert fs.deleted == []  # producer filtered it before any stat


@pytest.mark.asyncio
async def test_dlq_unavailable_falls_open_to_replication_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    monkeypatch.setattr(
        janitor,
        "get_all_dlq_object_ids",
        AsyncMock(side_effect=janitor.DLQProtectionUnavailable("redis down")),
    )
    fs = _FakeFs(stat_map={(OID_A, 1, 1): _stat(COLD)})
    conn = _conn_with_pages([_row(OID_A)], [])

    deleted = await _evict(conn, fs)

    assert deleted == 1  # fail-open: replicated part is safe to evict regardless of DLQ
    assert fs.deleted == [(OID_A, 1, 1)]


# ------------------------------------------------------------------- (f) cursor


@pytest.mark.asyncio
async def test_cursor_advances_to_last_row_after_page(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    janitor.config.janitor_sql_page_size = 2
    last_ts = datetime(2021, 6, 1, tzinfo=timezone.utc)
    page = [_row(OID_A, pn=1), _row(OID_B, pn=2, cached_at=last_ts)]
    fs = _FakeFs(stat_map={(OID_A, 1, 1): _stat(COLD), (OID_B, 1, 2): _stat(COLD)})
    conn = _conn_with_pages(page, [])

    await _evict(conn, fs)

    # A full page (len == page_size) does not short-circuit; the cursor is persisted at its LAST
    # row before the next fetch, so a crash resumes strictly after it.
    saved = [c.args for c in janitor.set_janitor_state.await_args_list]
    assert (
        "sql_evict_cursor",
        {"cached_at": last_ts.isoformat(), "object_id": OID_B, "object_version": 1, "part_number": 2},
    ) == saved[0][1:]


@pytest.mark.asyncio
async def test_empty_page_wraps_cursor_to_start(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    fs = _FakeFs()
    conn = _conn_with_pages([])

    deleted = await _evict(conn, fs)

    assert deleted == 0
    janitor.set_janitor_state.assert_awaited_once()
    assert janitor.set_janitor_state.await_args.args[1:] == ("sql_evict_cursor", {})  # ring wrap


@pytest.mark.asyncio
async def test_malformed_stored_cursor_falls_back_to_start() -> None:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    assert janitor._load_evict_cursor(None) == (epoch, "", 0, 0)
    assert janitor._load_evict_cursor({}) == (epoch, "", 0, 0)
    # Missing keys / a non-parseable value must not crash the phase — degrade to the ring start.
    assert janitor._load_evict_cursor({"object_version": "not-an-int"}) == (epoch, "", 0, 0)
    # A restored cursor: the ISO cached_at is parsed BACK to a datetime for the native $6 param.
    parsed = janitor._load_evict_cursor(
        {"cached_at": "2021-06-01T00:00:00+00:00", "object_id": OID_A, "object_version": 3, "part_number": 7}
    )
    assert parsed == (datetime(2021, 6, 1, tzinfo=timezone.utc), OID_A, 3, 7)
    assert isinstance(parsed[0], datetime)  # never a str — asyncpg would reject it for $6


@pytest.mark.asyncio
async def test_malformed_stored_cursor_does_not_crash_phase(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    monkeypatch.setattr(janitor, "get_janitor_state", AsyncMock(return_value={"object_version": object()}))
    fs = _FakeFs()
    conn = _conn_with_pages([])

    deleted = await _evict(conn, fs)  # must not raise

    assert deleted == 0
    # Fell back to the epoch start sentinel for $6-$9 (a real datetime, never None or a str).
    assert conn.fetch.await_args.args[5:] == (False, datetime(1970, 1, 1, tzinfo=timezone.utc), "", 0, 0)


@pytest.mark.asyncio
async def test_cursor_cached_at_is_a_datetime_on_fresh_start(monkeypatch: pytest.MonkeyPatch) -> None:
    # $6 is a NATIVE timestamptz param — asyncpg rejects a str — so the cold-start sentinel must be
    # a datetime (epoch), never the string '-infinity'.
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    fs = _FakeFs()
    conn = _conn_with_pages([])

    await _evict(conn, fs)

    cached_at_param = conn.fetch.await_args.args[6]  # $6
    assert isinstance(cached_at_param, datetime)
    assert cached_at_param == datetime(1970, 1, 1, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_cursor_cached_at_is_a_datetime_on_restore(monkeypatch: pytest.MonkeyPatch) -> None:
    # A resumed cursor: the stored ISO string is parsed back to a datetime before it reaches $6.
    stored = {"cached_at": "2021-06-01T00:00:00+00:00", "object_id": OID_A, "object_version": 3, "part_number": 7}
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    monkeypatch.setattr(janitor, "get_janitor_state", AsyncMock(return_value=stored))
    fs = _FakeFs()
    conn = _conn_with_pages([])

    await _evict(conn, fs)

    args = conn.fetch.await_args.args
    assert isinstance(args[6], datetime)
    assert args[6:] == (datetime(2021, 6, 1, tzinfo=timezone.utc), OID_A, 3, 7)


# ------------------------------------------------------------------- (g) kill switch


@pytest.mark.asyncio
async def test_kill_switch_issues_no_queries(monkeypatch: pytest.MonkeyPatch) -> None:
    janitor.config.janitor_sql_max_deletes_per_cycle = 0
    repl = AsyncMock(return_value=True)
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", repl)
    fs = _FakeFs()
    conn = _conn_with_pages([_row(OID_A)])

    deleted = await _evict(conn, fs)

    assert deleted == 0
    conn.fetch.assert_not_awaited()
    janitor.get_janitor_state.assert_not_awaited()
    janitor.get_all_dlq_object_ids.assert_not_awaited()


# ------------------------------------------------------------------- (h) pressure


@pytest.mark.asyncio
async def test_pressure_passes_ignore_age_true(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    fs = _FakeFs(stat_map={(OID_A, 1, 1): _stat(COLD)})
    conn = _conn_with_pages([_row(OID_A)], [])

    await _evict(conn, fs, pressure=1)

    # $5 (ignore_age) is True under any pressure; args = (query, backup, upload, page, max_age, ignore_age, *cursor)
    assert conn.fetch.await_args_list[0].args[5] is True


@pytest.mark.asyncio
async def test_no_pressure_passes_ignore_age_false(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    fs = _FakeFs(stat_map={(OID_A, 1, 1): _stat(COLD)})
    conn = _conn_with_pages([_row(OID_A)], [])

    await _evict(conn, fs, pressure=0)

    assert conn.fetch.await_args_list[0].args[5] is False


# ------------------------------------------------------------------- (i) budget


@pytest.mark.asyncio
async def test_budget_stops_paging_after_page_reaches_max(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    janitor.config.janitor_sql_page_size = 3
    janitor.config.janitor_sql_max_deletes_per_cycle = 2
    # A FULL page of 3 deletable parts (len == page_size, so the short-page break does NOT fire);
    # only the budget can stop the loop. A second page is queued to prove it is never fetched.
    full = [_row(OID_A, pn=1), _row(OID_A, pn=2), _row(OID_A, pn=3)]
    fs = _FakeFs(stat_map={(OID_A, 1, pn): _stat(COLD) for pn in (1, 2, 3)})
    conn = _conn_with_pages(full, [_row(OID_B)])

    deleted = await _evict(conn, fs)

    assert deleted == 3  # whole page drains; budget is a page-boundary gate, not per-item
    assert conn.fetch.await_count == 1  # budget reached → no second page fetched


# ------------------------------------------------------------------- query timeout


@pytest.mark.asyncio
async def test_query_timeout_ends_cycle_without_moving_cursor(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # A sparse-ring page that scans past the timeout must end discovery with the cursor UNMOVED:
    # deletes-so-far are returned, the cursor is neither advanced nor reset to {} (the ring-wrap
    # sentinel), so the next cycle resumes from exactly here — and it never crashes the phase.
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    janitor.config.janitor_sql_page_size = 1
    page1_ts = datetime(2021, 1, 1, tzinfo=timezone.utc)
    conn = AsyncMock()
    conn.fetch = AsyncMock(side_effect=[[_row(OID_A, cached_at=page1_ts)], asyncio.TimeoutError()])
    fs = _FakeFs(stat_map={(OID_A, 1, 1): _stat(COLD)})

    with caplog.at_level(logging.WARNING):
        deleted = await _evict(conn, fs)

    assert deleted == 1  # page-1 deletes-so-far are returned
    assert fs.deleted == [(OID_A, 1, 1)]
    # Cursor persisted ONLY at page-1's completed position; the timeout did not advance it or {}-reset it.
    saved_values = [c.args[2] for c in janitor.set_janitor_state.await_args_list]
    assert saved_values == [
        {"cached_at": page1_ts.isoformat(), "object_id": OID_A, "object_version": 1, "part_number": 1}
    ]
    assert "timed out" in caplog.text


@pytest.mark.asyncio
async def test_query_timeout_on_first_page_leaves_cursor_untouched(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # Timeout on the very first page: nothing deleted and set_janitor_state is never called, so the
    # durable cursor stays wherever the previous cycle left it in janitor_state.
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    conn = AsyncMock()
    conn.fetch = AsyncMock(side_effect=asyncio.TimeoutError())
    fs = _FakeFs()

    with caplog.at_level(logging.WARNING):
        deleted = await _evict(conn, fs)

    assert deleted == 0
    janitor.set_janitor_state.assert_not_awaited()  # cursor untouched
    assert "timed out" in caplog.text
