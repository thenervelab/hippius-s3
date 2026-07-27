"""Unit tests for the janitor's SQL-driven eviction phase (evict_from_inventory).

The phase runs SLICE-THEN-FILTER per page: a pure keyset window of fs_cache_inventory
(janitor_inventory_slice) advances the ring cursor, and a separate coverage/age filter
(janitor_evictable_candidates) selects which of that window's tuples are safe to evict. The worker
stats only the filter's candidates and re-runs the ABSOLUTE per-part replication gate before every
delete. These tests pin the safety seams the plan calls out — a stale inventory row self-heals
instead of being trusted; hot and under-replicated candidates are never deleted; the kill switch
issues no queries — AND the stall fix: the durable cursor advances by the SLICE rows scanned, so a
window that is 100% non-candidates still walks the ring instead of re-pinning the cursor forever.

Everything below the SQL is faked (the query files are exercised by the integration suite): get_query
returns the query NAME so the fake conn can route slice-vs-filter, and conn.fetch is patched, so these
tests exercise the Python control flow, not the SQL.
"""

from __future__ import annotations

import asyncio
import logging
import types
from datetime import datetime
from datetime import timezone
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from workers import run_janitor_in_loop as janitor


OID_A = "aaaaaaaa-0000-0000-0000-000000000001"
OID_B = "bbbbbbbb-0000-0000-0000-000000000002"
COLD = 1.0  # atime far in the past → never hot
HOT = None  # sentinel replaced with time.time() at build → always hot under normal pressure


def _row(
    oid: str,
    ov: int = 1,
    pn: int = 1,
    cached_at: datetime | None = None,
    last_access_at: datetime | None = None,
) -> dict:
    # last_access_at rides on the candidate row (janitor_evictable_candidates LEFT-joins
    # fs_cache_inventory), so the worker consumes it inline instead of a per-item fetchval.
    return {
        "object_id": oid,
        "object_version": ov,
        "part_number": pn,
        "cached_at": cached_at or datetime(2020, 1, 1, tzinfo=timezone.utc),
        "last_access_at": last_access_at,
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
    identity only matters for conn.fetch (the slice + filter queries)."""

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


class _RouterConn:
    """conn double for the two-query page. get_query is patched to return the query NAME, so .fetch
    routes on it: `janitor_inventory_slice` serves a page's slice rows, `janitor_evictable_candidates`
    serves that page's candidate rows, advancing to the next page after the filter fetch. A page's
    slice or cand entry may be an Exception instance to inject a timeout on that specific fetch."""

    def __init__(self, pages: list[tuple[Any, Any]]) -> None:
        self._pages = pages
        self._i = 0
        self.slice_args: list[tuple] = []
        self.filter_args: list[tuple] = []
        self.fetch = AsyncMock(side_effect=self._fetch)

    async def _fetch(self, query: str, *args: Any, **_kwargs: Any) -> Any:
        if query == "janitor_inventory_slice":
            self.slice_args.append(args)
            val = self._pages[self._i][0]
            if isinstance(val, Exception):
                raise val
            return val
        self.filter_args.append(args)
        val = self._pages[self._i][1]
        self._i += 1
        if isinstance(val, Exception):
            raise val
        return val


@pytest.fixture(autouse=True)
def _config(monkeypatch: pytest.MonkeyPatch) -> None:
    janitor.config.janitor_sql_max_deletes_per_cycle = 50000
    janitor.config.janitor_sql_page_size = 1000
    janitor.config.janitor_concurrency = 4
    janitor.config.fs_cache_gc_max_age_seconds = 86400
    janitor.config.fs_cache_hot_retention_seconds = 10800
    janitor.config.upload_backends = ["arion"]
    janitor.config.backup_backends = []
    # get_query returns the NAME so the fake conn can route slice-vs-filter; the flow is under test,
    # not the SQL text.
    monkeypatch.setattr(janitor, "get_query", lambda name: name)
    monkeypatch.setattr(janitor, "get_janitor_state", AsyncMock(return_value=None))
    monkeypatch.setattr(janitor, "set_janitor_state", AsyncMock())
    monkeypatch.setattr(janitor, "clear_cached", AsyncMock())
    monkeypatch.setattr(janitor, "get_all_dlq_object_ids", AsyncMock(return_value=set()))
    monkeypatch.setattr(janitor, "_janitor_deleted_counter", MagicMock())


def _conn(*pages: tuple[Any, Any]) -> _RouterConn:
    """Build a router conn from (slice_rows, cand_rows) pages. For the common case where every slice
    row is also a candidate, pass the same list for both. Seed read-recency via a row's
    last_access_at (it rides on the candidate row now, not a separate fetchval)."""
    return _RouterConn(list(pages))


def _same(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """A page whose slice rows are all candidates (slice == filter output)."""
    return (rows, rows)


async def _evict(conn: _RouterConn, fs: _FakeFs, *, pressure: int = 0) -> int:
    return await janitor.evict_from_inventory(_Pool(conn), fs, MagicMock(), pressure=pressure)


# ------------------------------------------------------------------- (a) stale row


@pytest.mark.asyncio
async def test_stat_miss_clears_row_and_does_not_delete(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    fs = _FakeFs(stat_map={})  # stat_part → None (dir gone)
    conn = _conn(_same([_row(OID_A)]), ([], []))

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
    conn = _conn(_same([_row(OID_A)]), ([], []))

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
    conn = _conn(_same([_row(OID_A)]), ([], []))

    deleted = await _evict(conn, fs)

    assert deleted == 0
    assert fs.deleted == []
    janitor.clear_cached.assert_not_awaited()  # prefilter/gate divergence is survivable — row kept


# ------------------------------------------------------------------- (d) delete


@pytest.mark.asyncio
async def test_replicated_cold_candidate_is_deleted_cleared_and_counted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    fs = _FakeFs(stat_map={(OID_A, 1, 1): _stat(COLD)})
    conn = _conn(_same([_row(OID_A)]), ([], []))

    deleted = await _evict(conn, fs)

    assert deleted == 1
    assert fs.deleted == [(OID_A, 1, 1)]
    janitor.clear_cached.assert_awaited_once()
    janitor._janitor_deleted_counter.add.assert_called_once_with(1, attributes={"reason": "sql_evict"})


@pytest.mark.asyncio
async def test_recently_read_candidate_is_protected_via_last_access(monkeypatch: pytest.MonkeyPatch) -> None:
    # atime is COLD (the read path no longer touches it), but the candidate row's
    # last_access_at is fresh — hot retention must protect the part.
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    fs = _FakeFs(stat_map={(OID_A, 1, 1): _stat(COLD)})
    conn = _conn(_same([_row(OID_A, last_access_at=datetime.now(timezone.utc))]), ([], []))

    deleted = await _evict(conn, fs)

    assert deleted == 0
    assert fs.deleted == []


@pytest.mark.asyncio
async def test_stale_last_access_does_not_pin_and_part_is_evicted(monkeypatch: pytest.MonkeyPatch) -> None:
    # A STALE last_access_at (read long ago) must NOT pin the part forever: atime is COLD and the
    # recorded read is older than the hot window, so recency lets the part fall through to the
    # replication gate and be evicted. Guards against a stale recency value becoming a permanent pin.
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    fs = _FakeFs(stat_map={(OID_A, 1, 1): _stat(COLD)})
    stale = datetime(2020, 1, 1, tzinfo=timezone.utc)
    conn = _conn(_same([_row(OID_A, last_access_at=stale)]), ([], []))

    deleted = await _evict(conn, fs)

    assert deleted == 1
    assert fs.deleted == [(OID_A, 1, 1)]


@pytest.mark.asyncio
async def test_delete_still_counted_when_clear_cached_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    # The narrow try around clear_cached: the delete already happened, so it must still count and
    # the row self-heals on the next stat-miss rather than the delete being lost.
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    monkeypatch.setattr(janitor, "clear_cached", AsyncMock(side_effect=RuntimeError("db blip")))
    fs = _FakeFs(stat_map={(OID_A, 1, 1): _stat(COLD)})
    conn = _conn(_same([_row(OID_A)]), ([], []))

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
    conn = _conn(_same([_row(OID_A)]), ([], []))

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
    conn = _conn(_same([_row(OID_A)]), ([], []))

    deleted = await _evict(conn, fs)

    assert deleted == 1  # fail-open: replicated part is safe to evict regardless of DLQ
    assert fs.deleted == [(OID_A, 1, 1)]


# ------------------------------------------------------------------- (f) cursor


@pytest.mark.asyncio
async def test_cursor_advances_to_last_slice_row_after_page(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    janitor.config.janitor_sql_page_size = 2
    last_ts = datetime(2021, 6, 1, tzinfo=timezone.utc)
    page = [_row(OID_A, pn=1), _row(OID_B, pn=2, cached_at=last_ts)]
    fs = _FakeFs(stat_map={(OID_A, 1, 1): _stat(COLD), (OID_B, 1, 2): _stat(COLD)})
    conn = _conn(_same(page), ([], []))

    await _evict(conn, fs)

    # A full page (len == page_size) does not short-circuit; the cursor is persisted at its LAST
    # SLICE row before the next fetch, so a crash resumes strictly after it.
    saved = [c.args for c in janitor.set_janitor_state.await_args_list]
    assert (
        "sql_evict_cursor",
        {"cached_at": last_ts.isoformat(), "object_id": OID_B, "object_version": 1, "part_number": 2},
    ) == saved[0][1:]


@pytest.mark.asyncio
async def test_zero_candidate_slice_still_advances_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    # THE STALL FIX. A full slice whose filter returns ZERO candidates (the prod shape: a head of
    # millions of non-replicated parts) must STILL advance the cursor to the slice's last row — the
    # old design re-pinned the cursor here and the phase froze forever. Nothing is deleted, but the
    # ring moves on so the next cycle scans past this window.
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    janitor.config.janitor_sql_page_size = 2
    last_ts = datetime(2021, 6, 1, tzinfo=timezone.utc)
    non_candidate_slice = [_row(OID_A, pn=1), _row(OID_B, pn=2, cached_at=last_ts)]
    fs = _FakeFs()  # never stat'd — the filter returned no candidates at all
    conn = _conn((non_candidate_slice, []), ([], []))

    deleted = await _evict(conn, fs)

    assert deleted == 0
    assert fs.deleted == []
    # The filter ran over the slice's tuples (arrays passed), and the cursor advanced to slice end.
    assert conn.filter_args[0][0] == [OID_A, OID_B]  # object_ids array = the slice's tuples
    saved = [c.args[1:] for c in janitor.set_janitor_state.await_args_list]
    assert saved[0] == (
        "sql_evict_cursor",
        {"cached_at": last_ts.isoformat(), "object_id": OID_B, "object_version": 1, "part_number": 2},
    )


@pytest.mark.asyncio
async def test_all_non_candidate_pages_walk_the_ring_and_wrap(monkeypatch: pytest.MonkeyPatch) -> None:
    # Multiple consecutive all-non-candidate slices must each advance the cursor and then the empty
    # slice wraps the ring — proving forward progress across a long run of non-candidates, not a stall.
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    janitor.config.janitor_sql_page_size = 1
    ts_a = datetime(2021, 1, 1, tzinfo=timezone.utc)
    ts_b = datetime(2021, 2, 1, tzinfo=timezone.utc)
    fs = _FakeFs()
    conn = _conn(
        ([_row(OID_A, cached_at=ts_a)], []),
        ([_row(OID_B, cached_at=ts_b)], []),
        ([], []),  # ring exhausted → wrap
    )

    deleted = await _evict(conn, fs)

    assert deleted == 0
    assert len(conn.slice_args) == 3  # two candidate-less pages walked, then the empty page
    saved_values = [c.args[2] for c in janitor.set_janitor_state.await_args_list]
    assert saved_values == [
        {"cached_at": ts_a.isoformat(), "object_id": OID_A, "object_version": 1, "part_number": 1},
        {"cached_at": ts_b.isoformat(), "object_id": OID_B, "object_version": 1, "part_number": 1},
        {},  # ring wrap
    ]


@pytest.mark.asyncio
async def test_empty_slice_wraps_cursor_to_start(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    fs = _FakeFs()
    conn = _conn(([], []))

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
    # A restored cursor: the ISO cached_at is parsed BACK to a datetime for the native $2 param.
    parsed = janitor._load_evict_cursor(
        {"cached_at": "2021-06-01T00:00:00+00:00", "object_id": OID_A, "object_version": 3, "part_number": 7}
    )
    assert parsed == (datetime(2021, 6, 1, tzinfo=timezone.utc), OID_A, 3, 7)
    assert isinstance(parsed[0], datetime)  # never a str — asyncpg would reject it for $2


@pytest.mark.asyncio
async def test_malformed_stored_cursor_does_not_crash_phase(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    monkeypatch.setattr(janitor, "get_janitor_state", AsyncMock(return_value={"object_version": object()}))
    fs = _FakeFs()
    conn = _conn(([], []))

    deleted = await _evict(conn, fs)  # must not raise

    assert deleted == 0
    # Fell back to the epoch start sentinel: the SLICE query binds ($1 page_size, $2-$5 cursor) with a
    # real datetime for $2 (never None or a str).
    assert conn.slice_args[0] == (1000, datetime(1970, 1, 1, tzinfo=timezone.utc), "", 0, 0)


@pytest.mark.asyncio
async def test_slice_cursor_cached_at_is_a_datetime_on_fresh_start(monkeypatch: pytest.MonkeyPatch) -> None:
    # $2 of the slice query is a NATIVE timestamptz param — asyncpg rejects a str — so the cold-start
    # sentinel must be a datetime (epoch), never the string '-infinity'.
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    fs = _FakeFs()
    conn = _conn(([], []))

    await _evict(conn, fs)

    cached_at_param = conn.slice_args[0][1]  # $2
    assert isinstance(cached_at_param, datetime)
    assert cached_at_param == datetime(1970, 1, 1, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_slice_cursor_cached_at_is_a_datetime_on_restore(monkeypatch: pytest.MonkeyPatch) -> None:
    # A resumed cursor: the stored ISO string is parsed back to a datetime before it reaches $2.
    stored = {"cached_at": "2021-06-01T00:00:00+00:00", "object_id": OID_A, "object_version": 3, "part_number": 7}
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    monkeypatch.setattr(janitor, "get_janitor_state", AsyncMock(return_value=stored))
    fs = _FakeFs()
    conn = _conn(([], []))

    await _evict(conn, fs)

    args = conn.slice_args[0]
    assert isinstance(args[1], datetime)
    assert args == (1000, datetime(2021, 6, 1, tzinfo=timezone.utc), OID_A, 3, 7)


# ------------------------------------------------------------------- (g) kill switch


@pytest.mark.asyncio
async def test_kill_switch_issues_no_queries(monkeypatch: pytest.MonkeyPatch) -> None:
    janitor.config.janitor_sql_max_deletes_per_cycle = 0
    repl = AsyncMock(return_value=True)
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", repl)
    fs = _FakeFs()
    conn = _conn(_same([_row(OID_A)]))

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
    conn = _conn(_same([_row(OID_A)]), ([], []))

    await _evict(conn, fs, pressure=1)

    # ignore_age is $7 of the FILTER query: (object_ids, versions, part_numbers, backup, upload, max_age, ignore_age)
    assert conn.filter_args[0][6] is True


@pytest.mark.asyncio
async def test_no_pressure_passes_ignore_age_false(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    fs = _FakeFs(stat_map={(OID_A, 1, 1): _stat(COLD)})
    conn = _conn(_same([_row(OID_A)]), ([], []))

    await _evict(conn, fs, pressure=0)

    assert conn.filter_args[0][6] is False


@pytest.mark.asyncio
async def test_filter_binds_slice_tuples_as_arrays(monkeypatch: pytest.MonkeyPatch) -> None:
    # The filter is fed exactly the slice's tuples, as three parallel arrays in (oids, versions,
    # part_numbers) order, followed by backup, upload, max_age, ignore_age.
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    janitor.config.backup_backends = ["ovh"]
    janitor.config.upload_backends = ["arion"]
    slice_rows = [_row(OID_A, ov=2, pn=5), _row(OID_B, ov=3, pn=9)]
    fs = _FakeFs()
    conn = _conn((slice_rows, []), ([], []))

    await _evict(conn, fs)

    assert conn.filter_args[0] == (
        [OID_A, OID_B],
        [2, 3],
        [5, 9],
        ["ovh"],
        ["arion"],
        86400,
        False,
    )


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
    conn = _conn(_same(full), _same([_row(OID_B)]))

    deleted = await _evict(conn, fs)

    assert deleted == 3  # whole page drains; budget is a page-boundary gate, not per-item
    assert len(conn.slice_args) == 1  # budget reached → no second slice fetched


# ------------------------------------------------------------------- query timeout


@pytest.mark.asyncio
async def test_slice_timeout_ends_cycle_without_moving_cursor(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # A slice scan that times out (only reachable on a genuinely degraded DB now the scan is bounded)
    # must end discovery with the cursor UNMOVED: deletes-so-far are returned, the cursor is neither
    # advanced nor reset to {}, so the next cycle resumes from exactly here — and it never crashes.
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    janitor.config.janitor_sql_page_size = 1
    page1_ts = datetime(2021, 1, 1, tzinfo=timezone.utc)
    fs = _FakeFs(stat_map={(OID_A, 1, 1): _stat(COLD)})
    conn = _conn(_same([_row(OID_A, cached_at=page1_ts)]), (asyncio.TimeoutError(), None))

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
async def test_slice_timeout_on_first_page_leaves_cursor_untouched(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # Slice timeout on the very first page: nothing deleted and set_janitor_state is never called, so
    # the durable cursor stays wherever the previous cycle left it in janitor_state.
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    fs = _FakeFs()
    conn = _conn((asyncio.TimeoutError(), None))

    with caplog.at_level(logging.WARNING):
        deleted = await _evict(conn, fs)

    assert deleted == 0
    janitor.set_janitor_state.assert_not_awaited()  # cursor untouched
    assert "timed out" in caplog.text


@pytest.mark.asyncio
async def test_filter_timeout_ends_cycle_without_moving_cursor(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # The filter query times out AFTER a successful slice: same semantics — no delete, cursor NOT
    # advanced (nothing processed), phase ends cleanly.
    monkeypatch.setattr(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True))
    janitor.config.janitor_sql_page_size = 1
    fs = _FakeFs()
    conn = _conn(([_row(OID_A)], asyncio.TimeoutError()))

    with caplog.at_level(logging.WARNING):
        deleted = await _evict(conn, fs)

    assert deleted == 0
    janitor.set_janitor_state.assert_not_awaited()  # slice succeeded but filter died → no advance
    assert len(conn.slice_args) == 1
    assert len(conn.filter_args) == 1
    assert "timed out" in caplog.text
