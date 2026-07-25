"""Unit tests for the janitor's hard-delete RING (gc_soft_deleted_objects).

The phase keyset-pages ALL soft-deleted candidates past grace — ready and not — via a durable
(deleted_at, object_id) cursor, hard-deletes only the ready rows through the guarded delete, and
advances the cursor over the WHOLE slice. These tests pin the seam the ring exists to fix: a
permanently-unready head no longer blocks the batch, because the cursor steps past it whether or not
anything was deleted. Everything below the SQL is faked (get_query, conn.fetch, conn.execute, and the
janitor_state get/set are patched), so the tests exercise the Python control flow, not the query.
"""

from __future__ import annotations

from datetime import datetime
from datetime import timezone
from unittest.mock import AsyncMock

import pytest

from workers import run_janitor_in_loop as janitor


OID_A = "aaaaaaaa-0000-0000-0000-000000000001"
OID_B = "bbbbbbbb-0000-0000-0000-000000000002"
OID_C = "cccccccc-0000-0000-0000-000000000003"
NIL = "00000000-0000-0000-0000-000000000000"
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _row(oid: str, ready: bool, deleted_at: datetime | None = None) -> dict:
    return {
        "object_id": oid,
        "ready": ready,
        "deleted_at": deleted_at or datetime(2020, 1, 1, tzinfo=timezone.utc),
    }


class _Pool:
    """Hands out one shared conn for every acquire() — gc_soft_deleted_objects takes exactly one."""

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
    janitor.config.janitor_hard_delete_batch = 30000
    monkeypatch.setattr(janitor, "get_query", lambda name: f"SQL::{name}")
    monkeypatch.setattr(janitor, "get_janitor_state", AsyncMock(return_value=None))
    monkeypatch.setattr(janitor, "set_janitor_state", AsyncMock())


def _conn(rows: list[dict], *, delete_tag: str = "DELETE 1") -> AsyncMock:
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=rows)
    conn.execute = AsyncMock(return_value=delete_tag)
    return conn


async def _gc(conn: AsyncMock) -> int:
    return await janitor.gc_soft_deleted_objects(_Pool(conn))


def _saved_cursor() -> dict:
    return janitor.set_janitor_state.await_args.args[2]


# ------------------------------------------------------------------- (a) clogged head


@pytest.mark.asyncio
async def test_all_unready_slice_deletes_nothing_but_advances_cursor() -> None:
    # THE regression test: a slice that is entirely unready (the CopyObject-destination head that
    # used to wedge the oldest slot forever) deletes nothing, issues no guarded delete, and STILL
    # advances the cursor past the whole slice so the next cycle moves on.
    last_ts = datetime(2021, 3, 3, tzinfo=timezone.utc)
    conn = _conn([_row(OID_A, ready=False), _row(OID_B, ready=False), _row(OID_C, ready=False, deleted_at=last_ts)])

    deleted = await _gc(conn)

    assert deleted == 0
    conn.execute.assert_not_awaited()  # no ready rows -> no guarded delete attempted
    assert _saved_cursor() == {"deleted_at": last_ts.isoformat(), "object_id": OID_C}  # stepped past the head


# ------------------------------------------------------------------- (b) ready behind head


@pytest.mark.asyncio
async def test_ready_rows_behind_unready_head_are_deleted_next_page(monkeypatch: pytest.MonkeyPatch) -> None:
    # Two cycles sharing a stateful janitor_state: page 1 is the unready head (cursor advances, nothing
    # deleted); page 2 resumes at that cursor and finds the ready rows that were behind it.
    store: dict[str, dict] = {}
    monkeypatch.setattr(janitor, "get_janitor_state", AsyncMock(side_effect=lambda db, k: store.get(k)))

    async def _set(db, k, v):  # type: ignore[no-untyped-def]
        store[k] = v

    monkeypatch.setattr(janitor, "set_janitor_state", AsyncMock(side_effect=_set))

    head_ts = datetime(2021, 1, 1, tzinfo=timezone.utc)
    conn1 = _conn([_row(OID_A, ready=False, deleted_at=head_ts)])
    assert await _gc(conn1) == 0
    assert store["hard_delete_cursor"] == {"deleted_at": head_ts.isoformat(), "object_id": OID_A}

    tail_ts = datetime(2021, 2, 2, tzinfo=timezone.utc)
    conn2 = _conn([_row(OID_B, ready=True, deleted_at=tail_ts)])
    assert await _gc(conn2) == 1
    # Page 2 resumed strictly after page 1's cursor...
    assert conn2.fetch.await_args.args[2:] == (head_ts, OID_A)
    conn2.execute.assert_awaited_once_with("SQL::hard_delete_object", OID_B)
    assert store["hard_delete_cursor"] == {"deleted_at": tail_ts.isoformat(), "object_id": OID_B}


# ------------------------------------------------------------------- (c) empty wrap


@pytest.mark.asyncio
async def test_empty_slice_wraps_cursor_to_start() -> None:
    conn = _conn([])

    deleted = await _gc(conn)

    assert deleted == 0
    conn.execute.assert_not_awaited()
    janitor.set_janitor_state.assert_awaited_once()
    assert _saved_cursor() == {}  # ring wrap: next cycle restarts at the head


# ------------------------------------------------------------------- (d) malformed cursor


def test_load_hard_delete_cursor_falls_back_to_start() -> None:
    assert janitor._load_hard_delete_cursor(None) == (EPOCH, NIL)
    assert janitor._load_hard_delete_cursor({}) == (EPOCH, NIL)
    # Missing key / non-parseable value must not crash — degrade to the ring start.
    assert janitor._load_hard_delete_cursor({"object_id": OID_A}) == (EPOCH, NIL)
    assert janitor._load_hard_delete_cursor({"deleted_at": "not-a-timestamp", "object_id": OID_A}) == (EPOCH, NIL)
    # A restored cursor: the ISO deleted_at is parsed BACK to a datetime for the native $2 param.
    parsed = janitor._load_hard_delete_cursor({"deleted_at": "2021-06-01T00:00:00+00:00", "object_id": OID_A})
    assert parsed == (datetime(2021, 6, 1, tzinfo=timezone.utc), OID_A)
    assert isinstance(parsed[0], datetime)  # never a str — asyncpg would reject it for $2


@pytest.mark.asyncio
async def test_malformed_stored_cursor_does_not_crash_and_uses_start(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(janitor, "get_janitor_state", AsyncMock(return_value={"deleted_at": object()}))
    conn = _conn([])

    deleted = await _gc(conn)  # must not raise

    assert deleted == 0
    # Fell back to the epoch/nil-uuid start sentinel for $2/$3 (a real datetime + uuid string).
    assert conn.fetch.await_args.args[1:] == (janitor.config.janitor_hard_delete_batch, EPOCH, NIL)


# ------------------------------------------------------------------- (e) DELETE 0 skip


@pytest.mark.asyncio
async def test_guarded_delete_zero_counted_as_skipped_and_cursor_advances() -> None:
    # A row the finder marked ready but that the atomic guard no longer agrees on (revived/in-flight):
    # "DELETE 0" is counted as skipped, not deleted, and the cursor STILL advances past it.
    last_ts = datetime(2021, 5, 5, tzinfo=timezone.utc)
    conn = _conn([_row(OID_A, ready=True, deleted_at=last_ts)], delete_tag="DELETE 0")

    deleted = await _gc(conn)

    assert deleted == 0
    conn.execute.assert_awaited_once_with("SQL::hard_delete_object", OID_A)  # guarded delete WAS attempted
    assert _saved_cursor() == {"deleted_at": last_ts.isoformat(), "object_id": OID_A}  # advanced despite skip


# ------------------------------------------------------------------- ready row deleted


@pytest.mark.asyncio
async def test_ready_row_is_deleted_and_cursor_advances() -> None:
    last_ts = datetime(2021, 7, 7, tzinfo=timezone.utc)
    conn = _conn([_row(OID_A, ready=False), _row(OID_B, ready=True, deleted_at=last_ts)])

    deleted = await _gc(conn)

    assert deleted == 1
    conn.execute.assert_awaited_once_with("SQL::hard_delete_object", OID_B)  # only the ready row
    assert _saved_cursor() == {"deleted_at": last_ts.isoformat(), "object_id": OID_B}


@pytest.mark.asyncio
async def test_fresh_start_cursor_is_epoch_and_nil_uuid() -> None:
    conn = _conn([])

    await _gc(conn)

    # $2 is a native timestamptz (asyncpg rejects a str) and $3 a uuid — cold start must be (epoch, nil).
    assert conn.fetch.await_args.args[1:] == (janitor.config.janitor_hard_delete_batch, EPOCH, NIL)
