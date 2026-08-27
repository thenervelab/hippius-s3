"""Unit tests for the janitor's version-reap RING (reap_deleted_object_versions).

The twin of test_janitor_hard_delete_ring.py, which this deliberately mirrors. The readiness SQL is
covered against real Postgres in tests/integration/test_version_reap_guard_sql.py; the Python
control flow around it was not covered anywhere, and that is where the ring's whole purpose lives:

  - reap only the READY rows, but advance the cursor over the WHOLE slice, so a permanently-unready
    head cannot wedge the oldest slot forever (head-of-line block);
  - an EMPTY slice wraps the ring back to the start, otherwise the cursor parks past the last row
    and the sweep never revisits anything.

A cursor that only advances over rows it actually reaped reintroduces exactly the block the ring was
built to remove, and it does so silently — the sweep keeps running, keeps logging, and keeps
reaping nothing.

Everything below the SQL is faked (get_query, conn.fetch/execute, janitor_state get/set), so these
exercise the Python, not the query.
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


def _row(oid: str, version: int, ready: bool, deleted_at: datetime | None = None) -> dict:
    return {
        "object_id": oid,
        "object_version": version,
        "ready": ready,
        "deleted_at": deleted_at or datetime(2020, 1, 1, tzinfo=timezone.utc),
    }


class _Pool:
    """Hands out one shared conn for every acquire() — the phase takes exactly one."""

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


async def _reap(conn: AsyncMock) -> int:
    return await janitor.reap_deleted_object_versions(_Pool(conn))


def _saved_cursor() -> dict:
    return janitor.set_janitor_state.await_args.args[2]


# ------------------------------------------------------------------- clogged head


@pytest.mark.asyncio
async def test_all_unready_slice_reaps_nothing_but_advances_the_cursor() -> None:
    """THE regression test. A slice that is entirely unready — versions whose upload has not landed,
    so they have no confirmed-gone backend copies — must reap nothing, issue no guarded delete, and
    still step the cursor past the whole slice."""
    last = datetime(2021, 3, 3, tzinfo=timezone.utc)
    conn = _conn(
        [
            _row(OID_A, 1, ready=False),
            _row(OID_B, 2, ready=False),
            _row(OID_C, 7, ready=False, deleted_at=last),
        ]
    )

    reaped = await _reap(conn)

    assert reaped == 0
    conn.execute.assert_not_awaited()
    assert _saved_cursor() == {
        "deleted_at": last.isoformat(),
        "object_id": OID_C,
        "object_version": 7,
    }


@pytest.mark.asyncio
async def test_cursor_advances_past_an_unready_TAIL_not_just_the_last_reaped_row() -> None:
    """The subtle half: the cursor must come from the last row of the SLICE, not the last row we
    reaped. Taking it from the last reaped row leaves the unready tail to be re-fetched every cycle,
    which is the head-of-line block again one position along."""
    tail = datetime(2022, 5, 5, tzinfo=timezone.utc)
    conn = _conn([_row(OID_A, 1, ready=True), _row(OID_B, 4, ready=False, deleted_at=tail)])

    reaped = await _reap(conn)

    assert reaped == 1
    assert _saved_cursor() == {"deleted_at": tail.isoformat(), "object_id": OID_B, "object_version": 4}


# ------------------------------------------------------------------- ring wrap


@pytest.mark.asyncio
async def test_an_empty_slice_wraps_the_ring_to_the_head() -> None:
    """Without the wrap the cursor stays parked past the newest row and the sweep, having reached
    the end once, never reaps anything again."""
    conn = _conn([])

    reaped = await _reap(conn)

    assert reaped == 0
    conn.execute.assert_not_awaited()
    assert _saved_cursor() == {}, "an empty slice must reset the cursor, not leave it at the end"


# ------------------------------------------------------------------- guarded delete


@pytest.mark.asyncio
async def test_only_ready_rows_are_offered_to_the_guarded_delete() -> None:
    conn = _conn([_row(OID_A, 1, ready=True), _row(OID_B, 2, ready=False), _row(OID_C, 3, ready=True)])

    reaped = await _reap(conn)

    assert reaped == 2
    reaped_keys = {(c.args[1], c.args[2]) for c in conn.execute.await_args_list}
    assert reaped_keys == {(OID_A, 1), (OID_C, 3)}


@pytest.mark.asyncio
async def test_a_guarded_delete_that_matches_nothing_is_not_counted_as_reaped() -> None:
    """`DELETE 0` means the version became unready between the find and the delete — a lagging
    upload landed. It must not be counted, or the metric reports work that did not happen."""
    conn = _conn([_row(OID_A, 1, ready=True), _row(OID_C, 3, ready=True)], delete_tag="DELETE 0")

    assert await _reap(conn) == 0


@pytest.mark.asyncio
async def test_one_failing_row_does_not_abort_the_slice_or_the_cursor() -> None:
    """A per-row failure is logged and stepped over; the remaining ready rows still reap and the
    cursor still advances, so one poison row cannot wedge the ring."""
    last = datetime(2023, 7, 7, tzinfo=timezone.utc)
    conn = _conn([_row(OID_A, 1, ready=True), _row(OID_C, 3, ready=True, deleted_at=last)])
    conn.execute = AsyncMock(side_effect=[RuntimeError("deadlock detected"), "DELETE 1"])

    reaped = await _reap(conn)

    assert reaped == 1
    assert _saved_cursor() == {"deleted_at": last.isoformat(), "object_id": OID_C, "object_version": 3}
