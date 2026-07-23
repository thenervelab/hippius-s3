"""Guards for the partless-MPU sweep — a script that DELETEs, so its brakes must work.

The two that matter: a run without --yes must not issue a DELETE at all, and a DLQ-protected
object must be excluded from the ids handed to one. Everything else in the script is I/O.
"""

from __future__ import annotations

import argparse
import uuid
from typing import Any

import pytest

from hippius_s3.scripts import sweep_partless_multipart_uploads as sweep


class FakeConnection:
    def __init__(self, batches: list[list[dict[str, Any]]]) -> None:
        self._batches = batches
        self.fetched: list[tuple[str, tuple]] = []
        self.executed: list[tuple[str, tuple]] = []

    async def fetch(self, sql: str, *params: Any) -> list[dict[str, Any]]:
        self.fetched.append((sql, params))
        return self._batches.pop(0) if self._batches else []

    async def execute(self, sql: str, *params: Any) -> str:
        self.executed.append((sql, params))
        return "DELETE"

    async def fetchval(self, sql: str, *params: Any) -> int:
        self.fetched.append((sql, params))
        return 0


class FakePool:
    def __init__(self, conn: FakeConnection) -> None:
        self.conn = conn
        self.closed = False

    def acquire(self) -> Any:
        conn = self.conn

        class _Ctx:
            async def __aenter__(self) -> FakeConnection:
                return conn

            async def __aexit__(self, *_: object) -> None:
                return None

        return _Ctx()

    async def close(self) -> None:
        self.closed = True


def _args(**overrides: Any) -> argparse.Namespace:
    base = dict(
        stale_seconds=3600,
        batch_size=10,
        max_batches=1,
        sleep_between=0.0,
        statement_timeout_ms=60_000,
        count=False,
        skip_dlq_check=True,
        yes=False,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


def _deletes(conn: FakeConnection) -> list[tuple[str, tuple]]:
    return [op for op in conn.executed if "DELETE" in op[0]]


@pytest.mark.asyncio
async def test_a_run_without_yes_issues_no_delete(monkeypatch: pytest.MonkeyPatch) -> None:
    """The dry run is the default, and it must be a genuine no-op against the database."""
    conn = FakeConnection([[{"upload_id": uuid.uuid4(), "object_id": uuid.uuid4()} for _ in range(3)]])

    async def fake_create_pool(*_a: Any, **_kw: Any) -> FakePool:
        return FakePool(conn)

    monkeypatch.setattr(sweep.asyncpg, "create_pool", fake_create_pool)

    assert await sweep.main_async(_args(yes=False)) == 0

    assert _deletes(conn) == [], "a dry run must not delete"


@pytest.mark.asyncio
async def test_dlq_protected_objects_are_never_deleted(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mirrors the janitor/reaper gate: an object with an in-flight DLQ entry is spared."""
    protected_id = uuid.uuid4()
    keep = uuid.uuid4()
    rows = [
        {"upload_id": uuid.uuid4(), "object_id": protected_id},
        {"upload_id": keep, "object_id": uuid.uuid4()},
    ]
    conn = FakeConnection([rows])

    async def fake_create_pool(*_a: Any, **_kw: Any) -> FakePool:
        return FakePool(conn)

    async def fake_protected(_config: Any) -> set[str]:
        return {str(protected_id)}

    monkeypatch.setattr(sweep.asyncpg, "create_pool", fake_create_pool)
    monkeypatch.setattr(sweep, "_dlq_protected_ids", fake_protected)

    assert await sweep.main_async(_args(yes=True, skip_dlq_check=False)) == 0

    deletes = _deletes(conn)
    assert len(deletes) == 1
    targets = deletes[0][1][0]
    assert targets == [keep], "only the unprotected upload is deleted"


@pytest.mark.asyncio
async def test_an_empty_batch_ends_the_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """Nothing left to sweep terminates rather than spinning on an empty query."""
    conn = FakeConnection([[]])

    async def fake_create_pool(*_a: Any, **_kw: Any) -> FakePool:
        return FakePool(conn)

    monkeypatch.setattr(sweep.asyncpg, "create_pool", fake_create_pool)

    assert await sweep.main_async(_args(yes=True, max_batches=0)) == 0

    assert _deletes(conn) == []


@pytest.mark.asyncio
async def test_every_statement_is_bounded_by_a_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """This script cleans up after an unbounded statement that pinned the xmin horizon for 96
    minutes and stopped VACUUM database-wide. It must not be capable of repeating that."""
    captured: dict = {}
    conn = FakeConnection([[]])

    async def fake_create_pool(*_a: Any, **kwargs: Any) -> FakePool:
        captured.update(kwargs)
        return FakePool(conn)

    monkeypatch.setattr(sweep.asyncpg, "create_pool", fake_create_pool)

    await sweep.main_async(_args(yes=True))

    assert captured.get("command_timeout"), "no client-side ceiling on a statement"
    assert captured.get("server_settings", {}).get("statement_timeout"), "no server-side ceiling"
