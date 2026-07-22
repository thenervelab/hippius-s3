"""HD-2: HEAD must not run the users INSERT-on-conflict write on every request.

`ensure_by_main_account` is a write path (WAL + conflict handling). On the highest-frequency read op
(HEAD) the row essentially always exists, so we add a read-first variant that only writes on a genuine
miss.
"""

from __future__ import annotations

from typing import Any

import pytest

from hippius_s3.repositories.users import UserRepository


class _FakeDB:
    def __init__(self, existing: Any) -> None:
        self._existing = existing
        self.reads: list[tuple[str, tuple]] = []
        self.writes: list[tuple[str, tuple]] = []

    async def fetchval(self, query: str, *args: Any) -> Any:
        self.reads.append((query, args))
        return self._existing

    async def fetchrow(self, query: str, *args: Any) -> Any:
        self.writes.append((query, args))
        return {"main_account_id": args[0]}


@pytest.mark.asyncio
async def test_read_first_skips_write_when_user_exists() -> None:
    db = _FakeDB(existing="acct-1")
    await UserRepository(db).ensure_by_main_account_read_first("acct-1")
    assert db.reads, "must issue the read"
    assert not db.writes, "existing user must not trigger the INSERT-on-conflict write"


@pytest.mark.asyncio
async def test_read_first_writes_on_miss() -> None:
    db = _FakeDB(existing=None)
    await UserRepository(db).ensure_by_main_account_read_first("acct-1")
    assert db.reads, "must issue the read"
    assert db.writes, "a genuine miss must create the user"
