"""Reusable fake asyncpg pool/connection for unit tests.

Models the two patterns the code uses:
  - direct ``pool.execute/fetch/fetchrow/fetchval``
  - ``async with pool.acquire(timeout=...) as conn, conn.transaction(): ...``

Every call and lifecycle event is appended to a shared ordered ``events`` log so
tests can assert *which connection* ran a query and *whether it was inside a
transaction* — the invariants the connection-scoping refactor depends on.

Return values are resolved by an optional ``router(method, query, args)``
callable; the default returns ``None`` (fine for ``execute``). Tests that need
``fetchrow``/``fetchval`` results either pass a router or, more commonly, patch
the higher-level helper functions (``upsert_object_basic`` etc.) directly.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any
from typing import Callable
from typing import Optional


Router = Callable[[str, Optional[str], tuple], Any]


class FakeConn:
    def __init__(self, recorder: "_Recorder", conn_id: int, router: Router) -> None:
        self._rec = recorder
        self.conn_id = conn_id
        self._router = router
        self.in_transaction = False

    async def _call(self, method: str, query: str, args: tuple) -> Any:
        self._rec.events.append(
            {"method": method, "conn": self.conn_id, "query": query, "in_txn": self.in_transaction, "args": args}
        )
        return self._router(method, query, args)

    async def execute(self, query: str, *args: Any) -> Any:
        return await self._call("execute", query, args)

    async def fetch(self, query: str, *args: Any) -> Any:
        return await self._call("fetch", query, args)

    async def fetchrow(self, query: str, *args: Any) -> Any:
        return await self._call("fetchrow", query, args)

    async def fetchval(self, query: str, *args: Any) -> Any:
        return await self._call("fetchval", query, args)

    def transaction(self) -> Any:
        rec = self._rec
        conn = self

        @asynccontextmanager
        async def _cm() -> Any:
            rec.events.append({"method": "txn_enter", "conn": conn.conn_id})
            conn.in_transaction = True
            try:
                yield
            finally:
                conn.in_transaction = False
                rec.events.append({"method": "txn_exit", "conn": conn.conn_id})

        return _cm()


class _Recorder:
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []
        self.acquire_count = 0


class FakePool:
    def __init__(self, router: Optional[Router] = None) -> None:
        self._rec = _Recorder()
        self._router: Router = router or (lambda method, query, args: None)
        self._next_id = 0

    # --- introspection helpers for assertions ---
    @property
    def events(self) -> list[dict[str, Any]]:
        return self._rec.events

    @property
    def acquire_count(self) -> int:
        return self._rec.acquire_count

    def calls(self, method: str | None = None) -> list[dict[str, Any]]:
        return [e for e in self._rec.events if method is None or e.get("method") == method]

    # --- direct pool-level calls (legacy / non-scoped usage) ---
    async def execute(self, query: str, *args: Any) -> Any:
        self._rec.events.append({"method": "execute", "conn": "pool", "query": query, "in_txn": False, "args": args})
        return self._router("execute", query, args)

    async def fetch(self, query: str, *args: Any) -> Any:
        self._rec.events.append({"method": "fetch", "conn": "pool", "query": query, "in_txn": False, "args": args})
        return self._router("fetch", query, args)

    async def fetchrow(self, query: str, *args: Any) -> Any:
        self._rec.events.append({"method": "fetchrow", "conn": "pool", "query": query, "in_txn": False, "args": args})
        return self._router("fetchrow", query, args)

    async def fetchval(self, query: str, *args: Any) -> Any:
        self._rec.events.append({"method": "fetchval", "conn": "pool", "query": query, "in_txn": False, "args": args})
        return self._router("fetchval", query, args)

    # --- scoped acquire ---
    def acquire(self, *, timeout: float | None = None) -> Any:
        self._rec.acquire_count += 1
        self._next_id += 1
        conn = FakeConn(self._rec, self._next_id, self._router)
        self._rec.events.append({"method": "acquire", "conn": conn.conn_id, "timeout": timeout})

        @asynccontextmanager
        async def _cm() -> Any:
            yield conn

        return _cm()


def make_fake_pool(router: Optional[Router] = None) -> FakePool:
    return FakePool(router)
