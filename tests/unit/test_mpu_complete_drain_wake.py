"""CompleteMultipartUpload must wake the drain's defer backoff for the completed version.

Parts of an in-progress MPU sit in cephor_replication_status under exponential defer backoff
(up to the cap) because their upload enqueue is not ready until the object address is written
at complete. Completion is the wake signal: the handler clears deferred_until/defer_attempts
for the version's still-'pending' rows so the drain claims them on its next poll instead of
waiting out the cap. The wake is best-effort — the complete is already committed, so a wake
failure must not turn a successful completion into a 500.

These drive the real complete_multipart_upload handler with fakes at the DB boundary only
(handler connection + writer pool), mirroring tests/unit/api/test_multipart_abort_version.py.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Any

import pytest

from hippius_s3.api.s3 import multipart
from hippius_s3.cache import FileSystemPartsStore


# One uploaded part; the ETag hex prefix must be valid even-length hex for mpu_complete's
# combined-ETag computation (bytes.fromhex).
_PART_ROWS = [{"etag": "aabbccdd-1", "part_number": 1, "size_bytes": 100}]

_COMPLETE_XML = (
    "<CompleteMultipartUpload>"
    "<Part><PartNumber>1</PartNumber><ETag>&quot;aabbccdd-1&quot;</ETag></Part>"
    "</CompleteMultipartUpload>"
).replace("&quot;", '"')

# current_object_version (7) deliberately diverges from the upload's own parts version (3):
# the wake MUST key on the version the replication rows carry (parts.object_version), not the
# pointer a later same-key upload may have advanced.
_POINTER_VERSION = 7
_PARTS_VERSION = 3


class _FakeDb:
    """The handler's DB connection. Routes fetches by query NAME (multipart.get_query is
    monkeypatched to identity) and records/faults `execute` — the wake's only surface."""

    def __init__(self, *, execute_error: Exception | None = None) -> None:
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self._execute_error = execute_error

    async def fetchrow(self, query: str, *args: Any) -> Any:
        if query == "get_multipart_upload":
            return {
                "object_id": "obj-1",
                "object_key": "k",
                "is_completed": False,
                "current_object_version": _POINTER_VERSION,
            }
        if query == "get_bucket_by_name":
            return {"bucket_id": "bkt-1"}
        if query == "get_multipart_version_by_upload":
            return {"object_version": _PARTS_VERSION}
        raise AssertionError(f"unexpected fetchrow in complete flow: {query!r}")

    async def fetch(self, query: str, *args: Any) -> list[Any]:
        if query == "list_parts_for_version":
            return list(_PART_ROWS)
        raise AssertionError(f"unexpected fetch in complete flow: {query!r}")

    async def execute(self, query: str, *args: Any) -> None:
        if self._execute_error is not None:
            raise self._execute_error
        self.executed.append((query, args))


class _FakeTxn:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_exc: Any) -> bool:
        return False


class _FakeConn:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...]]] = []

    def transaction(self) -> _FakeTxn:
        return _FakeTxn()

    async def execute(self, query: str, *args: Any) -> None:
        self.executed.append((query, args))


class _FakeAcquire:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, *_exc: Any) -> bool:
        return False


class _FakePool:
    """Writer-side pool surface: mpu_complete gets db_parts passed through (MPU-3), so it
    never fetches; set_object_version_address runs one pool-level execute."""

    def __init__(self) -> None:
        self.conn = _FakeConn()
        self.executed: list[tuple[str, tuple[Any, ...]]] = []

    async def fetch(self, query: str, *_args: Any) -> list[Any]:
        raise AssertionError(f"db_parts supplied — mpu_complete must not re-read parts: {query!r}")

    async def execute(self, query: str, *args: Any) -> None:
        self.executed.append((query, args))

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self.conn)


def _fake_request(tmp_path: Any) -> Any:
    return SimpleNamespace(
        headers={"Host": "test"},
        state=SimpleNamespace(account=SimpleNamespace(main_account="5MainAcct")),
        app=SimpleNamespace(
            state=SimpleNamespace(
                postgres_pool=_FakePool(),
                redis_client=object(),
                fs_store=FileSystemPartsStore(str(tmp_path)),
            )
        ),
    )


async def _run_complete(monkeypatch: Any, tmp_path: Any, db: _FakeDb) -> Any:
    monkeypatch.setattr(multipart, "get_query", lambda name: name)

    async def _body(_request: Any) -> bytes:
        return _COMPLETE_XML.encode()

    monkeypatch.setattr(multipart, "get_request_body", _body)
    return await multipart.complete_multipart_upload("b", "k", "up-1", _fake_request(tmp_path), db)


def _wake_updates(db: _FakeDb) -> list[tuple[str, tuple[Any, ...]]]:
    return [(q, a) for q, a in db.executed if "cephor_replication_status" in q]


@pytest.mark.asyncio
async def test_complete_mpu_clears_drain_backoff(monkeypatch: Any, tmp_path: Any) -> None:
    """After a successful complete, exactly one wake UPDATE ran against the handler's
    connection, keyed on the upload's OWN parts version — resetting both deferred_until
    and defer_attempts, and touching only still-'pending' rows."""
    db = _FakeDb()

    resp = await _run_complete(monkeypatch, tmp_path, db)

    assert resp.status_code == 200
    wakes = _wake_updates(db)
    assert len(wakes) == 1, f"expected exactly one drain wake, got {db.executed!r}"
    query, params = wakes[0]
    assert "deferred_until = NULL" in query
    assert "defer_attempts = 0" in query
    assert "status = 'pending'" in query, "wake must never resurrect failed/replicated rows"
    assert params == ("obj-1", _PARTS_VERSION), "wake must target the upload's own parts version, not the pointer"


@pytest.mark.asyncio
async def test_wake_failure_does_not_fail_committed_complete(monkeypatch: Any, tmp_path: Any, caplog: Any) -> None:
    """The complete is already committed when the wake runs: a wake failure must be
    swallowed (WARNING with object/version context), never surfaced as a 500."""
    db = _FakeDb(execute_error=RuntimeError("db went away"))

    with caplog.at_level(logging.WARNING, logger="hippius_s3.api.s3.multipart"):
        resp = await _run_complete(monkeypatch, tmp_path, db)

    assert resp.status_code == 200, "wake failure must not fail the already-committed complete"
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING and "wake" in r.getMessage()]
    assert warnings, "wake failure must be logged at WARNING"
    assert "obj-1" in warnings[0].getMessage()
    assert str(_PARTS_VERSION) in warnings[0].getMessage()
