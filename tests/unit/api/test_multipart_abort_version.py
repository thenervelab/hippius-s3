"""Regression tests for MPU abort targeting the upload's OWN version.

The bug: abort derived object_version from objects.current_object_version. A simple PUT (or
another MPU) on the same key bumps that pointer to a fresh version, so aborting an earlier,
still-open MPU would run the destructive cleanup (fail_version_replication + fs delete) against
the NEWER, innocent version — corrupting a freshly-PUT object's replication + cache.

These model the divergence the prior tests never did (current_object_version != the upload's own
parts.object_version) and assert the destructive ops hit the upload's own version, or are skipped
when the upload has no parts of its own (never falling back to the pointer).
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from hippius_s3.api.s3 import multipart


class _FakeTxn:
    async def __aenter__(self) -> _FakeTxn:
        return self

    async def __aexit__(self, *_: Any) -> bool:
        return False


class _FakeDb:
    """Routes by query NAME (get_query is monkeypatched to identity), modelling the key fact:
    parts exist ONLY under the upload's own version, while current_object_version points elsewhere."""

    def __init__(self, *, current_version: int, upload_version: int | None) -> None:
        self.current_version = current_version
        self.upload_version = upload_version

    async def fetchrow(self, query: str, *args: Any) -> Any:
        if query == "get_multipart_upload":
            return {
                "object_id": "obj-1",
                "object_key": "k",
                "is_completed": False,
                "current_object_version": self.current_version,
            }
        if query == "get_multipart_version_by_upload":
            return {"object_version": self.upload_version} if self.upload_version is not None else None
        return None

    async def fetch(self, query: str, *args: Any) -> list[Any]:
        return []

    def transaction(self) -> _FakeTxn:
        return _FakeTxn()


def _fake_request(upload_id: str, *, fs_delete: Any, redis: Any) -> Any:
    return SimpleNamespace(
        query_params={"uploadId": upload_id},
        app=SimpleNamespace(state=SimpleNamespace(redis_client=redis, fs_store=SimpleNamespace(delete_object=fs_delete))),
    )


class _RedisStub:
    async def setex(self, *_: Any, **__: Any) -> None:
        return None


@pytest.mark.asyncio
async def test_abort_targets_uploads_own_version_not_current_pointer(monkeypatch: Any) -> None:
    """current_object_version=2 (advanced by a later PUT), but this upload's parts are at v1.
    The destructive cleanup MUST hit v1 — hitting v2 would corrupt the newer object."""
    monkeypatch.setattr(multipart, "get_query", lambda name: name)
    failed: dict[str, Any] = {}
    deleted: dict[str, Any] = {}

    async def fake_fail(_db: Any, *, object_id: str, object_version: int) -> None:
        failed["version"] = object_version

    async def fake_delete(object_id: str, object_version: int) -> None:
        deleted["version"] = object_version

    monkeypatch.setattr(multipart, "fail_version_replication", fake_fail)

    db = _FakeDb(current_version=2, upload_version=1)
    resp = await multipart.abort_multipart_upload(
        "b", "k", _fake_request("up-1", fs_delete=fake_delete, redis=_RedisStub()), db
    )

    assert resp.status_code == 204
    assert failed["version"] == 1, "fail_version_replication must target the upload's own version, not the pointer"
    assert deleted["version"] == 1, "fs delete must target the upload's own version, not the pointer"


@pytest.mark.asyncio
async def test_abort_with_no_parts_skips_destructive_cleanup(monkeypatch: Any) -> None:
    """An upload with no parts (init → overwrite-PUT → abort) must NOT fall back to
    current_object_version and run the destructive cleanup against the newer version."""
    monkeypatch.setattr(multipart, "get_query", lambda name: name)
    called = {"fail": False, "delete": False}

    async def fake_fail(_db: Any, **_: Any) -> None:
        called["fail"] = True

    async def fake_delete(*_: Any) -> None:
        called["delete"] = True

    monkeypatch.setattr(multipart, "fail_version_replication", fake_fail)

    db = _FakeDb(current_version=2, upload_version=None)
    resp = await multipart.abort_multipart_upload(
        "b", "k", _fake_request("up-1", fs_delete=fake_delete, redis=_RedisStub()), db
    )

    assert resp.status_code == 204
    assert called["fail"] is False, "must not fail-replicate when the upload has no parts of its own"
    assert called["delete"] is False, "must not delete cache when the upload has no parts of its own"
