"""Regression tests for MPU complete targeting the upload's OWN version.

Mirror of the abort regression: complete derived object_version from
objects.current_object_version. A simple PUT (or another MPU) on the same key bumps that
pointer, so completing an earlier upload would write the address against — and pull the parts
of — a different version. These assert complete resolves the version from the upload's own
parts (keyed by upload_id), and refuses (no fallback) when the upload has no parts of its own.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from hippius_s3.api.s3 import multipart


class _FakeDb:
    def __init__(self, *, current_version: int, upload_version: int | None) -> None:
        self.current_version = current_version
        self.upload_version = upload_version

    async def fetchrow(self, query: str, *args: Any) -> Any:
        if query == "get_multipart_upload":
            return {"object_id": "obj-1", "is_completed": False, "current_object_version": self.current_version}
        if query == "get_bucket_by_name":
            return {"bucket_id": "bkt-1"}
        if query == "get_multipart_version_by_upload":
            return {"object_version": self.upload_version} if self.upload_version is not None else None
        return None

    async def fetch(self, query: str, *args: Any) -> list[Any]:
        if query == "list_parts_for_version":
            # parts exist only under the upload's own version
            if args[1] == self.upload_version:
                return [{"part_number": 1, "etag": "abc", "size_bytes": 100}]
            return []
        return []


def _request() -> Any:
    return SimpleNamespace(
        state=SimpleNamespace(account=SimpleNamespace(main_account="acct-main", id="sub-1"), seed_phrase="seed"),
        headers={"Host": "h"},
        app=SimpleNamespace(state=SimpleNamespace(postgres_pool=object(), redis_client=object(), fs_store=object())),
    )


_BODY = b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>abc</ETag></Part></CompleteMultipartUpload>"


def _patch_common(monkeypatch: Any) -> None:
    monkeypatch.setattr(multipart, "get_query", lambda name: name)

    async def _body(_req: Any) -> bytes:
        return _BODY

    monkeypatch.setattr(multipart, "get_request_body", _body)
    monkeypatch.setattr(
        multipart,
        "get_metrics_collector",
        lambda: SimpleNamespace(
            record_s3_operation=lambda **_: None, record_data_transfer=lambda **_: None, record_error=lambda **_: None
        ),
    )


@pytest.mark.asyncio
async def test_complete_resolves_version_from_parts_not_pointer(monkeypatch: Any) -> None:
    """current_object_version=2 (advanced by a later op), upload's parts are at v1.
    mpu_complete AND set_object_version_address must use v1, not the pointer."""
    _patch_common(monkeypatch)
    seen: dict[str, Any] = {}

    class _FakeWriter:
        def __init__(self, **_: Any) -> None: ...

        async def mpu_complete(self, **kw: Any) -> Any:
            seen["complete_version"] = kw["object_version"]
            return SimpleNamespace(etag="abc", size_bytes=100)

    async def _addr(_pool: Any, *, object_id: str, object_version: int, address: str) -> None:
        seen["address_version"] = object_version

    monkeypatch.setattr(multipart, "ObjectWriter", _FakeWriter)
    monkeypatch.setattr(multipart, "set_object_version_address", _addr)

    db = _FakeDb(current_version=2, upload_version=1)
    resp = await multipart.complete_multipart_upload("b", "k", "up-1", _request(), db)

    assert resp.status_code == 200
    assert seen["complete_version"] == 1, "mpu_complete must use the upload's own version, not the pointer"
    assert seen["address_version"] == 1, "address must be written against the upload's own version, not the pointer"


@pytest.mark.asyncio
async def test_complete_with_no_parts_refuses_without_falling_back(monkeypatch: Any) -> None:
    """No parts for this upload_id => 'No parts found' (400). Must NOT fall back to
    current_object_version and pull a different version's parts."""
    _patch_common(monkeypatch)
    called = {"complete": False, "address": False}

    class _FakeWriter:
        def __init__(self, **_: Any) -> None: ...

        async def mpu_complete(self, **_: Any) -> Any:
            called["complete"] = True
            return SimpleNamespace(etag="abc", size_bytes=100)

    async def _addr(*_: Any, **__: Any) -> None:
        called["address"] = True

    monkeypatch.setattr(multipart, "ObjectWriter", _FakeWriter)
    monkeypatch.setattr(multipart, "set_object_version_address", _addr)

    db = _FakeDb(current_version=2, upload_version=None)
    resp = await multipart.complete_multipart_upload("b", "k", "up-1", _request(), db)

    assert resp.status_code == 400
    assert b"No parts found" in bytes(resp.body)
    assert called["complete"] is False
    assert called["address"] is False
