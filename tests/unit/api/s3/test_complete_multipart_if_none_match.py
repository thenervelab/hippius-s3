"""CompleteMultipartUpload honours If-None-Match: * (412) and rejects other values (501)."""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from starlette.datastructures import Headers

from hippius_s3.api.s3 import multipart
from hippius_s3.utils import get_query
from hippius_s3.writer.types import CompleteResult
from hippius_s3.writer.types import PreconditionFailed


ETAG = "0" * 32
BODY = (
    b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>"
    + f"<ETag>&quot;{ETAG}&quot;</ETag>".encode()
    + b"</Part></CompleteMultipartUpload>"
)


def _db() -> Any:
    object_id = str(uuid.uuid4())
    rows = {
        get_query("get_multipart_upload"): {"object_id": object_id, "is_completed": False, "current_object_version": 1},
        get_query("get_bucket_by_name"): {"bucket_id": str(uuid.uuid4())},
        get_query("get_multipart_version_by_upload"): {"object_version": 1},
    }

    async def fetchrow(query: str, *_a: Any) -> Any:
        return rows.get(query)

    async def fetch(query: str, *_a: Any) -> Any:
        return [{"part_number": 1, "etag": ETAG, "size_bytes": 5}]

    return SimpleNamespace(fetchrow=AsyncMock(side_effect=fetchrow), fetch=AsyncMock(side_effect=fetch))


def _request(headers: dict[str, str]) -> Any:
    return SimpleNamespace(
        headers=Headers(headers),
        state=SimpleNamespace(main_account_id="acct-main"),
        # A stand-in store, not None: None makes ObjectWriter build the configured on-disk cache.
        app=SimpleNamespace(
            state=SimpleNamespace(postgres_pool=AsyncMock(), redis_client=AsyncMock(), fs_store=SimpleNamespace())
        ),
    )


def _patch(monkeypatch: Any, captured: dict[str, Any], raise_exc: Exception | None = None) -> AsyncMock:
    async def fake_complete(self: Any, **kw: Any) -> CompleteResult:
        captured.update(kw)
        if raise_exc is not None:
            raise raise_exc
        return CompleteResult(etag=f"{ETAG}-1", size_bytes=5)

    persisted = AsyncMock()
    monkeypatch.setattr(multipart.ObjectWriter, "mpu_complete", fake_complete)
    monkeypatch.setattr(multipart, "get_request_body", AsyncMock(return_value=BODY))
    monkeypatch.setattr(multipart, "set_object_version_address", persisted)
    monkeypatch.setattr(multipart, "wake_version_replication", AsyncMock())
    return persisted


@pytest.mark.asyncio
async def test_star_is_passed_to_the_writer(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}
    _patch(monkeypatch, captured)
    resp = await multipart.complete_multipart_upload("bkt", "big.bin", "up-1", _request({"If-None-Match": "*"}), _db())
    assert resp.status_code == 200
    assert captured["if_none_match"] is True


@pytest.mark.asyncio
async def test_existing_key_is_precondition_failed_and_nothing_follows(monkeypatch: Any) -> None:
    persisted = _patch(monkeypatch, {}, raise_exc=PreconditionFailed())
    resp = await multipart.complete_multipart_upload("bkt", "big.bin", "up-1", _request({"If-None-Match": "*"}), _db())
    assert resp.status_code == 412
    assert b"<Code>PreconditionFailed</Code>" in resp.body
    persisted.assert_not_called()  # the drain address is only written for a completed upload


@pytest.mark.asyncio
async def test_unsupported_value_is_not_implemented_before_any_db_work(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}
    _patch(monkeypatch, captured)
    db = _db()
    resp = await multipart.complete_multipart_upload("bkt", "big.bin", "up-1", _request({"If-None-Match": "etag"}), db)
    assert resp.status_code == 501
    assert captured == {}
    db.fetchrow.assert_not_called()


@pytest.mark.asyncio
async def test_no_header_is_an_ordinary_completion(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}
    _patch(monkeypatch, captured)
    resp = await multipart.complete_multipart_upload("bkt", "big.bin", "up-1", _request({}), _db())
    assert resp.status_code == 200
    assert captured["if_none_match"] is False


@pytest.mark.asyncio
async def test_unsupported_value_drains_the_body_before_answering(monkeypatch: Any) -> None:
    read: list[bytes] = []

    def stream() -> Any:
        async def gen() -> Any:
            read.append(BODY)
            yield BODY

        return gen()

    _patch(monkeypatch, {})
    req = _request({"If-None-Match": "etag"})
    req.stream = stream
    resp = await multipart.complete_multipart_upload("bkt", "big.bin", "up-1", req, _db())
    assert resp.status_code == 501
    assert read == [BODY]
