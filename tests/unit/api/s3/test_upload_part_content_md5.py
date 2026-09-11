"""UploadPart honours Content-MD5: malformed → InvalidDigest, mismatch → BadDigest."""

from __future__ import annotations

import base64
import hashlib
import uuid
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from starlette.datastructures import Headers
from starlette.datastructures import QueryParams

from hippius_s3.api.s3 import multipart
from hippius_s3.writer.types import BadDigest
from hippius_s3.writer.types import PartResult


def _request(headers: dict[str, str]) -> Any:
    return SimpleNamespace(
        query_params=QueryParams({"uploadId": "up-1", "partNumber": "1"}),
        headers=Headers(headers),
        state=SimpleNamespace(main_account_id="acct-main"),
        app=SimpleNamespace(state=SimpleNamespace(redis_client=AsyncMock(), fs_store=SimpleNamespace())),
    )


def _pool() -> Any:
    upload = {
        "is_completed": False,
        "object_id": str(uuid.uuid4()),
        "current_object_version": 1,
        "bucket_name": "bkt",
        "bucket_id": str(uuid.uuid4()),
    }
    return SimpleNamespace(fetchrow=AsyncMock(return_value=upload))


def _patch_part_stream(monkeypatch: Any, captured: dict[str, Any], raise_exc: Exception | None = None) -> None:
    async def fake_part_stream(self: Any, **kw: Any) -> PartResult:
        captured.update(kw)
        if raise_exc is not None:
            raise raise_exc
        return PartResult(etag=hashlib.md5(b"part").hexdigest(), size_bytes=4, part_number=1)

    monkeypatch.setattr(multipart.ObjectWriter, "mpu_upload_part_stream", fake_part_stream)


@pytest.mark.asyncio
async def test_malformed_content_md5_is_invalid_digest(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}
    _patch_part_stream(monkeypatch, captured)
    pool = _pool()

    resp = await multipart.upload_part(_request({"Content-MD5": "@@not-base64@@"}), pool)

    assert resp.status_code == 400
    assert b"<Code>InvalidDigest</Code>" in resp.body
    assert captured == {}
    pool.fetchrow.assert_not_called()  # rejected before any DB work or body read


@pytest.mark.asyncio
async def test_content_md5_reaches_the_part_writer(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}
    _patch_part_stream(monkeypatch, captured)
    digest = hashlib.md5(b"part").digest()

    resp = await multipart.upload_part(_request({"Content-MD5": base64.b64encode(digest).decode()}), _pool())

    assert resp.status_code == 200
    assert captured["expected_md5"] == digest


@pytest.mark.asyncio
async def test_digest_mismatch_is_bad_digest(monkeypatch: Any) -> None:
    digest = hashlib.md5(b"claimed").digest()
    _patch_part_stream(monkeypatch, {}, raise_exc=BadDigest(expected=digest, actual=hashlib.md5(b"got").digest()))

    resp = await multipart.upload_part(_request({"Content-MD5": base64.b64encode(digest).decode()}), _pool())

    assert resp.status_code == 400
    assert b"<Code>BadDigest</Code>" in resp.body


@pytest.mark.asyncio
async def test_upload_part_copy_ignores_content_md5(monkeypatch: Any) -> None:
    """UploadPartCopy has no request body to digest, so the header is not even parsed. The request
    then fails on its (deliberately unresolvable) copy source — anything but InvalidDigest."""
    pool = _pool()
    req = _request({"Content-MD5": "@@not-base64@@", "x-amz-copy-source": "/"})
    resp = await multipart.upload_part(req, pool)
    assert b"InvalidDigest" not in resp.body
