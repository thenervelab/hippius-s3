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
        "object_key": "k",
        "bucket_id": str(uuid.uuid4()),
        # Doubles as the get_multipart_version_by_upload row: the fake answers every fetchrow alike.
        "object_version": 1,
    }
    return SimpleNamespace(fetchrow=AsyncMock(return_value=upload), fetchval=AsyncMock(return_value=False))


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

    resp = await multipart.upload_part(
        _request({"Content-MD5": "@@not-base64@@"}), pool, bucket_name="bkt", object_key="k"
    )

    assert resp.status_code == 400
    assert b"<Code>InvalidDigest</Code>" in resp.body
    assert captured == {}  # rejected before the body is read


@pytest.mark.asyncio
async def test_unknown_upload_id_beats_a_malformed_digest(monkeypatch: Any) -> None:
    """S3 answers NoSuchUpload for an uploadId that does not exist, whatever the digest looks like."""
    captured: dict[str, Any] = {}
    _patch_part_stream(monkeypatch, captured)
    pool = SimpleNamespace(fetchrow=AsyncMock(return_value=None))

    resp = await multipart.upload_part(
        _request({"Content-MD5": "@@not-base64@@"}), pool, bucket_name="bkt", object_key="k"
    )

    assert resp.status_code == 404
    assert b"<Code>NoSuchUpload</Code>" in resp.body
    assert captured == {}


@pytest.mark.asyncio
async def test_content_md5_reaches_the_part_writer(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}
    _patch_part_stream(monkeypatch, captured)
    digest = hashlib.md5(b"part").digest()

    resp = await multipart.upload_part(
        _request({"Content-MD5": base64.b64encode(digest).decode()}), _pool(), bucket_name="bkt", object_key="k"
    )

    assert resp.status_code == 200
    assert captured["expected_md5"] == digest


@pytest.mark.asyncio
async def test_digest_mismatch_is_bad_digest(monkeypatch: Any) -> None:
    digest = hashlib.md5(b"claimed").digest()
    _patch_part_stream(monkeypatch, {}, raise_exc=BadDigest(expected=digest, actual=hashlib.md5(b"got").digest()))

    resp = await multipart.upload_part(
        _request({"Content-MD5": base64.b64encode(digest).decode()}), _pool(), bucket_name="bkt", object_key="k"
    )

    assert resp.status_code == 400
    assert b"<Code>BadDigest</Code>" in resp.body


@pytest.mark.asyncio
async def test_upload_part_copy_ignores_content_md5(monkeypatch: Any) -> None:
    """UploadPartCopy has no request body to digest, so the header is not even parsed. The request
    then fails on its (deliberately unresolvable) copy source — anything but InvalidDigest."""
    pool = _pool()
    req = _request({"Content-MD5": "@@not-base64@@", "x-amz-copy-source": "/"})
    resp = await multipart.upload_part(req, pool, bucket_name="bkt", object_key="k")
    assert b"InvalidDigest" not in resp.body


@pytest.mark.asyncio
async def test_malformed_content_md5_drains_the_body_before_answering(monkeypatch: Any) -> None:
    read: list[bytes] = []

    def stream() -> Any:
        async def gen() -> Any:
            read.append(b"part")
            yield b"part"

        return gen()

    _patch_part_stream(monkeypatch, {})
    req = _request({"Content-MD5": "@@not-base64@@"})
    req.stream = stream
    resp = await multipart.upload_part(req, _pool(), bucket_name="bkt", object_key="k")
    assert resp.status_code == 400
    assert read == [b"part"]


@pytest.mark.asyncio
@pytest.mark.parametrize("bucket,key", [("other-bucket", "k"), ("bkt", "other-key")])
async def test_upload_part_through_another_path_is_no_such_upload(monkeypatch: Any, bucket: str, key: str) -> None:
    """The ACL layer authorised the PATH. Writing parts into an upload that belongs to another
    bucket or key would let a grant on one bucket feed data into another's upload."""
    captured: dict[str, Any] = {}
    _patch_part_stream(monkeypatch, captured)

    resp = await multipart.upload_part(_request({}), _pool(), bucket_name=bucket, object_key=key)

    assert resp.status_code == 404
    assert b"<Code>NoSuchUpload</Code>" in resp.body
    assert captured == {}, "no part may be written"


@pytest.mark.asyncio
async def test_upload_part_refuses_to_write_into_a_finished_version(monkeypatch: Any) -> None:
    """The version resolved for this upload holds finished data — another write's object, reached
    through the current-version stand-in. Writing the part would rewrite that object in place."""
    captured: dict[str, Any] = {}
    _patch_part_stream(monkeypatch, captured)
    pool = _pool()
    pool.fetchval = AsyncMock(return_value=True)

    resp = await multipart.upload_part(_request({}), pool, bucket_name="bkt", object_key="k")

    assert resp.status_code == 409
    assert captured == {}, "no part may be written"
