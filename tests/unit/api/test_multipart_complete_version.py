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
from lxml import etree

from hippius_s3.api.s3 import multipart


def _parse(body: bytes) -> Any:
    """Parses a response body, failing the test if it is not well-formed XML."""
    return etree.fromstring(body)


def _text(root: Any, tag: str) -> str | None:
    """Text of the first `tag` child, namespace-agnostically."""
    nodes = root.xpath(f"./*[local-name()='{tag}']")
    return str(nodes[0].text) if nodes else None


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


@pytest.mark.asyncio
async def test_complete_rejects_wrong_part_etag(monkeypatch: Any) -> None:
    """The client asserts an ETag that does NOT match the stored part. S3 must reject with
    InvalidPart and never complete — otherwise the object is assembled from the wrong bytes."""
    _patch_common(monkeypatch)
    called = {"complete": False, "address": False}

    async def _wrong_etag_body(_req: Any) -> bytes:
        return b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>deadbeef</ETag></Part></CompleteMultipartUpload>"

    monkeypatch.setattr(multipart, "get_request_body", _wrong_etag_body)

    class _FakeWriter:
        def __init__(self, **_: Any) -> None: ...

        async def mpu_complete(self, **_: Any) -> Any:
            called["complete"] = True
            return SimpleNamespace(etag="abc", size_bytes=100)

    async def _addr(*_: Any, **__: Any) -> None:
        called["address"] = True

    monkeypatch.setattr(multipart, "ObjectWriter", _FakeWriter)
    monkeypatch.setattr(multipart, "set_object_version_address", _addr)

    db = _FakeDb(current_version=1, upload_version=1)  # stored part 1 etag == "abc"
    resp = await multipart.complete_multipart_upload("b", "k", "up-1", _request(), db)

    assert resp.status_code == 400
    assert b"InvalidPart" in bytes(resp.body)
    assert b"did not match" in bytes(resp.body)
    assert called["complete"] is False, "must not complete an object with a mismatched part ETag"
    assert called["address"] is False


@pytest.mark.asyncio
async def test_complete_rejects_out_of_order_parts(monkeypatch: Any) -> None:
    """Parts listed out of ascending order => InvalidPartOrder, before any DB/writer work."""
    _patch_common(monkeypatch)
    called = {"complete": False}

    async def _out_of_order_body(_req: Any) -> bytes:
        return (
            b"<CompleteMultipartUpload>"
            b"<Part><PartNumber>2</PartNumber><ETag>x</ETag></Part>"
            b"<Part><PartNumber>1</PartNumber><ETag>y</ETag></Part>"
            b"</CompleteMultipartUpload>"
        )

    monkeypatch.setattr(multipart, "get_request_body", _out_of_order_body)

    class _FakeWriter:
        def __init__(self, **_: Any) -> None: ...

        async def mpu_complete(self, **_: Any) -> Any:
            called["complete"] = True
            return SimpleNamespace(etag="abc", size_bytes=100)

    monkeypatch.setattr(multipart, "ObjectWriter", _FakeWriter)
    monkeypatch.setattr(multipart, "set_object_version_address", lambda *_, **__: None)

    db = _FakeDb(current_version=1, upload_version=1)
    resp = await multipart.complete_multipart_upload("b", "k", "up-1", _request(), db)

    assert resp.status_code == 400
    assert b"InvalidPartOrder" in bytes(resp.body)
    assert called["complete"] is False


@pytest.mark.asyncio
async def test_complete_accepts_matching_etag_case_insensitive(monkeypatch: Any) -> None:
    """A legitimate client that upper-cases the hex ETag must still succeed — the match is
    case-insensitive, so real SDK clients are never newly rejected by the ETag check."""
    _patch_common(monkeypatch)
    completed = {"ok": False}

    async def _upper_etag_body(_req: Any) -> bytes:
        return b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>ABC</ETag></Part></CompleteMultipartUpload>"

    monkeypatch.setattr(multipart, "get_request_body", _upper_etag_body)

    class _FakeWriter:
        def __init__(self, **_: Any) -> None: ...

        async def mpu_complete(self, **_: Any) -> Any:
            completed["ok"] = True
            return SimpleNamespace(etag="abc", size_bytes=100)

    async def _addr(*_: Any, **__: Any) -> None:
        return None

    monkeypatch.setattr(multipart, "ObjectWriter", _FakeWriter)
    monkeypatch.setattr(multipart, "set_object_version_address", _addr)

    db = _FakeDb(current_version=1, upload_version=1)  # stored part 1 etag == "abc"
    resp = await multipart.complete_multipart_upload("b", "k", "up-1", _request(), db)

    assert resp.status_code == 200
    assert completed["ok"] is True, "a case-differing but matching ETag must complete"


def _completing_writer(monkeypatch: Any, completed: dict[str, Any]) -> None:
    """Writer + address stubs that record completion, for the interop tests below."""

    class _FakeWriter:
        def __init__(self, **_: Any) -> None: ...

        async def mpu_complete(self, **_: Any) -> Any:
            completed["ok"] = True
            return SimpleNamespace(etag="abc", size_bytes=100)

    async def _addr(*_: Any, **__: Any) -> None:
        return None

    monkeypatch.setattr(multipart, "ObjectWriter", _FakeWriter)
    monkeypatch.setattr(multipart, "set_object_version_address", _addr)


def _body_returning(payload: bytes) -> Any:
    async def _body(_req: Any) -> bytes:
        return payload

    return _body


@pytest.mark.asyncio
async def test_complete_accepts_predefined_entity_quoted_etag(monkeypatch: Any) -> None:
    """aws-sdk-rust escapes the ETag's quotes as &quot;. Scraping element text with a regex
    captured the raw entity and matched no stored part, so every part failed InvalidPart and
    multipart was unusable from Rust clients."""
    _patch_common(monkeypatch)
    completed: dict[str, Any] = {"ok": False}
    _completing_writer(monkeypatch, completed)
    monkeypatch.setattr(
        multipart,
        "get_request_body",
        _body_returning(
            b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>"
            b"<ETag>&quot;abc&quot;</ETag></Part></CompleteMultipartUpload>"
        ),
    )

    db = _FakeDb(current_version=1, upload_version=1)
    resp = await multipart.complete_multipart_upload("b", "k", "up-1", _request(), db)

    assert resp.status_code == 200, bytes(resp.body)
    assert completed["ok"] is True


@pytest.mark.asyncio
async def test_complete_accepts_numeric_charref_quoted_etag(monkeypatch: Any) -> None:
    """aws-sdk-go (rclone, minio-go, the Terraform S3 provider) escapes the quotes as &#34;."""
    _patch_common(monkeypatch)
    completed: dict[str, Any] = {"ok": False}
    _completing_writer(monkeypatch, completed)
    monkeypatch.setattr(
        multipart,
        "get_request_body",
        _body_returning(
            b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>"
            b"<ETag>&#34;abc&#34;</ETag></Part></CompleteMultipartUpload>"
        ),
    )

    db = _FakeDb(current_version=1, upload_version=1)
    resp = await multipart.complete_multipart_upload("b", "k", "up-1", _request(), db)

    assert resp.status_code == 200, bytes(resp.body)
    assert completed["ok"] is True


@pytest.mark.asyncio
async def test_complete_accepts_prefixed_namespace_body(monkeypatch: Any) -> None:
    """A namespace-prefixed body is valid S3 XML; the regex only matched bare <PartNumber>."""
    _patch_common(monkeypatch)
    completed: dict[str, Any] = {"ok": False}
    _completing_writer(monkeypatch, completed)
    monkeypatch.setattr(
        multipart,
        "get_request_body",
        _body_returning(
            b'<m:CompleteMultipartUpload xmlns:m="http://s3.amazonaws.com/doc/2006-03-01/">'
            b"<m:Part><m:PartNumber>1</m:PartNumber><m:ETag>abc</m:ETag></m:Part>"
            b"</m:CompleteMultipartUpload>"
        ),
    )

    db = _FakeDb(current_version=1, upload_version=1)
    resp = await multipart.complete_multipart_upload("b", "k", "up-1", _request(), db)

    assert resp.status_code == 200, bytes(resp.body)
    assert completed["ok"] is True


@pytest.mark.asyncio
async def test_complete_accepts_default_namespace_body(monkeypatch: Any) -> None:
    """Guard for the common boto3 shape: a default xmlns on the root must keep working."""
    _patch_common(monkeypatch)
    completed: dict[str, Any] = {"ok": False}
    _completing_writer(monkeypatch, completed)
    monkeypatch.setattr(
        multipart,
        "get_request_body",
        _body_returning(
            b'<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            b'<Part><PartNumber>1</PartNumber><ETag>"abc"</ETag></Part>'
            b"</CompleteMultipartUpload>"
        ),
    )

    db = _FakeDb(current_version=1, upload_version=1)
    resp = await multipart.complete_multipart_upload("b", "k", "up-1", _request(), db)

    assert resp.status_code == 200, bytes(resp.body)
    assert completed["ok"] is True


@pytest.mark.asyncio
async def test_complete_ignores_etag_outside_part(monkeypatch: Any) -> None:
    """Part numbers and ETags were scraped as two flat lists and zipped, so a stray ETag
    anywhere in the document shifted the pairing (or failed the length check) instead of
    being ignored. Pairing must happen within each <Part>."""
    _patch_common(monkeypatch)
    completed: dict[str, Any] = {"ok": False}
    _completing_writer(monkeypatch, completed)
    monkeypatch.setattr(
        multipart,
        "get_request_body",
        _body_returning(
            b"<CompleteMultipartUpload><ETag>zzz</ETag>"
            b"<Part><PartNumber>1</PartNumber><ETag>abc</ETag></Part>"
            b"</CompleteMultipartUpload>"
        ),
    )

    db = _FakeDb(current_version=1, upload_version=1)
    resp = await multipart.complete_multipart_upload("b", "k", "up-1", _request(), db)

    assert resp.status_code == 200, bytes(resp.body)
    assert completed["ok"] is True


@pytest.mark.asyncio
async def test_complete_rejects_malformed_xml_with_malformed_xml_code(monkeypatch: Any) -> None:
    """A body that is not well-formed gets S3's MalformedXML, matching DeleteObjects."""
    _patch_common(monkeypatch)
    completed: dict[str, Any] = {"ok": False}
    _completing_writer(monkeypatch, completed)
    monkeypatch.setattr(multipart, "get_request_body", _body_returning(b"<CompleteMultipartUpload><Part>"))

    db = _FakeDb(current_version=1, upload_version=1)
    resp = await multipart.complete_multipart_upload("b", "k", "up-1", _request(), db)

    assert resp.status_code == 400
    assert b"MalformedXML" in bytes(resp.body)
    assert completed["ok"] is False


@pytest.mark.asyncio
async def test_complete_does_not_expand_entity_bomb(monkeypatch: Any) -> None:
    """A DTD-declared entity must not be expanded: a parser with default settings would inflate
    this body in memory. The reference stays unresolved, and because an empty ETag would skip
    the part comparison downstream, the body is rejected outright rather than completed."""
    _patch_common(monkeypatch)
    completed: dict[str, Any] = {"ok": False}
    _completing_writer(monkeypatch, completed)
    bomb = (
        b"<?xml version='1.0'?><!DOCTYPE lolz ["
        b"<!ENTITY lol 'lollollollollollollollollollol'>"
        b"<!ENTITY lol2 '&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;'>"
        b"<!ENTITY lol3 '&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;'>"
        b"]><CompleteMultipartUpload><Part><PartNumber>1</PartNumber>"
        b"<ETag>&lol3;</ETag></Part></CompleteMultipartUpload>"
    )
    monkeypatch.setattr(multipart, "get_request_body", _body_returning(bomb))

    db = _FakeDb(current_version=1, upload_version=1)
    resp = await multipart.complete_multipart_upload("b", "k", "up-1", _request(), db)

    assert resp.status_code == 400
    assert b"MalformedXML" in bytes(resp.body)
    assert b"lollol" not in bytes(resp.body), "entity was expanded into the response"
    assert completed["ok"] is False


@pytest.mark.asyncio
async def test_complete_response_parses_with_ampersand_key(monkeypatch: Any) -> None:
    """'&' is legal in a Hippius object key but must be escaped in XML text. Interpolated raw,
    it opens an entity reference and every conforming parser rejects the whole document."""
    _patch_common(monkeypatch)
    completed: dict[str, Any] = {"ok": False}
    _completing_writer(monkeypatch, completed)

    db = _FakeDb(current_version=1, upload_version=1)
    resp = await multipart.complete_multipart_upload("b", "a&b.txt", "up-1", _request(), db)

    assert resp.status_code == 200, bytes(resp.body)
    root = _parse(bytes(resp.body))
    assert _text(root, "Key") == "a&b.txt"
    assert _text(root, "ETag") == '"abc"'


@pytest.mark.asyncio
async def test_complete_response_escapes_host_header_in_location(monkeypatch: Any) -> None:
    """The Host header is client-controlled and lands in Location; unescaped it injects XML."""
    _patch_common(monkeypatch)
    completed: dict[str, Any] = {"ok": False}
    _completing_writer(monkeypatch, completed)
    request = _request()
    request.headers = {"Host": "h&x"}

    db = _FakeDb(current_version=1, upload_version=1)
    resp = await multipart.complete_multipart_upload("b", "k", "up-1", request, db)

    assert resp.status_code == 200, bytes(resp.body)
    root = _parse(bytes(resp.body))
    assert _text(root, "Location") == "http://h&x/b/k"


@pytest.mark.asyncio
async def test_complete_idempotent_replay_is_wellformed_and_uses_host(monkeypatch: Any) -> None:
    """Replaying a completed upload returned a str body with no Content-Length and a
    hardcoded localhost:8000 Location, wrong for every deployment."""
    _patch_common(monkeypatch)

    class _CompletedDb(_FakeDb):
        async def fetchrow(self, query: str, *args: Any) -> Any:
            if query == "get_multipart_upload":
                return {"object_id": "obj-1", "is_completed": True, "current_object_version": 1}
            if query == "get_object_by_path":
                return {"md5_hash": "m"}
            return await super().fetchrow(query, *args)

    db = _CompletedDb(current_version=1, upload_version=1)
    resp = await multipart.complete_multipart_upload("b", "a&b.txt", "up-1", _request(), db)

    assert resp.status_code == 200
    body = bytes(resp.body)
    root = _parse(body)
    assert _text(root, "Location") == "http://h/b/a&b.txt"
    assert _text(root, "Key") == "a&b.txt"
    assert resp.headers["content-length"] == str(len(body))


@pytest.mark.asyncio
async def test_complete_rejects_duplicate_part_numbers(monkeypatch: Any) -> None:
    """Duplicate part numbers are not strictly ascending => InvalidPartOrder."""
    _patch_common(monkeypatch)
    called = {"complete": False}

    async def _dup_body(_req: Any) -> bytes:
        return (
            b"<CompleteMultipartUpload>"
            b"<Part><PartNumber>1</PartNumber><ETag>x</ETag></Part>"
            b"<Part><PartNumber>1</PartNumber><ETag>y</ETag></Part>"
            b"</CompleteMultipartUpload>"
        )

    monkeypatch.setattr(multipart, "get_request_body", _dup_body)

    class _FakeWriter:
        def __init__(self, **_: Any) -> None: ...

        async def mpu_complete(self, **_: Any) -> Any:
            called["complete"] = True
            return SimpleNamespace(etag="abc", size_bytes=100)

    monkeypatch.setattr(multipart, "ObjectWriter", _FakeWriter)
    monkeypatch.setattr(multipart, "set_object_version_address", lambda *_, **__: None)

    db = _FakeDb(current_version=1, upload_version=1)
    resp = await multipart.complete_multipart_upload("b", "k", "up-1", _request(), db)

    assert resp.status_code == 400
    assert b"InvalidPartOrder" in bytes(resp.body)
    assert called["complete"] is False
