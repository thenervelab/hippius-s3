from types import SimpleNamespace
from typing import Any

import pytest
from lxml import etree as ET  # ty: ignore[unresolved-import]

from hippius_s3.api.s3.buckets import delete_objects_endpoint
from hippius_s3.api.s3.buckets.delete_objects_endpoint import parse_delete_request
from hippius_s3.xml_helpers import parse_untrusted_xml


S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/"


def _parse(body: str) -> tuple[bool, list[tuple[str, str]]]:
    # Deliberately the production parser, not a bare ET.fromstring: the endpoint switched to
    # parse_untrusted_xml, and a helper that keeps using the default parser would leave the
    # shape these tests describe untested on the path that actually runs.
    return parse_delete_request(parse_untrusted_xml(body.encode()))


def test_parses_namespaced_body() -> None:
    """botocore / aws-cli send the body with the S3 xmlns."""
    quiet, objects = _parse(f'<Delete xmlns="{S3_NS}"><Object><Key>a.txt</Key></Object></Delete>')

    assert quiet is False
    assert objects == [("a.txt", "")]


def test_parses_bare_body() -> None:
    """minio-go (mc) sends bare elements with no xmlns.

    Regression: a namespace-qualified XPath matched zero <Object> nodes here, so
    DeleteObjects returned 200 OK with an empty <DeleteResult/> and deleted nothing.
    """
    quiet, objects = _parse("<Delete><Object><Key>a.txt</Key></Object></Delete>")

    assert quiet is False
    assert objects == [("a.txt", "")]


def test_parses_bare_body_with_quiet_and_multiple_keys() -> None:
    """The exact shape minio-go emits for `mc rm`."""
    quiet, objects = _parse(
        "<Delete><Quiet>true</Quiet>"
        "<Object><Key>a.txt</Key></Object>"
        "<Object><Key>nested/deep/b.txt</Key></Object>"
        "</Delete>"
    )

    assert quiet is True
    assert objects == [("a.txt", ""), ("nested/deep/b.txt", "")]


def test_quiet_is_detected_in_both_forms() -> None:
    assert _parse(f'<Delete xmlns="{S3_NS}"><Quiet>true</Quiet></Delete>')[0] is True
    assert _parse("<Delete><Quiet>true</Quiet></Delete>")[0] is True
    assert _parse("<Delete><Quiet>false</Quiet></Delete>")[0] is False
    assert _parse("<Delete></Delete>")[0] is False


def test_version_id_is_extracted_in_both_forms() -> None:
    """VersionId drives the per-key NotImplemented error, so it must survive parsing."""
    namespaced = f'<Delete xmlns="{S3_NS}"><Object><Key>a.txt</Key><VersionId>v2</VersionId></Object></Delete>'
    bare = "<Delete><Object><Key>a.txt</Key><VersionId>v2</VersionId></Object></Delete>"

    assert _parse(namespaced)[1] == [("a.txt", "v2")]
    assert _parse(bare)[1] == [("a.txt", "v2")]


def test_missing_key_yields_empty_string() -> None:
    """Empty keys are reported as per-key MalformedXML rather than dropped silently."""
    assert _parse("<Delete><Object></Object></Delete>")[1] == [("", "")]


class _RecordingDb:
    """Records every key DeleteObjects tries to soft-delete."""

    def __init__(self) -> None:
        self.deleted: list[str] = []

    async def fetchrow(self, query: str, *args: Any) -> Any:
        if query == "soft_delete_object":
            self.deleted.append(args[1])
        return None


def _fake_request(body: bytes) -> Any:
    async def _body() -> bytes:
        return body

    return SimpleNamespace(
        body=_body,
        state=SimpleNamespace(
            account=SimpleNamespace(main_account="acct-main"), main_account_id="acct-main", ray_id="ray-1"
        ),
    )


@pytest.fixture
def _stub_repos(monkeypatch: Any) -> None:
    class _Users:
        def __init__(self, _db: Any) -> None: ...

        async def ensure_by_main_account(self, _account: str) -> dict[str, str]:
            return {"main_account_id": "acct-1"}

    class _Buckets:
        def __init__(self, _db: Any) -> None: ...

        async def get_by_name_and_owner(self, _name: str, _owner: str) -> dict[str, str]:
            return {"bucket_id": "bkt-1"}

    monkeypatch.setattr(delete_objects_endpoint, "UserRepository", _Users)
    monkeypatch.setattr(delete_objects_endpoint, "BucketRepository", _Buckets)
    monkeypatch.setattr(delete_objects_endpoint, "get_query", lambda name: name)


@pytest.mark.asyncio
async def test_handler_rejects_a_body_that_is_not_wellformed(_stub_repos: None) -> None:
    """The endpoint narrowed `except Exception` to `except ValueError` when it moved to
    parse_untrusted_xml. If the parser ever raises anything outside that contract, a
    malformed body turns into a 500 and the outer handler logs it as an internal error —
    so pin that a malformed body is still a 400 at the endpoint, not just at the parser."""
    db = _RecordingDb()

    resp = await delete_objects_endpoint.handle_delete_objects("b", _fake_request(b"<Delete><Object>"), db, None)

    assert resp.status_code == 400
    assert b"MalformedXML" in bytes(resp.body)
    assert db.deleted == []


@pytest.mark.asyncio
async def test_handler_does_not_resolve_an_entity_into_a_key(_stub_repos: None) -> None:
    """An entity reference must never name the key that gets deleted.

    This is the delete path, so resolution is not a memory question here: the body a client
    signed says `&e;` and the DTD says what that expands to, and expanding it would soft-delete
    a key the request never spelled out. Kept unresolved, `.text` is None, the key reads as
    empty and the entry comes back as a per-key MalformedXML error having deleted nothing.

    The baseline assertion is the point — a default parser resolves this, so the test fails if
    the endpoint ever drops back to a bare ET.fromstring.
    """
    body = (
        b"<?xml version='1.0'?><!DOCTYPE d [<!ENTITY e 'victim.txt'>]><Delete><Object><Key>&e;</Key></Object></Delete>"
    )
    baseline = ET.fromstring(body).xpath(".//*[local-name()='Key']")[0]
    assert baseline.text == "victim.txt", "default parser no longer resolves — this test lost its teeth"

    db = _RecordingDb()
    resp = await delete_objects_endpoint.handle_delete_objects("b", _fake_request(body), db, None)

    assert resp.status_code == 200
    assert db.deleted == [], "an entity reference named the key to delete"
    assert b"MalformedXML" in bytes(resp.body)


@pytest.mark.asyncio
async def test_handler_deletes_the_keys_a_bare_minio_body_names(_stub_repos: None) -> None:
    """The happy path through the new parser, end to end at the endpoint: mc's bare body
    still reaches soft_delete_object. parse_delete_request is unit-tested above, but nothing
    asserted the two halves are wired together after the parser swap."""
    db = _RecordingDb()
    body = b"<Delete><Object><Key>a.txt</Key></Object><Object><Key>nested/b.txt</Key></Object></Delete>"

    resp = await delete_objects_endpoint.handle_delete_objects("b", _fake_request(body), db, None)

    assert resp.status_code == 200
    assert db.deleted == ["a.txt", "nested/b.txt"]
