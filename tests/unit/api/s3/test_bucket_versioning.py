from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import lxml.etree as ET
import pytest

from hippius_s3.api.s3.buckets import bucket_versioning_endpoint as mod


NS = "http://s3.amazonaws.com/doc/2006-03-01/"


@pytest.fixture(autouse=True)
def _query_names(monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest) -> None:
    """Let the fake DB branch on query NAME instead of the loaded SQL text, and stand in for the
    bucket lookup (which goes through BucketRepository, with its own get_query import)."""
    monkeypatch.setattr(mod, "get_query", lambda name: name)

    class _FakeBucketRepo:
        def __init__(self, db: Any) -> None:
            self._db = db

        async def get_by_name_and_owner(self, _name: str, _owner: str) -> Any:
            return self._db.bucket

    monkeypatch.setattr(mod, "BucketRepository", _FakeBucketRepo)


class _FakeDb:
    def __init__(
        self,
        *,
        bucket: dict[str, Any] | None,
    ) -> None:
        self.bucket = bucket
        self.executed: list[tuple[str, tuple[Any, ...]]] = []

    async def execute(self, query: str, *args: Any) -> str:
        self.executed.append((query, args))
        return "UPDATE 1"


def _request(body: bytes = b"") -> Any:
    async def _body() -> bytes:
        return body

    return SimpleNamespace(
        state=SimpleNamespace(main_account_id="acct-main", account=SimpleNamespace(main_account="acct-main")),
        headers={"Host": "h"},
        query_params={"versioning": ""},
        body=_body,
    )


def _bucket(versioning_status: str | None = None) -> dict[str, Any]:
    return {
        "bucket_id": "bkt-1",
        "bucket_name": "b",
        "main_account_id": "acct-main",
        "versioning_status": versioning_status,
    }


def _cfg(status: str | None, *, namespaced: bool = True) -> bytes:
    ns = f' xmlns="{NS}"' if namespaced else ""
    inner = f"<Status>{status}</Status>" if status is not None else ""
    return (
        f'<?xml version="1.0" encoding="UTF-8"?><VersioningConfiguration{ns}>{inner}</VersioningConfiguration>'.encode()
    )


def _text(root: Any, local: str) -> str | None:
    nodes = root.xpath(f"./*[local-name()='{local}']")
    return str(nodes[0].text) if nodes and nodes[0].text is not None else None


# --- GetBucketVersioning ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_versioning_unset_omits_status() -> None:
    db = _FakeDb(bucket=_bucket(None))
    resp = await mod.handle_get_bucket_versioning("b", db, "acct-main")

    assert resp.status_code == 200
    root = ET.fromstring(resp.body)
    assert ET.QName(root).localname == "VersioningConfiguration"
    # AWS omits <Status> entirely for a bucket that never enabled versioning.
    assert _text(root, "Status") is None


@pytest.mark.asyncio
async def test_get_versioning_enabled_reports_status() -> None:
    db = _FakeDb(bucket=_bucket("Enabled"))
    resp = await mod.handle_get_bucket_versioning("b", db, "acct-main")

    assert resp.status_code == 200
    assert _text(ET.fromstring(resp.body), "Status") == "Enabled"


@pytest.mark.asyncio
async def test_get_versioning_missing_bucket_404() -> None:
    db = _FakeDb(bucket=None)
    resp = await mod.handle_get_bucket_versioning("b", db, "acct-main")

    assert resp.status_code == 404
    assert b"NoSuchBucket" in resp.body


# --- PutBucketVersioning ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_put_versioning_enabled_persists() -> None:
    db = _FakeDb(bucket=_bucket(None))
    resp = await mod.handle_put_bucket_versioning("b", _request(_cfg("Enabled")), db)

    assert resp.status_code == 200
    assert [q for q, _ in db.executed] == ["set_bucket_versioning"]
    assert db.executed[0][1] == ("bkt-1", "Enabled")


@pytest.mark.asyncio
async def test_put_versioning_accepts_bare_namespace_body() -> None:
    # minio-go (and anything built on it) sends the body without the S3 namespace.
    db = _FakeDb(bucket=_bucket(None))
    resp = await mod.handle_put_bucket_versioning("b", _request(_cfg("Enabled", namespaced=False)), db)

    assert resp.status_code == 200
    assert db.executed[0][1] == ("bkt-1", "Enabled")


@pytest.mark.asyncio
async def test_put_versioning_suspended_not_implemented() -> None:
    db = _FakeDb(bucket=_bucket("Enabled"))
    resp = await mod.handle_put_bucket_versioning("b", _request(_cfg("Suspended")), db)

    assert resp.status_code == 501
    assert b"NotImplemented" in resp.body
    assert db.executed == []


@pytest.mark.asyncio
async def test_put_versioning_missing_status_rejected() -> None:
    # An empty config would mean "return to unversioned", which AWS forbids.
    db = _FakeDb(bucket=_bucket("Enabled"))
    resp = await mod.handle_put_bucket_versioning("b", _request(_cfg(None)), db)

    assert resp.status_code == 400
    assert b"IllegalVersioningConfigurationException" in resp.body
    assert db.executed == []


@pytest.mark.asyncio
async def test_put_versioning_unknown_status_rejected() -> None:
    db = _FakeDb(bucket=_bucket(None))
    resp = await mod.handle_put_bucket_versioning("b", _request(_cfg("Nonsense")), db)

    assert resp.status_code == 400
    assert db.executed == []


@pytest.mark.asyncio
async def test_put_versioning_malformed_xml_rejected() -> None:
    db = _FakeDb(bucket=_bucket(None))
    resp = await mod.handle_put_bucket_versioning("b", _request(b"<VersioningConfiguration"), db)

    assert resp.status_code == 400
    assert b"MalformedXML" in resp.body
    assert db.executed == []


@pytest.mark.asyncio
async def test_put_versioning_empty_body_rejected() -> None:
    db = _FakeDb(bucket=_bucket(None))
    resp = await mod.handle_put_bucket_versioning("b", _request(b""), db)

    assert resp.status_code == 400
    assert db.executed == []


@pytest.mark.asyncio
async def test_put_versioning_missing_bucket_404() -> None:
    db = _FakeDb(bucket=None)
    resp = await mod.handle_put_bucket_versioning("b", _request(_cfg("Enabled")), db)

    assert resp.status_code == 404
    assert b"NoSuchBucket" in resp.body
    assert db.executed == []


@pytest.mark.asyncio
async def test_put_versioning_enabled_is_idempotent() -> None:
    db = _FakeDb(bucket=_bucket("Enabled"))
    resp = await mod.handle_put_bucket_versioning("b", _request(_cfg("Enabled")), db)

    assert resp.status_code == 200
    assert db.executed[0][1] == ("bkt-1", "Enabled")


# --- x-amz-expected-bucket-owner ----------------------------------------------------------
#
# AWS's guard against bucket-name confusion: the caller asserts which account it believes owns the
# bucket and S3 returns 403 if that is wrong, so an operation cannot land on a bucket that was
# deleted and re-created, or whose name was claimed by someone else, since the caller last looked.
# We accepted the header and ignored it, which is worse than not supporting it — a client that
# sends it believes it is protected.


def _request_with_owner_header(expected: str | None, body: bytes = b"") -> Any:
    req = _request(body)
    headers = {"Host": "h"}
    if expected is not None:
        headers["x-amz-expected-bucket-owner"] = expected
    req.headers = headers
    return req


@pytest.mark.asyncio
async def test_put_versioning_rejects_mismatched_expected_bucket_owner() -> None:
    db = _FakeDb(bucket=_bucket())
    resp = await mod.handle_put_bucket_versioning("b", _request_with_owner_header("someone-else", _cfg("Enabled")), db)

    assert resp.status_code == 403
    assert b"AccessDenied" in resp.body
    # The write must not have happened — a 403 that still mutates is the whole failure mode.
    assert db.executed == []


@pytest.mark.asyncio
async def test_put_versioning_allows_matching_expected_bucket_owner() -> None:
    db = _FakeDb(bucket=_bucket())
    resp = await mod.handle_put_bucket_versioning("b", _request_with_owner_header("acct-main", _cfg("Enabled")), db)

    assert resp.status_code == 200
    assert db.executed[0][1] == ("bkt-1", "Enabled")


@pytest.mark.asyncio
async def test_put_versioning_without_the_header_is_unchanged() -> None:
    """Absent header means "no expectation" — every existing caller must keep working."""
    db = _FakeDb(bucket=_bucket())
    resp = await mod.handle_put_bucket_versioning("b", _request_with_owner_header(None, _cfg("Enabled")), db)

    assert resp.status_code == 200
    assert db.executed[0][1] == ("bkt-1", "Enabled")


@pytest.mark.asyncio
async def test_get_versioning_rejects_mismatched_expected_bucket_owner() -> None:
    db = _FakeDb(bucket=_bucket("Enabled"))
    resp = await mod.handle_get_bucket_versioning("b", db, "acct-main", _request_with_owner_header("someone-else"))

    assert resp.status_code == 403
    assert b"AccessDenied" in resp.body


@pytest.mark.asyncio
async def test_get_versioning_allows_matching_expected_bucket_owner() -> None:
    db = _FakeDb(bucket=_bucket("Enabled"))
    resp = await mod.handle_get_bucket_versioning("b", db, "acct-main", _request_with_owner_header("acct-main"))

    assert resp.status_code == 200
    assert b"Enabled" in resp.body
