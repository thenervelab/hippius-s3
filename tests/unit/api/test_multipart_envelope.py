"""Regression tests for the MPU broken-v5 envelope window (M7).

InitiateMultipartUpload runs `upsert_object_multipart`, which bumps objects.current_object_version
and inserts an object_versions row with storage_version=5. Until this fix, the DEK envelope
(kek_id/wrapped_dek) was written lazily by ObjectWriter._ensure_and_get_v5_dek on the FIRST
UploadPart. Between initiate and that first part the live current version had a NULL envelope, so a
concurrent GET hit v5_missing_envelope_metadata and 500'd. An MPU that was initiated and never
continued left the row broken forever — that is the shape of the 203k broken-v5 rows on prod, all
multipart=t in the last 30 days.

These tests pin the simple-PUT invariant (object_writer.py `_reserve_version`) onto the MPU path:
the envelope is written in the SAME transaction as the version reserve, so no snapshot ever
observes a v5 row with a NULL envelope.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any

import pytest

from hippius_s3.api.s3 import multipart


BUCKET_ID = "11111111-1111-1111-1111-111111111111"
OBJECT_ID = "22222222-2222-2222-2222-222222222222"
KEK_ID = uuid.UUID("33333333-3333-3333-3333-333333333333")


class _FakeTxn:
    def __init__(self, log: list[tuple[str, Any]]) -> None:
        self._log = log

    async def __aenter__(self) -> _FakeTxn:
        self._log.append(("txn_enter", None))
        return self

    async def __aexit__(self, *_: Any) -> bool:
        self._log.append(("txn_commit", None))
        return False


class _FakeDb:
    """Records an ordered op log so tests can assert WHERE the envelope write lands relative to the
    reserve and to the transaction boundary — the whole point of the fix is atomicity, not presence."""

    def __init__(self, *, current_version: int = 1) -> None:
        self.current_version = current_version
        self.log: list[tuple[str, Any]] = []

    async def fetchrow(self, query: str, *args: Any) -> Any:
        self.log.append((query, args))
        if query == "get_bucket_by_name":
            return {"bucket_id": BUCKET_ID}
        if query == "upsert_object_multipart":
            return {
                "object_id": OBJECT_ID,
                "current_object_version": self.current_version,
            }
        return None

    async def execute(self, query: str, *args: Any) -> None:
        self.log.append(("EXECUTE:" + query, args))

    def transaction(self) -> _FakeTxn:
        return _FakeTxn(self.log)


def _fake_request() -> Any:
    return SimpleNamespace(
        headers={"Content-Type": "application/octet-stream"},
        state=SimpleNamespace(account=SimpleNamespace(main_account="acct-1"), main_account_id="acct-1"),
    )


ENVELOPE_WRITE = "EXECUTE:update_object_version_envelope"


def _envelope_writes(db: _FakeDb) -> list[tuple[str, Any]]:
    return [(q, a) for q, a in db.log if q == ENVELOPE_WRITE]


def _op_names(db: _FakeDb) -> list[str]:
    return [q for q, _ in db.log]


@pytest.fixture
def stub_crypto(monkeypatch: Any) -> None:
    monkeypatch.setattr(multipart, "get_query", lambda name: name)
    monkeypatch.setattr(multipart, "generate_dek", lambda: b"D" * 32)
    monkeypatch.setattr(multipart, "wrap_dek", lambda *, kek, dek, aad: b"wrapped:" + aad)

    async def _fake_kek(*, bucket_id: str) -> tuple[uuid.UUID, bytes]:
        assert bucket_id == BUCKET_ID
        return KEK_ID, b"K" * 32

    monkeypatch.setattr(multipart, "get_or_create_active_bucket_kek", _fake_kek)


@pytest.mark.asyncio
async def test_initiate_writes_envelope_before_any_part_is_uploaded(stub_crypto: None) -> None:
    """The NULL-envelope window: after initiate returns, kek_id and wrapped_dek must already be set.
    Pre-fix there is no envelope write at all — the row stays broken until the first UploadPart."""
    db = _FakeDb()

    resp = await multipart.initiate_multipart_upload(
        bucket_name="b",
        object_key="k",
        request=_fake_request(),
        db=db,
    )

    assert resp.status_code == 200, resp.body
    writes = _envelope_writes(db)
    assert len(writes) == 1, f"expected exactly one envelope write, op log was {_op_names(db)}"

    _, args = writes[0]
    assert KEK_ID in args, f"kek_id must be bound non-NULL, got {args}"
    assert any(isinstance(a, bytes) and a.startswith(b"wrapped:") for a in args), (
        f"wrapped_dek must be bound non-NULL, got {args}"
    )


@pytest.mark.asyncio
async def test_envelope_is_written_in_the_same_transaction_as_the_reserve(stub_crypto: None) -> None:
    """Atomicity, not just eventual presence: a commit between the reserve and the envelope UPDATE
    would still expose a v5 row with a NULL envelope to a concurrent GET."""
    db = _FakeDb()

    await multipart.initiate_multipart_upload(
        bucket_name="b",
        object_key="k",
        request=_fake_request(),
        db=db,
    )

    ops = _op_names(db)
    reserve = ops.index("upsert_object_multipart")
    envelope = ops.index(ENVELOPE_WRITE)
    commit = ops.index("txn_commit")
    enter = ops.index("txn_enter")

    assert enter < reserve < envelope < commit, f"envelope must land inside the reserve txn; op log {ops}"


@pytest.mark.asyncio
async def test_envelope_aad_binds_the_db_returned_object_id_and_version(stub_crypto: None) -> None:
    """The DB-returned object_id/version are authoritative under concurrent creates; binding the
    locally generated candidate UUID would produce a DEK that can never be unwrapped on read."""
    db = _FakeDb(current_version=7)

    await multipart.initiate_multipart_upload(
        bucket_name="b",
        object_key="k",
        request=_fake_request(),
        db=db,
    )

    _, args = _envelope_writes(db)[0]
    wrapped = next(a for a in args if isinstance(a, bytes) and a.startswith(b"wrapped:"))
    assert wrapped == b"wrapped:" + f"hippius-dek:{BUCKET_ID}:{OBJECT_ID}:7".encode()
    assert OBJECT_ID in [str(a) for a in args]
    assert 7 in args


@pytest.mark.asyncio
async def test_kek_lookup_happens_outside_the_reserve_transaction(monkeypatch: Any) -> None:
    """The KEK lives on a separate keystore pool and can make a KMS round-trip on cache miss.
    Resolving it inside the reserve txn would pin a main-pool connection across that network call."""
    db = _FakeDb()
    monkeypatch.setattr(multipart, "get_query", lambda name: name)
    monkeypatch.setattr(multipart, "generate_dek", lambda: b"D" * 32)
    monkeypatch.setattr(multipart, "wrap_dek", lambda *, kek, dek, aad: b"wrapped:" + aad)

    async def _fake_kek(*, bucket_id: str) -> tuple[uuid.UUID, bytes]:
        db.log.append(("kek_lookup", None))
        return KEK_ID, b"K" * 32

    monkeypatch.setattr(multipart, "get_or_create_active_bucket_kek", _fake_kek)

    await multipart.initiate_multipart_upload(
        bucket_name="b",
        object_key="k",
        request=_fake_request(),
        db=db,
    )

    ops = _op_names(db)
    assert ops.index("kek_lookup") < ops.index("txn_enter"), f"KEK must resolve before the txn; {ops}"


@pytest.mark.asyncio
async def test_initiate_response_is_wellformed_with_ampersand_key(stub_crypto: None) -> None:
    """'&' is legal in a Hippius object key. Interpolated into the response unescaped it opens an
    entity reference, so the document is not well-formed and clients cannot read the UploadId —
    the upload fails before any data is sent."""
    from lxml import etree

    db = _FakeDb()

    resp = await multipart.initiate_multipart_upload(
        bucket_name="b",
        object_key="a&b.txt",
        request=_fake_request(),
        db=db,
    )

    assert resp.status_code == 200, resp.body
    body = bytes(resp.body)
    root = etree.fromstring(body)
    keys = root.xpath("./*[local-name()='Key']")
    assert keys[0].text == "a&b.txt"
    assert root.xpath("./*[local-name()='UploadId']")[0].text
    assert resp.headers["content-length"] == str(len(body))
