"""DeleteObjects must refuse locked keys individually, and still delete everything else.

A bulk delete carries up to 1000 keys. Failing the whole batch because one key is locked would make
Object Lock unusable with ordinary tooling; silently reporting a locked key under `<Deleted>` would
tell the client its data is gone when it is not. AWS does neither — the locked key comes back in
`<Error>` with `AccessDenied`, its neighbours are deleted normally.

The response-code mapping matters as much as the refusal: a 403 reported as `InternalError` tells
a client to retry something that cannot succeed until the lock expires, and hides a compliance
control behind what looks like a server fault.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import lxml.etree as ET
import pytest
from starlette.responses import Response

from hippius_s3.api.s3.buckets import delete_objects_endpoint as mod


def _body(entries: list[tuple[str, str | None]]) -> bytes:
    objects = "".join(
        f"<Object><Key>{k}</Key>{f'<VersionId>{v}</VersionId>' if v else ''}</Object>" for k, v in entries
    )
    return f'<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">{objects}</Delete>'.encode()


class _FakeDb:
    """`locked_keys` are the keys count_locked_versions reports as holding a live lock."""

    def __init__(self, locked_keys: set[str] | None = None) -> None:
        self.locked_keys = locked_keys or set()
        self.soft_deleted: list[str] = []

    async def fetchrow(self, query: str, *args: Any) -> Any:
        if query == "count_locked_versions":
            return {"locked_count": 1 if args[1] in self.locked_keys else 0}
        if query == "soft_delete_object":
            self.soft_deleted.append(args[1])
            return {"object_id": "obj-1", "current_object_version": 1}
        return None

    async def fetch(self, query: str, *args: Any) -> list[Any]:
        return []

    async def execute(self, query: str, *args: Any) -> str:
        return "OK"


def _request(body: bytes) -> Any:
    async def _read() -> bytes:
        return body

    return SimpleNamespace(
        state=SimpleNamespace(main_account_id="acct-main", ray_id="ray-1"),
        headers={"Host": "h"},
        query_params={"delete": ""},
        body=_read,
    )


@pytest.fixture
def wiring(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    bucket: dict[str, Any] = {"bucket_id": "bkt-1", "bucket_name": "b", "versioning_status": None}
    monkeypatch.setattr(mod, "get_query", lambda name: name)

    class _FakeUserRepo:
        def __init__(self, _db: Any) -> None: ...

        async def ensure_by_main_account(self, account: str) -> dict[str, Any]:
            return {"main_account_id": account}

    class _FakeBucketRepo:
        def __init__(self, _db: Any) -> None: ...

        async def get_by_name_and_owner(self, _n: str, _o: str) -> Any:
            return bucket

    async def _noop_unpin(*_a: Any, **_kw: Any) -> None: ...

    async def _drop_s3_name(_db: Any, _b: str, _k: str) -> str:
        return "last"

    monkeypatch.setattr(mod, "UserRepository", _FakeUserRepo)
    monkeypatch.setattr(mod, "BucketRepository", _FakeBucketRepo)
    monkeypatch.setattr(mod, "enqueue_object_unpin", _noop_unpin)
    monkeypatch.setattr(mod, "drop_s3_name", _drop_s3_name)
    return {"bucket": bucket}


def _parse(resp: Response) -> tuple[list[str], list[dict[str, str]]]:
    root = ET.fromstring(bytes(resp.body))
    deleted = [d.findtext("{*}Key") or "" for d in root.xpath("./*[local-name()='Deleted']")]
    errors = [{ET.QName(c).localname: (c.text or "") for c in e} for e in root.xpath("./*[local-name()='Error']")]
    return deleted, errors


@pytest.mark.asyncio
class TestUnversionedBucket:
    async def test_locked_key_is_refused_and_neighbours_still_delete(self, wiring: dict[str, Any]) -> None:
        """The case that makes or breaks bulk tooling: one locked key must not fail the batch."""
        db = _FakeDb(locked_keys={"locked.txt"})
        resp = await mod.handle_delete_objects(
            "b", _request(_body([("free-a.txt", None), ("locked.txt", None), ("free-b.txt", None)])), db, None
        )

        deleted, errors = _parse(resp)
        assert sorted(deleted) == ["free-a.txt", "free-b.txt"]
        assert len(errors) == 1
        assert errors[0]["Key"] == "locked.txt"
        assert errors[0]["Code"] == "AccessDenied"

    async def test_locked_key_is_never_soft_deleted(self, wiring: dict[str, Any]) -> None:
        """The status code is the symptom; the soft delete is the harm."""
        db = _FakeDb(locked_keys={"locked.txt"})
        await mod.handle_delete_objects("b", _request(_body([("locked.txt", None), ("free.txt", None)])), db, None)

        assert db.soft_deleted == ["free.txt"], "a locked key was soft-deleted"

    async def test_all_keys_locked_yields_all_errors_and_no_deletes(self, wiring: dict[str, Any]) -> None:
        db = _FakeDb(locked_keys={"a.txt", "b.txt"})
        resp = await mod.handle_delete_objects("b", _request(_body([("a.txt", None), ("b.txt", None)])), db, None)

        deleted, errors = _parse(resp)
        assert deleted == []
        assert {e["Key"] for e in errors} == {"a.txt", "b.txt"}
        assert {e["Code"] for e in errors} == {"AccessDenied"}
        assert db.soft_deleted == []

    async def test_unlocked_batch_is_unaffected(self, wiring: dict[str, Any]) -> None:
        """The guard must cost nothing in the ordinary case."""
        db = _FakeDb()
        resp = await mod.handle_delete_objects("b", _request(_body([("a.txt", None), ("b.txt", None)])), db, None)

        deleted, errors = _parse(resp)
        assert sorted(deleted) == ["a.txt", "b.txt"]
        assert errors == []


@pytest.mark.asyncio
class TestVersionedRefusalMapping:
    async def test_403_from_the_versioned_path_maps_to_access_denied(
        self, wiring: dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A lock refusal must not be reported as InternalError — that reads as a retryable
        server fault for something that cannot succeed until the lock expires."""
        wiring["bucket"]["versioning_status"] = "Enabled"

        async def _refuse(*_a: Any, **_kw: Any) -> Response:
            return Response(status_code=403, content=b"<Error/>")

        monkeypatch.setattr(mod, "delete_object_version", _refuse)
        resp = await mod.handle_delete_objects("b", _request(_body([("k.txt", "3")])), _FakeDb(), None)

        deleted, errors = _parse(resp)
        assert deleted == []
        assert errors[0]["Code"] == "AccessDenied", f"a 403 was reported as {errors[0]['Code']}"

    async def test_501_still_maps_to_not_implemented(
        self, wiring: dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The pre-existing alias refusal must keep its own code."""
        wiring["bucket"]["versioning_status"] = "Enabled"

        async def _refuse(*_a: Any, **_kw: Any) -> Response:
            return Response(status_code=501, content=b"<Error/>")

        monkeypatch.setattr(mod, "delete_object_version", _refuse)
        resp = await mod.handle_delete_objects("b", _request(_body([("k.txt", "3")])), _FakeDb(), None)

        _, errors = _parse(resp)
        assert errors[0]["Code"] == "NotImplemented"

    async def test_versioned_bucket_simple_delete_writes_a_marker_even_when_locked(
        self, wiring: dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A versionId-less delete is additive and must NOT be refused — the locked version stays
        underneath the marker. Refusing here would break ordinary clients against a lock."""
        wiring["bucket"]["versioning_status"] = "Enabled"

        async def _marker(*_a: Any, **_kw: Any) -> Response:
            return Response(status_code=204, headers={"x-amz-version-id": "9"})

        monkeypatch.setattr(mod, "insert_delete_marker", _marker)
        resp = await mod.handle_delete_objects(
            "b", _request(_body([("locked.txt", None)])), _FakeDb({"locked.txt"}), None
        )

        deleted, errors = _parse(resp)
        assert errors == [], "a simple delete on a locked key was refused"
        assert deleted == ["locked.txt"]
