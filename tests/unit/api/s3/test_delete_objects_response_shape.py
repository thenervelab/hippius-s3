"""AWS splits the two ids in a bulk-delete result, and clients depend on which one appears.

Per API_DeletedObject: a removed VERSION reports `VersionId`; a delete marker that was created or
removed reports `DeleteMarkerVersionId` plus `DeleteMarker=true`. Tools that record markers so they
can undo a delete later read the marker's id from `DeleteMarkerVersionId` — putting it in
`VersionId` hands them nothing under the key they look up.

Also pins the projected-column contract that `copy_helpers` depends on: asyncpg's `Record.get()`
returns the default for a column the query never selected, so a missing `is_delete_marker` makes a
guard silently evaluate falsy rather than raising — it fails OPEN.
"""

from __future__ import annotations

import pathlib
from types import SimpleNamespace
from typing import Any

import lxml.etree as ET
import pytest

from hippius_s3.api.s3.buckets import delete_objects_endpoint as mod


def _body(entries: list[tuple[str, str | None]]) -> bytes:
    objects = "".join(
        f"<Object><Key>{k}</Key>{f'<VersionId>{v}</VersionId>' if v else ''}</Object>" for k, v in entries
    )
    return f'<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">{objects}</Delete>'.encode()


class _FakeDb:
    async def fetchrow(self, query: str, *args: Any) -> Any:
        if query == "soft_delete_object":
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

    monkeypatch.setattr(mod, "UserRepository", _FakeUserRepo)
    monkeypatch.setattr(mod, "BucketRepository", _FakeBucketRepo)
    monkeypatch.setattr(mod, "enqueue_object_unpin", _noop_unpin)
    return {"bucket": bucket}


def _deleted_entries(body: bytes) -> list[dict[str, str]]:
    root = ET.fromstring(body)
    out = []
    for d in root.xpath("./*[local-name()='Deleted']"):
        out.append({ET.QName(c).localname: (c.text or "") for c in d})
    return out


@pytest.mark.asyncio
async def test_created_marker_reports_delete_marker_version_id(
    wiring: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    wiring["bucket"]["versioning_status"] = "Enabled"

    from fastapi import Response

    async def _marker(*_a: Any, **_kw: Any) -> Response:
        return Response(status_code=204, headers={"x-amz-delete-marker": "true", "x-amz-version-id": "7"})

    monkeypatch.setattr(mod, "insert_delete_marker", _marker)

    resp = await mod.handle_delete_objects("b", _request(_body([("k", None)])), _FakeDb(), None)

    assert resp.status_code == 200
    entry = _deleted_entries(resp.body)[0]
    assert entry["Key"] == "k"
    assert entry["DeleteMarker"] == "true"
    assert entry["DeleteMarkerVersionId"] == "7"
    assert "VersionId" not in entry, "AWS reports a created marker only via DeleteMarkerVersionId"


@pytest.mark.asyncio
async def test_removed_version_reports_version_id_only(wiring: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    wiring["bucket"]["versioning_status"] = "Enabled"
    from fastapi import Response

    async def _del_version(*_a: Any, **_kw: Any) -> Response:
        return Response(status_code=204, headers={"x-amz-version-id": "2"})

    monkeypatch.setattr(mod, "delete_object_version", _del_version)

    resp = await mod.handle_delete_objects("b", _request(_body([("k", "2")])), _FakeDb(), None)

    entry = _deleted_entries(resp.body)[0]
    assert entry["VersionId"] == "2"
    assert "DeleteMarker" not in entry
    assert "DeleteMarkerVersionId" not in entry


@pytest.mark.asyncio
async def test_removed_marker_reports_both_ids(wiring: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    wiring["bucket"]["versioning_status"] = "Enabled"
    from fastapi import Response

    async def _del_version(*_a: Any, **_kw: Any) -> Response:
        return Response(
            status_code=204,
            headers={"x-amz-version-id": "5", "x-amz-delete-marker": "true"},
        )

    monkeypatch.setattr(mod, "delete_object_version", _del_version)

    resp = await mod.handle_delete_objects("b", _request(_body([("k", "5")])), _FakeDb(), None)

    entry = _deleted_entries(resp.body)[0]
    assert entry["VersionId"] == "5"
    assert entry["DeleteMarker"] == "true"
    assert entry["DeleteMarkerVersionId"] == "5"


@pytest.mark.asyncio
async def test_unversioned_delete_reports_key_only(wiring: dict[str, Any]) -> None:
    resp = await mod.handle_delete_objects("b", _request(_body([("k", None)])), _FakeDb(), None)

    assert _deleted_entries(resp.body) == [{"Key": "k"}]


@pytest.mark.asyncio
async def test_one_failing_key_does_not_fail_the_batch(wiring: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    async def _boom(*_a: Any, **_kw: Any) -> Any:
        raise RuntimeError("backend exploded")

    monkeypatch.setattr(mod, "delete_object_version", _boom)

    resp = await mod.handle_delete_objects("b", _request(_body([("bad", "2"), ("good", None)])), _FakeDb(), None)

    assert resp.status_code == 200
    root = ET.fromstring(resp.body)
    errors = [{ET.QName(c).localname: (c.text or "") for c in e} for e in root.xpath("./*[local-name()='Error']")]
    assert [e["Key"] for e in errors] == ["bad"]
    assert [d["Key"] for d in _deleted_entries(resp.body)] == ["good"]


# --- Projected-column contract -------------------------------------------------------------

_QUERIES = pathlib.Path(__file__).resolve().parents[4] / "hippius_s3" / "sql" / "queries"


@pytest.mark.parametrize(
    "query_name",
    [
        "get_object_by_path",
        "get_object_by_path_and_version",
        "get_object_head_by_path",
        "get_object_for_download_with_permissions",
        "get_object_for_download_with_permissions_by_version",
    ],
)
def test_version_resolving_queries_project_is_delete_marker(query_name: str) -> None:
    """Every query that can RESOLVE to a delete marker must expose the flag.

    These queries deliberately admit markers, because filtering them inside the version-resolution
    subquery would fall through to the previous content version and serve deleted data. That makes
    projecting the flag load-bearing: `Record.get("is_delete_marker")` returns None for a column the
    query never selected, so a caller's guard fails OPEN — which is how CopyObject briefly returned
    200 OK with a zero-byte body for a delete-marked key.
    """
    sql = (_QUERIES / f"{query_name}.sql").read_text()
    projections = [line.strip().rstrip(",") for line in sql.splitlines() if not line.strip().startswith("--")]
    assert any(p.endswith("is_delete_marker") for p in projections), (
        f"{query_name}.sql resolves versions but never projects is_delete_marker"
    )
