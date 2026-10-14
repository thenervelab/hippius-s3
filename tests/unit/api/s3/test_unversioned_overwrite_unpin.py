from __future__ import annotations

import uuid
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from types import SimpleNamespace
from typing import Any

import pytest
from starlette.datastructures import Headers
from starlette.responses import Response

from hippius_s3.api.s3 import copy_helpers
from hippius_s3.api.s3 import multipart
from hippius_s3.api.s3.copy_helpers import handle_streaming_copy
from hippius_s3.api.s3.multipart import abort_multipart_upload
from hippius_s3.api.s3.multipart import complete_multipart_upload
from hippius_s3.api.s3.objects import delete_object_endpoint as unpin_mod
from hippius_s3.api.s3.objects import put_object_endpoint
from hippius_s3.api.s3.objects.delete_object_endpoint import enqueue_unversioned_overwrite_unpin
from hippius_s3.api.s3.objects.put_object_endpoint import handle_put_object
from hippius_s3.config import get_config
from hippius_s3.utils import get_query
from hippius_s3.writer.types import PutResult
from tests.unit._fake_pool import make_fake_pool


_QUERY = "get_unversioned_superseded_version"
_NOW = datetime.now(timezone.utc)


class _Ver:
    def __init__(
        self,
        version: int,
        *,
        deleted: bool = False,
        marker: bool = False,
        size: int = 10,
        md5: str = "abc",
        hold: bool = False,
        retain: datetime | None = None,
        open_mpu: bool = False,
    ) -> None:
        self.version = version
        self.deleted = deleted
        self.marker = marker
        self.size = size
        self.md5 = md5
        self.hold = hold
        self.retain = retain
        self.open_mpu = open_mpu


def _choose(
    versions: list[_Ver],
    *,
    finalized: int,
    versioning_status: str | None,
    current: int,
    object_deleted: bool = False,
) -> int | None:
    """Mirror of get_unversioned_superseded_version.sql. The SQL text test locks the same clauses."""
    if object_deleted or versioning_status is not None or current != finalized:
        return None
    eligible: list[int] = []
    for v in versions:
        if v.version >= finalized or v.deleted or v.marker:
            continue
        if not (v.size > 0 or (v.md5 is not None and v.md5 != "")):
            continue
        if v.hold or (v.retain is not None and v.retain > _NOW):
            continue
        if v.open_mpu:
            continue
        eligible.append(v.version)
    return max(eligible) if eligible else None


class _Catalog:
    def __init__(
        self,
        versions: list[_Ver],
        *,
        finalized_is_current: int,
        versioning_status: str | None = None,
        object_deleted: bool = False,
        row_version: Any = "auto",
    ) -> None:
        self.versions = versions
        self.current = finalized_is_current
        self.versioning_status = versioning_status
        self.object_deleted = object_deleted
        self.row_version = row_version
        self.queries: list[str] = []

    async def fetchrow(self, query: str, *args: Any) -> Any:
        self.queries.append(query)
        assert args[0]
        finalized = int(args[1])
        if self.row_version != "auto":
            return {"object_version": self.row_version}
        chosen = _choose(
            self.versions,
            finalized=finalized,
            versioning_status=self.versioning_status,
            current=self.current,
            object_deleted=self.object_deleted,
        )
        return None if chosen is None else {"object_version": chosen}

    async def fetch(self, *_a: Any, **_k: Any) -> list[Any]:
        return []


def _install_unpin(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    enqueued: list[Any] = []

    async def _resolve(*_a: Any, **_k: Any) -> list[str]:
        return ["arion"]

    async def _enqueue(payload: Any = None, **_k: Any) -> None:
        enqueued.append(payload)

    monkeypatch.setattr(unpin_mod, "get_query", lambda name: name)
    monkeypatch.setattr(unpin_mod, "resolve_object_backends", _resolve)
    monkeypatch.setattr(unpin_mod, "enqueue_unpin_request", _enqueue)
    return enqueued


def _versions_of(enqueued: list[Any]) -> list[Any]:
    return [p.object_version for p in enqueued]


def _assert_specific(enqueued: list[Any], expected: list[int]) -> None:
    got = _versions_of(enqueued)
    assert got == expected
    assert all(v is not None and isinstance(v, int) for v in got)


async def _unpin(db: _Catalog, version: int) -> None:
    await enqueue_unversioned_overwrite_unpin(
        db,
        object_id="obj-1",
        object_version=version,
        address="acct-main",
        ray_id="ray-1",
    )


def test_superseded_sql_is_one_indexed_probe() -> None:
    sql = get_query(_QUERY)
    assert "versioning_status IS NULL" in sql
    assert "current_object_version = $2" in sql
    assert "o.deleted_at IS NULL" in sql
    assert "v.deleted_at IS NULL" in sql
    assert "NOT v.is_delete_marker" in sql
    assert "v.object_version < $2" in sql
    assert "object_lock_legal_hold" in sql
    assert "object_lock_retain_until > now()" in sql
    assert "COALESCE(mu.is_completed, false) = false" in sql
    assert "ORDER BY v.object_version DESC" in sql
    assert sql.count("LIMIT 1") == 1
    assert "object_version IS NULL" not in sql
    assert "object_versions_pkey" in sql


@pytest.mark.asyncio
async def test_helper_enqueues_only_the_greatest_eligible_version(monkeypatch: pytest.MonkeyPatch) -> None:
    enqueued = _install_unpin(monkeypatch)
    db = _Catalog(
        [_Ver(2), _Ver(4), _Ver(5, open_mpu=True)],
        finalized_is_current=6,
    )
    await _unpin(db, 6)
    _assert_specific(enqueued, [4])
    assert enqueued[0].object_id == "obj-1"
    assert enqueued[0].address == "acct-main"
    assert len(db.queries) == 1


@pytest.mark.asyncio
async def test_helper_version_1_does_not_query(monkeypatch: pytest.MonkeyPatch) -> None:
    enqueued = _install_unpin(monkeypatch)
    db = _Catalog([_Ver(1)], finalized_is_current=1)
    await _unpin(db, 1)
    assert enqueued == []
    assert db.queries == []


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["Enabled", "Suspended"])
async def test_helper_versioned_bucket_enqueues_nothing(monkeypatch: pytest.MonkeyPatch, status: str) -> None:
    enqueued = _install_unpin(monkeypatch)
    db = _Catalog([_Ver(4)], finalized_is_current=5, versioning_status=status)
    await _unpin(db, 5)
    assert enqueued == []


@pytest.mark.asyncio
async def test_helper_open_mpu_is_skipped_for_an_older_closed_version(monkeypatch: pytest.MonkeyPatch) -> None:
    """Version 7 is an in-flight upload (parts still open). Version 6 is a partless reserve.
    Neither is the object this write replaced; version 5 is."""
    enqueued = _install_unpin(monkeypatch)
    db = _Catalog(
        [
            _Ver(5),
            _Ver(6, size=0, md5=""),
            _Ver(7, open_mpu=True, size=100),
        ],
        finalized_is_current=8,
    )
    await _unpin(db, 8)
    _assert_specific(enqueued, [5])


@pytest.mark.asyncio
async def test_helper_locked_previous_version_is_not_chosen(monkeypatch: pytest.MonkeyPatch) -> None:
    enqueued = _install_unpin(monkeypatch)
    db = _Catalog(
        [
            _Ver(3),
            _Ver(4, hold=True),
            _Ver(5, retain=_NOW + timedelta(days=1)),
        ],
        finalized_is_current=6,
    )
    await _unpin(db, 6)
    _assert_specific(enqueued, [3])


@pytest.mark.asyncio
async def test_helper_expired_retention_is_eligible(monkeypatch: pytest.MonkeyPatch) -> None:
    enqueued = _install_unpin(monkeypatch)
    db = _Catalog([_Ver(4, retain=_NOW - timedelta(days=1))], finalized_is_current=5)
    await _unpin(db, 5)
    _assert_specific(enqueued, [4])


@pytest.mark.asyncio
async def test_helper_moved_pointer_or_deleted_object_enqueues_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    enqueued = _install_unpin(monkeypatch)
    moved = _Catalog([_Ver(4)], finalized_is_current=9)
    await _unpin(moved, 5)
    deleted = _Catalog([_Ver(4)], finalized_is_current=5, object_deleted=True)
    await _unpin(deleted, 5)
    assert enqueued == []


@pytest.mark.asyncio
async def test_helper_null_candidate_is_not_enqueued(monkeypatch: pytest.MonkeyPatch) -> None:
    enqueued = _install_unpin(monkeypatch)
    db = _Catalog([], finalized_is_current=5, row_version=None)
    await _unpin(db, 5)
    assert enqueued == []


@pytest.mark.asyncio
async def test_helper_enqueue_failure_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(unpin_mod, "get_query", lambda name: name)

    async def _resolve(*_a: Any, **_k: Any) -> list[str]:
        return ["arion"]

    async def _enqueue(*_a: Any, **_k: Any) -> None:
        raise RuntimeError("queue down")

    monkeypatch.setattr(unpin_mod, "resolve_object_backends", _resolve)
    monkeypatch.setattr(unpin_mod, "enqueue_unpin_request", _enqueue)
    db = _Catalog([_Ver(4)], finalized_is_current=5)
    with pytest.raises(RuntimeError, match="queue down"):
        await _unpin(db, 5)


def _put_request(headers: dict[str, str] | None = None) -> Any:
    return SimpleNamespace(
        state=SimpleNamespace(main_account_id="acct-main", ray_id="ray-1"),
        headers=Headers(headers or {}),
        app=SimpleNamespace(state=SimpleNamespace(fs_store=SimpleNamespace(), postgres_pool=SimpleNamespace())),
    )


def _patch_put(
    monkeypatch: pytest.MonkeyPatch,
    *,
    version: int,
    address_error: BaseException | None = None,
) -> list[Any]:
    enqueued = _install_unpin(monkeypatch)

    async def fake_put(self: Any, **_kw: Any) -> PutResult:
        return PutResult(
            object_id="obj-1",
            etag="etag",
            size_bytes=3,
            upload_id="up-1",
            object_version=version,
        )

    async def fake_address(*_a: Any, **_k: Any) -> None:
        if address_error is not None:
            raise address_error

    monkeypatch.setattr(put_object_endpoint.ObjectWriter, "put_simple_stream_full", fake_put)
    monkeypatch.setattr(put_object_endpoint, "set_object_version_address", fake_address)
    return enqueued


class _Redis:
    async def set(self, *_a: Any, **_k: Any) -> bool:
        return True


def _put_router(
    *,
    versioning: str | None,
    previous: int | None,
    seen: list[tuple[Any, ...]],
) -> Any:
    def router(method: str, query: str, args: tuple[Any, ...]) -> Any:
        q = query or ""
        if "Get bucket by name" in q:
            return {
                "bucket_id": "bkt-1",
                "bucket_name": "bkt",
                "main_account_id": "acct-main",
                "versioning_status": versioning,
            }
        if _QUERY in q:
            seen.append(args)
            return None if previous is None else {"object_version": previous}
        return None

    return router


@pytest.mark.asyncio
async def test_put_unversioned_overwrite_enqueues_the_previous_version_only(monkeypatch: pytest.MonkeyPatch) -> None:
    enqueued = _patch_put(monkeypatch, version=5)
    seen: list[tuple[Any, ...]] = []
    resp = await handle_put_object(
        "bkt",
        "k",
        _put_request(),
        make_fake_pool(_put_router(versioning=None, previous=4, seen=seen)),
        _Redis(),
    )
    assert resp.status_code == 200
    _assert_specific(enqueued, [4])
    assert seen and seen[0][1] == 5


@pytest.mark.asyncio
async def test_put_version_1_enqueues_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    enqueued = _patch_put(monkeypatch, version=1)
    seen: list[tuple[Any, ...]] = []
    resp = await handle_put_object(
        "bkt",
        "k",
        _put_request(),
        make_fake_pool(_put_router(versioning=None, previous=None, seen=seen)),
        _Redis(),
    )
    assert resp.status_code == 200
    assert enqueued == []
    assert seen == []


@pytest.mark.asyncio
async def test_put_enabled_bucket_enqueues_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    enqueued = _patch_put(monkeypatch, version=5)
    seen: list[tuple[Any, ...]] = []
    resp = await handle_put_object(
        "bkt",
        "k",
        _put_request(),
        make_fake_pool(_put_router(versioning="Enabled", previous=4, seen=seen)),
        _Redis(),
    )
    assert resp.status_code == 200
    assert enqueued == []
    assert seen == []


@pytest.mark.asyncio
async def test_put_address_failure_enqueues_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    enqueued = _patch_put(monkeypatch, version=5, address_error=RuntimeError("address write failed"))
    seen: list[tuple[Any, ...]] = []
    resp = await handle_put_object(
        "bkt",
        "k",
        _put_request(),
        make_fake_pool(_put_router(versioning=None, previous=4, seen=seen)),
        _Redis(),
    )
    assert resp.status_code == 500
    assert enqueued == []
    assert seen == []


@pytest.mark.asyncio
async def test_put_append_enqueues_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    enqueued = _patch_put(monkeypatch, version=5)
    seen: list[tuple[Any, ...]] = []

    async def fake_append(*_a: Any, **_k: Any) -> Response:
        return Response(status_code=200)

    monkeypatch.setattr(put_object_endpoint, "handle_append", fake_append)
    resp = await handle_put_object(
        "bkt",
        "k",
        _put_request({"x-amz-meta-append": "true"}),
        make_fake_pool(_put_router(versioning=None, previous=4, seen=seen)),
        _Redis(),
    )
    assert resp.status_code == 200
    assert enqueued == []
    assert seen == []


_COMPLETE_XML = (
    b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>abc</ETag></Part></CompleteMultipartUpload>"
)


class _MpuDb:
    def __init__(
        self,
        *,
        completed: bool = False,
        version: int = 4,
        previous: int | None = 2,
        versioning: str | None = None,
    ) -> None:
        self.completed = completed
        self.version = version
        self.previous = previous
        self.versioning = versioning
        self.queries: list[str] = []

    async def fetchrow(self, query: str, *args: Any) -> Any:
        self.queries.append(query)
        if _QUERY in query:
            return None if self.previous is None else {"object_version": self.previous}
        if query == "get_multipart_upload":
            return {
                "object_id": "obj-1",
                "is_completed": self.completed,
                "current_object_version": self.version,
                "key_existed_at_initiate": False,
            }
        if query == "get_bucket_by_name":
            return {"bucket_id": "bkt-1", "versioning_status": self.versioning}
        if query == "get_multipart_version_by_upload":
            return None if self.version == 0 else {"object_version": self.version}
        if query == "get_object_by_path":
            return {"md5_hash": "abc"}
        return None

    async def fetch(self, query: str, *_a: Any) -> list[Any]:
        if query == "list_parts_for_version":
            return [{"part_number": 1, "etag": "abc", "size_bytes": 100}]
        return []

    async def execute(self, *_a: Any, **_k: Any) -> None:
        return None


def _mpu_request() -> Any:
    return SimpleNamespace(
        headers={"Host": "h"},
        state=SimpleNamespace(main_account_id="acct-main", ray_id="ray-1"),
        app=SimpleNamespace(
            state=SimpleNamespace(
                postgres_pool=SimpleNamespace(), redis_client=SimpleNamespace(), fs_store=SimpleNamespace()
            )
        ),
    )


def _patch_complete(monkeypatch: pytest.MonkeyPatch, *, address_error: BaseException | None = None) -> list[Any]:
    enqueued = _install_unpin(monkeypatch)
    monkeypatch.setattr(multipart, "get_query", lambda name: name)

    async def _body(_req: Any) -> bytes:
        return _COMPLETE_XML

    class _Writer:
        def __init__(self, **_k: Any) -> None: ...

        async def mpu_complete(self, **_k: Any) -> Any:
            return SimpleNamespace(etag="abc-1", size_bytes=100)

    async def _addr(*_a: Any, **_k: Any) -> None:
        if address_error is not None:
            raise address_error

    monkeypatch.setattr(multipart, "get_request_body", _body)
    monkeypatch.setattr(multipart, "ObjectWriter", _Writer)
    monkeypatch.setattr(multipart, "set_object_version_address", _addr)
    return enqueued


@pytest.mark.asyncio
async def test_complete_mpu_enqueues_the_previous_version(monkeypatch: pytest.MonkeyPatch) -> None:
    enqueued = _patch_complete(monkeypatch)
    resp = await complete_multipart_upload("b", "k", "up-1", _mpu_request(), _MpuDb())
    assert resp.status_code == 200
    _assert_specific(enqueued, [2])


@pytest.mark.asyncio
async def test_complete_mpu_replay_does_not_unpin(monkeypatch: pytest.MonkeyPatch) -> None:
    enqueued = _patch_complete(monkeypatch)
    db = _MpuDb(completed=True, version=4, previous=2)
    resp = await complete_multipart_upload("b", "k", "up-1", _mpu_request(), db)
    assert resp.status_code == 200
    assert enqueued == []
    assert not any(_QUERY in q for q in db.queries)


@pytest.mark.asyncio
async def test_complete_mpu_enabled_bucket_enqueues_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    enqueued = _patch_complete(monkeypatch)
    db = _MpuDb(versioning="Enabled", previous=2)
    resp = await complete_multipart_upload("b", "k", "up-1", _mpu_request(), db)
    assert resp.status_code == 200
    assert enqueued == []
    assert not any(_QUERY in q for q in db.queries)


@pytest.mark.asyncio
async def test_complete_mpu_address_failure_enqueues_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    enqueued = _patch_complete(monkeypatch, address_error=RuntimeError("address write failed"))
    resp = await complete_multipart_upload("b", "k", "up-1", _mpu_request(), _MpuDb())
    assert resp.status_code == 500
    assert enqueued == []


@pytest.mark.asyncio
async def test_abort_does_not_unpin_the_live_version(monkeypatch: pytest.MonkeyPatch) -> None:
    enqueued = _install_unpin(monkeypatch)
    monkeypatch.setattr(multipart, "get_query", lambda name: name)

    async def _fail(*_a: Any, **_k: Any) -> None:
        return None

    monkeypatch.setattr(multipart, "fail_version_replication", _fail)

    class _Db:
        async def fetchrow(self, query: str, *_a: Any) -> Any:
            if query == "get_multipart_upload":
                return {"object_id": "obj-1", "is_completed": False, "current_object_version": 4}
            if query == "get_multipart_version_by_upload":
                return {"object_version": 4}
            return None

        async def fetch(self, *_a: Any) -> list[Any]:
            return []

        def transaction(self) -> Any:
            class _Txn:
                async def __aenter__(self) -> _Txn:
                    return self

                async def __aexit__(self, *_exc: Any) -> bool:
                    return False

            return _Txn()

    request = SimpleNamespace(
        query_params={"uploadId": "up-1"},
        app=SimpleNamespace(
            state=SimpleNamespace(redis_client=SimpleNamespace(), fs_store=SimpleNamespace(delete_object=_fail))
        ),
    )
    resp = await abort_multipart_upload("b", "k", request, _Db())
    assert resp.status_code == 204
    assert enqueued == []


def _copy_request() -> Any:
    return SimpleNamespace(
        state=SimpleNamespace(main_account_id="acct-main", ray_id="ray-1"),
        app=SimpleNamespace(state=SimpleNamespace(fs_store=SimpleNamespace(), obj_cache=SimpleNamespace())),
        headers={},
    )


def _patch_copy(
    monkeypatch: pytest.MonkeyPatch, *, version: int, address_error: BaseException | None = None
) -> list[Any]:
    enqueued = _install_unpin(monkeypatch)

    async def fake_stream(*_a: Any, **_k: Any) -> Any:
        async def _iter() -> Any:
            yield b"payload"

        return _iter()

    async def fake_put(_self: Any, **_k: Any) -> PutResult:
        return PutResult(
            object_id="obj-1",
            etag="etag",
            size_bytes=4,
            upload_id="up-1",
            object_version=version,
        )

    async def fake_address(*_a: Any, **_k: Any) -> None:
        if address_error is not None:
            raise address_error

    monkeypatch.setattr(copy_helpers, "stream_object", fake_stream)
    monkeypatch.setattr(copy_helpers.ObjectWriter, "put_simple_stream_full", fake_put)
    monkeypatch.setattr(copy_helpers, "set_object_version_address", fake_address)
    return enqueued


async def _run_copy(pool: Any, *, versioning: str | None) -> Any:
    return await handle_streaming_copy(
        pool=pool,
        redis_client=SimpleNamespace(),
        request=_copy_request(),
        source_bucket={"bucket_id": "src", "bucket_name": "src", "is_public": False},
        dest_bucket={"bucket_id": "dst", "bucket_name": "dst", "versioning_status": versioning},
        source_object={"object_key": "src-key", "content_type": "text/plain"},
        src_obj_row={
            "object_id": str(uuid.uuid4()),
            "bucket_id": "src",
            "object_key": "src-key",
            "object_version": 3,
            "storage_version": 5,
            "metadata": {},
            "multipart": False,
            "content_type": "text/plain",
        },
        object_id="obj-1",
        object_key="dst-key",
        copy_created_at=_NOW,
        config=get_config(),
    )


def _copy_router(previous: int | None, seen: list[tuple[Any, ...]]) -> Any:
    def router(method: str, query: str, args: tuple[Any, ...]) -> Any:
        if _QUERY in (query or ""):
            seen.append(args)
            return None if previous is None else {"object_version": previous}
        return None

    return router


@pytest.mark.asyncio
async def test_copy_overwrite_enqueues_the_previous_version(monkeypatch: pytest.MonkeyPatch) -> None:
    enqueued = _patch_copy(monkeypatch, version=4)
    seen: list[tuple[Any, ...]] = []
    resp = await _run_copy(make_fake_pool(_copy_router(2, seen)), versioning=None)
    assert resp.status_code == 200
    _assert_specific(enqueued, [2])
    assert seen and seen[0][1] == 4


@pytest.mark.asyncio
async def test_copy_version_1_enqueues_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    enqueued = _patch_copy(monkeypatch, version=1)
    seen: list[tuple[Any, ...]] = []
    resp = await _run_copy(make_fake_pool(_copy_router(None, seen)), versioning=None)
    assert resp.status_code == 200
    assert enqueued == []
    assert seen == []


@pytest.mark.asyncio
async def test_copy_enabled_bucket_enqueues_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    enqueued = _patch_copy(monkeypatch, version=4)
    seen: list[tuple[Any, ...]] = []
    resp = await _run_copy(make_fake_pool(_copy_router(2, seen)), versioning="Enabled")
    assert resp.status_code == 200
    assert enqueued == []
    assert seen == []


@pytest.mark.asyncio
async def test_copy_address_failure_enqueues_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    enqueued = _patch_copy(monkeypatch, version=4, address_error=RuntimeError("address write failed"))
    seen: list[tuple[Any, ...]] = []
    with pytest.raises(RuntimeError, match="address write failed"):
        await _run_copy(make_fake_pool(_copy_router(2, seen)), versioning=None)
    assert enqueued == []
    assert seen == []
