from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from hippius_s3.api.s3.objects import delete_object_endpoint as mod


class _FakeDb:
    """Branches on query NAME — `get_query` is monkeypatched to the identity function."""

    def __init__(
        self,
        *,
        versions: list[dict[str, Any]],
        current: int,
        object_id: str = "obj-1",
    ) -> None:
        self.versions = {int(v["object_version"]): v for v in versions}
        self.current = current
        self.object_id = object_id
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def _record(self, query: str, args: tuple[Any, ...]) -> None:
        self.calls.append((query, args))

    def names(self) -> list[str]:
        return [q for q, _ in self.calls]

    def args_for(self, name: str) -> list[tuple[Any, ...]]:
        return [a for q, a in self.calls if q == name]

    async def fetchrow(self, query: str, *args: Any) -> Any:
        self._record(query, args)
        if query == "lock_object_and_get_version":
            # LEFT JOIN: the locked objects row comes back even when the version is absent,
            # with NULL version columns.
            row = self.versions.get(int(args[2]))
            if row is not None and row.get("deleted"):
                row = None
            return {
                "object_id": self.object_id,
                "current_object_version": self.current,
                "object_version": row["object_version"] if row else None,
                "is_delete_marker": bool(row.get("is_delete_marker", False)) if row else False,
            }
        if query == "soft_delete_object_version":
            v = int(args[1])
            if v in self.versions:
                self.versions[v]["deleted"] = True
                return {"object_id": self.object_id, "object_version": v}
            return None
        if query == "repoint_current_version_after_delete":
            # Mirrors the SQL: newest live version strictly BELOW the deleted one, or no row.
            below = [v for v, row in self.versions.items() if not row.get("deleted") and v < int(args[1])]
            if not below:
                return None
            self.current = max(below)
            return {"current_object_version": self.current}
        if query == "insert_delete_marker":
            new_v = max(self.versions) + 1 if self.versions else 1
            self.versions[new_v] = {"object_version": new_v, "is_delete_marker": True}
            self.current = new_v
            return {"object_id": self.object_id, "object_version": new_v}
        if query == "soft_delete_object":
            return {"object_id": self.object_id, "current_object_version": self.current}
        return None

    async def fetch(self, query: str, *args: Any) -> list[Any]:
        self._record(query, args)
        return []

    async def execute(self, query: str, *args: Any) -> str:
        self._record(query, args)
        return "OK"

    def transaction(self) -> Any:
        db = self

        class _Txn:
            async def __aenter__(self) -> Any:
                db._record("BEGIN", ())
                return db

            async def __aexit__(self, *_exc: Any) -> bool:
                db._record("COMMIT", ())
                return False

        return _Txn()


def _request(version_id: str | None = None) -> Any:
    qp: dict[str, str] = {} if version_id is None else {"versionId": version_id}
    return SimpleNamespace(
        state=SimpleNamespace(main_account_id="acct-main", ray_id="ray-1"),
        query_params=qp,
        headers={"Host": "h"},
    )


@pytest.fixture
def wiring(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Neutralise collaborators and record every unpin enqueued."""
    enqueued: list[Any] = []
    bucket: dict[str, Any] = {
        "bucket_id": "bkt-1",
        "bucket_name": "b",
        "main_account_id": "acct-main",
        "versioning_status": None,
    }

    monkeypatch.setattr(mod, "get_query", lambda name: name)

    class _FakeUserRepo:
        def __init__(self, _db: Any) -> None: ...

        async def ensure_by_main_account(self, account: str) -> dict[str, Any]:
            return {"main_account_id": account}

    class _FakeBucketRepo:
        def __init__(self, _db: Any) -> None: ...

        async def get_by_name_and_owner(self, _name: str, _owner: str) -> Any:
            return bucket

    async def _resolve_backends(_db: Any, _oid: str, _ver: int | None = None) -> list[str]:
        return ["arion"]

    async def _enqueue(payload: Any = None, **_kw: Any) -> None:
        enqueued.append(payload)

    monkeypatch.setattr(mod, "UserRepository", _FakeUserRepo)
    monkeypatch.setattr(mod, "BucketRepository", _FakeBucketRepo)
    monkeypatch.setattr(mod, "resolve_object_backends", _resolve_backends)
    monkeypatch.setattr(mod, "enqueue_unpin_request", _enqueue)

    return {"enqueued": enqueued, "bucket": bucket}


def _unpinned_versions(enqueued: list[Any]) -> list[Any]:
    """Every unpin scope enqueued, INCLUDING None.

    None means "every version of this object" — the widest, most destructive scope there is. This
    helper used to filter it out, which made an all-versions unpin indistinguishable from no unpin
    at all, so no assertion here could tell those two apart.
    """
    return sorted(
        (None if p.object_version is None else int(p.object_version) for p in enqueued),
        key=lambda v: (v is not None, v),
    )


# --- The regression this whole change exists for ------------------------------------------


@pytest.mark.asyncio
async def test_versioned_delete_removes_only_that_version(wiring: dict[str, Any]) -> None:
    """DELETE ?versionId=1 must NOT destroy the object.

    Prod previously ignored the versionId and soft-deleted the whole object, taking every
    version with it. Guard that: `soft_delete_object` must not be reached.
    """
    wiring["bucket"]["versioning_status"] = "Enabled"
    db = _FakeDb(
        versions=[{"object_version": 1}, {"object_version": 2}, {"object_version": 3}],
        current=3,
    )
    resp = await mod.handle_delete_object("b", "k", _request("1"), db, None)

    assert resp.status_code == 204
    assert "soft_delete_object" not in db.names()
    assert db.args_for("soft_delete_object_version") == [("obj-1", 1)]
    assert _unpinned_versions(wiring["enqueued"]) == [1]
    assert db.current == 3


@pytest.mark.asyncio
async def test_versioned_delete_of_current_rolls_pointer_back(wiring: dict[str, Any]) -> None:
    wiring["bucket"]["versioning_status"] = "Enabled"
    db = _FakeDb(versions=[{"object_version": 1}, {"object_version": 2}], current=2)
    resp = await mod.handle_delete_object("b", "k", _request("2"), db, None)

    assert resp.status_code == 204
    assert db.args_for("repoint_current_version_after_delete") == [("obj-1", 2)]
    assert db.current == 1
    assert _unpinned_versions(wiring["enqueued"]) == [2]


@pytest.mark.asyncio
async def test_versioned_delete_of_only_version_soft_deletes_object(wiring: dict[str, Any]) -> None:
    wiring["bucket"]["versioning_status"] = "Enabled"
    db = _FakeDb(versions=[{"object_version": 1}], current=1)
    resp = await mod.handle_delete_object("b", "k", _request("1"), db, None)

    assert resp.status_code == 204
    # Nothing left to point at, so the object itself goes — and the unpin widens to match.
    assert "soft_delete_object" in db.names()
    assert _unpinned_versions(wiring["enqueued"]) == [None]


@pytest.mark.asyncio
async def test_versioned_delete_of_marker_is_an_undelete(wiring: dict[str, Any]) -> None:
    wiring["bucket"]["versioning_status"] = "Enabled"
    db = _FakeDb(
        versions=[{"object_version": 1}, {"object_version": 2, "is_delete_marker": True}],
        current=2,
    )
    resp = await mod.handle_delete_object("b", "k", _request("2"), db, None)

    assert resp.status_code == 204
    assert resp.headers.get("x-amz-delete-marker") == "true"
    assert resp.headers.get("x-amz-version-id") == "2"
    # A marker holds no data, so there is nothing to unpin.
    assert wiring["enqueued"] == []
    assert db.current == 1


@pytest.mark.asyncio
async def test_versioned_delete_unknown_version_is_idempotent(wiring: dict[str, Any]) -> None:
    wiring["bucket"]["versioning_status"] = "Enabled"
    db = _FakeDb(versions=[{"object_version": 1}], current=1)
    resp = await mod.handle_delete_object("b", "k", _request("99"), db, None)

    assert resp.status_code == 204
    assert "soft_delete_object" not in db.names()
    assert wiring["enqueued"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad",
    [
        "abc",
        "0",
        "-1",
        "1.5",
        # Bare int() accepted every one of these, each resolving to a DIFFERENT version than
        # the caller named: underscore grouping ("1_0" -> 10), a sign, surrounding whitespace,
        # and non-ASCII decimal digits (covered in test_parse_version_id.py).
        "1_0",
        "+3",
        " 2",
        # Parsed fine, then overflowed asyncpg's int8 encoder at bind time -> 500 instead of 400.
        "9" * 25,
    ],
)
async def test_invalid_version_id_rejected(wiring: dict[str, Any], bad: str) -> None:
    db = _FakeDb(versions=[{"object_version": 1}], current=1)
    resp = await mod.handle_delete_object("b", "k", _request(bad), db, None)

    assert resp.status_code == 400
    assert b"InvalidArgument" in resp.body
    assert db.names() == [] or "soft_delete_object" not in db.names()


# --- Delete markers on a versioning-enabled bucket ----------------------------------------


@pytest.mark.asyncio
async def test_simple_delete_on_enabled_bucket_inserts_marker(wiring: dict[str, Any]) -> None:
    wiring["bucket"]["versioning_status"] = "Enabled"
    db = _FakeDb(versions=[{"object_version": 1}, {"object_version": 2}], current=2)

    resp = await mod.handle_delete_object("b", "k", _request(), db, None)

    assert resp.status_code == 204
    assert resp.headers.get("x-amz-delete-marker") == "true"
    assert resp.headers.get("x-amz-version-id") == "3"
    assert "insert_delete_marker" in db.names()
    # A delete marker destroys nothing.
    assert "soft_delete_object" not in db.names()
    assert wiring["enqueued"] == []


@pytest.mark.asyncio
async def test_simple_delete_on_unversioned_bucket_unpins_every_version(
    wiring: dict[str, Any],
) -> None:
    """The storage leak: prod only ever unpinned `current_object_version`.

    The fix enqueues ONE request with object_version=None, meaning "every version". Fanning out
    per version here would be O(versions) work inside the request — prod holds an object with
    646,993 of them — so the unpinner resolves the list under its own batching instead.
    """
    db = _FakeDb(
        versions=[{"object_version": 1}, {"object_version": 2}, {"object_version": 3}],
        current=3,
    )
    resp = await mod.handle_delete_object("b", "k", _request(), db, None)

    assert resp.status_code == 204
    assert "soft_delete_object" in db.names()
    assert "insert_delete_marker" not in db.names()
    assert len(wiring["enqueued"]) == 1, "must not fan out per version on the request path"
    assert wiring["enqueued"][0].object_version is None


@pytest.mark.asyncio
async def test_version_id_null_alias_behaves_as_simple_delete(wiring: dict[str, Any]) -> None:
    db = _FakeDb(versions=[{"object_version": 1}], current=1)
    resp = await mod.handle_delete_object("b", "k", _request("null"), db, None)

    assert resp.status_code == 204
    assert "soft_delete_object" in db.names()


@pytest.mark.asyncio
async def test_missing_bucket_returns_404(wiring: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    class _NoBucketRepo:
        def __init__(self, _db: Any) -> None: ...

        async def get_by_name_and_owner(self, _n: str, _o: str) -> Any:
            return None

    monkeypatch.setattr(mod, "BucketRepository", _NoBucketRepo)
    db = _FakeDb(versions=[{"object_version": 1}], current=1)
    resp = await mod.handle_delete_object("b", "k", _request("1"), db, None)

    assert resp.status_code == 404
    assert b"NoSuchBucket" in resp.body


# --- Version ids are only addressable on a versioning-enabled bucket ------------------------


@pytest.mark.asyncio
async def test_versioned_delete_on_unversioned_bucket_is_refused(wiring: dict[str, Any]) -> None:
    """The dangerous case: an unversioned bucket still RETAINS superseded versions.

    Deleting the current version by id would repoint current_object_version back onto the row the
    user overwrote, resurrecting content they believe they replaced — and both `PutObject` and
    `ListObjectVersions` hand out the integer id needed to do it. Refuse; destroy nothing.
    """
    wiring["bucket"]["versioning_status"] = None
    db = _FakeDb(versions=[{"object_version": 1}, {"object_version": 2}], current=2)

    resp = await mod.handle_delete_object("b", "k", _request("2"), db, None)

    assert resp.status_code == 404
    assert b"NoSuchVersion" in resp.body
    # Nothing mutated: no tombstone, no repoint, no whole-object delete, no unpin.
    assert db.names() == []
    assert wiring["enqueued"] == []
    assert db.current == 2


@pytest.mark.asyncio
async def test_simple_delete_on_unversioned_bucket_still_works(wiring: dict[str, Any]) -> None:
    """The refusal above must not leak into the no-versionId path."""
    wiring["bucket"]["versioning_status"] = None
    db = _FakeDb(versions=[{"object_version": 1}], current=1)

    resp = await mod.handle_delete_object("b", "k", _request(), db, None)

    assert resp.status_code == 204
    assert "soft_delete_object" in db.names()
    # Whole-object delete unpins EVERY version, not just current.
    assert _unpinned_versions(wiring["enqueued"]) == [None]


@pytest.mark.asyncio
async def test_whole_object_fallback_unpins_every_version(wiring: dict[str, Any]) -> None:
    """Deleting the last live version falls back to the whole-object delete.

    The unpin must widen to match. Leaving it version-scoped re-opens the wedge this PR exists to
    fix: hard_delete_object's readiness gate waits on ALL versions, so any sibling still holding
    live chunk_backend rows keeps the object un-hard-deletable forever.
    """
    wiring["bucket"]["versioning_status"] = "Enabled"
    db = _FakeDb(versions=[{"object_version": 1}], current=1)

    resp = await mod.handle_delete_object("b", "k", _request("1"), db, None)

    assert resp.status_code == 204
    assert "soft_delete_object" in db.names()
    assert _unpinned_versions(wiring["enqueued"]) == [None]


@pytest.mark.asyncio
async def test_versioned_delete_that_repoints_stays_version_scoped(wiring: dict[str, Any]) -> None:
    """Converse of the above: when a predecessor survives, the unpin must NOT widen to None.

    A None here would destroy the sibling versions that are still live and readable.
    """
    wiring["bucket"]["versioning_status"] = "Enabled"
    db = _FakeDb(versions=[{"object_version": 1}, {"object_version": 2}], current=2)

    resp = await mod.handle_delete_object("b", "k", _request("2"), db, None)

    assert resp.status_code == 204
    assert "soft_delete_object" not in db.names()
    assert _unpinned_versions(wiring["enqueued"]) == [2]
