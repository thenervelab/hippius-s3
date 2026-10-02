"""Put/Get/Delete bucket policy: prefix rows never become a bucket ACL, and a
public bucket never stores a prefix that would look like it narrowed the grant.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import Response

from hippius_s3.api.s3.buckets import bucket_policy_endpoint as policy
from hippius_s3.api.s3.buckets import router as bucket_router
from hippius_s3.models.acl import ACL
from hippius_s3.models.acl import Grant
from hippius_s3.models.acl import Grantee
from hippius_s3.models.acl import GranteeType
from hippius_s3.models.acl import Owner
from hippius_s3.models.acl import Permission
from hippius_s3.models.acl import WellKnownGroups
from hippius_s3.services.public_prefix_policy import MAX_POLICY_BYTES


BUCKET = "evidence"
OWNER = "owner-acct"


def _acl(*perms: Permission) -> ACL:
    grants = [
        Grant(grantee=Grantee(type=GranteeType.CANONICAL_USER, id=OWNER), permission=Permission.FULL_CONTROL),
    ]
    grants.extend(
        Grant(grantee=Grantee(type=GranteeType.GROUP, uri=WellKnownGroups.ALL_USERS), permission=perm) for perm in perms
    )
    return ACL(owner=Owner(id=OWNER), grants=grants)


class _Caches:
    def __init__(self) -> None:
        self.prefixes: list[str] = []
        self.buckets: list[str] = []

    async def invalidate_public_prefixes(self, bucket_id: str) -> None:
        self.prefixes.append(bucket_id)

    async def invalidate_cache(self, bucket_name: str) -> None:
        self.buckets.append(bucket_name)


class _DB:
    def __init__(self, *, locked: bool = True, prefixes: list[str] | None = None) -> None:
        self.bucket: dict[str, str] | None = {"bucket_id": "bid-1", "main_account_id": OWNER}
        self.acl: ACL | None = _acl()
        self.owner_mismatch = False
        self.locked = locked
        self.prefix_rows = prefixes or []
        self.execs: list[tuple[str, tuple[Any, ...], bool]] = []
        self.set_acls: list[ACL] = []
        self.in_tx = False
        self.events: list[str] = []
        self.lock_calls = 0

    def transaction(self) -> _Tx:
        return _Tx(self)

    async def fetchrow(self, _query: str, *_args: Any) -> dict[str, str] | None:
        assert self.in_tx
        self.lock_calls += 1
        return {"bucket_id": "bid-1"} if self.locked else None

    async def execute(self, query: str, *args: Any) -> None:
        self.execs.append((query, args, self.in_tx))

    async def fetch(self, _query: str, *_args: Any) -> list[dict[str, str]]:
        return [{"prefix": prefix} for prefix in self.prefix_rows]


class _Tx:
    def __init__(self, db: _DB) -> None:
        self.db = db

    async def __aenter__(self) -> _Tx:
        self.db.in_tx = True
        self.db.events.append("begin")
        return self

    async def __aexit__(self, exc_type: Any, _exc: Any, _tb: Any) -> bool:
        self.db.in_tx = False
        self.db.events.append("rollback" if exc_type else "commit")
        return False


class _Buckets:
    def __init__(self, db: _DB) -> None:
        self.db = db

    async def get_by_name(self, _name: str) -> dict[str, str] | None:
        return self.db.bucket

    async def get_by_name_and_owner(self, _name: str, _owner: str) -> dict[str, str] | None:
        if self.db.owner_mismatch:
            return None
        return self.db.bucket


class _Acls:
    def __init__(self, db: _DB) -> None:
        self.db = db

    async def get_bucket_acl(self, _name: str) -> ACL | None:
        return self.db.acl

    async def set_bucket_acl(self, _name: str, _owner: str, acl: ACL) -> None:
        assert self.db.in_tx
        self.db.set_acls.append(acl)


class _Request:
    def __init__(self, body: bytes, caches: _Caches | None = None) -> None:
        self._body = body
        self.app = SimpleNamespace(state=SimpleNamespace(acl_service=caches))

    async def body(self) -> bytes:
        return self._body


def _install(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(policy, "BucketRepository", _Buckets)
    monkeypatch.setattr(policy, "ACLRepository", _Acls)


def _document(*resources: str) -> bytes:
    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": ["s3:GetObject"],
                    "Resource": list(resources),
                }
            ],
        }
    ).encode()


def _prefix(name: str = "public") -> bytes:
    return _document(f"arn:aws:s3:::{BUCKET}/{name}/*")


def _whole() -> bytes:
    return _document(f"arn:aws:s3:::{BUCKET}/*")


def _code(response: Response) -> str:
    return response.body.decode().split("<Code>")[1].split("</Code>")[0]


@pytest.mark.asyncio
async def test_prefix_put_replaces_prefixes_and_does_not_touch_the_acl(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch)
    db = _DB()
    caches = _Caches()

    response = await policy.set_bucket_policy(BUCKET, _Request(_prefix(), caches), db)

    assert response.status_code == 204
    assert db.set_acls == []
    assert len(db.execs) == 2
    assert db.execs[0][2] is True
    assert "DELETE FROM bucket_public_prefixes" in db.execs[0][0]
    assert "unnest" in db.execs[1][0]
    assert db.execs[1][1][1] == ["public"]
    assert db.events == ["begin", "commit"]
    assert caches.prefixes == ["bid-1"]
    assert caches.buckets == []


@pytest.mark.asyncio
async def test_a_second_prefix_put_replaces_the_set(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch)
    db = _DB()

    response = await policy.set_bucket_policy(BUCKET, _Request(_prefix("weights")), db)

    assert response.status_code == 204
    assert db.execs[1][1][1] == ["weights"]


@pytest.mark.asyncio
async def test_prefix_put_on_a_public_bucket_writes_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch)
    db = _DB()
    db.acl = _acl(Permission.READ)
    caches = _Caches()

    response = await policy.set_bucket_policy(BUCKET, _Request(_prefix(), caches), db)

    assert response.status_code == 409
    assert _code(response) == "PolicyAlreadyExists"
    assert "narrow" in response.body.decode()
    assert db.execs == []
    assert db.set_acls == []
    assert caches.prefixes == []


@pytest.mark.asyncio
async def test_full_control_all_users_is_already_public(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch)
    db = _DB()
    db.acl = _acl(Permission.FULL_CONTROL)

    response = await policy.set_bucket_policy(BUCKET, _Request(_prefix()), db)

    assert response.status_code == 409
    assert db.execs == []


@pytest.mark.asyncio
async def test_all_users_write_alone_can_still_take_a_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch)
    db = _DB()
    db.acl = _acl(Permission.WRITE)

    response = await policy.set_bucket_policy(BUCKET, _Request(_prefix()), db)

    assert response.status_code == 204
    assert db.set_acls == []
    assert db.execs[1][1][1] == ["public"]


@pytest.mark.asyncio
async def test_whole_bucket_put_sets_the_acl_and_clears_prefixes(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch)
    db = _DB()
    caches = _Caches()

    response = await policy.set_bucket_policy(BUCKET, _Request(_whole(), caches), db)

    assert response.status_code == 204
    assert len(db.set_acls) == 1
    assert any(
        grant.grantee.uri == WellKnownGroups.ALL_USERS and grant.permission == Permission.READ
        for grant in db.set_acls[0].grants
    )
    assert len(db.execs) == 1
    assert "DELETE FROM bucket_public_prefixes" in db.execs[0][0]
    assert caches.prefixes == ["bid-1"]
    assert caches.buckets == [BUCKET]


@pytest.mark.asyncio
async def test_whole_bucket_put_on_a_public_bucket_keeps_prefixes(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch)
    db = _DB()
    db.acl = _acl(Permission.READ)

    response = await policy.set_bucket_policy(BUCKET, _Request(_whole()), db)

    assert response.status_code == 409
    assert "already exists" in response.body.decode()
    assert db.execs == []
    assert db.set_acls == []


@pytest.mark.asyncio
async def test_other_bucket_resource_does_not_open_a_transaction(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch)
    db = _DB()

    response = await policy.set_bucket_policy(BUCKET, _Request(_document("arn:aws:s3:::other/public/*")), db)

    assert response.status_code == 400
    assert _code(response) == "InvalidPolicyDocument"
    assert db.events == []
    assert db.execs == []
    assert db.set_acls == []


@pytest.mark.asyncio
async def test_an_extra_deny_is_rejected_and_not_stored(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch)
    db = _DB()
    body = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                json.loads(_whole())["Statement"][0],
                {"Effect": "Deny", "Principal": "*", "Action": "s3:*", "Resource": f"arn:aws:s3:::{BUCKET}/*"},
            ],
        }
    ).encode()

    response = await policy.set_bucket_policy(BUCKET, _Request(body), db)

    assert response.status_code == 400
    assert db.events == []
    assert db.set_acls == []


@pytest.mark.asyncio
async def test_oversized_and_malformed_documents_are_400(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch)
    db = _DB()

    oversized = await policy.set_bucket_policy(BUCKET, _Request(b"{" * (MAX_POLICY_BYTES + 1)), db)
    malformed = await policy.set_bucket_policy(BUCKET, _Request(b"{"), db)
    empty = await policy.set_bucket_policy(BUCKET, _Request(b""), db)

    assert oversized.status_code == malformed.status_code == empty.status_code == 400
    assert _code(malformed) == "MalformedPolicy"
    assert db.events == []


@pytest.mark.asyncio
async def test_missing_bucket_is_404_before_any_write(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch)
    db = _DB()
    db.bucket = None

    response = await policy.set_bucket_policy(BUCKET, _Request(_prefix()), db)

    assert response.status_code == 404
    assert _code(response) == "NoSuchBucket"
    assert db.events == []


@pytest.mark.asyncio
async def test_bucket_deleted_before_the_lock_is_404(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch)
    db = _DB(locked=False)

    response = await policy.set_bucket_policy(BUCKET, _Request(_prefix()), db)

    assert response.status_code == 404
    assert db.execs == []
    assert db.events[-1] == "commit"


@pytest.mark.asyncio
async def test_get_returns_prefixes_without_a_public_acl(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch)
    db = _DB(prefixes=["weights", "public"])

    response = await policy.get_bucket_policy(BUCKET, db, OWNER)

    document = json.loads(response.body)
    assert response.status_code == 200
    assert [item["Resource"] for item in document["Statement"]] == [
        [f"arn:aws:s3:::{BUCKET}/public/*"],
        [f"arn:aws:s3:::{BUCKET}/weights/*"],
    ]


@pytest.mark.asyncio
async def test_get_shows_both_when_the_acl_was_opened_later(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch)
    db = _DB(prefixes=["public"])
    db.acl = _acl(Permission.READ)

    response = await policy.get_bucket_policy(BUCKET, db, OWNER)

    resources = [item["Resource"][0] for item in json.loads(response.body)["Statement"]]
    assert resources == [f"arn:aws:s3:::{BUCKET}/*", f"arn:aws:s3:::{BUCKET}/public/*"]


@pytest.mark.asyncio
async def test_get_is_404_when_nothing_is_published(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch)
    response = await policy.get_bucket_policy(BUCKET, _DB(), OWNER)
    assert response.status_code == 404
    assert _code(response) == "NoSuchBucketPolicy"


@pytest.mark.asyncio
async def test_get_for_another_account_is_404(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch)
    db = _DB(prefixes=["public"])
    db.owner_mismatch = True

    response = await policy.get_bucket_policy(BUCKET, db, "someone-else")

    assert response.status_code == 404
    assert _code(response) == "NoSuchBucket"


@pytest.mark.asyncio
async def test_delete_clears_prefixes_and_leaves_the_acl(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch)
    db = _DB()
    caches = _Caches()

    response = await policy.delete_bucket_policy(BUCKET, db, _Request(b"", caches))

    assert response.status_code == 204
    assert db.lock_calls == 1
    assert len(db.execs) == 1
    assert "DELETE FROM bucket_public_prefixes" in db.execs[0][0]
    assert db.execs[0][2] is True
    assert db.events == ["begin", "commit"]
    assert db.set_acls == []
    assert caches.prefixes == ["bid-1"]
    assert caches.buckets == []


@pytest.mark.asyncio
@pytest.mark.parametrize("perm", [Permission.READ, Permission.FULL_CONTROL])
async def test_delete_refuses_while_the_acl_grants_anonymous_read(
    monkeypatch: pytest.MonkeyPatch, perm: Permission
) -> None:
    _install(monkeypatch)
    db = _DB(prefixes=["public"])
    db.acl = _acl(perm)
    caches = _Caches()

    response = await policy.delete_bucket_policy(BUCKET, db, _Request(b"", caches))

    assert response.status_code == 409
    assert _code(response) == "InvalidBucketState"
    assert db.lock_calls == 1
    assert db.execs == []
    assert db.set_acls == []
    assert db.events == ["begin", "commit"]
    assert caches.prefixes == []
    assert caches.buckets == []


@pytest.mark.asyncio
async def test_delete_clears_prefixes_when_all_users_has_only_write(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch)
    db = _DB()
    db.acl = _acl(Permission.WRITE)
    caches = _Caches()

    response = await policy.delete_bucket_policy(BUCKET, db, _Request(b"", caches))

    assert response.status_code == 204
    assert len(db.execs) == 1
    assert "DELETE FROM bucket_public_prefixes" in db.execs[0][0]
    assert db.set_acls == []
    assert caches.prefixes == ["bid-1"]


@pytest.mark.asyncio
async def test_delete_of_a_bucket_that_vanishes_before_the_lock_writes_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install(monkeypatch)
    db = _DB(locked=False)
    caches = _Caches()

    response = await policy.delete_bucket_policy(BUCKET, db, _Request(b"", caches))

    assert response.status_code == 404
    assert _code(response) == "NoSuchBucket"
    assert db.lock_calls == 1
    assert db.execs == []
    assert caches.prefixes == []


@pytest.mark.asyncio
async def test_delete_of_a_missing_bucket_is_404(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch)
    db = _DB()
    db.bucket = None

    response = await policy.delete_bucket_policy(BUCKET, db, _Request(b""))

    assert response.status_code == 404
    assert db.execs == []


class _Pool:
    def acquire(self) -> Any:
        class _Ctx:
            async def __aenter__(self) -> object:
                return object()

            async def __aexit__(self, *_: Any) -> None:
                return None

        return _Ctx()


@pytest.mark.asyncio
async def test_delete_policy_route_does_not_delete_the_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    async def _boom(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("DeleteBucket")

    async def _policy(bucket_name: str, _conn: Any, request: Any) -> Response:
        seen["bucket"] = bucket_name
        seen["params"] = dict(request.query_params)
        return Response(status_code=204)

    monkeypatch.setattr(bucket_router, "handle_delete_bucket", _boom)
    monkeypatch.setattr(bucket_router, "delete_bucket_policy", _policy)
    request = SimpleNamespace(query_params={"policy": ""})

    response = await bucket_router.delete_bucket_tags_route("some-bucket", request, _Pool(), None)

    assert response.status_code == 204
    assert seen == {"bucket": "some-bucket", "params": {"policy": ""}}


@pytest.mark.asyncio
async def test_unknown_delete_subresource_is_not_a_bucket_delete(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _boom(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("DeleteBucket")

    monkeypatch.setattr(bucket_router, "handle_delete_bucket", _boom)
    request = SimpleNamespace(query_params={"cors": ""})

    response = await bucket_router.delete_bucket_tags_route("some-bucket", request, _Pool(), None)

    assert response.status_code == 501
