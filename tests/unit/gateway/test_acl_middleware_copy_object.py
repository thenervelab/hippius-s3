"""ACL middleware tests for CopyObject / UploadPartCopy source-bucket scope check.

The destination bucket scope is enforced via the resolver in
`gateway/services/sub_token_scope.py` (covered by test_sub_token_scope_matrix.py).
The source bucket — supplied via `x-amz-copy-source` header — is checked
separately in `acl_middleware`. These tests exercise that source-side check
end-to-end through a FastAPI app + ASGI transport, with stub auth and mock
acl_service / scope_repo / redis fixtures.

Mostly negative-path tests: the source check failing must produce a 403 with
no body leakage. The positive paths confirm that legitimate intra-account
copies and cross-account fallthroughs both keep working.
"""

from __future__ import annotations

from typing import Awaitable
from typing import Callable
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from httpx import ASGITransport
from httpx import AsyncClient

from hippius_s3.gateway.middlewares.acl import acl_middleware
from hippius_s3.gateway.services.acl_service import BucketLookup
from hippius_s3.models.sub_token import BucketScope
from hippius_s3.models.sub_token import Permission
from hippius_s3.models.sub_token import SubTokenScope
from tests.unit.gateway._suspension_fakes import install_no_suspension_state


# ---- Helpers ---------------------------------------------------------------


def _scope(permission: Permission, bucket_scope: BucketScope, bucket_ids: list[str]) -> SubTokenScope:
    return SubTokenScope(
        access_key_id="hip_sub_alice",
        account_id="alice",
        permission=permission,
        bucket_scope=bucket_scope,
        bucket_ids=tuple(bucket_ids),
    )


def _make_app(
    *,
    scope: SubTokenScope | None,
    bucket_owner_lookup: dict[str, tuple[str | None, str | None]],
    account_id: str = "alice",
    access_key: str = "hip_sub_alice",
    token_type: str = "sub",
) -> FastAPI:
    """Build a FastAPI app that runs acl_middleware end-to-end with stub auth.

    `bucket_owner_lookup` maps bucket name → (owner_id, bucket_id). Buckets not
    in the dict resolve to (None, None) — i.e. NoSuchBucket.
    """
    app = FastAPI()

    acl_service = AsyncMock()
    acl_service.check_permission = AsyncMock(return_value=True)

    async def _lookup(bucket_name: str) -> BucketLookup | None:
        owner_id, bucket_id = bucket_owner_lookup.get(bucket_name, (None, None))
        if owner_id is None or bucket_id is None:
            return None
        return BucketLookup(owner_id=owner_id, bucket_id=bucket_id, is_cache_warm=False)

    acl_service.get_bucket_owner_and_id = AsyncMock(side_effect=_lookup)
    app.state.acl_service = acl_service
    install_no_suspension_state(app)

    repo = AsyncMock()
    repo.get = AsyncMock(return_value=scope)
    app.state.sub_token_scope_repo = repo

    redis_client = AsyncMock()
    redis_client.get = AsyncMock(return_value=None)
    redis_client.setex = AsyncMock()
    redis_client.delete = AsyncMock()
    app.state.redis_client = redis_client

    @app.api_route("/{path:path}", methods=["GET", "HEAD", "PUT", "POST", "DELETE"])
    async def catch_all(request: Request) -> Response:
        return Response(status_code=200, content=b"ok")

    async def stub_auth(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        request.state.auth_method = "access_key"
        request.state.token_type = token_type
        request.state.account_id = account_id
        request.state.access_key = access_key
        return await call_next(request)

    app.middleware("http")(acl_middleware)
    app.middleware("http")(stub_auth)

    return app


# ---- Source-bucket scope check (the actual bug we're fixing) --------------


@pytest.mark.asyncio
async def test_copy_object_intra_account_source_in_scope_allowed() -> None:
    """Both src and dest in scope → copy succeeds."""
    scope = _scope(Permission.object_read_write, BucketScope.specific, ["dest-id", "src-id"])
    app = _make_app(
        scope=scope,
        bucket_owner_lookup={
            "dest-bucket": ("alice", "dest-id"),
            "src-bucket": ("alice", "src-id"),
        },
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.put("/dest-bucket/dest-key", headers={"x-amz-copy-source": "/src-bucket/src-key"})
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_copy_object_source_out_of_scope_denied() -> None:
    """Dest in scope but src is intra-account and out of scope → 403.

    This is the bug PR #149's first cut introduced: a sub-token scoped only to
    `dest-bucket` could exfiltrate data from any other bucket of the same
    account.
    """
    scope = _scope(Permission.object_read_write, BucketScope.specific, ["dest-id"])
    app = _make_app(
        scope=scope,
        bucket_owner_lookup={
            "dest-bucket": ("alice", "dest-id"),
            "secret-bucket": ("alice", "secret-id"),
        },
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.put("/dest-bucket/dest-key", headers={"x-amz-copy-source": "/secret-bucket/private.txt"})
    assert r.status_code == 403


@pytest.mark.asyncio
async def test_copy_object_with_bucket_scope_all_allows_any_source() -> None:
    """`bucket_scope='all'` covers any bucket of the account, source included."""
    scope = _scope(Permission.object_read_write, BucketScope.all, [])
    app = _make_app(
        scope=scope,
        bucket_owner_lookup={
            "dest-bucket": ("alice", "dest-id"),
            "src-bucket": ("alice", "src-id"),
        },
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.put("/dest-bucket/dest-key", headers={"x-amz-copy-source": "/src-bucket/src-key"})
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_copy_object_cross_account_source_falls_through_to_backend() -> None:
    """Cross-account source: middleware does not block — the existing
    bucket-ACL grant flow handles delegation. Without this, a sub-token
    couldn't read a contractor-shared bucket via copy.
    """
    scope = _scope(Permission.object_read_write, BucketScope.specific, ["dest-id"])
    app = _make_app(
        scope=scope,
        bucket_owner_lookup={
            "dest-bucket": ("alice", "dest-id"),
            "shared-bucket": ("bob", "shared-id"),
        },
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.put("/dest-bucket/dest-key", headers={"x-amz-copy-source": "/shared-bucket/file.txt"})
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_copy_object_nonexistent_source_falls_through_to_backend() -> None:
    """Source bucket not in DB: pass through so the backend returns NoSuchBucket
    instead of an opaque 403. (Matches the existing dest-bucket-not-found
    fallthrough at the same layer.)
    """
    scope = _scope(Permission.object_read_write, BucketScope.specific, ["dest-id"])
    app = _make_app(
        scope=scope,
        bucket_owner_lookup={"dest-bucket": ("alice", "dest-id")},
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.put("/dest-bucket/dest-key", headers={"x-amz-copy-source": "/ghost-bucket/file.txt"})
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_copy_object_with_an_arn_copy_source_is_authorised_as_a_literal_bucket() -> None:
    """The ARN form is not special-cased — it is authorised as the name the handlers will use.

    This test previously asserted the opposite (200, "the backend will reject it itself"). That
    contract was the bug: neither handler recognises the ARN form, both read it as a literal
    bucket name, so skipping the check here left the source unauthorised while the handler went
    on to resolve it. Failing open whenever THIS parser disagrees with the handler's parser is
    precisely the split-view class. The middleware now authorises whatever the handler will act
    on, so an ARN source is a permission check against a bucket that does not exist -> 403.
    """
    scope = _scope(Permission.object_read_write, BucketScope.specific, ["dest-id"])
    app = _make_app(
        scope=scope,
        bucket_owner_lookup={"dest-bucket": ("alice", "dest-id")},
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.put("/dest-bucket/dest-key", headers={"x-amz-copy-source": "arn:aws:s3:::other/key"})
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_copy_source_with_no_slash_is_refused() -> None:
    """A header naming no key at all cannot be authorised, so it must not be waved through."""
    scope = _scope(Permission.object_read_write, BucketScope.specific, ["dest-id"])
    app = _make_app(
        scope=scope,
        bucket_owner_lookup={"dest-bucket": ("alice", "dest-id")},
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.put("/dest-bucket/dest-key", headers={"x-amz-copy-source": "just-a-bucket"})
    assert r.status_code == 403


@pytest.mark.asyncio
async def test_upload_part_copy_enforces_source_scope() -> None:
    """UploadPartCopy (PUT with ?partNumber&uploadId + x-amz-copy-source) gets
    the same source-bucket check — no by-passing the gap via MPU."""
    scope = _scope(Permission.object_read_write, BucketScope.specific, ["dest-id"])
    app = _make_app(
        scope=scope,
        bucket_owner_lookup={
            "dest-bucket": ("alice", "dest-id"),
            "secret-bucket": ("alice", "secret-id"),
        },
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.put(
            "/dest-bucket/dest-key?partNumber=1&uploadId=u",
            headers={"x-amz-copy-source": "/secret-bucket/private.txt"},
        )
    assert r.status_code == 403


@pytest.mark.asyncio
async def test_copy_with_same_source_and_destination_in_scope_allowed() -> None:
    """Self-copy (rename within a single bucket) is a valid pattern; scope on
    that bucket alone must allow it."""
    scope = _scope(Permission.object_read_write, BucketScope.specific, ["bucket-id"])
    app = _make_app(
        scope=scope,
        bucket_owner_lookup={"my-bucket": ("alice", "bucket-id")},
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.put("/my-bucket/new-key", headers={"x-amz-copy-source": "/my-bucket/old-key"})
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_object_read_only_token_cannot_use_copy_to_write() -> None:
    """`object_read` doesn't allow write_object — the dest scope check fails
    first, before we even look at the source. No need for source-bucket
    misclassification to leak bytes through CopyObject.
    """
    scope = _scope(Permission.object_read, BucketScope.specific, ["dest-id", "src-id"])
    app = _make_app(
        scope=scope,
        bucket_owner_lookup={
            "dest-bucket": ("alice", "dest-id"),
            "src-bucket": ("alice", "src-id"),
        },
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.put("/dest-bucket/dest-key", headers={"x-amz-copy-source": "/src-bucket/src-key"})
    assert r.status_code == 403


@pytest.mark.asyncio
async def test_copy_source_check_skipped_when_no_header() -> None:
    """Plain PutObject (no copy header) doesn't trigger the source-bucket
    lookup — saves a DB round-trip on the hot path.
    """
    scope = _scope(Permission.object_read_write, BucketScope.specific, ["dest-id"])
    app = _make_app(
        scope=scope,
        bucket_owner_lookup={"dest-bucket": ("alice", "dest-id")},
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.put("/dest-bucket/dest-key", content=b"hello")
    assert r.status_code == 200
    # bucket lookup is called ONCE (for the destination), not twice.
    assert app.state.acl_service.get_bucket_owner_and_id.await_count == 1


@pytest.mark.asyncio
async def test_master_token_copy_object_bypasses_source_scope_check() -> None:
    """Master tokens never enter the sub-token branch — copy-source scope check
    must not trigger for them, regardless of bucket layout. Master is
    authoritative; ACL subsequently runs through `check_permission` as usual."""
    scope = _scope(Permission.object_read, BucketScope.specific, ["unrelated"])
    app = _make_app(
        scope=scope,
        bucket_owner_lookup={
            "dest-bucket": ("alice", "dest-id"),
            "src-bucket": ("alice", "src-id"),
        },
        token_type="master",
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.put("/dest-bucket/dest-key", headers={"x-amz-copy-source": "/src-bucket/src-key"})
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_percent_encoded_copy_source_cannot_skip_the_source_scope() -> None:
    """`secret-bucket%2Fprivate.txt` decodes to bucket `secret-bucket` in the handlers. The scope
    pass used to split BEFORE decoding, saw a nonexistent bucket, skipped the check, and let a
    token scoped to the destination copy out of a bucket it cannot read."""
    scope = _scope(Permission.object_read_write, BucketScope.specific, ["dest-id"])
    app = _make_app(
        scope=scope,
        bucket_owner_lookup={
            "dest-bucket": ("alice", "dest-id"),
            "secret-bucket": ("alice", "secret-id"),
        },
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.put("/dest-bucket/dest-key", headers={"x-amz-copy-source": "secret-bucket%2Fprivate.txt"})
    assert r.status_code == 403


# ---- The tier is a ceiling on cross-account grants -------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method,path,params,headers",
    [
        pytest.param("DELETE", "/shared-bucket/k", {}, {}, id="DeleteObject"),
        pytest.param("DELETE", "/shared-bucket/k", {"versionId": "1"}, {}, id="DeleteObjectVersion"),
        pytest.param("POST", "/shared-bucket", {"delete": ""}, {}, id="DeleteObjects"),
        pytest.param("PUT", "/shared-bucket/k", {"legal-hold": ""}, {}, id="PutObjectLegalHold"),
        pytest.param("PUT", "/shared-bucket/k", {"acl": ""}, {}, id="PutObjectAcl"),
        pytest.param("PUT", "/shared-bucket/k", {}, {"x-amz-grant-full-control": "id=hip_other"}, id="PutWithGrant"),
    ],
)
async def test_cross_account_grant_cannot_lift_a_no_delete_key_past_its_tier(
    method: str, path: str, params: dict[str, str], headers: dict[str, str]
) -> None:
    """bob's bucket grants alice's write-once key everything (check_permission is True). The grant
    authorises the request, but the key's own tier still caps what it may do."""
    scope = _scope(Permission.object_read_write_no_delete, BucketScope.specific, ["alice-bucket-id"])
    app = _make_app(scope=scope, bucket_owner_lookup={"shared-bucket": ("bob", "shared-id")})
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.request(method, path, params=params, headers=headers)
    assert r.status_code == 403
    app.state.acl_service.check_permission.assert_not_awaited()


@pytest.mark.asyncio
async def test_cross_account_grant_still_authorises_what_the_tier_allows() -> None:
    scope = _scope(Permission.object_read_write_no_delete, BucketScope.specific, ["alice-bucket-id"])
    app = _make_app(scope=scope, bucket_owner_lookup={"shared-bucket": ("bob", "shared-id")})
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.put("/shared-bucket/k", content=b"x")
    assert r.status_code == 200
    app.state.acl_service.check_permission.assert_awaited()


@pytest.mark.asyncio
async def test_cross_account_token_without_a_scope_keeps_the_contractor_flow() -> None:
    app = _make_app(scope=None, bucket_owner_lookup={"shared-bucket": ("bob", "shared-id")})
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.delete("/shared-bucket/k")
    assert r.status_code == 200
