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

from gateway.middlewares.acl import _parse_copy_source_bucket
from gateway.middlewares.acl import acl_middleware
from hippius_s3.models.sub_token import BucketScope
from hippius_s3.models.sub_token import Permission
from hippius_s3.models.sub_token import SubTokenScope


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

    async def _lookup(bucket_name: str) -> tuple[str | None, str | None]:
        return bucket_owner_lookup.get(bucket_name, (None, None))

    acl_service.get_bucket_owner_and_id = AsyncMock(side_effect=_lookup)
    app.state.acl_service = acl_service

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


# ---- Header parsing --------------------------------------------------------


@pytest.mark.parametrize(
    "header,expected",
    [
        ("/srcbucket/srckey", "srcbucket"),
        ("srcbucket/srckey", "srcbucket"),
        ("/srcbucket/folder/file.txt", "srcbucket"),
        ("srcbucket/folder/sub/file.txt?versionId=v1", "srcbucket"),
        ("/srcbucket/srckey?versionId=v1", "srcbucket"),
        ("/onlyonepart", "onlyonepart"),  # degenerate but extractable
        ("", None),
        ("/", None),
        ("arn:aws:s3:::bucket/key", None),  # ARN form not supported
    ],
)
def test_parse_copy_source_bucket(header: str, expected: str | None) -> None:
    assert _parse_copy_source_bucket(header) == expected


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
async def test_copy_object_with_malformed_copy_source_header_does_not_block() -> None:
    """Malformed/empty x-amz-copy-source: no source check is performed.

    The backend will reject the malformed header itself; the middleware
    shouldn't 403 on un-parseable headers (would mask the real S3 error)."""
    scope = _scope(Permission.object_read_write, BucketScope.specific, ["dest-id"])
    app = _make_app(
        scope=scope,
        bucket_owner_lookup={"dest-bucket": ("alice", "dest-id")},
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.put("/dest-bucket/dest-key", headers={"x-amz-copy-source": "arn:aws:s3:::other/key"})
    assert r.status_code == 200


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
