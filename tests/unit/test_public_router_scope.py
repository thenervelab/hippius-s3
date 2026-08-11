"""The anonymous `/public/...` routes read only buckets that are actually marked public.

These two routes are the one api entry point the gateway does not authorize. It parses
`/public/<bucket>/<key>` as bucket `public` with key `<bucket>/<key>`; `public` is a reserved
segment so no such bucket can exist, which puts the ACL middleware on its "bucket not found, let
the backend return the proper S3 error" path — where no permission check runs. `handle_get_object`
in turn documents that the gateway has already checked permissions, and its query is keyed on
bucket name and object key alone.

So publicness has to be established by the router itself. These tests pin that it is, that a
non-public bucket is refused *before* any object read is attempted, and that the refusal does not
distinguish private from absent.
"""

from __future__ import annotations

from typing import Any

import pytest

from hippius_s3.api.s3 import public_router


_ALL_USERS = "http://acs.amazonaws.com/groups/global/AllUsers"


def _acl(*, public: bool) -> dict[str, Any]:
    """A bucket ACL as the schema really stores it: publicness is an AllUsers READ grant."""
    grants: list[dict[str, Any]] = [
        {"grantee": {"type": "CanonicalUser", "id": "owner"}, "permission": "FULL_CONTROL"}
    ]
    if public:
        grants.append({"grantee": {"type": "Group", "uri": _ALL_USERS}, "permission": "READ"})
    return {"owner": {"id": "owner"}, "grants": grants}


class _Pool:
    """Answers the ACL lookup the publicness check makes."""

    def __init__(self, row: dict[str, Any] | None) -> None:
        self._row = row
        self.queries: list[str] = []

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        self.queries.append(query)
        return self._row


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("row", "why"),
    [
        ({"acl_json": _acl(public=False)}, "a bucket whose ACL grants no AllUsers READ"),
        (None, "a bucket that does not exist"),
        ({"acl_json": None}, "a bucket with no ACL row at all"),
    ],
)
async def test_a_bucket_that_is_not_public_is_not_readable(row: dict[str, Any] | None, why: str) -> None:
    assert await public_router._bucket_is_public(_Pool(row), "victimbucket") is False, why


@pytest.mark.asyncio
async def test_a_public_bucket_is_readable() -> None:
    assert await public_router._bucket_is_public(_Pool({"acl_json": _acl(public=True)}), "sharedbucket") is True


@pytest.mark.asyncio
async def test_the_object_is_never_read_when_the_bucket_is_not_public(monkeypatch) -> None:
    """The check has to short-circuit: reaching the object handler is the whole failure."""
    delegated: list[str] = []

    async def _should_not_run(*args: Any, **kwargs: Any) -> Any:  # pragma: no cover - must not run
        delegated.append("get")
        raise AssertionError("handle_get_object ran for a non-public bucket")

    monkeypatch.setattr(public_router, "handle_get_object", _should_not_run)

    class _Req:
        query_params: dict[str, str] = {}

    resp = await public_router.get_public_object(
        bucket_name="victimbucket",
        object_key="secret.txt",
        request=_Req(),
        pool=_Pool({"acl_json": _acl(public=False)}),
        redis_client=None,
    )

    assert delegated == []
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_a_public_bucket_still_serves(monkeypatch) -> None:
    """The gate must not break the feature it protects."""
    from fastapi import Response

    async def _ok(*args: Any, **kwargs: Any) -> Response:
        return Response(status_code=200, content=b"hello")

    monkeypatch.setattr(public_router, "handle_get_object", _ok)

    class _Req:
        query_params: dict[str, str] = {}

    resp = await public_router.get_public_object(
        bucket_name="sharedbucket",
        object_key="readme.txt",
        request=_Req(),
        pool=_Pool({"acl_json": _acl(public=True)}),
        redis_client=None,
    )

    assert resp.status_code == 200
    assert resp.headers["x-hippius-access-mode"] == "anon"


@pytest.mark.asyncio
async def test_private_and_absent_are_indistinguishable() -> None:
    """A 403 on a private bucket would confirm the object exists; both must answer the same."""
    private = await public_router._bucket_is_public(_Pool({"acl_json": _acl(public=False)}), "private")
    absent = await public_router._bucket_is_public(_Pool(None), "nosuchbucket")

    assert private == absent is False


@pytest.mark.asyncio
async def test_head_is_gated_too(monkeypatch) -> None:
    """HEAD leaks existence and size, so it needs the same gate as GET."""
    async def _should_not_run(*args: Any, **kwargs: Any) -> Any:  # pragma: no cover - must not run
        raise AssertionError("handle_head_object ran for a non-public bucket")

    monkeypatch.setattr(public_router, "handle_head_object", _should_not_run)

    class _Req:
        query_params: dict[str, str] = {}

    resp = await public_router.head_public_object(
        bucket_name="victimbucket",
        object_key="secret.txt",
        request=_Req(),
        pool=_Pool({"acl_json": _acl(public=False)}),
    )

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_publicness_is_read_from_the_acl_and_not_from_buckets_is_public() -> None:
    """The column is dead, and gating on it would refuse EVERY bucket.

    Migration 20251121000000_migrate_public_buckets_to_acl moved public buckets to an AllUsers
    READ grant and then ran `UPDATE buckets SET is_public = false` across the whole table;
    `bucket_create_endpoint` has hardcoded `is_public = False` ever since, so the only thing that
    still writes True is that migration's own `migrate:down`.

    This pins the predicate against a row shaped exactly like the wrong answer: a bucket whose
    ACL grants AllUsers READ while `is_public` is False, which is what every genuinely public
    bucket in the database looks like. A gate on the column returns False here and 404s public
    content; the ACL predicate returns True.
    """
    row = {"acl_json": _acl(public=True), "is_public": False}

    assert await public_router._bucket_is_public(_Pool(row), "sharedbucket") is True


@pytest.mark.asyncio
async def test_a_stale_is_public_true_does_not_grant_access_on_its_own() -> None:
    """The converse, so the two signals can never be conflated in the other direction either.

    A row left with `is_public = True` by the migration's down path must not make a bucket
    readable when its ACL grants nobody public read.
    """
    row = {"acl_json": _acl(public=False), "is_public": True}

    assert await public_router._bucket_is_public(_Pool(row), "victimbucket") is False


@pytest.mark.asyncio
async def test_a_grant_to_all_users_that_is_not_read_does_not_make_it_public() -> None:
    """WRITE to AllUsers is a different (alarming) thing and must not open the anonymous read."""
    row = {
        "acl_json": {
            "owner": {"id": "owner"},
            "grants": [{"grantee": {"type": "Group", "uri": _ALL_USERS}, "permission": "WRITE"}],
        }
    }

    assert await public_router._bucket_is_public(_Pool(row), "oddbucket") is False


@pytest.mark.asyncio
async def test_an_acl_stored_as_a_json_string_is_still_understood() -> None:
    """asyncpg hands back jsonb as str on some paths; the helper accepts both."""
    import json as _json

    row = {"acl_json": _json.dumps(_acl(public=True))}

    assert await public_router._bucket_is_public(_Pool(row), "sharedbucket") is True
