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


class _Pool:
    """Answers the publicness lookup and records whether it was asked."""

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
        ({"is_public": False}, "a private bucket"),
        (None, "a bucket that does not exist"),
        ({"is_public": None}, "a NULL is_public, which is not True"),
    ],
)
async def test_a_bucket_that_is_not_public_is_not_readable(row: dict[str, Any] | None, why: str) -> None:
    assert await public_router._bucket_is_public(_Pool(row), "victimbucket") is False, why


@pytest.mark.asyncio
async def test_a_public_bucket_is_readable() -> None:
    assert await public_router._bucket_is_public(_Pool({"is_public": True}), "sharedbucket") is True


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
        pool=_Pool({"is_public": False}),
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
        pool=_Pool({"is_public": True}),
        redis_client=None,
    )

    assert resp.status_code == 200
    assert resp.headers["x-hippius-access-mode"] == "anon"


@pytest.mark.asyncio
async def test_private_and_absent_are_indistinguishable() -> None:
    """A 403 on a private bucket would confirm the object exists; both must answer the same."""
    private = await public_router._bucket_is_public(_Pool({"is_public": False}), "private")
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
        pool=_Pool({"is_public": False}),
    )

    assert resp.status_code == 404
