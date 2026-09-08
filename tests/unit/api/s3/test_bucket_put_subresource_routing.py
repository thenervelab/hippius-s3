"""No bucket PUT subresource may fall through to CreateBucket.

`BUCKET_PUT_SUBRESOURCES` and the PUT router are two halves of one contract, and nothing checked
that they agreed. Listing a query param in that set tells the ACL middleware "this is not a
CreateBucket", so `is_create_bucket_shape()` returns False and the middleware skips the CreateBucket
branch — which is where the sentinel-account check and the `x-amz-acl` rejection live. If the router
then has no branch for that param, the request lands in `handle_create_bucket` anyway, with both
guards already behind it.

That is not hypothetical. Measured against staging before the fix, with `x-amz-acl: public-read`:

    PUT /b?retention   -> 200, bucket created, anonymous list 200, ACL grants AllUsers
    PUT /b?legal-hold  -> 200, bucket created, anonymous list 200, ACL grants AllUsers
    PUT /b             -> 400 InvalidBucketAclWithObjectOwnership

Appending a query parameter created a public bucket that the normal API refuses to create. A full
sweep of 28 subresource shapes found exactly these two, and `?versioning` had already been the same
bug once before — hence a test over the whole set rather than two more special cases.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from hippius_s3.api.s3.buckets import router as bucket_router
from hippius_s3.gateway.middlewares.acl import BUCKET_PUT_SUBRESOURCES


class _Unreachable(Exception):
    """Raised in place of creating a bucket, so a fallthrough is loud instead of silent."""


def _request(subresource: str) -> Any:
    return SimpleNamespace(
        query_params={subresource: ""},
        headers={"x-amz-acl": "public-read"},
        state=SimpleNamespace(main_account_id="acct", bucket_object_lock=None),
    )


class _Pool:
    def acquire(self) -> Any:
        class _Ctx:
            async def __aenter__(self) -> Any:
                return object()

            async def __aexit__(self, *_: Any) -> None: ...

        return _Ctx()


@pytest.fixture
def no_create(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make reaching handle_create_bucket a hard failure, and stub every legitimate branch."""

    async def _boom(*_a: Any, **_k: Any) -> None:
        raise _Unreachable

    async def _handled(*_a: Any, **_k: Any) -> str:
        return "handled"

    monkeypatch.setattr(bucket_router, "handle_create_bucket", _boom)
    for name in (
        "handle_put_bucket_object_lock",
        "put_bucket_acl",
        "handle_put_bucket_versioning",
    ):
        monkeypatch.setattr(bucket_router, name, _handled)


@pytest.mark.asyncio
@pytest.mark.parametrize("subresource", sorted(BUCKET_PUT_SUBRESOURCES))
async def test_no_bucket_put_subresource_reaches_create_bucket(
    subresource: str, no_create: None
) -> None:
    """Every member of the set must be routed or refused — never silently created.

    Parametrised over the set itself, so adding a subresource without a router branch fails here
    rather than in production. `tagging`, `lifecycle` and `policy` are handled *inside*
    handle_create_bucket, so they are the deliberate exceptions: reaching it is correct for them.
    """
    # tagging/lifecycle/policy are implemented INSIDE handle_create_bucket, so reaching it is
    # correct for them.
    #
    # `cors` is there for a different and worse reason: it reaches handle_create_bucket, matches
    # nothing, and returns 200 having stored nothing. Measured on staging: PUT ?cors answers 200
    # and GET ?cors answers 200 with a ListBucketResult — it falls through to ListObjects. So the
    # API claims a CORS configuration was accepted, returns the wrong document when asked for it,
    # and CORS silently never works. It is not a guard bypass (no bucket is created, so no ACL
    # escalation) and it predates this release, which is why it is pinned here as known-broken
    # rather than changed underneath callers. The honest answer is 501 NotImplemented; that is a
    # deliberate behaviour change and belongs in its own PR.
    handled_inside_create = {"tagging", "lifecycle", "policy", "cors"}
    try:
        result = await bucket_router.create_or_modify_bucket(
            "some-bucket", _request(subresource), _Pool()
        )
    except _Unreachable:
        assert subresource in handled_inside_create, (
            f"PUT /bucket?{subresource} fell through to handle_create_bucket. Because "
            f"'{subresource}' is in BUCKET_PUT_SUBRESOURCES the ACL middleware skipped the "
            f"CreateBucket branch, so the sentinel-account check and the x-amz-acl rejection "
            f"never ran — this creates a bucket with both guards behind it."
        )
        return

    assert result is not None, f"PUT /bucket?{subresource} returned nothing"


@pytest.mark.asyncio
@pytest.mark.parametrize("subresource", ["retention", "legal-hold"])
async def test_object_level_subresources_are_refused_on_a_bucket_path(
    subresource: str, no_create: None
) -> None:
    """Retention and legal hold address an object version; on a bucket path they address nothing.

    Asserted as a 4xx specifically, rather than merely "did not create": a 200 here would be the
    same silent lie `?cors` currently tells, where PUT reports success and stores nothing.
    """
    result = await bucket_router.create_or_modify_bucket("some-bucket", _request(subresource), _Pool())
    assert getattr(result, "status_code", None) == 405, (
        f"PUT /bucket?{subresource} should be refused 405, got {getattr(result, 'status_code', result)}"
    )
