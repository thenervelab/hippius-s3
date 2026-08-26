"""The permission demanded must match the operation performed, and the two layers must agree.

Two failure shapes are covered here, both privilege confusion rather than authentication bugs:

  1. `get_required_permission` grading an operation weaker than what the handler does. `?policy`
     was the live case: PUT ?policy replaces the bucket ACL, but with no entry in the subresource
     table it fell through to the method-only mapping and was graded WRITE, so a write-only
     grantee could publish a bucket to AllUsers.
  2. this middleware and the router disagreeing about WHICH operation a request is. The
     CreateBucket shape was `len(query_params) == 0` here while the router treats any
     unrecognised query param as a create, so `PUT /new?x=1` was a create to the handler and
     "some other bucket op" to the ACL layer — skipping the CreateBucket branch entirely,
     including its `x-amz-acl` rejection.
"""

import pytest

from hippius_s3.api.s3.buckets import router as buckets_router
from hippius_s3.gateway.middlewares.acl import BUCKET_PUT_SUBRESOURCES
from hippius_s3.gateway.middlewares.acl import get_required_permission
from hippius_s3.gateway.middlewares.acl import is_create_bucket_shape
from hippius_s3.models.acl import Permission


class TestAccessControlSubresources:
    @pytest.mark.parametrize("method", ["PUT", "POST", "DELETE"])
    def test_policy_writes_demand_write_acp(self, method: str) -> None:
        """PUT ?policy rewrites the bucket ACL — WRITE must not be sufficient."""
        assert get_required_permission(method, {"policy": ""}, has_key=False) == Permission.WRITE_ACP

    @pytest.mark.parametrize("method", ["GET", "HEAD"])
    def test_policy_reads_demand_read_acp(self, method: str) -> None:
        assert get_required_permission(method, {"policy": ""}, has_key=False) == Permission.READ_ACP

    def test_acl_mapping_is_unchanged(self) -> None:
        """Regression guard: the existing `acl` grading must not drift while adding neighbours."""
        assert get_required_permission("GET", {"acl": ""}, has_key=False) == Permission.READ_ACP
        assert get_required_permission("PUT", {"acl": ""}, has_key=False) == Permission.WRITE_ACP
        assert get_required_permission("DELETE", {"acl": ""}, has_key=False) == Permission.WRITE_ACP

    def test_data_operations_are_unchanged(self) -> None:
        assert get_required_permission("GET", {}, has_key=True) == Permission.READ
        assert get_required_permission("PUT", {}, has_key=True) == Permission.WRITE
        assert get_required_permission("POST", {"delete": ""}, has_key=False) == Permission.WRITE
        assert get_required_permission("POST", {"uploads": ""}, has_key=True) == Permission.WRITE
        assert get_required_permission("PUT", {"uploadId": "x"}, has_key=True) == Permission.WRITE

    def test_acl_takes_precedence_when_both_are_present(self) -> None:
        """`?acl&policy` must still be graded as an access-control write, not fall through."""
        assert get_required_permission("PUT", {"acl": "", "policy": ""}, has_key=False) == Permission.WRITE_ACP

    def test_unknown_method_still_raises(self) -> None:
        with pytest.raises(ValueError):
            get_required_permission("PATCH", {}, has_key=False)


class TestCreateBucketShapeMatchesTheRouter:
    @pytest.mark.parametrize(
        "query,expected",
        [
            ({}, True),
            ({"x": "1"}, True),
            ({"": "1"}, True),
            ({"unknown-subresource": ""}, True),
            ({"acl": ""}, False),
            ({"tagging": ""}, False),
            ({"lifecycle": ""}, False),
            ({"policy": ""}, False),
            ({"cors": ""}, False),
            ({"acl": "", "tagging": ""}, False),
            ({"x": "1", "policy": ""}, False),
        ],
    )
    def test_shape(self, query: dict, expected: bool) -> None:
        assert is_create_bucket_shape("PUT", None, query) is expected

    def test_object_writes_are_never_create_bucket(self) -> None:
        assert is_create_bucket_shape("PUT", "some/key", {}) is False

    @pytest.mark.parametrize("method", ["GET", "POST", "DELETE", "HEAD"])
    def test_only_put_is_create_bucket(self, method: str) -> None:
        assert is_create_bucket_shape(method, None, {}) is False

    def test_an_unrecognised_param_is_a_create_on_both_sides(self) -> None:
        """The bypass: the router falls through to handle_create_bucket for `?x=1`, so we must
        classify it as a create too, or its guards never run."""
        assert is_create_bucket_shape("PUT", None, {"x": "1"}) is True

    def test_subresource_set_covers_every_branch_the_put_router_dispatches(self) -> None:
        """Pins the two lists together. If the router grows a subresource and this set does not,
        that param becomes a create here and something else there — the bug this closes."""
        source = __import__("inspect").getsource(buckets_router.create_or_modify_bucket)
        from hippius_s3.api.s3.buckets import bucket_create_endpoint

        source += __import__("inspect").getsource(bucket_create_endpoint.handle_create_bucket)
        dispatched = {name for name in BUCKET_PUT_SUBRESOURCES if f'"{name}"' in source}
        assert dispatched == set(BUCKET_PUT_SUBRESOURCES), (
            f"router dispatches on {dispatched}, BUCKET_PUT_SUBRESOURCES is {set(BUCKET_PUT_SUBRESOURCES)}"
        )


class TestDeleteBucketDispatch:
    """DeleteBucket takes no subresource, so an unrecognised one must not fall through to it.

    `DELETE /{b}?policy` (DeleteBucketPolicy) and `?cors` are ordinary S3 calls that every SDK
    can emit; routing them to handle_delete_bucket destroyed the bucket. `?acl` was worse: it
    arrives graded WRITE_ACP, so an ACL-admin grantee could delete an empty bucket.
    """

    @pytest.mark.parametrize("query", ["policy", "cors", "lifecycle", "acl", "website", "x"])
    @pytest.mark.asyncio
    async def test_unknown_subresource_is_not_a_bucket_delete(self, query: str) -> None:
        from unittest.mock import AsyncMock
        from unittest.mock import MagicMock

        from fastapi import Request

        called: list[str] = []

        async def _fail(*a: object, **k: object) -> None:
            called.append("delete_bucket")

        req = MagicMock(spec=Request)
        req.query_params = {query: ""}
        req.state = MagicMock()

        pool = MagicMock()
        pool.acquire = MagicMock()

        import hippius_s3.api.s3.buckets.router as r

        original = r.handle_delete_bucket
        r.handle_delete_bucket = _fail  # type: ignore[assignment]
        try:
            resp = await r.delete_bucket_tags_route(
                bucket_name="victim", request=req, pool=pool, redis_client=AsyncMock()
            )
        finally:
            r.handle_delete_bucket = original  # type: ignore[assignment]

        assert called == [], f"?{query} must not reach handle_delete_bucket"
        assert resp.status_code == 501
