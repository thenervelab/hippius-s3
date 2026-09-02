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
from starlette.datastructures import QueryParams

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

    @pytest.mark.parametrize("method", ["PUT", "POST", "DELETE"])
    def test_versioning_writes_demand_write_acp(self, method: str) -> None:
        """WRITE means "may create objects". It must not be enough to turn versioning on.

        AWS has no ACL grant that confers s3:PutBucketVersioning at all — bucket ACLs only express
        READ / WRITE / READ_ACP / WRITE_ACP / FULL_CONTROL — so anything short of an ACL-admin
        grade over-permits. Enabling is also irreversible here (Suspended is a 501) and rewrites
        DELETE semantics for every key in the bucket.
        """
        assert get_required_permission(method, {"versioning": ""}, has_key=False) == Permission.WRITE_ACP

    @pytest.mark.parametrize("method", ["GET", "HEAD"])
    def test_versioning_reads_demand_read_acp(self, method: str) -> None:
        assert get_required_permission(method, {"versioning": ""}, has_key=False) == Permission.READ_ACP

    def test_versioning_is_not_a_create_bucket_shape(self) -> None:
        """The vulnerability, pinned directly.

        While `versioning` was absent from BUCKET_PUT_SUBRESOURCES this returned True, so the ACL
        middleware took its CreateBucket branch — which deliberately bypasses the permission check
        because a bucket being created has nothing to authorise against — and PutBucketVersioning
        ran ungated on an EXISTING bucket.
        """
        assert is_create_bucket_shape("PUT", None, {"versioning": ""}) is False

    @pytest.mark.parametrize("method", ["PUT", "POST", "DELETE"])
    def test_object_lock_writes_demand_write_acp(self, method: str) -> None:
        """Object Lock is WORM. "May upload" must never imply "may make objects undeletable"."""
        assert get_required_permission(method, {"object-lock": ""}, has_key=False) == Permission.WRITE_ACP

    @pytest.mark.parametrize("method", ["GET", "HEAD"])
    def test_object_lock_reads_demand_read_acp(self, method: str) -> None:
        assert get_required_permission(method, {"object-lock": ""}, has_key=False) == Permission.READ_ACP

    @pytest.mark.parametrize("subresource", ["retention", "legal-hold"])
    def test_per_object_worm_subresources_demand_acp(self, subresource: str) -> None:
        """Tier 2 today (object_lock_guard answers 501), graded now so the gate is already correct
        when a handler lands — defaulting to WRITE at that moment is how ?versioning shipped
        ungated."""
        assert get_required_permission("PUT", {subresource: ""}, has_key=True) == Permission.WRITE_ACP
        assert get_required_permission("GET", {subresource: ""}, has_key=True) == Permission.READ_ACP

    def test_object_lock_is_not_a_create_bucket_shape(self) -> None:
        """`PUT /b?object-lock` reaches a real persisting handler, so it must not take the
        CreateBucket bypass — the same coupling that ?versioning missed."""
        assert is_create_bucket_shape("PUT", None, {"object-lock": ""}) is False

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
        """Pins the two lists together, in the direction that actually catches the bug.

        The previous version of this test derived its candidates FROM `BUCKET_PUT_SUBRESOURCES`
        (`{n for n in BUCKET_PUT_SUBRESOURCES if f'"{n}"' in source}`) and then asserted that set
        equalled `BUCKET_PUT_SUBRESOURCES` — which it does by construction unless a set member is
        missing from the router. It could never see a param the ROUTER dispatches that the set
        omits, and that is the failure it was written for: `?versioning` shipped with a router
        branch and no entry here, so `PUT /b?versioning` was graded a CreateBucket by the ACL
        middleware, took the create bypass, and reached PutBucketVersioning with no permission
        check at all.

        So: parse the names out of the SOURCE and require the set to cover them.
        """
        import inspect
        import re

        from hippius_s3.api.s3.buckets import bucket_create_endpoint

        source = inspect.getsource(buckets_router.create_or_modify_bucket)
        source += inspect.getsource(bucket_create_endpoint.handle_create_bucket)
        dispatched = set(re.findall(r'"([A-Za-z][\w-]*)"\s+in\s+request\.query_params', source))
        assert dispatched, "found no subresource branches — the extraction regex has gone stale"
        missing = dispatched - set(BUCKET_PUT_SUBRESOURCES)
        assert not missing, (
            f"the bucket PUT router dispatches on {sorted(missing)} but BUCKET_PUT_SUBRESOURCES "
            f"({sorted(BUCKET_PUT_SUBRESOURCES)}) omits it. Such a param is classified as "
            f"CreateBucket by the ACL middleware, which BYPASSES the permission check, while the "
            f"router sends it to a real subresource handler. Add it to the set."
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


class TestWormSubresourcesCannotTakeTheCreateBypass:
    """The `?versioning` bug had two halves, and grading was only one of them.

    `?versioning` was graded WRITE_ACP correctly and STILL bypassed the permission check, because
    `is_create_bucket_shape` did not know the name: an unrecognised param on a keyless PUT is a
    CreateBucket, and CreateBucket skips the bucket-permission check. Both halves have to name a
    subresource for it to be gated.

    `?retention` / `?legal-hold` answer 501 today, so nothing is reachable through them — but the
    grading is already in place for Tier 2, and this pins the other half so the surface cannot land
    ungated whichever half a future change forgets.
    """

    @pytest.mark.parametrize("param", ["object-lock", "retention", "legal-hold"])
    def test_worm_params_are_not_create_shapes(self, param: str) -> None:
        assert is_create_bucket_shape("PUT", None, {param: ""}) is False, (
            f"?{param} takes the CreateBucket bypass, so the ACL check never runs on it"
        )

    @pytest.mark.parametrize("param", ["object-lock", "retention", "legal-hold"])
    def test_worm_params_demand_acp_on_write(self, param: str) -> None:
        assert get_required_permission("PUT", {param: ""}, has_key=False) is Permission.WRITE_ACP

    def test_percent_encoded_spellings_cannot_split_the_two_halves(self) -> None:
        """The ACL middleware and the bucket router must never disagree about what a param IS.

        Both read the same decoded QueryParams, so `%6fbject-lock` and `object%2Dlock` decode to
        the canonical name on both sides. Pinned because a divergence here — one side seeing the
        raw spelling, the other the decoded one — recreates the ungated-subresource bug with a
        param the set cannot list.
        """
        for raw in ("%6fbject-lock", "object%2Dlock", "object-lock&acl"):
            decoded = dict(QueryParams(raw))
            assert is_create_bucket_shape("PUT", None, decoded) is False, f"{raw} took the bypass"
            assert "object-lock" in QueryParams(raw), f"{raw} did not decode to the router's name"
