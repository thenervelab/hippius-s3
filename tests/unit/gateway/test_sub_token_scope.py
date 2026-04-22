"""Unit tests for the R2-style sub-token scope evaluator."""

from __future__ import annotations

import pytest

from gateway.services.sub_token_scope import OP_CREATE_BUCKET
from gateway.services.sub_token_scope import OP_DELETE_BUCKET
from gateway.services.sub_token_scope import OP_DELETE_OBJECT
from gateway.services.sub_token_scope import OP_LIST_BUCKET
from gateway.services.sub_token_scope import OP_LIST_BUCKETS
from gateway.services.sub_token_scope import OP_READ_BUCKET_META
from gateway.services.sub_token_scope import OP_READ_OBJECT
from gateway.services.sub_token_scope import OP_WRITE_BUCKET_META
from gateway.services.sub_token_scope import OP_WRITE_OBJECT
from gateway.services.sub_token_scope import bucket_in_scope
from gateway.services.sub_token_scope import evaluate
from gateway.services.sub_token_scope import permission_allows
from gateway.services.sub_token_scope import required_op
from hippius_s3.repositories.sub_token_scope_repository import SubTokenScope


# ---- required_op -----------------------------------------------------------


@pytest.mark.parametrize(
    "method,has_key,query_params,expected",
    [
        # Object-level
        ("GET", True, {}, OP_READ_OBJECT),
        ("HEAD", True, {}, OP_READ_OBJECT),
        ("PUT", True, {}, OP_WRITE_OBJECT),
        ("POST", True, {}, OP_WRITE_OBJECT),
        ("DELETE", True, {}, OP_DELETE_OBJECT),
        # Object subresource reads are still object reads
        ("GET", True, {"tagging": ""}, OP_READ_OBJECT),
        ("GET", True, {"acl": ""}, OP_READ_OBJECT),
        # Object subresource writes are still object writes
        ("PUT", True, {"tagging": ""}, OP_WRITE_OBJECT),
        ("PUT", True, {"acl": ""}, OP_WRITE_OBJECT),
        # Bucket-level, no subresource
        ("GET", False, {}, OP_LIST_BUCKET),
        ("HEAD", False, {}, OP_LIST_BUCKET),
        ("PUT", False, {}, OP_CREATE_BUCKET),
        ("DELETE", False, {}, OP_DELETE_BUCKET),
        # Bucket subresource reads
        ("GET", False, {"acl": ""}, OP_READ_BUCKET_META),
        ("GET", False, {"tagging": ""}, OP_READ_BUCKET_META),
        ("GET", False, {"policy": ""}, OP_READ_BUCKET_META),
        # Bucket subresource writes
        ("PUT", False, {"acl": ""}, OP_WRITE_BUCKET_META),
        ("PUT", False, {"tagging": ""}, OP_WRITE_BUCKET_META),
    ],
)
def test_required_op(method: str, has_key: bool, query_params: dict, expected: str) -> None:
    assert required_op(method, has_key, query_params) == expected


# ---- permission_allows (matrix) -------------------------------------------


ALL_OPS = [
    OP_READ_OBJECT,
    OP_WRITE_OBJECT,
    OP_DELETE_OBJECT,
    OP_LIST_BUCKET,
    OP_LIST_BUCKETS,
    OP_CREATE_BUCKET,
    OP_DELETE_BUCKET,
    OP_READ_BUCKET_META,
    OP_WRITE_BUCKET_META,
]

EXPECTED_MATRIX = {
    "admin_read_write": set(ALL_OPS),
    "admin_read": {OP_READ_OBJECT, OP_LIST_BUCKET, OP_LIST_BUCKETS, OP_READ_BUCKET_META},
    "object_read_write": {OP_READ_OBJECT, OP_WRITE_OBJECT, OP_DELETE_OBJECT, OP_LIST_BUCKET},
    "object_read": {OP_READ_OBJECT, OP_LIST_BUCKET},
}


@pytest.mark.parametrize("tier", list(EXPECTED_MATRIX.keys()))
@pytest.mark.parametrize("op", ALL_OPS)
def test_permission_matrix(tier: str, op: str) -> None:
    expected_allowed = op in EXPECTED_MATRIX[tier]
    assert permission_allows(tier, op) is expected_allowed, f"{tier} + {op} mismatch"


def test_permission_allows_unknown_tier() -> None:
    assert permission_allows("bogus", OP_READ_OBJECT) is False


# ---- bucket_in_scope -------------------------------------------------------


def _scope(bucket_scope: str, bucket_ids: list[str] = []) -> SubTokenScope:
    return SubTokenScope(
        access_key_id="hip_x",
        account_id="acct1",
        permission="object_read",
        bucket_scope=bucket_scope,
        bucket_ids=tuple(bucket_ids),
    )


def test_bucket_in_scope_all_matches_anything() -> None:
    assert bucket_in_scope("bucket-a", _scope("all")) is True
    assert bucket_in_scope("bucket-b", _scope("all")) is True


def test_bucket_in_scope_specific_matches_listed() -> None:
    scope = _scope("specific", ["bucket-a", "bucket-b"])
    assert bucket_in_scope("bucket-a", scope) is True
    assert bucket_in_scope("bucket-b", scope) is True
    assert bucket_in_scope("bucket-c", scope) is False


def test_bucket_in_scope_none_bucket_with_specific() -> None:
    scope = _scope("specific", ["bucket-a"])
    assert bucket_in_scope(None, scope) is False


def test_bucket_in_scope_none_bucket_with_all() -> None:
    assert bucket_in_scope(None, _scope("all")) is True


# ---- evaluate --------------------------------------------------------------


def test_evaluate_no_scope_denies() -> None:
    allowed, reason = evaluate(scope=None, bucket_id="b", method="GET", has_key=True, query_params={})
    assert allowed is False
    assert reason == "no_scope"


def test_evaluate_object_read_allows_get() -> None:
    scope = _scope("specific", ["b"])
    allowed, _ = evaluate(scope=scope, bucket_id="b", method="GET", has_key=True, query_params={})
    assert allowed is True


def test_evaluate_object_read_denies_put() -> None:
    scope = _scope("specific", ["b"])
    allowed, reason = evaluate(scope=scope, bucket_id="b", method="PUT", has_key=True, query_params={})
    assert allowed is False
    assert reason == "op_not_allowed"


def test_evaluate_object_read_denies_bucket_outside_scope() -> None:
    scope = _scope("specific", ["b"])
    allowed, reason = evaluate(scope=scope, bucket_id="other", method="GET", has_key=True, query_params={})
    assert allowed is False
    assert reason == "bucket_out_of_scope"


# ListBuckets (GET /) is handled directly in the middleware via
# permission_allows(scope.permission, OP_LIST_BUCKETS) before calling evaluate(),
# so evaluate() is not exercised with bucket=None for GET.


def test_evaluate_create_bucket_requires_admin_rw_with_scope_all() -> None:
    scope_all_admin = SubTokenScope(
        access_key_id="hip_x",
        account_id="acct1",
        permission="admin_read_write",
        bucket_scope="all",
        bucket_ids=(),
    )
    allowed, _ = evaluate(scope=scope_all_admin, bucket_id=None, method="PUT", has_key=False, query_params={})
    assert allowed is True


def test_evaluate_create_bucket_denied_on_specific_scope() -> None:
    scope_admin_specific = SubTokenScope(
        access_key_id="hip_x",
        account_id="acct1",
        permission="admin_read_write",
        bucket_scope="specific",
        bucket_ids=("b",),
    )
    allowed, reason = evaluate(
        scope=scope_admin_specific, bucket_id=None, method="PUT", has_key=False, query_params={}
    )
    assert allowed is False
    assert reason == "create_bucket_requires_scope_all"


def test_evaluate_create_bucket_denied_for_non_admin_rw() -> None:
    for permission in ("admin_read", "object_read_write", "object_read"):
        scope = SubTokenScope(
            access_key_id="hip_x",
            account_id="acct1",
            permission=permission,
            bucket_scope="all",
            bucket_ids=(),
        )
        allowed, reason = evaluate(scope=scope, bucket_id=None, method="PUT", has_key=False, query_params={})
        assert allowed is False, f"{permission} should not allow CreateBucket"
        assert reason == "op_not_allowed"


# ---- Full end-to-end matrix ------------------------------------------------
#
# Axes:
#   - permission tier     (4 values)
#   - bucket_scope        (2 values: 'all', 'specific')
#   - target_in_scope     (2 values: True, False) — only meaningful when bucket_scope='specific'
#   - op_input            (8 request shapes, one per internal op)
#
# Expected outcome is computed from the R2 model, not copy-pasted per cell.
# That gives us a single source of truth + catches matrix-level regressions.

TIERS = ("admin_read_write", "admin_read", "object_read_write", "object_read")

# (method, has_key, query_params, internal_op_name)
# bucket_id is supplied separately per test case (None for create_bucket,
# otherwise an actual UUID-like placeholder).
OP_INPUTS = [
    ("GET", True, {}, OP_READ_OBJECT),
    ("PUT", True, {}, OP_WRITE_OBJECT),
    ("DELETE", True, {}, OP_DELETE_OBJECT),
    ("GET", False, {}, OP_LIST_BUCKET),
    ("PUT", False, {}, OP_CREATE_BUCKET),
    ("DELETE", False, {}, OP_DELETE_BUCKET),
    ("GET", False, {"acl": ""}, OP_READ_BUCKET_META),
    ("PUT", False, {"acl": ""}, OP_WRITE_BUCKET_META),
]


def _expected(tier: str, bucket_scope: str, target_in_scope: bool, op: str) -> tuple[bool, str]:
    """Compute the expected (allowed, reason) from the R2 model, per-axis."""
    # Rule 1: op must be covered by the tier.
    if op not in EXPECTED_MATRIX[tier]:
        return False, "op_not_allowed"

    # Rule 2: CreateBucket requires bucket_scope='all' regardless of target.
    if op == OP_CREATE_BUCKET:
        if bucket_scope != "all":
            return False, "create_bucket_requires_scope_all"
        return True, ""

    # Rule 3: ListBuckets is not per-bucket. The middleware handles it outside
    # of evaluate(); we don't assert on it here.
    #
    # Rule 4: For every other op, bucket_scope='specific' requires target_in_scope.
    if bucket_scope == "specific" and not target_in_scope:
        return False, "bucket_out_of_scope"

    return True, ""


@pytest.mark.parametrize("tier", TIERS)
@pytest.mark.parametrize("bucket_scope", ["all", "specific"])
@pytest.mark.parametrize("target_in_scope", [True, False])
@pytest.mark.parametrize("op_input", OP_INPUTS, ids=lambda oi: oi[3])
def test_full_evaluate_matrix(
    tier: str, bucket_scope: str, target_in_scope: bool, op_input: tuple
) -> None:
    method, has_key, qparams, op = op_input

    # Under bucket_scope='all', target_in_scope is irrelevant — skip one duplicate.
    if bucket_scope == "all" and target_in_scope is False:
        pytest.skip("bucket_scope='all' makes target_in_scope moot")

    # Build the scope: when 'specific', list exactly one bucket id.
    in_scope_bucket_id = "bucket-a"
    out_of_scope_bucket_id = "bucket-b"
    bucket_ids = (in_scope_bucket_id,) if bucket_scope == "specific" else ()
    scope = SubTokenScope(
        access_key_id="hip_matrix",
        account_id="acct1",
        permission=tier,
        bucket_scope=bucket_scope,
        bucket_ids=bucket_ids,
    )

    # CreateBucket is evaluated with bucket_id=None (target doesn't exist yet).
    if op == OP_CREATE_BUCKET:
        bucket_id_arg: str | None = None
    else:
        bucket_id_arg = in_scope_bucket_id if target_in_scope else out_of_scope_bucket_id

    expected_allowed, expected_reason = _expected(tier, bucket_scope, target_in_scope, op)

    actual_allowed, actual_reason = evaluate(
        scope=scope,
        bucket_id=bucket_id_arg,
        method=method,
        has_key=has_key,
        query_params=qparams,
    )

    assert actual_allowed is expected_allowed, (
        f"tier={tier} bucket_scope={bucket_scope} in_scope={target_in_scope} op={op}: "
        f"expected allowed={expected_allowed} (reason={expected_reason!r}), "
        f"got allowed={actual_allowed} (reason={actual_reason!r})"
    )
    if not expected_allowed:
        assert actual_reason == expected_reason, (
            f"tier={tier} bucket_scope={bucket_scope} in_scope={target_in_scope} op={op}: "
            f"reason mismatch (expected {expected_reason!r}, got {actual_reason!r})"
        )


# ---- Explicit spot checks for R2 fidelity ---------------------------------
# The matrix above is exhaustive, but a few concrete scenarios help document
# the intended user-facing behavior and catch silent semantic drift.


def test_object_read_on_scoped_bucket_mirrors_r2_readonly_token() -> None:
    """R2 'Object Read only' → GET/HEAD allowed on listed bucket; writes/deletes denied."""
    scope = SubTokenScope(
        access_key_id="hip_r", account_id="acct",
        permission="object_read", bucket_scope="specific", bucket_ids=("books",),
    )
    assert evaluate(scope=scope, bucket_id="books", method="GET", has_key=True, query_params={})[0] is True
    assert evaluate(scope=scope, bucket_id="books", method="HEAD", has_key=True, query_params={})[0] is True
    assert evaluate(scope=scope, bucket_id="books", method="PUT", has_key=True, query_params={})[0] is False
    assert evaluate(scope=scope, bucket_id="books", method="DELETE", has_key=True, query_params={})[0] is False


def test_object_read_write_on_scoped_bucket_mirrors_r2_rw_token() -> None:
    """R2 'Object Read & Write' → GET/HEAD/PUT/DELETE all allowed on listed bucket."""
    scope = SubTokenScope(
        access_key_id="hip_rw", account_id="acct",
        permission="object_read_write", bucket_scope="specific", bucket_ids=("books",),
    )
    for method in ("GET", "HEAD", "PUT", "DELETE"):
        ok, _ = evaluate(scope=scope, bucket_id="books", method=method, has_key=True, query_params={})
        assert ok is True, f"{method} should be allowed for object_read_write"


def test_admin_read_only_mirrors_r2_admin_read_token() -> None:
    """R2 'Admin Read only' → object reads + bucket listing + metadata reads; no writes."""
    scope = SubTokenScope(
        access_key_id="hip_ar", account_id="acct",
        permission="admin_read", bucket_scope="all", bucket_ids=(),
    )
    assert evaluate(scope=scope, bucket_id="b", method="GET", has_key=True, query_params={})[0] is True
    # list_bucket (object listing within a bucket) permitted
    assert evaluate(scope=scope, bucket_id="b", method="GET", has_key=False, query_params={})[0] is True
    # read bucket metadata permitted
    assert evaluate(scope=scope, bucket_id="b", method="GET", has_key=False, query_params={"acl": ""})[0] is True
    # any write is denied
    assert evaluate(scope=scope, bucket_id="b", method="PUT", has_key=True, query_params={})[0] is False
    assert evaluate(scope=scope, bucket_id="b", method="PUT", has_key=False, query_params={"acl": ""})[0] is False
    # CreateBucket denied (admin_read can list but not create)
    assert evaluate(scope=scope, bucket_id=None, method="PUT", has_key=False, query_params={})[0] is False


def test_admin_read_write_with_scope_all_can_create_and_delete_buckets() -> None:
    """R2 'Admin Read & Write' with all-buckets scope → full control including create/delete."""
    scope = SubTokenScope(
        access_key_id="hip_arw", account_id="acct",
        permission="admin_read_write", bucket_scope="all", bucket_ids=(),
    )
    # CreateBucket
    assert evaluate(scope=scope, bucket_id=None, method="PUT", has_key=False, query_params={})[0] is True
    # DeleteBucket
    assert evaluate(scope=scope, bucket_id="b", method="DELETE", has_key=False, query_params={})[0] is True
    # Every object op
    for method in ("GET", "HEAD", "PUT", "DELETE"):
        assert evaluate(scope=scope, bucket_id="b", method=method, has_key=True, query_params={})[0] is True


def test_admin_read_write_specific_scope_cannot_create_new_buckets() -> None:
    """Admin R&W scoped to specific buckets: full access *within* those buckets,
    but no CreateBucket because the new bucket wouldn't be in the list."""
    scope = SubTokenScope(
        access_key_id="hip_arw_specific", account_id="acct",
        permission="admin_read_write", bucket_scope="specific", bucket_ids=("alpha",),
    )
    # Full control of listed bucket
    for method in ("GET", "HEAD", "PUT", "DELETE"):
        assert evaluate(scope=scope, bucket_id="alpha", method=method, has_key=True, query_params={})[0] is True
    # DeleteBucket on listed bucket permitted
    assert evaluate(scope=scope, bucket_id="alpha", method="DELETE", has_key=False, query_params={})[0] is True
    # But CreateBucket is denied
    allowed, reason = evaluate(scope=scope, bucket_id=None, method="PUT", has_key=False, query_params={})
    assert allowed is False
    assert reason == "create_bucket_requires_scope_all"
    # And any op on a different bucket is denied
    allowed, reason = evaluate(scope=scope, bucket_id="beta", method="GET", has_key=True, query_params={})
    assert allowed is False
    assert reason == "bucket_out_of_scope"
