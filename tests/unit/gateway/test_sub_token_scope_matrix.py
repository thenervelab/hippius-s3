"""Comprehensive permission matrix for sub-token scope enforcement.

Complements `test_sub_token_scope.py` by enumerating every S3 operation a
real SDK might issue and asserting:

1. The verb + subresource lands on the right internal `Op`.
2. Each tier's allow/deny outcome for that op is what R2-equivalent tokens do.
3. Cross-cutting invariants hold: master tokens ignore scope, cross-account
   sub-tokens fall through to bucket ACL grants, anonymous reads are unaffected.
4. Every recognised bucket subresource routes to a meta-op (read/write_bucket_meta);
   `?delete` POST routes to delete_object; data-listing subresources (`?uploads`,
   `?versions`) stay as `list_bucket`.

CopyObject's source-bucket scope check is enforced in the middleware (not the
resolver) and is covered by `test_acl_middleware_copy_object.py`.

If you add a new S3 endpoint to the gateway / API, append a row to
`ALL_S3_OPS` so it stays in coverage.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field

import pytest

from hippius_s3.gateway.services.sub_token_scope import bucket_in_scope
from hippius_s3.gateway.services.sub_token_scope import evaluate
from hippius_s3.gateway.services.sub_token_scope import permission_allows
from hippius_s3.gateway.services.sub_token_scope import required_op
from hippius_s3.gateway.services.sub_token_scope import sets_acl
from hippius_s3.models.sub_token import BucketScope
from hippius_s3.models.sub_token import Op
from hippius_s3.models.sub_token import Permission
from hippius_s3.models.sub_token import SubTokenScope


# ---------------------------------------------------------------------------
# Section A — every real S3 operation, mapped to its expected internal Op.
# ---------------------------------------------------------------------------
#
# Add a row when the gateway/API gains a new endpoint. Keep `expected_op`
# matched to whatever the resolver returns *today*; if the row carries a
# `KNOWN GAP` note it means we believe the classification is wrong but the
# fix is out of scope for this PR.


@dataclass
class S3Op:
    name: str
    method: str
    has_key: bool
    query: dict[str, str] = field(default_factory=dict)
    expected_op: Op = Op.read_object
    note: str = ""


ALL_S3_OPS: list[S3Op] = [
    # NOTE: ListBuckets (`GET /`) is intentionally absent from this matrix.
    # The middleware short-circuits on `bucket is None` and checks
    # OP_LIST_BUCKETS directly before reaching the resolver — see the
    # dedicated `test_list_buckets_*` cases below.
    # --- Bucket lifecycle --------------------------------------------------
    S3Op("HeadBucket", "HEAD", has_key=False, expected_op=Op.list_bucket),
    S3Op("CreateBucket", "PUT", has_key=False, expected_op=Op.create_bucket),
    S3Op("DeleteBucket", "DELETE", has_key=False, expected_op=Op.delete_bucket),
    # --- Bucket reads (object listings + bucket-meta variants) -------------
    S3Op("ListObjects", "GET", has_key=False, expected_op=Op.list_bucket),
    S3Op("ListObjectsV2", "GET", has_key=False, query={"list-type": "2"}, expected_op=Op.list_bucket),
    S3Op("ListMultipartUploads", "GET", has_key=False, query={"uploads": ""}, expected_op=Op.list_bucket),
    S3Op("ListObjectVersions", "GET", has_key=False, query={"versions": ""}, expected_op=Op.list_bucket),
    # --- Bucket-meta resources currently in _BUCKET_META_SUBRESOURCES ------
    S3Op("GetBucketAcl", "GET", has_key=False, query={"acl": ""}, expected_op=Op.read_bucket_meta),
    S3Op("PutBucketAcl", "PUT", has_key=False, query={"acl": ""}, expected_op=Op.write_bucket_meta),
    S3Op("GetBucketTagging", "GET", has_key=False, query={"tagging": ""}, expected_op=Op.read_bucket_meta),
    S3Op("PutBucketTagging", "PUT", has_key=False, query={"tagging": ""}, expected_op=Op.write_bucket_meta),
    S3Op("DeleteBucketTagging", "DELETE", has_key=False, query={"tagging": ""}, expected_op=Op.write_bucket_meta),
    S3Op("GetBucketPolicy", "GET", has_key=False, query={"policy": ""}, expected_op=Op.read_bucket_meta),
    S3Op("PutBucketPolicy", "PUT", has_key=False, query={"policy": ""}, expected_op=Op.write_bucket_meta),
    S3Op("DeleteBucketPolicy", "DELETE", has_key=False, query={"policy": ""}, expected_op=Op.write_bucket_meta),
    S3Op("GetBucketCors", "GET", has_key=False, query={"cors": ""}, expected_op=Op.read_bucket_meta),
    S3Op("PutBucketCors", "PUT", has_key=False, query={"cors": ""}, expected_op=Op.write_bucket_meta),
    S3Op("DeleteBucketCors", "DELETE", has_key=False, query={"cors": ""}, expected_op=Op.write_bucket_meta),
    S3Op("GetBucketLifecycle", "GET", has_key=False, query={"lifecycle": ""}, expected_op=Op.read_bucket_meta),
    S3Op("PutBucketLifecycle", "PUT", has_key=False, query={"lifecycle": ""}, expected_op=Op.write_bucket_meta),
    S3Op("DeleteBucketLifecycle", "DELETE", has_key=False, query={"lifecycle": ""}, expected_op=Op.write_bucket_meta),
    # --- Bucket-meta resources (the rest, all in _BUCKET_META_SUBRESOURCES) -
    S3Op("GetBucketAccelerate", "GET", has_key=False, query={"accelerate": ""}, expected_op=Op.read_bucket_meta),
    S3Op("PutBucketAccelerate", "PUT", has_key=False, query={"accelerate": ""}, expected_op=Op.write_bucket_meta),
    S3Op(
        "GetBucketAnalytics", "GET", has_key=False, query={"analytics": "", "id": "x"}, expected_op=Op.read_bucket_meta
    ),
    S3Op(
        "PutBucketAnalytics", "PUT", has_key=False, query={"analytics": "", "id": "x"}, expected_op=Op.write_bucket_meta
    ),
    S3Op(
        "DeleteBucketAnalytics",
        "DELETE",
        has_key=False,
        query={"analytics": "", "id": "x"},
        expected_op=Op.write_bucket_meta,
    ),
    S3Op("GetBucketEncryption", "GET", has_key=False, query={"encryption": ""}, expected_op=Op.read_bucket_meta),
    S3Op("PutBucketEncryption", "PUT", has_key=False, query={"encryption": ""}, expected_op=Op.write_bucket_meta),
    S3Op("DeleteBucketEncryption", "DELETE", has_key=False, query={"encryption": ""}, expected_op=Op.write_bucket_meta),
    S3Op(
        "GetBucketIntelligentTiering",
        "GET",
        has_key=False,
        query={"intelligent-tiering": "", "id": "x"},
        expected_op=Op.read_bucket_meta,
    ),
    S3Op(
        "PutBucketIntelligentTiering",
        "PUT",
        has_key=False,
        query={"intelligent-tiering": "", "id": "x"},
        expected_op=Op.write_bucket_meta,
    ),
    S3Op(
        "GetBucketInventory", "GET", has_key=False, query={"inventory": "", "id": "x"}, expected_op=Op.read_bucket_meta
    ),
    S3Op(
        "PutBucketInventory", "PUT", has_key=False, query={"inventory": "", "id": "x"}, expected_op=Op.write_bucket_meta
    ),
    S3Op("GetBucketLocation", "GET", has_key=False, query={"location": ""}, expected_op=Op.read_bucket_meta),
    S3Op("GetBucketLogging", "GET", has_key=False, query={"logging": ""}, expected_op=Op.read_bucket_meta),
    S3Op("PutBucketLogging", "PUT", has_key=False, query={"logging": ""}, expected_op=Op.write_bucket_meta),
    S3Op("GetBucketMetrics", "GET", has_key=False, query={"metrics": "", "id": "x"}, expected_op=Op.read_bucket_meta),
    S3Op("PutBucketMetrics", "PUT", has_key=False, query={"metrics": "", "id": "x"}, expected_op=Op.write_bucket_meta),
    S3Op("GetBucketNotification", "GET", has_key=False, query={"notification": ""}, expected_op=Op.read_bucket_meta),
    S3Op("PutBucketNotification", "PUT", has_key=False, query={"notification": ""}, expected_op=Op.write_bucket_meta),
    S3Op("GetBucketObjectLock", "GET", has_key=False, query={"object-lock": ""}, expected_op=Op.read_bucket_meta),
    S3Op("PutBucketObjectLock", "PUT", has_key=False, query={"object-lock": ""}, expected_op=Op.write_bucket_meta),
    S3Op(
        "GetBucketOwnershipControls",
        "GET",
        has_key=False,
        query={"ownershipControls": ""},
        expected_op=Op.read_bucket_meta,
    ),
    S3Op(
        "PutBucketOwnershipControls",
        "PUT",
        has_key=False,
        query={"ownershipControls": ""},
        expected_op=Op.write_bucket_meta,
    ),
    S3Op(
        "DeleteBucketOwnershipControls",
        "DELETE",
        has_key=False,
        query={"ownershipControls": ""},
        expected_op=Op.write_bucket_meta,
    ),
    S3Op("GetBucketPolicyStatus", "GET", has_key=False, query={"policyStatus": ""}, expected_op=Op.read_bucket_meta),
    S3Op(
        "GetBucketPublicAccessBlock",
        "GET",
        has_key=False,
        query={"publicAccessBlock": ""},
        expected_op=Op.read_bucket_meta,
    ),
    S3Op(
        "PutBucketPublicAccessBlock",
        "PUT",
        has_key=False,
        query={"publicAccessBlock": ""},
        expected_op=Op.write_bucket_meta,
    ),
    S3Op(
        "DeleteBucketPublicAccessBlock",
        "DELETE",
        has_key=False,
        query={"publicAccessBlock": ""},
        expected_op=Op.write_bucket_meta,
    ),
    S3Op("GetBucketReplication", "GET", has_key=False, query={"replication": ""}, expected_op=Op.read_bucket_meta),
    S3Op("PutBucketReplication", "PUT", has_key=False, query={"replication": ""}, expected_op=Op.write_bucket_meta),
    S3Op(
        "DeleteBucketReplication", "DELETE", has_key=False, query={"replication": ""}, expected_op=Op.write_bucket_meta
    ),
    S3Op(
        "GetBucketRequestPayment", "GET", has_key=False, query={"requestPayment": ""}, expected_op=Op.read_bucket_meta
    ),
    S3Op(
        "PutBucketRequestPayment", "PUT", has_key=False, query={"requestPayment": ""}, expected_op=Op.write_bucket_meta
    ),
    S3Op("GetBucketVersioning", "GET", has_key=False, query={"versioning": ""}, expected_op=Op.read_bucket_meta),
    S3Op("PutBucketVersioning", "PUT", has_key=False, query={"versioning": ""}, expected_op=Op.write_bucket_meta),
    S3Op("GetBucketWebsite", "GET", has_key=False, query={"website": ""}, expected_op=Op.read_bucket_meta),
    S3Op("PutBucketWebsite", "PUT", has_key=False, query={"website": ""}, expected_op=Op.write_bucket_meta),
    S3Op("DeleteBucketWebsite", "DELETE", has_key=False, query={"website": ""}, expected_op=Op.write_bucket_meta),
    # --- Bulk delete on a bucket -------------------------------------------
    # POST /bucket?delete is the bulk DeleteObjects operation. Special-cased
    # in required_op so it requires `delete_object` scope (matches single
    # DeleteObject). Without that special-case, it falls to `write_bucket_meta`
    # and locks out object_read_write tokens — see the regression guard below.
    S3Op("DeleteObjects", "POST", has_key=False, query={"delete": ""}, expected_op=Op.delete_object),
    # --- Object operations -------------------------------------------------
    S3Op("GetObject", "GET", has_key=True, expected_op=Op.read_object),
    S3Op("HeadObject", "HEAD", has_key=True, expected_op=Op.read_object),
    S3Op("PutObject", "PUT", has_key=True, expected_op=Op.write_object),
    S3Op("DeleteObject", "DELETE", has_key=True, expected_op=Op.delete_object),
    S3Op(
        "CopyObject",
        "PUT",
        has_key=True,
        expected_op=Op.write_object,
        note="resolver only sees the destination; source bucket is enforced separately in middleware",
    ),
    # --- Object subresources ---------------------------------------------
    S3Op("GetObjectAcl", "GET", has_key=True, query={"acl": ""}, expected_op=Op.read_object),
    S3Op("PutObjectAcl", "PUT", has_key=True, query={"acl": ""}, expected_op=Op.write_object_meta),
    S3Op("GetObjectTagging", "GET", has_key=True, query={"tagging": ""}, expected_op=Op.read_object),
    S3Op("PutObjectTagging", "PUT", has_key=True, query={"tagging": ""}, expected_op=Op.write_object_meta),
    S3Op("DeleteObjectTagging", "DELETE", has_key=True, query={"tagging": ""}, expected_op=Op.delete_object),
    S3Op("GetObjectVersion", "GET", has_key=True, query={"versionId": "v1"}, expected_op=Op.read_object),
    S3Op(
        "DeleteObjectVersion",
        "DELETE",
        has_key=True,
        query={"versionId": "7"},
        expected_op=Op.delete_object_version,
        note="permanent delete of one named version; Object Lock refuses it while the version is retained",
    ),
    # Only a concrete version id is a version delete. The handler reads empty and "null" as "the
    # current version" and runs the marker-writing DELETE, and refuses a malformed one with 400.
    S3Op("DeleteObjectEmptyVersion", "DELETE", has_key=True, query={"versionId": ""}, expected_op=Op.delete_object),
    S3Op("DeleteObjectNullVersion", "DELETE", has_key=True, query={"versionId": "null"}, expected_op=Op.delete_object),
    S3Op(
        "DeleteObjectMalformedVersion",
        "DELETE",
        has_key=True,
        query={"versionId": "v1"},
        expected_op=Op.delete_object,
    ),
    # parse_version_id special-cases only the exact string "null". Other casings and non-positive
    # ids raise, and names_a_version treats that as "not a version" so the write-once tier cannot
    # reach the handler's 400 — or, if the casing were ever accepted as current, write a marker.
    S3Op(
        "DeleteObjectUpperNullVersion",
        "DELETE",
        has_key=True,
        query={"versionId": "NULL"},
        expected_op=Op.delete_object,
    ),
    S3Op("DeleteObjectZeroVersion", "DELETE", has_key=True, query={"versionId": "0"}, expected_op=Op.delete_object),
    S3Op(
        "DeleteObjectTaggingVersion",
        "DELETE",
        has_key=True,
        query={"tagging": "", "versionId": "7"},
        expected_op=Op.delete_object,
    ),
    S3Op("GetObjectAttributes", "GET", has_key=True, query={"attributes": ""}, expected_op=Op.read_object),
    # --- Object Lock (per-object) ------------------------------------------
    # Reads are object reads. Writes decide whether a version may be deleted, so "may upload" must
    # never imply them — they need write_object_lock, which only admin_read_write holds.
    S3Op("GetObjectRetention", "GET", has_key=True, query={"retention": ""}, expected_op=Op.read_object),
    S3Op("PutObjectRetention", "PUT", has_key=True, query={"retention": ""}, expected_op=Op.write_object_lock),
    S3Op(
        "PutObjectRetentionVersion",
        "PUT",
        has_key=True,
        query={"retention": "", "versionId": "v1"},
        expected_op=Op.write_object_lock,
    ),
    S3Op("GetObjectLegalHold", "GET", has_key=True, query={"legal-hold": ""}, expected_op=Op.read_object),
    S3Op("PutObjectLegalHold", "PUT", has_key=True, query={"legal-hold": ""}, expected_op=Op.write_object_lock),
    S3Op("RestoreObject", "POST", has_key=True, query={"restore": ""}, expected_op=Op.write_object),
    S3Op(
        "SelectObjectContent",
        "POST",
        has_key=True,
        query={"select": "", "select-type": "2"},
        expected_op=Op.write_object,
        note="POST without ?uploadId mapped to write_object; SelectObject is read-only in S3",
    ),
    # --- Multipart upload (object-level — has_key=True) -------------------
    S3Op("InitiateMultipartUpload", "POST", has_key=True, query={"uploads": ""}, expected_op=Op.write_object),
    S3Op("UploadPart", "PUT", has_key=True, query={"partNumber": "1", "uploadId": "u"}, expected_op=Op.write_object),
    S3Op("CompleteMultipartUpload", "POST", has_key=True, query={"uploadId": "u"}, expected_op=Op.write_object),
    S3Op(
        "AbortMultipartUpload",
        "DELETE",
        has_key=True,
        query={"uploadId": "u"},
        expected_op=Op.write_object,
        note="discards an upload that never completed; the handler refuses a completed one",
    ),
    S3Op("ListParts", "GET", has_key=True, query={"uploadId": "u"}, expected_op=Op.read_object),
    S3Op(
        "UploadPartCopy",
        "PUT",
        has_key=True,
        query={"partNumber": "1", "uploadId": "u"},
        expected_op=Op.write_object,
        note="resolver only sees the destination; source bucket is enforced separately in middleware",
    ),
]


@pytest.mark.parametrize("s3op", ALL_S3_OPS, ids=lambda op: op.name)
def test_required_op_for_every_s3_operation(s3op: S3Op) -> None:
    """Pin every S3 op to the Op the resolver currently returns.

    If you change the resolver and an assertion fails, either:
      1. you fixed a KNOWN GAP — update the row's expected_op + drop the note
      2. you regressed a working case — revert
    """
    actual = required_op(s3op.method, s3op.has_key, s3op.query)
    assert actual is s3op.expected_op, f"{s3op.name}: expected {s3op.expected_op}, got {actual}"


# ---------------------------------------------------------------------------
# Section B — token-type cross-cutting invariants.
# ---------------------------------------------------------------------------
#
# These are evaluator-level checks. The middleware's master-bypass and
# cross-account fallthrough live in `gateway/middlewares/acl.py` and are
# integration-tested separately; here we cover only what `evaluate()`
# guarantees in isolation.


def _scope(permission: Permission, bucket_scope: BucketScope, bucket_ids: list[str]) -> SubTokenScope:
    return SubTokenScope(
        access_key_id="hip_x",
        account_id="acct1",
        permission=permission,
        bucket_scope=bucket_scope,
        bucket_ids=tuple(bucket_ids),
    )


def test_evaluator_returns_no_scope_for_none_input() -> None:
    """Default-deny: caller without a scope row gets denied with a stable reason."""
    allowed, reason = evaluate(scope=None, bucket_id="b", method="GET", has_key=True, query_params={})
    assert allowed is False
    assert reason == "no_scope"


@pytest.mark.parametrize(
    "permission,expected",
    [
        (Permission.admin_read_write, True),
        (Permission.admin_read, True),
        (Permission.object_read_write, False),
        (Permission.object_read_write_no_delete, False),
        (Permission.object_read, False),
    ],
)
def test_list_buckets_permission_per_tier(permission: Permission, expected: bool) -> None:
    """ListBuckets (`GET /`) is checked at the middleware level via
    `permission_allows(tier, Op.list_buckets)` — the resolver is bypassed.
    Only admin tiers should be able to list account-wide buckets.
    """
    assert permission_allows(permission, Op.list_buckets) is expected


def test_list_buckets_op_is_distinct_from_list_bucket() -> None:
    """`Op.list_bucket` (objects in a bucket) and `Op.list_buckets`
    (account-wide bucket list) must remain distinct — collapsing them would
    let `object_read` tokens enumerate the account."""
    assert Op.list_bucket is not Op.list_buckets


def test_evaluator_create_bucket_blocked_when_scope_specific_even_for_admin_rw() -> None:
    """admin_read_write can't create new buckets unless bucket_scope='all'.

    A new bucket has no UUID yet, so 'specific' can never authorise it.
    """
    scope = _scope(Permission.admin_read_write, BucketScope.specific, ["bucket-a"])
    allowed, reason = evaluate(scope=scope, bucket_id=None, method="PUT", has_key=False, query_params={})
    assert allowed is False
    assert reason == "create_bucket_requires_scope_all"


def test_evaluator_object_op_with_bucket_id_none_is_denied() -> None:
    """Object op with no bucket in play is denied (bucket_in_scope returns False).

    The middleware shouldn't dispatch this — but if it does, the evaluator
    must not crash or grant blindly.
    """
    scope = _scope(Permission.object_read_write, BucketScope.specific, ["bucket-a"])
    allowed, reason = evaluate(scope=scope, bucket_id=None, method="GET", has_key=True, query_params={})
    assert allowed is False
    assert reason == "bucket_out_of_scope"


def test_bucket_in_scope_with_empty_bucket_ids_under_specific_denies_everything() -> None:
    """An empty `specific` scope (DB CHECK forbids it but the dataclass allows it)
    must deny every bucket — never accidentally permit by matching `None`."""
    scope = _scope(Permission.object_read, BucketScope.specific, [])
    assert bucket_in_scope("bucket-a", scope) is False
    assert bucket_in_scope(None, scope) is False


def test_bucket_in_scope_all_grants_even_with_unrelated_bucket_ids() -> None:
    """`bucket_scope='all'` ignores the bucket_ids field entirely (defensive)."""
    scope = _scope(Permission.admin_read_write, BucketScope.all, ["this-is-ignored"])
    assert bucket_in_scope("any-bucket-at-all", scope) is True


# ---------------------------------------------------------------------------
# Section C — full operation × tier expectation matrix.
# ---------------------------------------------------------------------------
#
# For every S3 op above (using its resolved Op), enumerate the four tiers and
# assert allow/deny. This is the same shape as the matrix in
# `test_sub_token_scope.py` but enumerated against named real-world ops, so
# regressions are reported in human-readable terms (e.g. "PutObjectTagging
# denied for object_read tier" instead of "tier=object_read op=write_object").


# Map every Op to a per-tier allow expectation. Source of truth: PERMISSION_MATRIX
# in gateway/services/sub_token_scope.py — duplicated here so a reviewer reading
# this file can see the full matrix at a glance without cross-referencing.
EXPECTED_ALLOW: dict[Permission, set[Op]] = {
    Permission.admin_read_write: {
        Op.read_object,
        Op.write_object,
        Op.delete_object,
        Op.list_bucket,
        Op.list_buckets,
        Op.create_bucket,
        Op.delete_bucket,
        Op.read_bucket_meta,
        Op.write_bucket_meta,
        Op.write_object_lock,
        Op.write_object_meta,
        Op.delete_object_version,
    },
    Permission.admin_read: {
        Op.read_object,
        Op.list_bucket,
        Op.list_buckets,
        Op.read_bucket_meta,
    },
    Permission.object_read_write: {
        Op.read_object,
        Op.write_object,
        Op.write_object_meta,
        Op.delete_object,
        Op.delete_object_version,
        Op.list_bucket,
    },
    Permission.object_read_write_no_delete: {
        Op.read_object,
        Op.write_object,
        Op.delete_object_version,
        Op.list_bucket,
    },
    Permission.object_read: {
        Op.read_object,
        Op.list_bucket,
    },
}


@pytest.mark.parametrize("permission", list(Permission))
@pytest.mark.parametrize("s3op", ALL_S3_OPS, ids=lambda op: op.name)
def test_tier_allows_or_denies_each_named_s3_operation(permission: Permission, s3op: S3Op) -> None:
    """For every (tier, op) cell, derive the expectation from PERMISSION_MATRIX.

    This is the human-readable face of the matrix — when something breaks the
    failure says "object_read denied PutObjectTagging" instead of an opaque
    op-tuple mismatch.
    """
    expected = s3op.expected_op in EXPECTED_ALLOW[permission]
    assert permission_allows(permission, s3op.expected_op) is expected, (
        f"{permission.value} should {'allow' if expected else 'deny'} {s3op.name}"
    )


# ---------------------------------------------------------------------------
# Section D — middleware-level invariants (using a stub middleware ctx).
# ---------------------------------------------------------------------------
#
# These don't import the actual middleware (which is integration-tested);
# they replay its decision logic in compact form to assert the invariants
# stay true under refactoring.


@dataclass
class FakeRequestState:
    auth_method: str
    token_type: str
    account_id: str | None
    access_key: str | None


def _is_master_bypass(state: FakeRequestState, bucket_owner_id: str | None) -> bool:
    """Mirrors the master-token short-circuit in acl_middleware.py:211.

    Pure function — no FastAPI / asyncpg / Redis. Lets us pin the invariants
    without spinning up a real middleware harness.
    """
    return state.auth_method == "access_key" and state.token_type == "master" and bucket_owner_id == state.account_id


def _should_take_subtoken_branch(state: FakeRequestState, bucket_owner_id: str | None) -> bool:
    """Mirrors acl_middleware.py:126-128 — sub-token branch is intra-account only."""
    if state.auth_method not in ("access_key", "bearer_access_key"):
        return False
    if state.token_type != "sub":
        return False
    if not state.access_key:
        return False
    is_cross_account = bucket_owner_id is not None and bucket_owner_id != state.account_id
    return not is_cross_account


def test_master_token_bypasses_scope_completely() -> None:
    """Master tokens never consult sub_token_scopes, even when a row exists.

    A token that was sub-tier yesterday and got upgraded to master today should
    immediately get full bypass — not be bottlenecked by a stale scope row.
    """
    state = FakeRequestState(
        auth_method="access_key",
        token_type="master",
        account_id="alice",
        access_key="hip_master_alice",
    )
    bucket_owner_id = "alice"
    assert _is_master_bypass(state, bucket_owner_id) is True
    assert _should_take_subtoken_branch(state, bucket_owner_id) is False


def test_master_token_does_not_bypass_for_other_accounts_buckets() -> None:
    """Master + cross-account → still goes through normal ACL (contractor case)."""
    state = FakeRequestState(
        auth_method="access_key",
        token_type="master",
        account_id="alice",
        access_key="hip_master_alice",
    )
    assert _is_master_bypass(state, "bob") is False


def test_subtoken_intra_account_takes_scope_branch() -> None:
    """Sub-token of bucket owner → scope is authoritative (not bucket_acls)."""
    state = FakeRequestState(
        auth_method="access_key",
        token_type="sub",
        account_id="alice",
        access_key="hip_sub_alice_1",
    )
    assert _should_take_subtoken_branch(state, "alice") is True


def test_subtoken_cross_account_falls_through_to_acl() -> None:
    """Cross-account sub-token (contractor pattern) → falls through to bucket ACL grants.

    Critical: this preserves the existing ACCESS_KEY-grant flow that lets
    account A delegate a single bucket to account B's sub-token.
    """
    state = FakeRequestState(
        auth_method="access_key",
        token_type="sub",
        account_id="alice",
        access_key="hip_sub_alice_1",
    )
    assert _should_take_subtoken_branch(state, "bob") is False


def test_subtoken_on_nonexistent_bucket_takes_scope_branch() -> None:
    """bucket_owner_id=None means bucket doesn't exist — sub-token still goes
    through the scope path (so CreateBucket can be evaluated against scope='all')."""
    state = FakeRequestState(
        auth_method="access_key",
        token_type="sub",
        account_id="alice",
        access_key="hip_sub_alice_1",
    )
    assert _should_take_subtoken_branch(state, None) is True


def test_anonymous_request_takes_neither_branch() -> None:
    """Anonymous reads of public buckets must not be evaluated against scope."""
    state = FakeRequestState(
        auth_method="anonymous",
        token_type="anonymous",
        account_id=None,
        access_key=None,
    )
    assert _is_master_bypass(state, "alice") is False
    assert _should_take_subtoken_branch(state, "alice") is False


# ---------------------------------------------------------------------------
# Section E — cache invariants (lightweight, non-Redis).
# ---------------------------------------------------------------------------
#
# Properties the cache layer must preserve. The full fail-closed behavior is
# in `test_sub_token_scope_cache.py`; here we cover semantic invariants that
# are easier to reason about with the data model than with mocks.


def test_scope_cache_payload_round_trips_through_json_serialisation() -> None:
    """Cache stores JSON; round-trip must preserve all enum + tuple semantics."""
    import json

    original = SubTokenScope(
        access_key_id="hip_test",
        account_id="acct1",
        permission=Permission.object_read_write,
        bucket_scope=BucketScope.specific,
        bucket_ids=("11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"),
    )

    # Same shape the cache writes (gateway/services/sub_token_scope_cache.py).
    payload = {
        "access_key_id": original.access_key_id,
        "account_id": original.account_id,
        "permission": original.permission.value,
        "bucket_scope": original.bucket_scope.value,
        "bucket_ids": list(original.bucket_ids),
    }
    blob = json.dumps(payload)
    rebuilt = SubTokenScope(
        access_key_id=payload["access_key_id"],
        account_id=payload["account_id"],
        permission=Permission(payload["permission"]),
        bucket_scope=BucketScope(payload["bucket_scope"]),
        bucket_ids=tuple(json.loads(blob)["bucket_ids"]),
    )

    assert rebuilt == original
    assert rebuilt.permission is original.permission
    assert rebuilt.bucket_scope is original.bucket_scope
    assert rebuilt.bucket_ids == original.bucket_ids


# ---------------------------------------------------------------------------
# Section F — explicit regression guards for the resolver fix.
# ---------------------------------------------------------------------------
#
# These tests pin individual edge-case classifications so a refactor that
# changes one of them surfaces as a clear named failure rather than a flood
# of parametrize id mismatches.


def test_put_bucket_versioning_is_classified_as_write_bucket_meta() -> None:
    """`?versioning` is in _BUCKET_META_SUBRESOURCES — PutBucketVersioning is
    a meta write, not a bucket lifecycle op. Without this, admin_read_write +
    bucket_scope='specific' loses the ability to enable versioning on its own
    buckets (because create_bucket needs scope='all')."""
    assert required_op("PUT", has_key=False, query_params={"versioning": ""}) is Op.write_bucket_meta


def test_get_bucket_versioning_is_classified_as_read_bucket_meta() -> None:
    assert required_op("GET", has_key=False, query_params={"versioning": ""}) is Op.read_bucket_meta


def test_post_bucket_delete_is_classified_as_delete_object() -> None:
    """Bulk DeleteObjects (POST /bucket?delete) is the only POST-on-bucket op
    we recognise. It must require `delete_object` so that `object_read_write`
    tokens can issue bulk deletes (they're already allowed per-object DELETE)."""
    assert required_op("POST", has_key=False, query_params={"delete": ""}) is Op.delete_object


def test_post_bucket_without_delete_query_falls_through_to_write_bucket_meta() -> None:
    """Defence-in-depth: only `?delete` triggers the bulk-delete special case.
    A POST without that query has no defined S3 op; default to bucket-meta
    write so we don't accidentally grant delete authority elsewhere."""
    assert required_op("POST", has_key=False, query_params={}) is Op.write_bucket_meta
    assert required_op("POST", has_key=False, query_params={"unknown": ""}) is Op.write_bucket_meta


def test_copy_object_destination_check_passes_for_in_scope_dest() -> None:
    """The resolver only sees the destination — the source bucket is checked
    separately in middleware (see test_acl_middleware_copy_object.py).
    This test pins the resolver-level behaviour: dest in scope → allowed."""
    scope = _scope(Permission.object_read_write, BucketScope.specific, ["dest-bucket-id"])
    allowed, _ = evaluate(
        scope=scope,
        bucket_id="dest-bucket-id",
        method="PUT",
        has_key=True,
        query_params={},
    )
    assert allowed is True


@pytest.mark.parametrize(
    "subresource",
    [
        "accelerate",
        "acl",
        "analytics",
        "cors",
        "encryption",
        "intelligent-tiering",
        "inventory",
        "lifecycle",
        "location",
        "logging",
        "metrics",
        "notification",
        "object-lock",
        "ownershipControls",
        "policy",
        "policyStatus",
        "publicAccessBlock",
        "replication",
        "requestPayment",
        "tagging",
        "versioning",
        "website",
    ],
)
def test_every_recognised_bucket_subresource_routes_to_meta_ops(subresource: str) -> None:
    """Sanity guard that the meta-subresource set covers every bucket
    configuration knob. If you accidentally remove one from the frozenset,
    the corresponding GET/PUT/DELETE silently changes classification — this
    test pins that contract."""
    assert required_op("GET", has_key=False, query_params={subresource: ""}) is Op.read_bucket_meta
    assert required_op("PUT", has_key=False, query_params={subresource: ""}) is Op.write_bucket_meta
    assert required_op("DELETE", has_key=False, query_params={subresource: ""}) is Op.write_bucket_meta


@pytest.mark.parametrize(
    "data_listing_subresource",
    ["uploads", "versions"],
)
def test_data_listing_subresources_remain_list_bucket(data_listing_subresource: str) -> None:
    """`?uploads` (list MPU) and `?versions` (list object versions) are *data*
    listings, not metadata. They must stay classified as `list_bucket` so
    `object_read` tokens can use them."""
    assert required_op("GET", has_key=False, query_params={data_listing_subresource: ""}) is Op.list_bucket


# ---------------------------------------------------------------------------
# Section G — scope-table boundary cases.
# ---------------------------------------------------------------------------


def test_scope_with_single_bucket_id_matches_only_that_bucket() -> None:
    scope = _scope(Permission.object_read_write, BucketScope.specific, ["uuid-a"])
    assert bucket_in_scope("uuid-a", scope) is True
    assert bucket_in_scope("uuid-b", scope) is False


def test_scope_with_many_bucket_ids_matches_each_one() -> None:
    """Iterating tuple membership is O(N) but with the 1000-item DB cap it stays
    fast. This test prevents a future refactor (e.g. swapping tuple → set)
    from silently changing membership semantics."""
    ids = [f"uuid-{i:03d}" for i in range(20)]
    scope = _scope(Permission.object_read, BucketScope.specific, ids)
    for uid in ids:
        assert bucket_in_scope(uid, scope) is True
    assert bucket_in_scope("uuid-not-listed", scope) is False


def test_scope_at_bucket_ids_cap_still_resolves() -> None:
    """1000 is the DB CHECK ceiling; the evaluator must not slow down or fail
    near the cap. (Smoke: 1000 lookups must finish in < 0.5s on any laptop.)"""
    import time

    ids = [f"uuid-{i:04d}" for i in range(1000)]
    scope = _scope(Permission.object_read, BucketScope.specific, ids)
    start = time.perf_counter()
    for uid in ("uuid-0000", "uuid-0500", "uuid-0999", "uuid-not-found"):
        bucket_in_scope(uid, scope)
    elapsed = time.perf_counter() - start
    assert elapsed < 0.5


# ---------------------------------------------------------------------------
# Section H — meta-test: the matrix itself.
# ---------------------------------------------------------------------------


def test_no_two_s3_ops_share_a_name() -> None:
    """Names must be unique so parametrize ids don't collide."""
    names = [op.name for op in ALL_S3_OPS]
    assert len(names) == len(set(names)), f"duplicates: {[n for n in names if names.count(n) > 1]}"


def test_every_op_in_expected_allow_appears_in_at_least_one_tier() -> None:
    """Sanity guard against tier matrix drift — every Op must be reachable."""
    all_ops_used: set[Op] = set()
    for ops in EXPECTED_ALLOW.values():
        all_ops_used.update(ops)
    for op in Op:
        assert op in all_ops_used, f"{op} unreachable in EXPECTED_ALLOW"


def test_no_known_gap_notes_remain_in_matrix() -> None:
    """Once a gap is fixed, drop its KNOWN GAP marker from ALL_S3_OPS."""
    for op in ALL_S3_OPS:
        assert not op.note.startswith("KNOWN GAP:"), f"{op.name}: stale KNOWN GAP note ({op.note})"


# ---------------------------------------------------------------------------
# Section E — the write-once tier, named by what a backup writer does.
# ---------------------------------------------------------------------------

_NO_DELETE = _scope(Permission.object_read_write_no_delete, BucketScope.specific, ["bkt"])

_WRITE_ONCE_ALLOWED = [
    "GetObject",
    "HeadObject",
    "PutObject",
    "CopyObject",
    "ListObjects",
    "ListObjectsV2",
    "ListObjectVersions",
    "GetObjectVersion",
    "GetObjectRetention",
    "GetObjectLegalHold",
    "InitiateMultipartUpload",
    "UploadPart",
    "CompleteMultipartUpload",
    "AbortMultipartUpload",
    "ListParts",
    # Pruning: a permanent delete of one named version. COMPLIANCE refuses it while retained.
    "DeleteObjectVersion",
]

_WRITE_ONCE_DENIED = [
    "PutObjectAcl",
    "PutObjectTagging",
    "DeleteObjectTagging",
    "DeleteObject",
    "DeleteObjectEmptyVersion",
    "DeleteObjectNullVersion",
    "DeleteObjectMalformedVersion",
    "DeleteObjectUpperNullVersion",
    "DeleteObjectZeroVersion",
    "DeleteObjectTaggingVersion",
    "DeleteObjects",
    "PutObjectRetention",
    "PutObjectRetentionVersion",
    "PutObjectLegalHold",
    "DeleteBucket",
    "PutBucketObjectLock",
    "PutBucketVersioning",
    "PutBucketLifecycle",
]

_BY_NAME = {op.name: op for op in ALL_S3_OPS}


@pytest.mark.parametrize("name", _WRITE_ONCE_ALLOWED)
def test_no_delete_tier_can_write_and_read(name: str) -> None:
    op = _BY_NAME[name]
    allowed, reason = evaluate(
        scope=_NO_DELETE, bucket_id="bkt", method=op.method, has_key=op.has_key, query_params=op.query
    )
    assert allowed, f"object_read_write_no_delete must allow {name} (denied: {reason})"


@pytest.mark.parametrize("name", _WRITE_ONCE_DENIED)
def test_no_delete_tier_can_never_remove_or_unlock(name: str) -> None:
    """The whole point of the tier: a stolen key cannot destroy, hide or unlock what was written."""
    op = _BY_NAME[name]
    allowed, reason = evaluate(
        scope=_NO_DELETE, bucket_id="bkt", method=op.method, has_key=op.has_key, query_params=op.query
    )
    assert not allowed, f"object_read_write_no_delete must deny {name}"
    assert reason == "op_not_allowed"


@pytest.mark.parametrize("permission", [p for p in Permission if p is not Permission.admin_read_write])
@pytest.mark.parametrize("name", ["PutObjectRetention", "PutObjectRetentionVersion", "PutObjectLegalHold"])
def test_only_admin_read_write_may_change_an_object_lock(permission: Permission, name: str) -> None:
    op = _BY_NAME[name]
    scope = _scope(permission, BucketScope.all, [])
    allowed, _ = evaluate(scope=scope, bucket_id="bkt", method=op.method, has_key=op.has_key, query_params=op.query)
    assert not allowed, f"{permission.value} must not be able to {name}"


@pytest.mark.parametrize(
    "headers,expected",
    [
        ({"x-amz-acl": "public-read-write"}, True),
        ({"x-amz-grant-write": "id=hip_other"}, True),
        ({"X-Amz-Grant-Full-Control": "id=hip_other"}, True),
        ({"x-amz-object-lock-mode": "COMPLIANCE", "content-type": "a/b"}, False),
    ],
)
def test_sets_acl_detects_canned_and_explicit_grants(headers: dict[str, str], expected: bool) -> None:
    assert sets_acl(headers) is expected


@pytest.mark.parametrize("method,query", [("PUT", {}), ("POST", {"uploads": ""})])
def test_a_write_carrying_an_acl_is_an_object_meta_write(method: str, query: dict[str, str]) -> None:
    """PutObject / CreateMultipartUpload with x-amz-acl can grant WRITE — and so delete — to others."""
    assert required_op(method, True, query, carries_acl=True) is Op.write_object_meta
    allowed, _ = evaluate(
        scope=_NO_DELETE, bucket_id="bkt", method=method, has_key=True, query_params=query, carries_acl=True
    )
    assert not allowed


@pytest.mark.parametrize(
    "method,has_key,query",
    [
        ("DELETE", True, {"versionId": "7"}),
        ("DELETE", True, {}),
        ("POST", False, {"delete": ""}),
    ],
)
def test_a_governance_bypass_needs_the_lock_op(method: str, has_key: bool, query: dict[str, str]) -> None:
    """x-amz-bypass-governance-retention overrides a lock, so it is graded like changing one.

    Without this, the write-once tier's version delete — and object_read_write's — would remove a
    GOVERNANCE-retained version whenever the sub-token belongs to the bucket owner's account.
    """
    assert required_op(method, has_key, query, bypasses_governance=True) is Op.write_object_lock
    for permission in Permission:
        allowed, _ = evaluate(
            scope=_scope(permission, BucketScope.all, []),
            bucket_id="bkt",
            method=method,
            has_key=has_key,
            query_params=query,
            bypasses_governance=True,
        )
        assert allowed is (permission is Permission.admin_read_write), permission.value


def test_the_bypass_header_does_not_regrade_non_deletes() -> None:
    assert required_op("PUT", True, {}, bypasses_governance=True) is Op.write_object
    assert required_op("GET", True, {"versionId": "7"}, bypasses_governance=True) is Op.read_object
