"""R2-style sub-token scope evaluation.

Four permission tiers × bucket list. No prefix restrictions, no IP allowlist.
Mirrors Cloudflare R2's model.
"""

from __future__ import annotations

from hippius_s3.repositories.sub_token_scope_repository import SubTokenScope


# Internal operation vocabulary. Every S3 request maps to exactly one of these.
OP_READ_OBJECT = "read_object"        # GET/HEAD object (includes ?tagging, ?acl reads)
OP_WRITE_OBJECT = "write_object"      # PUT/POST object (includes ?tagging writes, multipart)
OP_DELETE_OBJECT = "delete_object"    # DELETE object
OP_LIST_BUCKET = "list_bucket"        # GET/HEAD bucket (list objects)
OP_LIST_BUCKETS = "list_buckets"      # GET /
OP_CREATE_BUCKET = "create_bucket"    # PUT /bucket
OP_DELETE_BUCKET = "delete_bucket"    # DELETE bucket (no key)
OP_READ_BUCKET_META = "read_bucket_meta"   # GetBucketAcl, GetBucketLocation, GetBucketTagging, etc.
OP_WRITE_BUCKET_META = "write_bucket_meta" # PutBucketAcl, PutBucketTagging, etc.


PERMISSION_MATRIX: dict[str, frozenset[str]] = {
    "admin_read_write": frozenset({
        OP_READ_OBJECT, OP_WRITE_OBJECT, OP_DELETE_OBJECT,
        OP_LIST_BUCKET, OP_LIST_BUCKETS,
        OP_CREATE_BUCKET, OP_DELETE_BUCKET,
        OP_READ_BUCKET_META, OP_WRITE_BUCKET_META,
    }),
    "admin_read": frozenset({
        OP_READ_OBJECT, OP_LIST_BUCKET, OP_LIST_BUCKETS, OP_READ_BUCKET_META,
    }),
    "object_read_write": frozenset({
        OP_READ_OBJECT, OP_WRITE_OBJECT, OP_DELETE_OBJECT, OP_LIST_BUCKET,
    }),
    "object_read": frozenset({
        OP_READ_OBJECT, OP_LIST_BUCKET,
    }),
}


def required_op(method: str, has_key: bool, query_params: dict[str, str]) -> str:
    """Map an incoming S3 HTTP request to the single op name required to authorise it.

    Subresource queries (?acl, ?tagging, etc.) on the bucket map to bucket-meta ops.
    On an object, they are treated as object reads/writes — same as AWS.
    """
    is_meta_subresource = any(q in query_params for q in ("acl", "tagging", "policy", "cors", "lifecycle"))

    if has_key:
        # object-level
        if method in ("GET", "HEAD"):
            return OP_READ_OBJECT
        if method in ("PUT", "POST"):
            return OP_WRITE_OBJECT
        if method == "DELETE":
            return OP_DELETE_OBJECT
        return OP_WRITE_OBJECT  # conservative default

    # bucket-level
    if is_meta_subresource:
        if method in ("GET", "HEAD"):
            return OP_READ_BUCKET_META
        return OP_WRITE_BUCKET_META

    if method in ("GET", "HEAD"):
        return OP_LIST_BUCKET
    if method == "PUT":
        return OP_CREATE_BUCKET
    if method == "DELETE":
        return OP_DELETE_BUCKET
    return OP_WRITE_BUCKET_META  # conservative default


def permission_allows(permission: str, op: str) -> bool:
    """Return True if the tier's operation set covers the required op."""
    allowed = PERMISSION_MATRIX.get(permission)
    if allowed is None:
        return False
    return op in allowed


def bucket_in_scope(bucket_id: str | None, scope: SubTokenScope) -> bool:
    """Return True if the sub-token's bucket scope permits this bucket.

    `bucket_id` may be None for list-buckets (no single bucket in play).
    """
    if scope.bucket_scope == "all":
        return True
    if bucket_id is None:
        return False
    return bucket_id in scope.bucket_ids


def evaluate(
    scope: SubTokenScope | None,
    *,
    bucket_id: str | None,
    method: str,
    has_key: bool,
    query_params: dict[str, str],
) -> tuple[bool, str]:
    """Evaluate a sub-token's stored scope against an incoming request.

    Returns (allowed, reason). `reason` is a short tag for logging; empty when allowed.
    """
    if scope is None:
        return False, "no_scope"

    op = required_op(method, has_key, query_params)
    if not permission_allows(scope.permission, op):
        return False, "op_not_allowed"

    # ListBuckets doesn't belong to any particular bucket.
    if op == OP_LIST_BUCKETS:
        return True, ""

    # CreateBucket requires admin_read_write with bucket_scope='all'.
    if op == OP_CREATE_BUCKET:
        if scope.bucket_scope != "all":
            return False, "create_bucket_requires_scope_all"
        return True, ""

    if not bucket_in_scope(bucket_id, scope):
        return False, "bucket_out_of_scope"

    return True, ""
