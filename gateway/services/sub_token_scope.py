"""R2-style sub-token scope evaluation.

Four permission tiers × bucket list. No prefix restrictions, no IP allowlist.
Mirrors Cloudflare R2's model.
"""

from __future__ import annotations

from hippius_s3.models.sub_token import BucketScope
from hippius_s3.models.sub_token import Op
from hippius_s3.models.sub_token import Permission
from hippius_s3.models.sub_token import SubTokenScope


# Re-exports — several callers still reference these by their old names.
OP_READ_OBJECT = Op.read_object
OP_WRITE_OBJECT = Op.write_object
OP_DELETE_OBJECT = Op.delete_object
OP_LIST_BUCKET = Op.list_bucket
OP_LIST_BUCKETS = Op.list_buckets
OP_CREATE_BUCKET = Op.create_bucket
OP_DELETE_BUCKET = Op.delete_bucket
OP_READ_BUCKET_META = Op.read_bucket_meta
OP_WRITE_BUCKET_META = Op.write_bucket_meta


PERMISSION_MATRIX: dict[Permission, frozenset[Op]] = {
    Permission.admin_read_write: frozenset(
        {
            Op.read_object,
            Op.write_object,
            Op.delete_object,
            Op.list_bucket,
            Op.list_buckets,
            Op.create_bucket,
            Op.delete_bucket,
            Op.read_bucket_meta,
            Op.write_bucket_meta,
        }
    ),
    Permission.admin_read: frozenset(
        {
            Op.read_object,
            Op.list_bucket,
            Op.list_buckets,
            Op.read_bucket_meta,
        }
    ),
    Permission.object_read_write: frozenset(
        {
            Op.read_object,
            Op.write_object,
            Op.delete_object,
            Op.list_bucket,
        }
    ),
    Permission.object_read: frozenset(
        {
            Op.read_object,
            Op.list_bucket,
        }
    ),
}


# Subresource query params that turn a bucket op into a bucket-metadata op.
_BUCKET_META_SUBRESOURCES = frozenset({"acl", "tagging", "policy", "cors", "lifecycle"})

_OBJECT_OPS: dict[str, Op] = {
    "GET": Op.read_object,
    "HEAD": Op.read_object,
    "PUT": Op.write_object,
    "POST": Op.write_object,
    "DELETE": Op.delete_object,
}

_BUCKET_OPS: dict[str, Op] = {
    "GET": Op.list_bucket,
    "HEAD": Op.list_bucket,
    "PUT": Op.create_bucket,
    "DELETE": Op.delete_bucket,
}

_BUCKET_META_OPS: dict[str, Op] = {
    "GET": Op.read_bucket_meta,
    "HEAD": Op.read_bucket_meta,
    "PUT": Op.write_bucket_meta,
    "POST": Op.write_bucket_meta,
    "DELETE": Op.write_bucket_meta,
}


def required_op(method: str, has_key: bool, query_params: dict[str, str]) -> Op:
    """Map an incoming S3 HTTP request to the single op required to authorise it.

    Subresource queries (?acl, ?tagging, …) on a bucket map to bucket-meta ops;
    on an object they're treated as regular reads/writes, same as AWS.
    """
    if has_key:
        return _OBJECT_OPS.get(method, Op.write_object)

    is_meta_subresource = any(q in query_params for q in _BUCKET_META_SUBRESOURCES)
    if is_meta_subresource:
        return _BUCKET_META_OPS.get(method, Op.write_bucket_meta)

    return _BUCKET_OPS.get(method, Op.write_bucket_meta)


def permission_allows(permission: Permission, op: Op) -> bool:
    """Return True if the tier's operation set covers the required op."""
    return op in PERMISSION_MATRIX.get(permission, frozenset())


def bucket_in_scope(bucket_id: str | None, scope: SubTokenScope) -> bool:
    """Return True if the sub-token's bucket scope permits this bucket.

    `bucket_id` may be None for list-buckets (no single bucket in play).
    """
    if scope.bucket_scope is BucketScope.all:
        return True
    if bucket_id is None:
        return False
    return bucket_id in scope.bucket_ids


def evaluate(
    *,
    scope: SubTokenScope | None,
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

    if op is Op.list_buckets:
        return True, ""

    if op is Op.create_bucket:
        if scope.bucket_scope is not BucketScope.all:
            return False, "create_bucket_requires_scope_all"
        return True, ""

    if not bucket_in_scope(bucket_id, scope):
        return False, "bucket_out_of_scope"

    return True, ""
