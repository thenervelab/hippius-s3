"""Sub-token scope types shared across the gateway, API, and persistence layers.

Keeping `Permission`, `BucketScope`, and `Op` as proper enums (instead of bare
strings) means the repository, evaluator, API body, and tests all use the same
typed vocabulary and typos fail at import time rather than at runtime.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from dataclasses import field
from enum import Enum


# ---- validation patterns (single source of truth) -------------------------

ACCESS_KEY_PATTERN = re.compile(r"^hip_[a-zA-Z0-9_-]{1,240}$")
SS58_PATTERN = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{47,48}$")


# ---- enums ----------------------------------------------------------------


class Permission(str, Enum):
    """R2-style permission tiers."""

    admin_read_write = "admin_read_write"
    admin_read = "admin_read"
    object_read_write = "object_read_write"
    # Write-once: may add data and read it back, never remove it. Meant for keys handed to a system
    # that must not be able to destroy what it wrote (backup targets), so a stolen key cannot erase
    # history. See PERMISSION_MATRIX for exactly what it withholds.
    object_read_write_no_delete = "object_read_write_no_delete"
    object_read = "object_read"


class BucketScope(str, Enum):
    """Bucket-level scope for a sub-token."""

    all = "all"
    specific = "specific"


class Op(str, Enum):
    """Internal operation vocabulary. Every S3 request maps to exactly one of these."""

    read_object = "read_object"
    write_object = "write_object"
    delete_object = "delete_object"
    list_bucket = "list_bucket"
    list_buckets = "list_buckets"
    create_bucket = "create_bucket"
    delete_bucket = "delete_bucket"
    read_bucket_meta = "read_bucket_meta"
    write_bucket_meta = "write_bucket_meta"
    # PutObjectRetention / PutObjectLegalHold. Separate from write_object because setting, extending
    # or lifting a lock is authority over WHETHER data can be deleted, not over what data exists.
    write_object_lock = "write_object_lock"
    # PutObjectAcl, PutObjectTagging, and an object write carrying x-amz-acl / x-amz-grant-*.
    # Separate from write_object because an ACL can hand WRITE (and so delete) to another key, and
    # a tag write replaces the whole set — both destroy or delegate rather than add.
    write_object_meta = "write_object_meta"
    # DELETE ?versionId=<N>: permanently remove ONE named version. Separate from delete_object
    # because it cannot hide anything (no delete marker) and Object Lock refuses it while the
    # version is retained — so a write-once key may hold it to prune backups whose lock expired.
    delete_object_version = "delete_object_version"


# ---- dataclass ------------------------------------------------------------


@dataclass(frozen=True)
class SubTokenScope:
    """Immutable snapshot of one row in `sub_token_scopes`.

    `permission` and `bucket_scope` accept either enum members or their string
    values; the dataclass coerces strings to enums post-init so downstream code
    can rely on `is BucketScope.all` identity checks instead of `== "all"`.
    """

    access_key_id: str
    account_id: str
    permission: Permission
    bucket_scope: BucketScope
    bucket_ids: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if not isinstance(self.permission, Permission):
            object.__setattr__(self, "permission", Permission(self.permission))
        if not isinstance(self.bucket_scope, BucketScope):
            object.__setattr__(self, "bucket_scope", BucketScope(self.bucket_scope))
