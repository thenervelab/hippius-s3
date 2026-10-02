"""The two bucket-policy documents this gateway stores.

A whole-bucket document is the existing public-read helper: Allow, Principal *,
s3:GetObject, ``arn:aws:s3:::<bucket>/*``. It is implemented by the bucket ACL.

A prefix document is one or more of the same statement aimed at
``arn:aws:s3:::<bucket>/<prefix>/*``. Each prefix publishes anonymous GET and HEAD
of the *current* object version when the key is strictly under it. Overlapping
prefixes are a union: ``public`` also covers ``public/secret/x``. There is no Deny,
so a policy cannot carve a private child out of a public parent.

Anything else — another bucket's ARN, a wildcard that is not one trailing ``/*``,
Deny, a condition, a specific principal, any action other than s3:GetObject — is
rejected. A document the old validator would have accepted because *one* statement
matched, while another granted something else, is now rejected.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from hippius_s3.gateway.utils.paths import collapse_dot_segments
from hippius_s3.models.acl import ACL
from hippius_s3.models.acl import Permission
from hippius_s3.models.acl import WellKnownGroups
from hippius_s3.object_key_chars import OBJECT_KEY_AVOID_CHARS


_ARN = "arn:aws:s3:::"
_VERSION = "2012-10-17"
_ACTION = "s3:GetObject"
_TOP_KEYS = frozenset({"Version", "Id", "Statement"})
_STATEMENT_KEYS = frozenset({"Sid", "Effect", "Principal", "Action", "Resource"})
_AVOID = frozenset(OBJECT_KEY_AVOID_CHARS)
MAX_STATEMENTS = 32
MAX_PREFIX_BYTES = 1024
MAX_POLICY_BYTES = 20 * 1024


class PolicyKind(str, Enum):
    WHOLE = "whole"
    PREFIX = "prefix"


class PolicyDocumentError(Exception):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


@dataclass(frozen=True)
class ParsedBucketPolicy:
    kind: PolicyKind
    prefixes: tuple[str, ...]


def bucket_grants_anonymous_read(acl: ACL | None) -> bool:
    """True when the bucket ACL already lets an anonymous caller read every object.

    FULL_CONTROL implies READ. WRITE alone does not: a public-read-write grant's
    WRITE half must not be what marks the bucket public, and it must not block a
    prefix policy on a bucket that is not anonymously readable.
    """
    if acl is None:
        return False
    return any(
        grant.grantee.uri == WellKnownGroups.ALL_USERS
        and grant.permission in (Permission.READ, Permission.FULL_CONTROL)
        for grant in acl.grants
    )


def parse_bucket_policy(policy: Any, bucket_name: str) -> ParsedBucketPolicy:
    if not isinstance(policy, dict) or set(policy) - _TOP_KEYS:
        raise PolicyDocumentError("InvalidPolicyDocument", "Policy document is not a supported bucket policy")
    if policy.get("Version") != _VERSION:
        raise PolicyDocumentError("InvalidPolicyDocument", "Policy document is not a supported bucket policy")
    if "Id" in policy and not isinstance(policy["Id"], str):
        raise PolicyDocumentError("InvalidPolicyDocument", "Policy document is not a supported bucket policy")

    raw_statements = policy.get("Statement")
    if isinstance(raw_statements, dict):
        statements = [raw_statements]
    elif isinstance(raw_statements, list):
        statements = raw_statements
    else:
        raise PolicyDocumentError("InvalidPolicyDocument", "Policy document is not a supported bucket policy")
    if not statements or len(statements) > MAX_STATEMENTS:
        raise PolicyDocumentError("InvalidPolicyDocument", "Policy document is not a supported bucket policy")

    kinds: list[PolicyKind] = []
    prefixes: list[str] = []
    for statement in statements:
        kind, statement_prefixes = _parse_statement(statement, bucket_name)
        kinds.append(kind)
        prefixes.extend(statement_prefixes)
        if len(prefixes) > MAX_STATEMENTS:
            raise PolicyDocumentError("InvalidPolicyDocument", "Policy document is not a supported bucket policy")

    if PolicyKind.WHOLE in kinds and PolicyKind.PREFIX in kinds:
        raise PolicyDocumentError(
            "InvalidPolicyDocument",
            "A bucket policy cannot mix a whole-bucket grant with a prefix grant",
        )
    if kinds[0] is PolicyKind.WHOLE:
        return ParsedBucketPolicy(kind=PolicyKind.WHOLE, prefixes=())
    return ParsedBucketPolicy(kind=PolicyKind.PREFIX, prefixes=tuple(sorted(set(prefixes))))


def policy_document(bucket_name: str, *, public_acl: bool, prefixes: list[str]) -> dict[str, Any] | None:
    """The GetBucketPolicy body. A mixed document is what a later ACL change produces.

    PutBucketPolicy rejects that mixed shape. Get still returns it, so making the
    bucket ACL private again cannot hide prefixes that are still in force.
    """
    statements: list[dict[str, Any]] = []
    if public_acl:
        statements.append(_statement(f"{_ARN}{bucket_name}/*"))
    statements.extend(_statement(f"{_ARN}{bucket_name}/{prefix}/*") for prefix in sorted(prefixes))
    if not statements:
        return None
    return {"Version": _VERSION, "Statement": statements}


def stored_object_key(key: str) -> str:
    """Key after the same dot-segment collapse ``routing_path`` applies to a GET.

    A copy-source header is not collapsed before the ACL check. The prefix test
    and the object-ACL lookup both have to use this form, or ``foo/../sealed/x``
    matches a public prefix while the private ACL on ``sealed/x`` is never read.

    A leading slash stays. ``path_normalization`` does not collapse empty
    segments, and the object route captures them: ``/bucket//public/a`` is
    stored and served as ``/public/a``, a different object from ``public/a``.
    Stripping the slash would publish that object under the prefix ``public``.
    The matcher collapses again, so the strip also has to be idempotent —
    ``/bucket///public/a`` is the key ``//public/a``, and a second strip of a
    one-slash result is ``public/a``.
    """
    return collapse_dot_segments(key)


def key_matches_public_prefix(key: str, prefixes: list[str] | tuple[str, ...]) -> bool:
    """Literal ``prefix + '/'`` match against :func:`stored_object_key`.

    ``public`` matches ``public/a`` and ``public/`` and does not match ``public``,
    ``publicity``, or ``a/public``. ``public/../sealed`` is ``sealed``.
    """
    normalized = stored_object_key(key)
    if not normalized or normalized == "/":
        return False
    for prefix in prefixes:
        if not _prefix_shape_ok(prefix):
            continue
        if normalized.startswith(prefix + "/"):
            return True
    return False


def _statement(resource: str) -> dict[str, Any]:
    return {
        "Effect": "Allow",
        "Principal": "*",
        "Action": [_ACTION],
        "Resource": [resource],
    }


def _parse_statement(statement: Any, bucket_name: str) -> tuple[PolicyKind, list[str]]:
    if not isinstance(statement, dict) or set(statement) - _STATEMENT_KEYS:
        raise PolicyDocumentError("InvalidPolicyDocument", "Policy document is not a supported bucket policy")
    if "Sid" in statement and not isinstance(statement["Sid"], str):
        raise PolicyDocumentError("InvalidPolicyDocument", "Policy document is not a supported bucket policy")
    if statement.get("Effect") != "Allow" or not _principal_ok(statement.get("Principal")):
        raise PolicyDocumentError("InvalidPolicyDocument", "Policy document is not a supported bucket policy")
    if not _action_ok(statement.get("Action")):
        raise PolicyDocumentError("InvalidPolicyDocument", "Policy document is not a supported bucket policy")

    resources = _resources(statement.get("Resource"))
    kinds: list[PolicyKind] = []
    prefixes: list[str] = []
    for resource in resources:
        kind, prefix = _parse_resource(resource, bucket_name)
        kinds.append(kind)
        if prefix is not None:
            prefixes.append(prefix)
    if PolicyKind.WHOLE in kinds and PolicyKind.PREFIX in kinds:
        raise PolicyDocumentError(
            "InvalidPolicyDocument",
            "A bucket policy cannot mix a whole-bucket grant with a prefix grant",
        )
    if kinds[0] is PolicyKind.WHOLE:
        return PolicyKind.WHOLE, []
    return PolicyKind.PREFIX, prefixes


def _resources(resource: Any) -> list[str]:
    if isinstance(resource, str):
        return [resource]
    if isinstance(resource, list) and resource and all(isinstance(item, str) for item in resource):
        if len(resource) > MAX_STATEMENTS:
            raise PolicyDocumentError("InvalidPolicyDocument", "Policy document is not a supported bucket policy")
        return resource
    raise PolicyDocumentError("InvalidPolicyDocument", "Policy document is not a supported bucket policy")


def _principal_ok(principal: Any) -> bool:
    if principal == "*":
        return True
    return isinstance(principal, dict) and set(principal) == {"AWS"} and principal.get("AWS") == "*"


def _action_ok(action: Any) -> bool:
    if action == _ACTION:
        return True
    return isinstance(action, list) and action == [_ACTION]


def _parse_resource(resource: str, bucket_name: str) -> tuple[PolicyKind, str | None]:
    if not resource.startswith(_ARN):
        raise PolicyDocumentError("InvalidPolicyDocument", "Policy resource must name this bucket")
    rest = resource[len(_ARN) :]
    bucket_part, sep, key_pattern = rest.partition("/")
    if not sep or bucket_part != bucket_name:
        raise PolicyDocumentError("InvalidPolicyDocument", "Policy resource must name this bucket")
    if key_pattern == "*":
        return PolicyKind.WHOLE, None
    if not key_pattern.endswith("/*"):
        raise PolicyDocumentError("InvalidPolicyDocument", "Policy resource is not a supported prefix")
    prefix = key_pattern[: -len("/*")]
    _require_prefix(prefix)
    return PolicyKind.PREFIX, prefix


def _require_prefix(prefix: str) -> None:
    if _prefix_shape_ok(prefix) and len(prefix.encode("utf-8")) <= MAX_PREFIX_BYTES:
        return
    raise PolicyDocumentError("InvalidPolicyDocument", "Policy resource is not a supported prefix")


def _prefix_shape_ok(prefix: str) -> bool:
    if not prefix or prefix.startswith("/") or prefix.endswith("/"):
        return False
    if len(prefix.encode("utf-8")) > MAX_PREFIX_BYTES:
        return False
    segments = prefix.split("/")
    if any(segment in ("", ".", "..") for segment in segments):
        return False
    return not any(char in _AVOID or char in "*?" for char in prefix)
