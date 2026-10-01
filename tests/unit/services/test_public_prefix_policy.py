"""Parser and matcher for the two bucket policies this gateway stores.

The parser is the gate that keeps a policy from naming another tenant's bucket,
from widening a trailing wildcard into a prefix match, and from smuggling a
second grant next to a public one.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from hippius_s3.models.acl import ACL
from hippius_s3.models.acl import Grant
from hippius_s3.models.acl import Grantee
from hippius_s3.models.acl import GranteeType
from hippius_s3.models.acl import Owner
from hippius_s3.models.acl import Permission
from hippius_s3.models.acl import WellKnownGroups
from hippius_s3.services.public_prefix_policy import MAX_POLICY_BYTES
from hippius_s3.services.public_prefix_policy import PolicyDocumentError
from hippius_s3.services.public_prefix_policy import PolicyKind
from hippius_s3.services.public_prefix_policy import bucket_grants_anonymous_read
from hippius_s3.services.public_prefix_policy import key_matches_public_prefix
from hippius_s3.services.public_prefix_policy import parse_bucket_policy
from hippius_s3.services.public_prefix_policy import policy_document
from hippius_s3.utils import get_query


BUCKET = "evidence"
ROOT = Path(__file__).resolve().parents[3]


def _policy(*resources: str, effect: str = "Allow", principal: object = "*", action: object = None) -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": effect,
                "Principal": principal,
                "Action": ["s3:GetObject"] if action is None else action,
                "Resource": list(resources),
            }
        ],
    }


def _arn(bucket: str, key_pattern: str) -> str:
    return f"arn:aws:s3:::{bucket}/{key_pattern}"


def test_whole_bucket_document_round_trips() -> None:
    parsed = parse_bucket_policy(_policy(_arn(BUCKET, "*")), BUCKET)
    assert parsed.kind is PolicyKind.WHOLE
    assert parsed.prefixes == ()
    again = parse_bucket_policy(policy_document(BUCKET, public_acl=True, prefixes=[]), BUCKET)
    assert again == parsed


def test_prefix_document_round_trips_sorted_and_deduped() -> None:
    parsed = parse_bucket_policy(
        _policy(_arn(BUCKET, "public/*"), _arn(BUCKET, "weights/*"), _arn(BUCKET, "public/*")),
        BUCKET,
    )
    assert parsed.kind is PolicyKind.PREFIX
    assert parsed.prefixes == ("public", "weights")
    again = parse_bucket_policy(policy_document(BUCKET, public_acl=False, prefixes=list(parsed.prefixes)), BUCKET)
    assert again == parsed


def test_statement_object_string_action_and_aws_star_principal_are_accepted() -> None:
    policy = {
        "Version": "2012-10-17",
        "Id": "one",
        "Statement": {
            "Sid": "pub",
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": "s3:GetObject",
            "Resource": _arn(BUCKET, "public/*"),
        },
    }
    parsed = parse_bucket_policy(policy, BUCKET)
    assert parsed.prefixes == ("public",)


def test_nested_prefix_is_literal() -> None:
    parsed = parse_bucket_policy(_policy(_arn(BUCKET, "public/weights/*")), BUCKET)
    assert parsed.prefixes == ("public/weights",)


@pytest.mark.parametrize(
    "resource",
    [
        "arn:aws:s3:::other/public/*",
        "arn:aws:s3:::evidence-evil/public/*",
        "arn:aws:s3:::Evidence/public/*",
        "arn:aws:s3:::*/public/*",
        "arn:aws:s3:::evidence",
        "arn:aws:s3:::evidence/",
        "*",
    ],
)
def test_resource_must_name_this_bucket(resource: str) -> None:
    with pytest.raises(PolicyDocumentError) as exc:
        parse_bucket_policy(_policy(resource), BUCKET)
    assert exc.value.code == "InvalidPolicyDocument"


def test_other_bucket_mixed_into_a_valid_statement_is_rejected() -> None:
    policy = _policy(_arn(BUCKET, "public/*"), "arn:aws:s3:::other/public/*")
    with pytest.raises(PolicyDocumentError):
        parse_bucket_policy(policy, BUCKET)


@pytest.mark.parametrize(
    "pattern",
    [
        "public*",
        "public*/*",
        "*/*",
        "public/*/*",
        "public/?",
        "public/a?",
        "/*",
        "//*",
        "/public/*",
        "public//secret/*",
        "./secret/*",
        "public/../sealed/*",
        "public/./open/*",
        "../sealed/*",
        "public/%2Fsealed/*",
    ],
)
def test_wildcard_and_dot_prefixes_are_rejected(pattern: str) -> None:
    with pytest.raises(PolicyDocumentError):
        parse_bucket_policy(_policy(_arn(BUCKET, pattern)), BUCKET)


def test_a_space_in_a_prefix_is_a_literal_prefix() -> None:
    parsed = parse_bucket_policy(_policy(_arn(BUCKET, "pub lic/*")), BUCKET)
    assert parsed.prefixes == ("pub lic",)


def test_avoid_chars_and_empty_and_overlong_prefix_are_rejected() -> None:
    with pytest.raises(PolicyDocumentError):
        parse_bucket_policy(_policy(_arn(BUCKET, "public%2Fsecret/*")), BUCKET)
    with pytest.raises(PolicyDocumentError):
        parse_bucket_policy(_policy(_arn(BUCKET, "has space?/*")), BUCKET)
    overlong = "p" * 1025
    with pytest.raises(PolicyDocumentError):
        parse_bucket_policy(_policy(_arn(BUCKET, f"{overlong}/*")), BUCKET)
    exact = "p" * 1024
    assert parse_bucket_policy(_policy(_arn(BUCKET, f"{exact}/*")), BUCKET).prefixes == (exact,)


def test_extra_statement_is_not_ignored() -> None:
    """The previous validator returned success when any one statement matched."""
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            _policy(_arn(BUCKET, "*"))["Statement"][0],
            {"Effect": "Allow", "Principal": "*", "Action": "s3:DeleteObject", "Resource": _arn(BUCKET, "*")},
        ],
    }
    with pytest.raises(PolicyDocumentError):
        parse_bucket_policy(policy, BUCKET)


@pytest.mark.parametrize(
    "statement",
    [
        {"Effect": "Deny", "Principal": "*", "Action": ["s3:GetObject"], "Resource": [_arn(BUCKET, "public/*")]},
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": ["s3:GetObject"],
            "Resource": [_arn(BUCKET, "public/*")],
            "Condition": {"StringEquals": {"s3:prefix": "public/"}},
        },
        {
            "Effect": "Allow",
            "NotPrincipal": "*",
            "Action": ["s3:GetObject"],
            "Resource": [_arn(BUCKET, "public/*")],
        },
        {"Effect": "Allow", "Principal": "arn:aws:iam::1:root", "Action": ["s3:GetObject"], "Resource": ["*"]},
        {"Effect": "Allow", "Principal": ["*"], "Action": ["s3:GetObject"], "Resource": [_arn(BUCKET, "public/*")]},
        {"Effect": "Allow", "Principal": {"AWS": ["*"]}, "Action": ["s3:GetObject"], "Resource": [_arn(BUCKET, "*")]},
        {
            "Effect": "Allow",
            "Principal": {"AWS": "*", "CanonicalUser": "*"},
            "Action": ["s3:GetObject"],
            "Resource": [_arn(BUCKET, "*")],
        },
        {"Effect": "Allow", "Principal": "*", "Action": "s3:*", "Resource": [_arn(BUCKET, "*")]},
        {"Effect": "Allow", "Principal": "*", "Action": "s3:GetObject*", "Resource": [_arn(BUCKET, "*")]},
        {"Effect": "Allow", "Principal": "*", "Action": "s3:GetObjectVersion", "Resource": [_arn(BUCKET, "*")]},
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": ["s3:GetObject", "s3:GetObjectVersion"],
            "Resource": [_arn(BUCKET, "*")],
        },
        {"Effect": "allow", "Principal": "*", "Action": ["s3:GetObject"], "Resource": [_arn(BUCKET, "*")]},
    ],
)
def test_anything_beyond_anonymous_get_object_is_rejected(statement: dict) -> None:
    with pytest.raises(PolicyDocumentError):
        parse_bucket_policy({"Version": "2012-10-17", "Statement": [statement]}, BUCKET)


def test_mixed_whole_and_prefix_document_is_rejected() -> None:
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            _policy(_arn(BUCKET, "*"))["Statement"][0],
            _policy(_arn(BUCKET, "public/*"))["Statement"][0],
        ],
    }
    with pytest.raises(PolicyDocumentError):
        parse_bucket_policy(policy, BUCKET)
    mixed = policy_document(BUCKET, public_acl=True, prefixes=["public"])
    assert mixed is not None
    with pytest.raises(PolicyDocumentError):
        parse_bucket_policy(mixed, BUCKET)


def test_too_many_statements_are_rejected() -> None:
    statements = [_policy(_arn(BUCKET, f"p{i}/*"))["Statement"][0] for i in range(33)]
    with pytest.raises(PolicyDocumentError):
        parse_bucket_policy({"Version": "2012-10-17", "Statement": statements}, BUCKET)


def test_unknown_top_level_key_is_rejected() -> None:
    policy = _policy(_arn(BUCKET, "*"))
    policy["Effect"] = "Allow"
    with pytest.raises(PolicyDocumentError):
        parse_bucket_policy(policy, BUCKET)


@pytest.mark.parametrize(
    ("key", "matches"),
    [
        ("public/a", True),
        ("public/", True),
        ("public/secret/x", True),
        ("public", False),
        ("publicity", False),
        ("publicity/a", False),
        ("a/public/b", False),
        ("Public/a", False),
        ("public/../sealed/x", False),
        ("foo/../public/a", True),
        ("public/./open", True),
        ("public/../../other/x", False),
    ],
)
def test_matcher_is_a_literal_child_prefix(key: str, matches: bool) -> None:
    assert key_matches_public_prefix(key, ["public"]) is matches


def test_matcher_does_not_treat_nfc_and_nfd_as_the_same_prefix() -> None:
    nfc = "caf\u00e9"
    nfd = "cafe\u0301"
    assert key_matches_public_prefix(f"{nfc}/a", [nfc]) is True
    assert key_matches_public_prefix(f"{nfd}/a", [nfc]) is False


def test_matcher_skips_a_prefix_the_table_should_not_hold() -> None:
    assert key_matches_public_prefix("public/a", ["public*"]) is False
    assert key_matches_public_prefix("public/a", ["../public"]) is False
    assert key_matches_public_prefix("public/a", ["public", "public*"]) is True


def test_anonymous_read_detects_full_control_and_not_write() -> None:
    owner = Owner(id="owner")

    def acl(*perms: Permission) -> ACL:
        return ACL(
            owner=owner,
            grants=[
                Grant(grantee=Grantee(type=GranteeType.GROUP, uri=WellKnownGroups.ALL_USERS), permission=perm)
                for perm in perms
            ],
        )

    assert bucket_grants_anonymous_read(acl(Permission.READ)) is True
    assert bucket_grants_anonymous_read(acl(Permission.FULL_CONTROL)) is True
    assert bucket_grants_anonymous_read(acl(Permission.WRITE)) is False
    assert bucket_grants_anonymous_read(acl(Permission.READ_ACP)) is False
    assert bucket_grants_anonymous_read(None) is False


def test_list_query_is_scoped_to_the_live_bucket() -> None:
    sql = get_query("list_bucket_public_prefixes").lower()
    assert "deleted_at is null" in sql
    assert "like" not in sql
    assert "bucket_id = $1" in sql


def test_migration_is_a_new_table_with_a_bounded_lock() -> None:
    text = (ROOT / "hippius_s3/sql/migrations/20261001140000_bucket_public_prefixes.sql").read_text()
    assert "REFERENCES buckets" in text
    assert "ON DELETE CASCADE" in text
    assert "lock_timeout" in text
    assert "DISABLE TRIGGER" not in text
    assert MAX_POLICY_BYTES == 20 * 1024
