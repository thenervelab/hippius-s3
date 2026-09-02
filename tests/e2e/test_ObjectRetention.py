"""E2E tests for S3 Object Lock — per-object retention.

Covers:
- Tier 0 (must pass): PutObjectRetention / GetObjectRetention return 501 NotImplemented.
- Tier 2 (xfail strict=False): real enforcement (DELETE refused, COMPLIANCE hard-fail,
  GOVERNANCE bypass, expiry, override semantics).

See specs/s3-object-lock.md.
"""

from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from typing import Callable

import pytest
from botocore.exceptions import ClientError  # type: ignore[import-untyped]


def _error(exc: ClientError) -> tuple[int, str]:
    meta = exc.response.get("ResponseMetadata", {})
    status = int(meta.get("HTTPStatusCode", 0))
    code = exc.response.get("Error", {}).get("Code") or exc.response.get("Code")
    return status, code or ""


# ---------------------------------------------------------------------------
# Tier 0 — must pass after implementation lands
# ---------------------------------------------------------------------------


def test_put_and_get_object_retention_round_trip(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Tier 2 implements these; they used to assert 501.

    Retention needs a versioned bucket, which is why the bucket is created lock-enabled — that
    turns versioning on implicitly, exactly as AWS does.
    """
    bucket_name = unique_bucket_name("ret-put")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    boto3_client.put_object(Bucket=bucket_name, Key="k", Body=b"x")

    retain_until = datetime.now(timezone.utc) + timedelta(days=1)
    boto3_client.put_object_retention(
        Bucket=bucket_name,
        Key="k",
        Retention={"Mode": "GOVERNANCE", "RetainUntilDate": retain_until},
    )

    got = boto3_client.get_object_retention(Bucket=bucket_name, Key="k")
    assert got["Retention"]["Mode"] == "GOVERNANCE"
    assert abs((got["Retention"]["RetainUntilDate"] - retain_until).total_seconds()) < 2


def test_get_object_retention_on_unlocked_object_is_404(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """AWS distinguishes 'no retention on this object' from 'no such key'."""
    bucket_name = unique_bucket_name("ret-get")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    boto3_client.put_object(Bucket=bucket_name, Key="k", Body=b"x")

    with pytest.raises(ClientError) as excinfo:
        boto3_client.get_object_retention(Bucket=bucket_name, Key="k")
    status, code = _error(excinfo.value)
    assert status == 404, f"expected 404, got {status} (code={code})"


def test_put_object_retention_with_version_id_addresses_that_version(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """A retention set on an OLD version must not leak onto the current one — locks are per
    version, and applying one to the wrong version is both a false lock and a missing one."""
    bucket_name = unique_bucket_name("ret-ver")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    first = boto3_client.put_object(Bucket=bucket_name, Key="k", Body=b"v1")
    boto3_client.put_object(Bucket=bucket_name, Key="k", Body=b"v2")

    retain_until = datetime.now(timezone.utc) + timedelta(days=1)
    boto3_client.put_object_retention(
        Bucket=bucket_name,
        Key="k",
        VersionId=first["VersionId"],
        Retention={"Mode": "GOVERNANCE", "RetainUntilDate": retain_until},
    )

    on_old = boto3_client.get_object_retention(Bucket=bucket_name, Key="k", VersionId=first["VersionId"])
    assert on_old["Retention"]["Mode"] == "GOVERNANCE"

    with pytest.raises(ClientError) as excinfo:
        boto3_client.get_object_retention(Bucket=bucket_name, Key="k")
    status, _ = _error(excinfo.value)
    assert status == 404, "the retention leaked onto the current version"


def test_tier2_put_with_compliance_then_delete_versioned_refused(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("ret-comp")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)

    retain_until = datetime.now(timezone.utc) + timedelta(days=1)
    put = boto3_client.put_object(
        Bucket=bucket_name,
        Key="locked",
        Body=b"x",
        ObjectLockMode="COMPLIANCE",
        ObjectLockRetainUntilDate=retain_until,
    )
    version_id = put.get("VersionId")
    assert version_id, "expected VersionId on a versioned bucket"

    with pytest.raises(ClientError) as excinfo:
        boto3_client.delete_object(Bucket=bucket_name, Key="locked", VersionId=version_id)
    status, code = _error(excinfo.value)
    assert status == 403
    assert code == "AccessDenied"


def test_tier2_governance_bypass_succeeds_with_header(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("ret-bypass")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)

    retain_until = datetime.now(timezone.utc) + timedelta(days=1)
    put = boto3_client.put_object(
        Bucket=bucket_name,
        Key="locked",
        Body=b"x",
        ObjectLockMode="GOVERNANCE",
        ObjectLockRetainUntilDate=retain_until,
    )
    version_id = put["VersionId"]

    boto3_client.delete_object(
        Bucket=bucket_name,
        Key="locked",
        VersionId=version_id,
        BypassGovernanceRetention=True,
    )


def test_tier2_compliance_cannot_be_bypassed(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("ret-comp-bypass")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)

    retain_until = datetime.now(timezone.utc) + timedelta(days=1)
    put = boto3_client.put_object(
        Bucket=bucket_name,
        Key="locked",
        Body=b"x",
        ObjectLockMode="COMPLIANCE",
        ObjectLockRetainUntilDate=retain_until,
    )
    version_id = put["VersionId"]

    with pytest.raises(ClientError) as excinfo:
        boto3_client.delete_object(
            Bucket=bucket_name,
            Key="locked",
            VersionId=version_id,
            BypassGovernanceRetention=True,
        )
    status, _ = _error(excinfo.value)
    assert status == 403


def test_tier2_unversioned_delete_creates_marker_locked_version_remains(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """DELETE without VersionId on a locked object → 200 with delete marker; the locked
    version is still readable via VersionId."""
    bucket_name = unique_bucket_name("ret-marker")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)

    retain_until = datetime.now(timezone.utc) + timedelta(days=1)
    put = boto3_client.put_object(
        Bucket=bucket_name,
        Key="k",
        Body=b"original",
        ObjectLockMode="GOVERNANCE",
        ObjectLockRetainUntilDate=retain_until,
    )
    locked_vid = put["VersionId"]

    boto3_client.delete_object(Bucket=bucket_name, Key="k")  # no VersionId

    got = boto3_client.get_object(Bucket=bucket_name, Key="k", VersionId=locked_vid)
    assert got["Body"].read() == b"original"


def test_tier2_object_retention_roundtrip(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("ret-roundtrip")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    boto3_client.put_object(Bucket=bucket_name, Key="k", Body=b"x")

    retain_until = datetime.now(timezone.utc) + timedelta(days=1)
    boto3_client.put_object_retention(
        Bucket=bucket_name,
        Key="k",
        Retention={"Mode": "GOVERNANCE", "RetainUntilDate": retain_until},
    )

    got = boto3_client.get_object_retention(Bucket=bucket_name, Key="k")
    assert got["Retention"]["Mode"] == "GOVERNANCE"


def test_tier2_default_bucket_retention_applied_on_put(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("ret-default")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    boto3_client.put_object_lock_configuration(
        Bucket=bucket_name,
        ObjectLockConfiguration={
            "ObjectLockEnabled": "Enabled",
            "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": 7}},
        },
    )

    boto3_client.put_object(Bucket=bucket_name, Key="k", Body=b"x")

    head = boto3_client.head_object(Bucket=bucket_name, Key="k")
    assert head.get("ObjectLockMode") == "GOVERNANCE"
    assert head.get("ObjectLockRetainUntilDate") is not None


def test_tier2_object_lock_headers_echoed_on_head_object(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("ret-headers")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)

    retain_until = datetime.now(timezone.utc) + timedelta(days=1)
    boto3_client.put_object(
        Bucket=bucket_name,
        Key="k",
        Body=b"x",
        ObjectLockMode="GOVERNANCE",
        ObjectLockRetainUntilDate=retain_until,
    )

    head = boto3_client.head_object(Bucket=bucket_name, Key="k")
    assert head["ObjectLockMode"] == "GOVERNANCE"
    assert head["ObjectLockRetainUntilDate"] is not None


def test_tier2_object_lock_requires_versioning(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Enabling Object Lock on a non-versioned bucket must be rejected."""
    bucket_name = unique_bucket_name("ret-noversion")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name)  # no ObjectLockEnabledForBucket

    with pytest.raises(ClientError) as excinfo:
        boto3_client.put_object_lock_configuration(
            Bucket=bucket_name,
            ObjectLockConfiguration={
                "ObjectLockEnabled": "Enabled",
                "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": 1}},
            },
        )
    _, code = _error(excinfo.value)
    assert code in ("InvalidBucketState", "InvalidRequest")
