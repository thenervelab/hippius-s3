"""E2E tests for S3 Object Lock — per-object legal hold.

Covers:
- Tier 0 (must pass): PutObjectLegalHold / GetObjectLegalHold return 501 NotImplemented.
- Tier 2 (xfail strict=False): legal-hold enforcement and interaction with retention.

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


TIER2_REASON = "Tier 2 — legal hold enforcement not implemented; see specs/s3-object-lock.md"


def _error(exc: ClientError) -> tuple[int, str]:
    meta = exc.response.get("ResponseMetadata", {})
    status = int(meta.get("HTTPStatusCode", 0))
    code = exc.response.get("Error", {}).get("Code") or exc.response.get("Code")
    return status, code or ""


# ---------------------------------------------------------------------------
# Tier 0 — must pass after implementation lands
# ---------------------------------------------------------------------------


def test_put_object_legal_hold_returns_not_implemented(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("lh-put")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name)
    boto3_client.put_object(Bucket=bucket_name, Key="k", Body=b"x")

    with pytest.raises(ClientError) as excinfo:
        boto3_client.put_object_legal_hold(
            Bucket=bucket_name,
            Key="k",
            LegalHold={"Status": "ON"},
        )

    status, code = _error(excinfo.value)
    assert status == 501, f"expected 501, got {status} (code={code})"
    assert code == "NotImplemented"


def test_get_object_legal_hold_returns_not_implemented(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("lh-get")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name)
    boto3_client.put_object(Bucket=bucket_name, Key="k", Body=b"x")

    with pytest.raises(ClientError) as excinfo:
        boto3_client.get_object_legal_hold(Bucket=bucket_name, Key="k")

    status, code = _error(excinfo.value)
    assert status == 501
    assert code == "NotImplemented"


# ---------------------------------------------------------------------------
# Tier 2 — xfail until enforcement is implemented
# ---------------------------------------------------------------------------


@pytest.mark.xfail(strict=False, reason=TIER2_REASON)
def test_tier2_legal_hold_blocks_delete(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("lh-blocks")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    put = boto3_client.put_object(
        Bucket=bucket_name,
        Key="k",
        Body=b"x",
        ObjectLockLegalHoldStatus="ON",
    )
    version_id = put["VersionId"]

    with pytest.raises(ClientError) as excinfo:
        boto3_client.delete_object(Bucket=bucket_name, Key="k", VersionId=version_id)
    status, _ = _error(excinfo.value)
    assert status == 403


@pytest.mark.xfail(strict=False, reason=TIER2_REASON)
def test_tier2_legal_hold_blocks_delete_even_after_retention_expires(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("lh-orth")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)

    expired_retain_until = datetime.now(timezone.utc) - timedelta(days=1)
    put = boto3_client.put_object(
        Bucket=bucket_name,
        Key="k",
        Body=b"x",
        ObjectLockMode="GOVERNANCE",
        ObjectLockRetainUntilDate=expired_retain_until,
        ObjectLockLegalHoldStatus="ON",
    )
    version_id = put["VersionId"]

    # Retention is past but legal hold is on — still refused.
    with pytest.raises(ClientError):
        boto3_client.delete_object(Bucket=bucket_name, Key="k", VersionId=version_id)


@pytest.mark.xfail(strict=False, reason=TIER2_REASON)
def test_tier2_remove_legal_hold_then_delete_succeeds(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("lh-remove")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    put = boto3_client.put_object(
        Bucket=bucket_name,
        Key="k",
        Body=b"x",
        ObjectLockLegalHoldStatus="ON",
    )
    version_id = put["VersionId"]

    boto3_client.put_object_legal_hold(
        Bucket=bucket_name,
        Key="k",
        VersionId=version_id,
        LegalHold={"Status": "OFF"},
    )
    boto3_client.delete_object(Bucket=bucket_name, Key="k", VersionId=version_id)


@pytest.mark.xfail(strict=False, reason=TIER2_REASON)
def test_tier2_legal_hold_roundtrip(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("lh-rt")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    boto3_client.put_object(Bucket=bucket_name, Key="k", Body=b"x")

    boto3_client.put_object_legal_hold(Bucket=bucket_name, Key="k", LegalHold={"Status": "ON"})
    got = boto3_client.get_object_legal_hold(Bucket=bucket_name, Key="k")
    assert got["LegalHold"]["Status"] == "ON"
