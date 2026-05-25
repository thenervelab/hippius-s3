"""E2E tests for S3 Object Lock — bucket-level configuration.

Covers:
- Tier 0 (must pass): per-object Object Lock surface still returns 501 NotImplemented.
- Tier 1 (must pass): bucket-level configuration round-trips through Postgres and the
  CreateBucket lock-enabled header is honoured.

See specs/s3-object-lock.md for the full surface and tier definitions.
"""

from __future__ import annotations

from typing import Any
from typing import Callable

import pytest
from botocore.awsrequest import AWSRequest  # type: ignore[import-untyped]
from botocore.exceptions import ClientError  # type: ignore[import-untyped]


def _error(exc: ClientError) -> tuple[int, str]:
    """Extract (status_code, error_code) from a botocore ClientError."""
    meta = exc.response.get("ResponseMetadata", {})
    status = int(meta.get("HTTPStatusCode", 0))
    code = exc.response.get("Error", {}).get("Code") or exc.response.get("Code")
    return status, code or ""


# ---------------------------------------------------------------------------
# Tier 0 — must still pass: per-object surface remains unimplemented
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "header_name,header_value",
    [
        ("x-amz-object-lock-mode", "GOVERNANCE"),
        ("x-amz-object-lock-retain-until-date", "2099-01-01T00:00:00Z"),
        ("x-amz-object-lock-legal-hold", "ON"),
    ],
)
def test_put_object_with_object_lock_headers_returns_not_implemented(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    header_name: str,
    header_value: str,
) -> None:
    """PutObject carrying any x-amz-object-lock-* header must 501 — otherwise the
    client believes the lock was applied when it was silently ignored.
    """
    bucket_name = unique_bucket_name("ol-put-obj")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name)

    def _inject_header(request: AWSRequest, **_: Any) -> None:
        request.headers[header_name] = header_value

    boto3_client.meta.events.register("before-sign.s3.PutObject", _inject_header)
    try:
        with pytest.raises(ClientError) as excinfo:
            boto3_client.put_object(Bucket=bucket_name, Key="locked-key", Body=b"hello")
    finally:
        boto3_client.meta.events.unregister("before-sign.s3.PutObject", _inject_header)

    status, code = _error(excinfo.value)
    assert status == 501, f"{header_name} should yield 501, got {status} (code={code})"
    assert code == "NotImplemented"


@pytest.mark.parametrize(
    "header_name,header_value",
    [
        ("x-amz-object-lock-mode", "GOVERNANCE"),
        ("x-amz-object-lock-retain-until-date", "2099-01-01T00:00:00Z"),
        ("x-amz-object-lock-legal-hold", "ON"),
    ],
)
def test_create_multipart_upload_with_object_lock_headers_returns_not_implemented(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    header_name: str,
    header_value: str,
) -> None:
    """CreateMultipartUpload (POST ?uploads) carrying any x-amz-object-lock-* must 501."""
    bucket_name = unique_bucket_name("ol-cmu")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name)

    def _inject_header(request: AWSRequest, **_: Any) -> None:
        request.headers[header_name] = header_value

    boto3_client.meta.events.register("before-sign.s3.CreateMultipartUpload", _inject_header)
    try:
        with pytest.raises(ClientError) as excinfo:
            boto3_client.create_multipart_upload(Bucket=bucket_name, Key="cmu-locked")
    finally:
        boto3_client.meta.events.unregister("before-sign.s3.CreateMultipartUpload", _inject_header)

    status, code = _error(excinfo.value)
    assert status == 501
    assert code == "NotImplemented"


def test_delete_with_bypass_governance_header_is_noop(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """x-amz-bypass-governance-retention: true is meaningless without per-object locks
    (still Tier 2) but should not break a normal DELETE.
    """
    bucket_name = unique_bucket_name("ol-bypass")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name)
    boto3_client.put_object(Bucket=bucket_name, Key="k", Body=b"x")

    def _inject_header(request: AWSRequest, **_: Any) -> None:
        request.headers["x-amz-bypass-governance-retention"] = "true"

    boto3_client.meta.events.register("before-sign.s3.DeleteObject", _inject_header)
    try:
        resp = boto3_client.delete_object(Bucket=bucket_name, Key="k")
    finally:
        boto3_client.meta.events.unregister("before-sign.s3.DeleteObject", _inject_header)

    status = resp.get("ResponseMetadata", {}).get("HTTPStatusCode")
    assert status in (200, 204), f"unexpected status {status}"


# ---------------------------------------------------------------------------
# Tier 1 — bucket-level configuration round-trips
# ---------------------------------------------------------------------------


def test_put_then_get_object_lock_configuration_roundtrips(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("ol-roundtrip")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)

    boto3_client.put_object_lock_configuration(
        Bucket=bucket_name,
        ObjectLockConfiguration={
            "ObjectLockEnabled": "Enabled",
            "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": 30}},
        },
    )

    got = boto3_client.get_object_lock_configuration(Bucket=bucket_name)
    cfg = got["ObjectLockConfiguration"]
    assert cfg["ObjectLockEnabled"] == "Enabled"
    assert cfg["Rule"]["DefaultRetention"]["Mode"] == "GOVERNANCE"
    assert cfg["Rule"]["DefaultRetention"]["Days"] == 30


def test_put_compliance_days_roundtrips(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("ol-comp-days")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)

    boto3_client.put_object_lock_configuration(
        Bucket=bucket_name,
        ObjectLockConfiguration={
            "ObjectLockEnabled": "Enabled",
            "Rule": {"DefaultRetention": {"Mode": "COMPLIANCE", "Days": 10}},
        },
    )

    got = boto3_client.get_object_lock_configuration(Bucket=bucket_name)
    cfg = got["ObjectLockConfiguration"]
    assert cfg["Rule"]["DefaultRetention"]["Mode"] == "COMPLIANCE"
    assert cfg["Rule"]["DefaultRetention"]["Days"] == 10


def test_put_governance_years_roundtrips(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("ol-gov-years")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)

    boto3_client.put_object_lock_configuration(
        Bucket=bucket_name,
        ObjectLockConfiguration={
            "ObjectLockEnabled": "Enabled",
            "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Years": 1}},
        },
    )

    got = boto3_client.get_object_lock_configuration(Bucket=bucket_name)
    assert got["ObjectLockConfiguration"]["Rule"]["DefaultRetention"]["Years"] == 1


def test_put_replaces_previous_configuration(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("ol-replace")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)

    boto3_client.put_object_lock_configuration(
        Bucket=bucket_name,
        ObjectLockConfiguration={
            "ObjectLockEnabled": "Enabled",
            "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": 30}},
        },
    )
    boto3_client.put_object_lock_configuration(
        Bucket=bucket_name,
        ObjectLockConfiguration={
            "ObjectLockEnabled": "Enabled",
            "Rule": {"DefaultRetention": {"Mode": "COMPLIANCE", "Days": 7}},
        },
    )

    got = boto3_client.get_object_lock_configuration(Bucket=bucket_name)
    cfg = got["ObjectLockConfiguration"]
    assert cfg["Rule"]["DefaultRetention"]["Mode"] == "COMPLIANCE"
    assert cfg["Rule"]["DefaultRetention"]["Days"] == 7


def test_get_on_bucket_without_lock_enabled_returns_not_found(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """A bucket created WITHOUT ObjectLockEnabledForBucket=True returns
    ObjectLockConfigurationNotFoundError on GET."""
    bucket_name = unique_bucket_name("ol-noconf")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name)  # no lock-enabled header

    with pytest.raises(ClientError) as excinfo:
        boto3_client.get_object_lock_configuration(Bucket=bucket_name)
    _, code = _error(excinfo.value)
    assert code == "ObjectLockConfigurationNotFoundError"


def test_create_bucket_with_lock_enabled_then_get(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Bucket born with ObjectLockEnabledForBucket=True: GET returns Enabled, no Rule."""
    bucket_name = unique_bucket_name("ol-creat")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)

    got = boto3_client.get_object_lock_configuration(Bucket=bucket_name)
    cfg = got["ObjectLockConfiguration"]
    assert cfg["ObjectLockEnabled"] == "Enabled"
    assert "Rule" not in cfg


def test_put_on_missing_bucket_returns_no_such_bucket(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
) -> None:
    bucket_name = unique_bucket_name("ol-nobucket")

    with pytest.raises(ClientError) as excinfo:
        boto3_client.put_object_lock_configuration(
            Bucket=bucket_name,
            ObjectLockConfiguration={"ObjectLockEnabled": "Enabled"},
        )
    _, code = _error(excinfo.value)
    assert code == "NoSuchBucket"


@pytest.mark.parametrize(
    "bad_config",
    [
        {"ObjectLockEnabled": "Enabled", "Rule": {"DefaultRetention": {"Mode": "WHATEVER", "Days": 1}}},
        {"ObjectLockEnabled": "Enabled", "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": 1, "Years": 1}}},
        {"ObjectLockEnabled": "Enabled", "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE"}}},
        {"ObjectLockEnabled": "Enabled", "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": 0}}},
        {"ObjectLockEnabled": "Disabled", "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": 1}}},
    ],
    ids=["bad-mode", "days-and-years", "no-period", "zero-days", "disabled-not-allowed"],
)
def test_invalid_configuration_rejected(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    bad_config: dict[str, Any],
) -> None:
    bucket_name = unique_bucket_name("ol-bad")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)

    with pytest.raises(ClientError) as excinfo:
        boto3_client.put_object_lock_configuration(
            Bucket=bucket_name,
            ObjectLockConfiguration=bad_config,
        )
    status, _ = _error(excinfo.value)
    assert status == 400
