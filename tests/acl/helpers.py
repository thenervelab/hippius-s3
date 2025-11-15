"""Helper functions for ACL tests."""

from typing import Any

import pytest
from botocore.exceptions import ClientError


def get_canonical_id(s3_client: Any, bucket: str) -> str:
    """Get canonical user ID from bucket ACL."""
    acl = s3_client.get_bucket_acl(Bucket=bucket)
    return str(acl["Owner"]["ID"])


def assert_has_permission(grants: list, grantee_id: str, permission: str) -> None:
    """Assert that grants list contains specified permission for grantee."""
    matching_grant = next(
        (
            g
            for g in grants
            if g["Grantee"].get("Type") == "CanonicalUser"
            and g["Grantee"].get("ID") == grantee_id
            and g["Permission"] == permission
        ),
        None,
    )

    assert matching_grant is not None, (
        f"Expected grant with grantee={grantee_id}, permission={permission} not found. Available grants: {grants}"
    )


def assert_has_group_permission(grants: list, group_uri: str, permission: str) -> None:
    """Assert that grants list contains specified permission for group."""
    matching_grant = next(
        (
            g
            for g in grants
            if g["Grantee"].get("Type") == "Group"
            and group_uri in g["Grantee"].get("URI", "")
            and g["Permission"] == permission
        ),
        None,
    )

    assert matching_grant is not None, (
        f"Expected grant with group={group_uri}, permission={permission} not found. Available grants: {grants}"
    )


def create_test_content(size_bytes: int = 1024) -> bytes:
    """Create test content of specified size."""
    return b"X" * size_bytes


def expect_access_denied() -> Any:
    """Context manager to assert ClientError with AccessDenied code."""
    return pytest.raises(ClientError, match="AccessDenied")


def expect_error_code(error_code: str) -> Any:
    """Context manager to assert ClientError with specific error code."""
    return pytest.raises(ClientError, match=error_code)
