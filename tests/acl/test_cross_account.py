"""
Cross-Account ACL Tests

Tests for cross-account scenarios like bucket-owner-read and bucket-owner-full-control.
These tests require actual separate AWS accounts and are skipped for now.
"""

import pytest


pytestmark = [pytest.mark.acl, pytest.mark.skip(reason="Requires actual cross-account setup")]


class TestCrossAccountObjectOwnership:
    """Test object ownership in cross-account scenarios."""

    def test_cross_account_upload_with_bucket_owner_read(self) -> None:
        """Test bucket-owner-read ACL in cross-account upload."""
        pytest.skip("Requires separate AWS account for testing")

    def test_cross_account_upload_with_bucket_owner_full_control(self) -> None:
        """Test bucket-owner-full-control ACL in cross-account upload."""
        pytest.skip("Requires separate AWS account for testing")

    def test_cross_account_object_ownership(self) -> None:
        """Test that uploader owns object in cross-account scenario."""
        pytest.skip("Requires separate AWS account for testing")


class TestCrossAccountAccess:
    """Test cross-account access control."""

    def test_bucket_owner_cannot_read_without_grant(self) -> None:
        """Test that bucket owner cannot read object without grant."""
        pytest.skip("Requires separate AWS account for testing")

    def test_grant_cross_account_then_access(self) -> None:
        """Test granting cross-account access and verifying it works."""
        pytest.skip("Requires separate AWS account for testing")

    def test_revoke_cross_account_then_deny(self) -> None:
        """Test revoking cross-account access and verifying denial."""
        pytest.skip("Requires separate AWS account for testing")
