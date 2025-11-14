"""
Error Case Tests

Tests for error handling and validation.
Verifies that invalid ACL operations return appropriate error codes.
"""

import pytest
from botocore.exceptions import ClientError


pytestmark = pytest.mark.acl


class TestInvalidArguments:
    """Test error handling for invalid arguments."""

    def test_error_mix_canned_acl_with_grant_headers(self, s3_acc1_uploaddelete, clean_bucket, canonical_ids) -> None:
        """Test that mixing canned ACL with grant headers returns error."""
        bucket = clean_bucket

        with pytest.raises(ClientError, match="InvalidRequest"):
            s3_acc1_uploaddelete.put_bucket_acl(
                Bucket=bucket, ACL="public-read", GrantRead=f'id="{canonical_ids["acc2"]}"'
            )

    def test_error_invalid_canned_acl_name(self, s3_acc1_uploaddelete, clean_bucket) -> None:
        """Test that invalid canned ACL name returns error."""
        bucket = clean_bucket

        with pytest.raises(ClientError, match="InvalidArgument"):
            s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, ACL="invalid-acl-name")

    def test_error_object_mix_canned_acl_with_grant_headers(
        self, s3_acc1_uploaddelete, test_object, canonical_ids
    ) -> None:
        """Test that mixing canned ACL with grant headers for objects returns error."""
        bucket, key = test_object

        with pytest.raises(ClientError, match="InvalidRequest"):
            s3_acc1_uploaddelete.put_object_acl(
                Bucket=bucket, Key=key, ACL="public-read", GrantRead=f'id="{canonical_ids["acc2"]}"'
            )

    def test_error_object_invalid_canned_acl_name(self, s3_acc1_uploaddelete, test_object) -> None:
        """Test that invalid canned ACL name for objects returns error."""
        bucket, key = test_object

        with pytest.raises(ClientError, match="InvalidArgument"):
            s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, ACL="invalid-acl")


class TestAccessDenied:
    """Test access denied scenarios."""

    def test_cannot_set_acl_without_permission(self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, clean_bucket) -> None:
        """Test that non-owner cannot set bucket ACL."""
        bucket = clean_bucket

        with pytest.raises(ClientError, match="AccessDenied"):
            s3_acc2_uploaddelete.put_bucket_acl(Bucket=bucket, ACL="public-read")

    def test_cannot_set_object_acl_without_permission(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, test_object
    ) -> None:
        """Test that non-owner cannot set object ACL."""
        bucket, key = test_object

        with pytest.raises(ClientError, match="AccessDenied"):
            s3_acc2_uploaddelete.put_object_acl(Bucket=bucket, Key=key, ACL="public-read")
