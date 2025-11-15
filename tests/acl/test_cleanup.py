"""
ACL Cleanup and Reset Tests

Tests for deleting resources with custom ACLs and resetting ACLs.
"""

import pytest
from botocore.exceptions import ClientError

from .helpers import assert_has_permission


pytestmark = pytest.mark.acl


class TestCleanup:
    """Test deletion of resources with custom ACLs."""

    def test_delete_bucket_with_custom_acl(self, s3_acc1_uploaddelete) -> None:
        """Test that bucket with custom ACL can be deleted."""
        import uuid

        bucket = f"acl-test-delete-{uuid.uuid4().hex[:8]}"

        s3_acc1_uploaddelete.create_bucket(Bucket=bucket)
        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, ACL="public-read")

        acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)
        assert len(acl["Grants"]) >= 2, "public-read should have multiple grants"

        response = s3_acc1_uploaddelete.delete_bucket(Bucket=bucket)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

    def test_delete_object_with_custom_acl(self, s3_acc1_uploaddelete, clean_bucket) -> None:
        """Test that object with custom ACL can be deleted."""
        bucket = clean_bucket
        key = "test-delete.txt"

        s3_acc1_uploaddelete.put_object(Bucket=bucket, Key=key, Body=b"test")
        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, ACL="public-read")

        s3_acc1_uploaddelete.delete_object(Bucket=bucket, Key=key)

        with pytest.raises(ClientError, match="NoSuchKey"):
            s3_acc1_uploaddelete.get_object_acl(Bucket=bucket, Key=key)

    def test_reset_acl_to_default_private(self, s3_acc1_uploaddelete, test_object, canonical_ids) -> None:
        """Test that ACL can be reset from public back to private."""
        bucket, key = test_object

        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, ACL="public-read")

        acl = s3_acc1_uploaddelete.get_object_acl(Bucket=bucket, Key=key)
        grants = acl["Grants"]
        group_grants = [g for g in grants if g["Grantee"].get("Type") == "Group"]
        assert len(group_grants) >= 1, "public-read should have group grants"

        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, ACL="private")

        acl = s3_acc1_uploaddelete.get_object_acl(Bucket=bucket, Key=key)
        grants = acl["Grants"]

        assert len(grants) == 1, "private ACL should have only owner grant"
        assert_has_permission(grants, canonical_ids["acc1"], "FULL_CONTROL")

        group_grants = [g for g in grants if g["Grantee"].get("Type") == "Group"]
        assert len(group_grants) == 0, "private ACL should have no group grants"
