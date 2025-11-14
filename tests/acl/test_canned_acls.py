"""
ACL Canned ACL Tests

Tests for both bucket and object canned ACLs (Categories 2-3 from bash script).
Each test verifies that the appropriate grants are created for the canned ACL.
"""

import pytest

from .helpers import assert_has_group_permission
from .helpers import assert_has_permission


pytestmark = pytest.mark.acl


class TestBucketCannedACLs:
    """Test bucket canned ACL functionality."""

    def test_bucket_private_acl(self, s3_acc1_uploaddelete, clean_bucket, canonical_ids) -> None:
        """Test that private (default) bucket ACL only grants owner FULL_CONTROL."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, ACL="private")

        acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)
        grants = acl["Grants"]

        assert len(grants) == 1, f"Expected 1 grant for private ACL, got {len(grants)}"
        assert_has_permission(grants, canonical_ids["acc1"], "FULL_CONTROL")

    def test_bucket_public_read_acl(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, clean_bucket, canonical_ids
    ) -> None:
        """Test that public-read bucket ACL grants AllUsers READ permission."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, ACL="public-read")

        acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)
        grants = acl["Grants"]

        assert_has_permission(grants, canonical_ids["acc1"], "FULL_CONTROL")
        assert_has_group_permission(grants, "http://acs.amazonaws.com/groups/global/AllUsers", "READ")

        response = s3_acc2_uploaddelete.list_objects_v2(Bucket=bucket)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_bucket_public_read_write_acl(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, clean_bucket, canonical_ids
    ):
        """Test that public-read-write bucket ACL grants AllUsers READ and WRITE."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, ACL="public-read-write")

        acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)
        grants = acl["Grants"]

        assert_has_permission(grants, canonical_ids["acc1"], "FULL_CONTROL")
        assert_has_group_permission(grants, "http://acs.amazonaws.com/groups/global/AllUsers", "READ")
        assert_has_group_permission(grants, "http://acs.amazonaws.com/groups/global/AllUsers", "WRITE")

        response = s3_acc2_uploaddelete.list_objects_v2(Bucket=bucket)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        s3_acc2_uploaddelete.put_object(Bucket=bucket, Key="test-write.txt", Body=b"test")

    def test_bucket_authenticated_read_acl(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, clean_bucket, canonical_ids
    ):
        """Test that authenticated-read bucket ACL grants AuthenticatedUsers READ."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, ACL="authenticated-read")

        acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)
        grants = acl["Grants"]

        assert_has_permission(grants, canonical_ids["acc1"], "FULL_CONTROL")
        assert_has_group_permission(grants, "http://acs.amazonaws.com/groups/global/AuthenticatedUsers", "READ")

        response = s3_acc2_uploaddelete.list_objects_v2(Bucket=bucket)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_bucket_log_delivery_write_acl(self, s3_acc1_uploaddelete, clean_bucket, canonical_ids) -> None:
        """Test that log-delivery-write bucket ACL grants LogDelivery group permissions."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, ACL="log-delivery-write")

        acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)
        grants = acl["Grants"]

        assert_has_permission(grants, canonical_ids["acc1"], "FULL_CONTROL")
        assert_has_group_permission(grants, "http://acs.amazonaws.com/groups/s3/LogDelivery", "WRITE")
        assert_has_group_permission(grants, "http://acs.amazonaws.com/groups/s3/LogDelivery", "READ_ACP")

    def test_bucket_acl_set_during_creation(self, s3_acc1_uploaddelete) -> None:
        """Test that canned ACL can be set during bucket creation."""
        import uuid

        bucket = f"acl-test-create-{uuid.uuid4().hex[:8]}"

        s3_acc1_uploaddelete.create_bucket(Bucket=bucket, ACL="public-read")

        acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)
        grants = acl["Grants"]

        assert_has_group_permission(grants, "http://acs.amazonaws.com/groups/global/AllUsers", "READ")

        s3_acc1_uploaddelete.delete_bucket(Bucket=bucket)

    def test_bucket_acl_overwrite_previous(self, s3_acc1_uploaddelete, clean_bucket, canonical_ids) -> None:
        """Test that setting new ACL overwrites previous ACL."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, ACL="public-read")
        acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)
        grants = acl["Grants"]
        assert_has_group_permission(grants, "http://acs.amazonaws.com/groups/global/AllUsers", "READ")

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, ACL="private")
        acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)
        grants = acl["Grants"]

        assert len(grants) == 1, "Expected only owner grant after setting private ACL"
        assert_has_permission(grants, canonical_ids["acc1"], "FULL_CONTROL")

    def test_bucket_get_acl_returns_correct_structure(self, s3_acc1_uploaddelete, clean_bucket) -> None:
        """Test that get_bucket_acl returns correctly structured response."""
        bucket = clean_bucket

        acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)

        assert "Owner" in acl
        assert "ID" in acl["Owner"]
        assert "Grants" in acl
        assert isinstance(acl["Grants"], list)


class TestObjectCannedACLs:
    """Test object canned ACL functionality."""

    def test_object_private_acl(self, s3_acc1_uploaddelete, test_object, canonical_ids) -> None:
        """Test that private (default) object ACL only grants owner FULL_CONTROL."""
        bucket, key = test_object

        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, ACL="private")

        acl = s3_acc1_uploaddelete.get_object_acl(Bucket=bucket, Key=key)
        grants = acl["Grants"]

        assert len(grants) == 1, f"Expected 1 grant for private ACL, got {len(grants)}"
        assert_has_permission(grants, canonical_ids["acc1"], "FULL_CONTROL")

    def test_object_public_read_acl(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, test_object, canonical_ids
    ) -> None:
        """Test that public-read object ACL grants AllUsers READ permission."""
        bucket, key = test_object

        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, ACL="public-read")

        acl = s3_acc1_uploaddelete.get_object_acl(Bucket=bucket, Key=key)
        grants = acl["Grants"]

        assert_has_permission(grants, canonical_ids["acc1"], "FULL_CONTROL")
        assert_has_group_permission(grants, "http://acs.amazonaws.com/groups/global/AllUsers", "READ")

        response = s3_acc2_uploaddelete.get_object(Bucket=bucket, Key=key)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_object_public_read_write_acl(self, s3_acc1_uploaddelete, test_object, canonical_ids) -> None:
        """Test that public-read-write object ACL grants AllUsers READ."""
        bucket, key = test_object

        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, ACL="public-read-write")

        acl = s3_acc1_uploaddelete.get_object_acl(Bucket=bucket, Key=key)
        grants = acl["Grants"]

        assert_has_permission(grants, canonical_ids["acc1"], "FULL_CONTROL")
        assert_has_group_permission(grants, "http://acs.amazonaws.com/groups/global/AllUsers", "READ")

    def test_object_authenticated_read_acl(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, test_object, canonical_ids
    ):
        """Test that authenticated-read object ACL grants AuthenticatedUsers READ."""
        bucket, key = test_object

        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, ACL="authenticated-read")

        acl = s3_acc1_uploaddelete.get_object_acl(Bucket=bucket, Key=key)
        grants = acl["Grants"]

        assert_has_permission(grants, canonical_ids["acc1"], "FULL_CONTROL")
        assert_has_group_permission(grants, "http://acs.amazonaws.com/groups/global/AuthenticatedUsers", "READ")

        response = s3_acc2_uploaddelete.get_object(Bucket=bucket, Key=key)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_object_bucket_owner_read_acl(self, s3_acc1_uploaddelete, test_object, canonical_ids) -> None:
        """Test that bucket-owner-read ACL works (single account scenario)."""
        bucket, key = test_object

        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, ACL="bucket-owner-read")

        acl = s3_acc1_uploaddelete.get_object_acl(Bucket=bucket, Key=key)
        grants = acl["Grants"]

        assert_has_permission(grants, canonical_ids["acc1"], "FULL_CONTROL")

    def test_object_bucket_owner_full_control_acl(self, s3_acc1_uploaddelete, test_object, canonical_ids) -> None:
        """Test that bucket-owner-full-control ACL works (single account scenario)."""
        bucket, key = test_object

        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, ACL="bucket-owner-full-control")

        acl = s3_acc1_uploaddelete.get_object_acl(Bucket=bucket, Key=key)
        grants = acl["Grants"]

        assert_has_permission(grants, canonical_ids["acc1"], "FULL_CONTROL")

    def test_object_acl_set_during_upload(self, s3_acc1_uploaddelete, clean_bucket) -> None:
        """Test that canned ACL can be set during object upload."""
        bucket = clean_bucket
        key = "test-upload-with-acl.txt"

        s3_acc1_uploaddelete.put_object(Bucket=bucket, Key=key, Body=b"test content", ACL="public-read")

        acl = s3_acc1_uploaddelete.get_object_acl(Bucket=bucket, Key=key)
        grants = acl["Grants"]

        assert_has_group_permission(grants, "http://acs.amazonaws.com/groups/global/AllUsers", "READ")

    def test_object_acl_overwrite_previous(self, s3_acc1_uploaddelete, test_object, canonical_ids) -> None:
        """Test that setting new object ACL overwrites previous ACL."""
        bucket, key = test_object

        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, ACL="public-read")
        acl = s3_acc1_uploaddelete.get_object_acl(Bucket=bucket, Key=key)
        grants = acl["Grants"]
        assert_has_group_permission(grants, "http://acs.amazonaws.com/groups/global/AllUsers", "READ")

        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, ACL="private")
        acl = s3_acc1_uploaddelete.get_object_acl(Bucket=bucket, Key=key)
        grants = acl["Grants"]

        assert len(grants) == 1, "Expected only owner grant after setting private ACL"
        assert_has_permission(grants, canonical_ids["acc1"], "FULL_CONTROL")

    def test_object_get_acl_returns_correct_structure(self, s3_acc1_uploaddelete, test_object) -> None:
        """Test that get_object_acl returns correctly structured response."""
        bucket, key = test_object

        acl = s3_acc1_uploaddelete.get_object_acl(Bucket=bucket, Key=key)

        assert "Owner" in acl
        assert "ID" in acl["Owner"]
        assert "Grants" in acl
        assert isinstance(acl["Grants"], list)
