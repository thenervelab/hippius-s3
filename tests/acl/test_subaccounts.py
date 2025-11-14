"""
Subaccount Permission Enforcement Tests

Tests that subaccounts can perform ACL operations based on their permissions.
Upload-only subaccounts should be able to read ACLs but not write them.
UploadDelete subaccounts should be able to both read and write ACLs.
"""

import pytest
from botocore.exceptions import ClientError


pytestmark = pytest.mark.acl


class TestSubaccountBucketACL:
    """Test subaccount access to bucket ACLs."""

    def test_upload_subaccount_can_read_bucket_acl(self, s3_acc1_upload, clean_bucket) -> None:
        """Test that upload-only subaccount can read bucket ACL."""
        bucket = clean_bucket

        response = s3_acc1_upload.get_bucket_acl(Bucket=bucket)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Owner" in response
        assert "Grants" in response

    def test_upload_subaccount_cannot_write_bucket_acl(self, s3_acc1_upload, clean_bucket) -> None:
        """Test that upload-only subaccount cannot write bucket ACL."""
        bucket = clean_bucket

        with pytest.raises(ClientError, match="AccessDenied"):
            s3_acc1_upload.put_bucket_acl(Bucket=bucket, ACL="public-read")

    def test_uploaddelete_subaccount_can_write_bucket_acl(self, s3_acc1_uploaddelete, clean_bucket) -> None:
        """Test that upload/delete subaccount can write bucket ACL."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, ACL="private")

        response = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSubaccountObjectACL:
    """Test subaccount access to object ACLs."""

    def test_upload_subaccount_can_read_object_acl(self, s3_acc1_upload, s3_acc1_uploaddelete, test_object) -> None:
        """Test that upload-only subaccount can read object ACL."""
        bucket, key = test_object

        response = s3_acc1_uploaddelete.get_object_acl(Bucket=bucket, Key=key)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_upload_subaccount_cannot_write_object_acl(self, s3_acc1_upload, test_object) -> None:
        """Test that upload-only subaccount cannot write object ACL."""
        bucket, key = test_object

        with pytest.raises(ClientError, match="AccessDenied"):
            s3_acc1_upload.put_object_acl(Bucket=bucket, Key=key, ACL="public-read")

    def test_uploaddelete_subaccount_can_write_object_acl(self, s3_acc1_uploaddelete, test_object) -> None:
        """Test that upload/delete subaccount can write object ACL."""
        bucket, key = test_object

        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, ACL="private")

        response = s3_acc1_uploaddelete.get_object_acl(Bucket=bucket, Key=key)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    @pytest.mark.skip(reason="Requires chain permission verification")
    def test_upload_subaccount_can_upload_with_acl_header(self, s3_acc1_upload, clean_bucket) -> None:
        """Test that upload subaccount can upload with ACL header."""
        bucket = clean_bucket

        s3_acc1_upload.put_object(Bucket=bucket, Key="test.txt", Body=b"test", ACL="private")

    @pytest.mark.skip(reason="Requires chain permission verification")
    def test_upload_subaccount_cannot_modify_existing_acl(self, s3_acc1_upload, test_object) -> None:
        """Test that upload subaccount cannot modify existing object ACL."""
        bucket, key = test_object

        with pytest.raises(ClientError, match="AccessDenied"):
            s3_acc1_upload.put_object_acl(Bucket=bucket, Key=key, ACL="public-read")
