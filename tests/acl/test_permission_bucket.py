"""
Bucket Permission Enforcement Tests

Tests that bucket ACL permissions actually enforce access control.
Tests READ, WRITE, READ_ACP, WRITE_ACP, and FULL_CONTROL permissions.
"""

import pytest
from botocore.exceptions import ClientError


pytestmark = pytest.mark.acl


class TestBucketReadPermission:
    """Test READ permission on buckets."""

    def test_bucket_read_allows_list_objects(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, clean_bucket, canonical_ids
    ):
        """Test that READ permission allows listing objects."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_object(Bucket=bucket, Key="test.txt", Body=b"test")

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, GrantRead=f'id="{canonical_ids["acc2"]}"')

        response = s3_acc2_uploaddelete.list_objects_v2(Bucket=bucket)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_bucket_read_allows_list_objects_v2(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, clean_bucket, canonical_ids
    ):
        """Test that READ permission allows listing objects with v2 API."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, GrantRead=f'id="{canonical_ids["acc2"]}"')

        response = s3_acc2_uploaddelete.list_objects_v2(Bucket=bucket)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_bucket_read_denies_other_operations(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, clean_bucket, canonical_ids
    ):
        """Test that READ permission does not allow write operations."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, GrantRead=f'id="{canonical_ids["acc2"]}"')

        with pytest.raises(ClientError, match="AccessDenied"):
            s3_acc2_uploaddelete.put_object(Bucket=bucket, Key="test.txt", Body=b"test")


class TestBucketWritePermission:
    """Test WRITE permission on buckets."""

    @pytest.mark.xfail(reason="Bucket WRITE permission may not grant PutObject access - needs investigation")
    def test_bucket_write_allows_put_object(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, clean_bucket, canonical_ids
    ):
        """Test that WRITE permission allows uploading objects."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, GrantWrite=f'id="{canonical_ids["acc2"]}"')

        s3_acc2_uploaddelete.put_object(Bucket=bucket, Key="test.txt", Body=b"test content")

    def test_bucket_write_allows_delete_object(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, clean_bucket, canonical_ids
    ):
        """Test that WRITE permission allows deleting objects."""
        bucket = clean_bucket
        key = "test-delete.txt"

        s3_acc1_uploaddelete.put_object(Bucket=bucket, Key=key, Body=b"test")

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, GrantWrite=f'id="{canonical_ids["acc2"]}"')

        s3_acc2_uploaddelete.delete_object(Bucket=bucket, Key=key)

    def test_bucket_write_allows_initiate_multipart(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, clean_bucket, canonical_ids
    ):
        """Test that WRITE permission allows initiating multipart uploads."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, GrantWrite=f'id="{canonical_ids["acc2"]}"')

        response = s3_acc2_uploaddelete.create_multipart_upload(Bucket=bucket, Key="multipart.txt")
        assert "UploadId" in response

        s3_acc2_uploaddelete.abort_multipart_upload(Bucket=bucket, Key="multipart.txt", UploadId=response["UploadId"])

    def test_bucket_write_denies_list(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, clean_bucket, canonical_ids
    ) -> None:
        """Test that WRITE permission does not allow listing objects."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, GrantWrite=f'id="{canonical_ids["acc2"]}"')

        with pytest.raises(ClientError, match="AccessDenied"):
            s3_acc2_uploaddelete.list_objects_v2(Bucket=bucket)


class TestBucketReadACPPermission:
    """Test READ_ACP permission on buckets."""

    def test_bucket_read_acp_allows_get_bucket_acl(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, clean_bucket, canonical_ids
    ):
        """Test that READ_ACP permission allows reading bucket ACL."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, GrantReadACP=f'id="{canonical_ids["acc2"]}"')

        response = s3_acc2_uploaddelete.get_bucket_acl(Bucket=bucket)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Owner" in response
        assert "Grants" in response

    def test_bucket_read_acp_denies_put_bucket_acl(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, clean_bucket, canonical_ids
    ):
        """Test that READ_ACP permission does not allow modifying ACL."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, GrantReadACP=f'id="{canonical_ids["acc2"]}"')

        with pytest.raises(ClientError, match="AccessDenied"):
            s3_acc2_uploaddelete.put_bucket_acl(Bucket=bucket, ACL="private")


class TestBucketWriteACPPermission:
    """Test WRITE_ACP permission on buckets."""

    def test_bucket_write_acp_allows_put_bucket_acl(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, clean_bucket, canonical_ids
    ):
        """Test that WRITE_ACP permission allows modifying bucket ACL."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, GrantWriteACP=f'id="{canonical_ids["acc2"]}"')

        s3_acc2_uploaddelete.put_bucket_acl(Bucket=bucket, GrantWriteACP=f'id="{canonical_ids["acc2"]}"')

    @pytest.mark.xfail(reason="WRITE_ACP may not implicitly grant READ_ACP - needs spec verification")
    def test_bucket_write_acp_allows_get_bucket_acl(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, clean_bucket, canonical_ids
    ):
        """Test that WRITE_ACP permission also allows reading bucket ACL."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, GrantWriteACP=f'id="{canonical_ids["acc2"]}"')

        response = s3_acc2_uploaddelete.get_bucket_acl(Bucket=bucket)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestBucketFullControlPermission:
    """Test FULL_CONTROL permission on buckets."""

    def test_bucket_full_control_allows_all_operations(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, clean_bucket, canonical_ids
    ):
        """Test that FULL_CONTROL grants all permissions."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, GrantFullControl=f'id="{canonical_ids["acc2"]}"')

        response = s3_acc2_uploaddelete.list_objects_v2(Bucket=bucket)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        s3_acc2_uploaddelete.put_object(Bucket=bucket, Key="test.txt", Body=b"test")

        response = s3_acc2_uploaddelete.get_bucket_acl(Bucket=bucket)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        s3_acc2_uploaddelete.put_bucket_acl(Bucket=bucket, GrantFullControl=f'id="{canonical_ids["acc2"]}"')

        s3_acc2_uploaddelete.delete_object(Bucket=bucket, Key="test.txt")
