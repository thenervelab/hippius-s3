"""
ACL Grant Headers Tests

Tests for grant header functionality (Categories 4-5 from bash script).
Tests using --grant-read, --grant-write, --grant-read-acp, --grant-write-acp,
and --grant-full-control headers instead of canned ACLs.

NOTE: These tests may reveal the known bug where object grant headers don't work.
"""

import pytest

from .helpers import assert_has_permission


pytestmark = pytest.mark.acl


class TestBucketGrantHeaders:
    """Test bucket ACL grant header functionality."""

    def test_bucket_grant_read(self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, clean_bucket, canonical_ids) -> None:
        """Test granting READ permission via header."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, GrantRead=f'id="{canonical_ids["acc2"]}"')

        acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)
        grants = acl["Grants"]

        assert_has_permission(grants, canonical_ids["acc2"], "READ")

        response = s3_acc2_uploaddelete.list_objects_v2(Bucket=bucket)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_bucket_grant_write(self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, clean_bucket, canonical_ids) -> None:
        """Test granting WRITE permission via header."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, GrantWrite=f'id="{canonical_ids["acc2"]}"')

        acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)
        grants = acl["Grants"]

        assert_has_permission(grants, canonical_ids["acc2"], "WRITE")

        s3_acc2_uploaddelete.put_object(Bucket=bucket, Key="test-write.txt", Body=b"test")

    def test_bucket_grant_read_acp(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, clean_bucket, canonical_ids
    ) -> None:
        """Test granting READ_ACP permission via header."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, GrantReadACP=f'id="{canonical_ids["acc2"]}"')

        acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)
        grants = acl["Grants"]

        assert_has_permission(grants, canonical_ids["acc2"], "READ_ACP")

        response = s3_acc2_uploaddelete.get_bucket_acl(Bucket=bucket)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_bucket_grant_write_acp(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, clean_bucket, canonical_ids
    ) -> None:
        """Test granting WRITE_ACP permission via header."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, GrantWriteACP=f'id="{canonical_ids["acc2"]}"')

        acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)
        grants = acl["Grants"]

        assert_has_permission(grants, canonical_ids["acc2"], "WRITE_ACP")

        s3_acc2_uploaddelete.put_bucket_acl(Bucket=bucket, GrantWriteACP=f'id="{canonical_ids["acc2"]}"')

    def test_bucket_grant_full_control(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, clean_bucket, canonical_ids
    ) -> None:
        """Test granting FULL_CONTROL permission via header."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, GrantFullControl=f'id="{canonical_ids["acc2"]}"')

        acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)
        grants = acl["Grants"]

        assert_has_permission(grants, canonical_ids["acc2"], "FULL_CONTROL")

        response = s3_acc2_uploaddelete.list_objects_v2(Bucket=bucket)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        s3_acc2_uploaddelete.put_object(Bucket=bucket, Key="test.txt", Body=b"test")

        response = s3_acc2_uploaddelete.get_bucket_acl(Bucket=bucket)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        s3_acc2_uploaddelete.put_bucket_acl(Bucket=bucket, GrantFullControl=f'id="{canonical_ids["acc2"]}"')

    def test_bucket_multiple_grants(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, clean_bucket, canonical_ids
    ) -> None:
        """Test granting multiple permissions in single request."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(
            Bucket=bucket, GrantRead=f'id="{canonical_ids["acc2"]}"', GrantWrite=f'id="{canonical_ids["acc2"]}"'
        )

        acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)
        grants = acl["Grants"]

        assert_has_permission(grants, canonical_ids["acc2"], "READ")
        assert_has_permission(grants, canonical_ids["acc2"], "WRITE")

        response = s3_acc2_uploaddelete.list_objects_v2(Bucket=bucket)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        s3_acc2_uploaddelete.put_object(Bucket=bucket, Key="test.txt", Body=b"test")

    def test_bucket_grant_overwrites_previous(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, clean_bucket, canonical_ids
    ):
        """Test that grant headers overwrite previous ACL."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, GrantRead=f'id="{canonical_ids["acc2"]}"')

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, ACL="private")

        acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)
        grants = acl["Grants"]

        acc2_grants = [g for g in grants if g["Grantee"].get("ID") == canonical_ids["acc2"]]
        assert len(acc2_grants) == 0, "ACC2 should not have any grants after reset to private"


class TestObjectGrantHeaders:
    """Test object ACL grant header functionality."""

    def test_object_grant_read(self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, test_object, canonical_ids) -> None:
        """Test granting READ permission via header."""
        bucket, key = test_object

        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, GrantRead=f'id="{canonical_ids["acc2"]}"')

        acl = s3_acc1_uploaddelete.get_object_acl(Bucket=bucket, Key=key)
        grants = acl["Grants"]

        assert_has_permission(grants, canonical_ids["acc2"], "READ")

        response = s3_acc2_uploaddelete.get_object(Bucket=bucket, Key=key)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_object_grant_read_acp(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, test_object, canonical_ids
    ) -> None:
        """Test granting READ_ACP permission via header."""
        bucket, key = test_object

        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, GrantReadACP=f'id="{canonical_ids["acc2"]}"')

        acl = s3_acc1_uploaddelete.get_object_acl(Bucket=bucket, Key=key)
        grants = acl["Grants"]

        assert_has_permission(grants, canonical_ids["acc2"], "READ_ACP")

        response = s3_acc2_uploaddelete.get_object_acl(Bucket=bucket, Key=key)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_object_grant_write_acp(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, test_object, canonical_ids
    ) -> None:
        """Test granting WRITE_ACP permission via header."""
        bucket, key = test_object

        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, GrantWriteACP=f'id="{canonical_ids["acc2"]}"')

        acl = s3_acc1_uploaddelete.get_object_acl(Bucket=bucket, Key=key)
        grants = acl["Grants"]

        assert_has_permission(grants, canonical_ids["acc2"], "WRITE_ACP")

        s3_acc2_uploaddelete.put_object_acl(Bucket=bucket, Key=key, GrantWriteACP=f'id="{canonical_ids["acc2"]}"')

    def test_object_grant_full_control(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, test_object, canonical_ids
    ) -> None:
        """Test granting FULL_CONTROL permission via header."""
        bucket, key = test_object

        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, GrantFullControl=f'id="{canonical_ids["acc2"]}"')

        acl = s3_acc1_uploaddelete.get_object_acl(Bucket=bucket, Key=key)
        grants = acl["Grants"]

        assert_has_permission(grants, canonical_ids["acc2"], "FULL_CONTROL")

        response = s3_acc2_uploaddelete.get_object(Bucket=bucket, Key=key)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        response = s3_acc2_uploaddelete.head_object(Bucket=bucket, Key=key)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        response = s3_acc2_uploaddelete.get_object_acl(Bucket=bucket, Key=key)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        s3_acc2_uploaddelete.put_object_acl(Bucket=bucket, Key=key, GrantFullControl=f'id="{canonical_ids["acc2"]}"')

    def test_object_multiple_grants(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, test_object, canonical_ids
    ) -> None:
        """Test granting multiple permissions in single request."""
        bucket, key = test_object

        s3_acc1_uploaddelete.put_object_acl(
            Bucket=bucket,
            Key=key,
            GrantRead=f'id="{canonical_ids["acc2"]}"',
            GrantReadACP=f'id="{canonical_ids["acc2"]}"',
        )

        acl = s3_acc1_uploaddelete.get_object_acl(Bucket=bucket, Key=key)
        grants = acl["Grants"]

        assert_has_permission(grants, canonical_ids["acc2"], "READ")
        assert_has_permission(grants, canonical_ids["acc2"], "READ_ACP")

        response = s3_acc2_uploaddelete.get_object(Bucket=bucket, Key=key)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        response = s3_acc2_uploaddelete.get_object_acl(Bucket=bucket, Key=key)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_object_grant_overwrites_previous(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, test_object, canonical_ids
    ):
        """Test that grant headers overwrite previous ACL."""
        bucket, key = test_object

        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, GrantRead=f'id="{canonical_ids["acc2"]}"')

        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, ACL="private")

        acl = s3_acc1_uploaddelete.get_object_acl(Bucket=bucket, Key=key)
        grants = acl["Grants"]

        acc2_grants = [g for g in grants if g["Grantee"].get("ID") == canonical_ids["acc2"]]
        assert len(acc2_grants) == 0, "ACC2 should not have any grants after reset to private"

    @pytest.mark.xfail(reason="Grant headers during PutObject may not be fully implemented")
    def test_object_grant_during_upload(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, clean_bucket, canonical_ids
    ) -> None:
        """Test granting permissions during object upload."""
        bucket = clean_bucket
        key = "test-upload-with-grant.txt"

        s3_acc1_uploaddelete.put_object(
            Bucket=bucket, Key=key, Body=b"test content", GrantRead=f'id="{canonical_ids["acc2"]}"'
        )

        acl = s3_acc1_uploaddelete.get_object_acl(Bucket=bucket, Key=key)
        grants = acl["Grants"]

        assert_has_permission(grants, canonical_ids["acc2"], "READ")

        response = s3_acc2_uploaddelete.get_object(Bucket=bucket, Key=key)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
