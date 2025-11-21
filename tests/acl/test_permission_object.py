"""
Object Permission Enforcement Tests

Tests that object ACL permissions actually enforce access control.
Tests READ, READ_ACP, WRITE_ACP, and FULL_CONTROL permissions.
Note: WRITE permission does not apply to objects (objects are immutable).
"""

import pytest
from botocore.exceptions import ClientError


pytestmark = pytest.mark.acl


class TestObjectReadPermission:
    """Test READ permission on objects."""

    @pytest.mark.skipif(
        "config.getoption('--r2')",
        reason="PutObjectAcl/GetObjectAcl not implemented in R2. See: https://developers.cloudflare.com/r2/api/s3/api/"
    )
    def test_object_read_allows_get_object(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, test_object, canonical_ids
    ):
        """Test that READ permission allows getting object."""
        bucket, key = test_object

        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, GrantRead=f'id="{canonical_ids["acc2"]}"')

        response = s3_acc2_uploaddelete.get_object(Bucket=bucket, Key=key)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert response["Body"].read() == b"This is test content for ACL testing"

    @pytest.mark.skipif(
        "config.getoption('--r2')",
        reason="PutObjectAcl/GetObjectAcl not implemented in R2. See: https://developers.cloudflare.com/r2/api/s3/api/"
    )
    def test_object_read_allows_head_object(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, test_object, canonical_ids
    ):
        """Test that READ permission allows head object."""
        bucket, key = test_object

        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, GrantRead=f'id="{canonical_ids["acc2"]}"')

        response = s3_acc2_uploaddelete.head_object(Bucket=bucket, Key=key)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    @pytest.mark.skipif(
        "config.getoption('--r2')",
        reason="PutObjectAcl/GetObjectAcl not implemented in R2. See: https://developers.cloudflare.com/r2/api/s3/api/"
    )
    def test_object_read_denies_delete(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, test_object, canonical_ids
    ) -> None:
        """Test that READ permission does not allow deleting object."""
        bucket, key = test_object

        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, GrantRead=f'id="{canonical_ids["acc2"]}"')

        with pytest.raises(ClientError, match="AccessDenied"):
            s3_acc2_uploaddelete.delete_object(Bucket=bucket, Key=key)


class TestObjectWritePermission:
    """Test WRITE permission on objects (N/A - objects don't have WRITE)."""

    def test_object_write_not_applicable(self) -> None:
        """WRITE permission does not apply to objects per S3 spec."""
        pytest.skip("WRITE permission doesn't apply to objects (objects are immutable)")


class TestObjectReadACPPermission:
    """Test READ_ACP permission on objects."""

    @pytest.mark.skipif(
        "config.getoption('--r2')",
        reason="PutObjectAcl/GetObjectAcl not implemented in R2. See: https://developers.cloudflare.com/r2/api/s3/api/"
    )
    def test_object_read_acp_allows_get_object_acl(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, test_object, canonical_ids
    ):
        """Test that READ_ACP permission allows reading object ACL."""
        bucket, key = test_object

        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, GrantReadACP=f'id="{canonical_ids["acc2"]}"')

        response = s3_acc2_uploaddelete.get_object_acl(Bucket=bucket, Key=key)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Owner" in response
        assert "Grants" in response

    @pytest.mark.skipif(
        "config.getoption('--r2')",
        reason="PutObjectAcl/GetObjectAcl not implemented in R2. See: https://developers.cloudflare.com/r2/api/s3/api/"
    )
    def test_object_read_acp_denies_put_object_acl(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, test_object, canonical_ids
    ):
        """Test that READ_ACP permission does not allow modifying ACL."""
        bucket, key = test_object

        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, GrantReadACP=f'id="{canonical_ids["acc2"]}"')

        with pytest.raises(ClientError, match="AccessDenied"):
            s3_acc2_uploaddelete.put_object_acl(Bucket=bucket, Key=key, ACL="private")


class TestObjectWriteACPPermission:
    """Test WRITE_ACP permission on objects."""

    @pytest.mark.skipif(
        "config.getoption('--r2')",
        reason="PutObjectAcl/GetObjectAcl not implemented in R2. See: https://developers.cloudflare.com/r2/api/s3/api/"
    )
    def test_object_write_acp_allows_put_object_acl(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, test_object, canonical_ids
    ):
        """Test that WRITE_ACP permission allows modifying object ACL."""
        bucket, key = test_object

        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, GrantWriteACP=f'id="{canonical_ids["acc2"]}"')

        s3_acc2_uploaddelete.put_object_acl(Bucket=bucket, Key=key, GrantWriteACP=f'id="{canonical_ids["acc2"]}"')

    @pytest.mark.skipif(
        "config.getoption('--r2')",
        reason="PutObjectAcl/GetObjectAcl not implemented in R2. See: https://developers.cloudflare.com/r2/api/s3/api/"
    )
    def test_object_write_acp_denies_get_object(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, test_object, canonical_ids
    ):
        """Test that WRITE_ACP permission does not allow reading object."""
        bucket, key = test_object

        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, GrantWriteACP=f'id="{canonical_ids["acc2"]}"')

        with pytest.raises(ClientError, match="AccessDenied"):
            s3_acc2_uploaddelete.get_object(Bucket=bucket, Key=key)


class TestObjectFullControlPermission:
    """Test FULL_CONTROL permission on objects."""

    @pytest.mark.skipif(
        "config.getoption('--r2')",
        reason="PutObjectAcl/GetObjectAcl not implemented in R2. See: https://developers.cloudflare.com/r2/api/s3/api/"
    )
    def test_object_full_control_allows_all_operations(
        self, s3_acc1_uploaddelete, s3_acc2_uploaddelete, test_object, canonical_ids
    ):
        """Test that FULL_CONTROL grants all object permissions."""
        bucket, key = test_object

        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, GrantFullControl=f'id="{canonical_ids["acc2"]}"')

        response = s3_acc2_uploaddelete.get_object(Bucket=bucket, Key=key)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        response = s3_acc2_uploaddelete.head_object(Bucket=bucket, Key=key)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        response = s3_acc2_uploaddelete.get_object_acl(Bucket=bucket, Key=key)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        s3_acc2_uploaddelete.put_object_acl(Bucket=bucket, Key=key, GrantFullControl=f'id="{canonical_ids["acc2"]}"')
