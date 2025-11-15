"""
Anonymous Access Tests

Tests for public bucket and object access without authentication.
Requires ENABLE_PUBLIC_READ=true in configuration.
"""

from typing import Any

import boto3
import pytest
from botocore.client import Config
from botocore.exceptions import ClientError


pytestmark = pytest.mark.acl


@pytest.fixture(scope="session")
def s3_anonymous(config: Any) -> Any:
    """Boto3 S3 client without credentials for anonymous access."""
    return boto3.client(  # type: ignore[call-overload]
        "s3",
        endpoint_url=config["endpoint_url"],
        region_name="us-east-1",
        config=Config(signature_version=Config.UNSIGNED),
    )


class TestAnonymousBucketAccess:
    """Test anonymous access to public buckets."""

    @pytest.mark.skip(reason="Requires ENABLE_PUBLIC_READ=true and public endpoint")
    def test_anonymous_can_list_public_read_bucket(self, s3_acc1_uploaddelete, s3_anonymous, clean_bucket) -> None:
        """Test that anonymous users can list objects in public-read bucket."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_object(Bucket=bucket, Key="test.txt", Body=b"test")
        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, ACL="public-read")

        response = s3_anonymous.list_objects_v2(Bucket=bucket)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    @pytest.mark.skip(reason="Requires ENABLE_PUBLIC_READ=true and public endpoint")
    def test_anonymous_denied_on_private_bucket(self, s3_anonymous, clean_bucket) -> None:
        """Test that anonymous users cannot list objects in private bucket."""
        bucket = clean_bucket

        with pytest.raises(ClientError, match="AccessDenied"):
            s3_anonymous.list_objects_v2(Bucket=bucket)


class TestAnonymousObjectAccess:
    """Test anonymous access to public objects."""

    @pytest.mark.skip(reason="Requires ENABLE_PUBLIC_READ=true and public endpoint")
    def test_anonymous_can_get_public_read_object(self, s3_acc1_uploaddelete, s3_anonymous, test_object) -> None:
        """Test that anonymous users can download public-read objects."""
        bucket, key = test_object

        s3_acc1_uploaddelete.put_object_acl(Bucket=bucket, Key=key, ACL="public-read")

        response = s3_anonymous.get_object(Bucket=bucket, Key=key)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    @pytest.mark.skip(reason="Requires ENABLE_PUBLIC_READ=true and public endpoint")
    def test_anonymous_denied_on_private_object(self, s3_anonymous, test_object) -> None:
        """Test that anonymous users cannot download private objects."""
        bucket, key = test_object

        with pytest.raises(ClientError, match="AccessDenied"):
            s3_anonymous.get_object(Bucket=bucket, Key=key)
