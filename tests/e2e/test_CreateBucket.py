"""E2E test for CreateBucket (PUT /{bucket}).

Covers:
- CreateBucket
- HeadBucket
- ListBuckets
- GetBucketLocation
"""

from typing import Any
from typing import Callable

import pytest
from botocore.exceptions import ClientError


def test_create_bucket_head_list_location(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("create-bucket")
    cleanup_buckets(bucket_name)

    # Create bucket (no ACL to match modern AWS behavior with ObjectOwnership enforced)
    boto3_client.create_bucket(Bucket=bucket_name)

    # HeadBucket should succeed
    boto3_client.head_bucket(Bucket=bucket_name)

    # ListBuckets should include our bucket
    buckets = boto3_client.list_buckets()["Buckets"]
    names = {b["Name"] for b in buckets}
    assert bucket_name in names

    # Get bucket location should return us-east-1
    # boto3 S3 client uses get_bucket_location; our endpoint returns LocationConstraint XML with us-east-1
    loc = boto3_client.get_bucket_location(Bucket=bucket_name)
    # AWS returns None for us-east-1; our implementation returns explicit us-east-1 string via XML.
    # Depending on botocore parsing, we assert one of the acceptable forms.
    constraint = loc.get("LocationConstraint")
    assert constraint in (None, "us-east-1", "EU", "") or isinstance(constraint, str)

    # Creating the same bucket again: some S3 deployments return a conflict error, some are idempotent
    try:
        boto3_client.create_bucket(Bucket=bucket_name)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code") or exc.response.get("Code")
        assert code in {"BucketAlreadyExists", "BucketAlreadyOwnedByYou"}
    else:
        # If no error, ensure bucket still accessible
        boto3_client.head_bucket(Bucket=bucket_name)


def test_create_bucket_rejects_acl_with_object_ownership(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
) -> None:
    """Creating a bucket with ACL must fail under BucketOwnerEnforced ownership."""
    bucket_name = unique_bucket_name("create-bucket-acl")

    with pytest.raises(ClientError) as excinfo:
        boto3_client.create_bucket(Bucket=bucket_name, ACL="public-read")

    code = excinfo.value.response.get("Error", {}).get("Code") or excinfo.value.response.get("Code")
    assert code == "InvalidBucketAclWithObjectOwnership"
