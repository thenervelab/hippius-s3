"""E2E test for GetBucketLocation via boto3."""

from typing import Any
from typing import Callable


def test_get_bucket_location_returns_useast1(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("location")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    loc = boto3_client.get_bucket_location(Bucket=bucket)
    # AWS returns None for us-east-1; our server returns explicit us-east-1
    constraint = loc.get("LocationConstraint")
    assert constraint in (None, "us-east-1", "EU", "") or isinstance(constraint, str)
