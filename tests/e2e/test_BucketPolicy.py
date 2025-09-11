"""E2E tests for Bucket Policy (PUT/GET /{bucket}?policy)."""

import json
from typing import Any
from typing import Callable

import pytest
from botocore.exceptions import ClientError


PUBLIC_READ = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Principal": "*", "Action": ["s3:GetObject"], "Resource": []}],
}


@pytest.mark.skip(
    reason="TODO: Enable when PublicAccessBlock (BlockPublicPolicy toggles) is supported. Both acceptance and denial paths to be asserted based on toggle."
)
def test_bucket_policy_public_read(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("policy")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:GetObject"],
                "Resource": [f"arn:aws:s3:::{bucket}/*"],
            }
        ],
    }

    # Put policy (public-read helper). On many AWS accounts, BPAb (Block Public Access)
    # denies this with AccessDenied.
    # For now, we skip: on real AWS with BPA it's AccessDenied; locally we accept until BPA is implemented.
    resp = boto3_client.put_bucket_policy(Bucket=bucket, Policy=json.dumps(policy))
    assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    got = boto3_client.get_bucket_policy(Bucket=bucket)
    assert "Policy" in got
    loaded = json.loads(got["Policy"]) if isinstance(got["Policy"], str) else got["Policy"]
    assert loaded["Statement"][0]["Action"] == ["s3:GetObject"]

    # When implemented, we will:
    # 1) Toggle BPA off → assert successful Put/Get policy
    # 2) Toggle BPA on  → assert AccessDenied and NoSuchBucketPolicy


def test_bucket_policy_invalid_document(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("policy-bad")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)

    bad_policy = {"Version": "2012-10-17", "Statement": [{"Effect": "Deny"}]}
    with pytest.raises(ClientError) as excinfo:
        boto3_client.put_bucket_policy(Bucket=bucket, Policy=json.dumps(bad_policy))
    code = excinfo.value.response.get("Error", {}).get("Code") or excinfo.value.response.get("Code")
    assert code in {"InvalidPolicyDocument", "MalformedPolicy"}


def test_get_bucket_policy_private_bucket_returns_404(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("policy-none")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)

    # No policy set → should be 404 NoSuchBucketPolicy
    with pytest.raises(ClientError) as excinfo:
        boto3_client.get_bucket_policy(Bucket=bucket)
    code = excinfo.value.response.get("Error", {}).get("Code") or excinfo.value.response.get("Code")
    assert code == "NoSuchBucketPolicy"
