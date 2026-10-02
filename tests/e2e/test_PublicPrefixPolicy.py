"""Anonymous reads published by a prefix policy, and the keys that stay sealed.

One account is enough here. Cross-bucket ARN rejection is asserted against this
bucket; a second tenant's prefixes are covered by the ACL unit tests, which pass
the source bucket id explicitly.
"""

from __future__ import annotations

import json
import os
from typing import Any
from typing import Callable

import pytest
import requests
from botocore.exceptions import ClientError


@pytest.fixture
def s3_base_url(boto3_client: Any) -> str:
    """Anonymous requests go to the local gateway unless this run is pointed at AWS."""
    if os.getenv("RUN_REAL_AWS") == "1" or os.getenv("AWS") == "1":
        env_url = os.getenv("S3_ENDPOINT_URL")
        if env_url and env_url.strip():
            return env_url.strip()
        endpoint = getattr(getattr(boto3_client, "meta", None), "endpoint_url", None)
        if endpoint:
            return str(endpoint)
    return "http://localhost:8080"


_ALL_USERS = "http://acs.amazonaws.com/groups/global/AllUsers"


def _prefix_policy(bucket: str, prefix: str) -> str:
    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{bucket}/{prefix}/*"],
                }
            ],
        }
    )


def _put(client: Any, bucket: str, key: str, body: bytes) -> None:
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="text/plain")


def _anon(
    base: str, bucket: str, key: str, *, method: str = "GET", params: dict[str, str] | None = None
) -> requests.Response:
    return requests.request(method, f"{base}/{bucket}/{key}", params=params, timeout=10)


def test_prefix_policy_publishes_only_that_prefix(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    s3_base_url: str,
) -> None:
    bucket = unique_bucket_name("prefix-policy")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    boto3_client.put_bucket_policy(Bucket=bucket, Policy=_prefix_policy(bucket, "public"))

    _put(boto3_client, bucket, "public/open.txt", b"published")
    _put(boto3_client, bucket, "public/also.txt", b"also")
    _put(boto3_client, bucket, "public/secret/x.txt", b"nested")
    _put(boto3_client, bucket, "public", b"bare")
    _put(boto3_client, bucket, "publicity.txt", b"not-the-prefix")
    _put(boto3_client, bucket, "sealed/secret.txt", b"sealed")

    grants = boto3_client.get_bucket_acl(Bucket=bucket)["Grants"]
    assert not any(grant.get("Grantee", {}).get("URI") == _ALL_USERS for grant in grants)

    assert _anon(s3_base_url, bucket, "public/open.txt").content == b"published"
    assert _anon(s3_base_url, bucket, "public/also.txt").status_code == 200
    assert _anon(s3_base_url, bucket, "public/secret/x.txt").status_code == 200
    assert _anon(s3_base_url, bucket, "public/open.txt", method="HEAD").status_code == 200
    for key in ("public", "publicity.txt", "sealed/secret.txt"):
        assert _anon(s3_base_url, bucket, key).status_code == 403

    assert _anon(s3_base_url, bucket, "public/open.txt", method="PUT").status_code == 403
    assert (
        requests.get(f"{s3_base_url}/{bucket}", params={"list-type": "2", "prefix": "public/"}, timeout=10).status_code
        == 403
    )
    assert _anon(s3_base_url, bucket, "public/open.txt", params={"tagging": ""}).status_code == 403
    assert _anon(s3_base_url, bucket, "public/open.txt", params={"acl": ""}).status_code == 403
    assert _anon(s3_base_url, bucket, "public/open.txt", params={"versionId": "1"}).status_code == 403
    # A client that collapses `..` requests the sealed key. A client that does not
    # still must not be told the sealed body is the public one.
    assert _anon(s3_base_url, bucket, "public/../sealed/secret.txt").status_code == 403

    got = json.loads(boto3_client.get_bucket_policy(Bucket=bucket)["Policy"])
    assert got["Statement"][0]["Resource"] == [f"arn:aws:s3:::{bucket}/public/*"]

    boto3_client.put_object_acl(Bucket=bucket, Key="public/open.txt", ACL="private")
    assert _anon(s3_base_url, bucket, "public/open.txt").status_code == 403
    assert _anon(s3_base_url, bucket, "public/also.txt").status_code == 200

    boto3_client.put_object_acl(Bucket=bucket, Key="sealed/secret.txt", ACL="public-read")
    assert _anon(s3_base_url, bucket, "sealed/secret.txt").content == b"sealed"

    boto3_client.delete_bucket_policy(Bucket=bucket)
    assert _anon(s3_base_url, bucket, "public/also.txt").status_code == 403
    boto3_client.head_bucket(Bucket=bucket)
    with pytest.raises(ClientError) as missing:
        boto3_client.get_bucket_policy(Bucket=bucket)
    assert missing.value.response["Error"]["Code"] == "NoSuchBucketPolicy"


def test_prefix_policy_rejects_another_bucket_and_keeps_the_object_sealed(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    s3_base_url: str,
) -> None:
    bucket = unique_bucket_name("prefix-cross")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    _put(boto3_client, bucket, "sealed/secret.txt", b"sealed")

    other = _prefix_policy("other-bucket", "sealed")
    with pytest.raises(ClientError) as exc:
        boto3_client.put_bucket_policy(Bucket=bucket, Policy=other)
    assert exc.value.response["Error"]["Code"] == "InvalidPolicyDocument"
    assert _anon(s3_base_url, bucket, "sealed/secret.txt").status_code == 403


def test_whole_bucket_policy_still_publishes_every_key(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    s3_base_url: str,
) -> None:
    bucket = unique_bucket_name("prefix-whole")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    boto3_client.put_bucket_policy(Bucket=bucket, Policy=_prefix_policy(bucket, "public"))
    _put(boto3_client, bucket, "sealed/secret.txt", b"sealed")
    assert _anon(s3_base_url, bucket, "sealed/secret.txt").status_code == 403

    whole = json.dumps(
        {
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
    )
    boto3_client.put_bucket_policy(Bucket=bucket, Policy=whole)
    assert _anon(s3_base_url, bucket, "sealed/secret.txt").content == b"sealed"
    with pytest.raises(ClientError) as again:
        boto3_client.put_bucket_policy(Bucket=bucket, Policy=_prefix_policy(bucket, "public"))
    assert again.value.response["Error"]["Code"] == "PolicyAlreadyExists"
