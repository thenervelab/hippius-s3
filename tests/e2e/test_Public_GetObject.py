"""E2E tests for anonymous public GET/HEAD object operations."""

import os
from typing import Any
from typing import Callable

import pytest
import requests


@pytest.fixture
def s3_base_url(boto3_client: Any) -> str:
    """Get the S3 service base URL (match boto3 endpoint unless overridden)."""
    env_url = os.getenv("S3_ENDPOINT_URL")
    if env_url and env_url.strip():
        return env_url.strip()
    try:
        endpoint = getattr(getattr(boto3_client, "meta", None), "endpoint_url", None)
        if endpoint:
            return str(endpoint)
    except Exception:
        pass
    # Default local API port used in e2e compose
    return "http://localhost:8000"


def test_public_get_object_anonymous(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    s3_base_url: str,
) -> None:
    """Test anonymous GET on a public bucket object returns 200."""
    bucket_name = unique_bucket_name("public-get")
    cleanup_buckets(bucket_name)

    # Create bucket
    boto3_client.create_bucket(Bucket=bucket_name)

    # Set public read policy
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:GetObject"],
                "Resource": [f"arn:aws:s3:::{bucket_name}/*"],
            }
        ],
    }
    boto3_client.put_bucket_policy(Bucket=bucket_name, Policy=str(policy).replace("'", '"'))

    # Upload object
    key = "public-file.txt"
    body = b"Hello, public world!"
    boto3_client.put_object(Bucket=bucket_name, Key=key, Body=body, ContentType="text/plain")

    # Anonymous GET via HTTP
    url = f"{s3_base_url}/{bucket_name}/{key}"
    response = requests.get(url, timeout=10)

    assert response.status_code == 200
    assert response.content == body
    assert response.headers.get("content-type") == "text/plain"
    assert "x-hippius-access-mode" in response.headers
    assert response.headers["x-hippius-access-mode"] == "anon"


def test_public_head_object_anonymous(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    s3_base_url: str,
) -> None:
    """Test anonymous HEAD on a public bucket object returns 200."""
    bucket_name = unique_bucket_name("public-head")
    cleanup_buckets(bucket_name)

    # Create bucket
    boto3_client.create_bucket(Bucket=bucket_name)

    # Set public read policy
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:GetObject"],
                "Resource": [f"arn:aws:s3:::{bucket_name}/*"],
            }
        ],
    }
    boto3_client.put_bucket_policy(Bucket=bucket_name, Policy=str(policy).replace("'", '"'))

    # Upload object
    key = "public-file-head.txt"
    body = b"Head test content"
    boto3_client.put_object(Bucket=bucket_name, Key=key, Body=body, ContentType="text/plain")

    # Anonymous HEAD via HTTP
    url = f"{s3_base_url}/{bucket_name}/{key}"
    response = requests.head(url, timeout=10)

    assert response.status_code == 200
    assert int(response.headers.get("content-length", 0)) == len(body)
    assert response.headers.get("content-type") == "text/plain"
    assert "x-hippius-access-mode" in response.headers
    assert response.headers["x-hippius-access-mode"] == "anon"


def test_private_get_object_anonymous_fails(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    s3_base_url: str,
) -> None:
    """Test anonymous GET on a private bucket object returns 403."""
    bucket_name = unique_bucket_name("private-get")
    cleanup_buckets(bucket_name)

    # Create bucket (private by default)
    boto3_client.create_bucket(Bucket=bucket_name)

    # Upload object
    key = "private-file.txt"
    body = b"This is private"
    boto3_client.put_object(Bucket=bucket_name, Key=key, Body=body, ContentType="text/plain")

    # Anonymous GET via HTTP should fail
    url = f"{s3_base_url}/{bucket_name}/{key}"
    response = requests.get(url, timeout=10)

    assert response.status_code == 403
    assert "SignatureDoesNotMatch" in response.text


def test_signed_get_object_still_works(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test that signed GET requests still work on private buckets."""
    bucket_name = unique_bucket_name("signed-get")
    cleanup_buckets(bucket_name)

    # Create bucket (private)
    boto3_client.create_bucket(Bucket=bucket_name)

    # Upload object
    key = "signed-file.txt"
    body = b"Signed access"
    boto3_client.put_object(Bucket=bucket_name, Key=key, Body=body, ContentType="text/plain")

    # Signed GET via boto3
    response = boto3_client.get_object(Bucket=bucket_name, Key=key)
    content = response["Body"].read()

    assert content == body


def test_public_bucket_list_anonymous_fails(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    s3_base_url: str,
) -> None:
    """Test that anonymous GET on bucket list (no object key) still returns 403."""
    bucket_name = unique_bucket_name("public-list")
    cleanup_buckets(bucket_name)

    # Create bucket and make public
    boto3_client.create_bucket(Bucket=bucket_name)
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:GetObject"],
                "Resource": [f"arn:aws:s3:::{bucket_name}/*"],
            }
        ],
    }
    boto3_client.put_bucket_policy(Bucket=bucket_name, Policy=str(policy).replace("'", '"'))

    # Anonymous GET on bucket root should fail (no ListBucket permission)
    url = f"{s3_base_url}/{bucket_name}"
    response = requests.get(url, timeout=10)

    assert response.status_code == 403
    assert "SignatureDoesNotMatch" in response.text


def test_public_get_object_with_query_params_anonymous_fails(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    s3_base_url: str,
) -> None:
    """Test that anonymous GET with mutating query params fails even on public bucket."""
    bucket_name = unique_bucket_name("public-query")
    cleanup_buckets(bucket_name)

    # Create bucket and make public
    boto3_client.create_bucket(Bucket=bucket_name)
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:GetObject"],
                "Resource": [f"arn:aws:s3:::{bucket_name}/*"],
            }
        ],
    }
    boto3_client.put_bucket_policy(Bucket=bucket_name, Policy=str(policy).replace("'", '"'))

    # Upload object
    key = "query-file.txt"
    body = b"Query test"
    boto3_client.put_object(Bucket=bucket_name, Key=key, Body=body, ContentType="text/plain")

    # Anonymous GET with tagging query should fail
    url = f"{s3_base_url}/{bucket_name}/{key}?tagging"
    response = requests.get(url)

    assert response.status_code == 403
    assert "SignatureDoesNotMatch" in response.text
