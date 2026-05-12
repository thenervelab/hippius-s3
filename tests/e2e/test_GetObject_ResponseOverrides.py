"""E2E tests for S3 response-header override query parameters on GET/HEAD."""

from typing import Any
from typing import Callable

import httpx
import requests

from .conftest import is_real_aws


def test_get_object_with_response_content_disposition(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Presigned GET with ResponseContentDisposition returns the override header."""
    bucket_name = unique_bucket_name("rcd-get")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name)

    key = "file.bin"
    body = b"hello world payload"
    boto3_client.put_object(Bucket=bucket_name, Key=key, Body=body, ContentType="application/octet-stream")

    resp = boto3_client.get_object(
        Bucket=bucket_name,
        Key=key,
        ResponseContentDisposition='attachment; filename="custom.bin"',
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert resp["Body"].read() == body
    headers = resp["ResponseMetadata"]["HTTPHeaders"]
    assert headers.get("content-disposition") == 'attachment; filename="custom.bin"'
    assert headers.get("content-type") == "application/octet-stream"


def test_get_object_with_all_six_overrides(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Presigned GET with all six response-* overrides returns each as a response header."""
    bucket_name = unique_bucket_name("rcd-all6")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name)

    key = "doc.bin"
    body = b"the body content"
    boto3_client.put_object(Bucket=bucket_name, Key=key, Body=body, ContentType="application/octet-stream")

    resp = boto3_client.get_object(
        Bucket=bucket_name,
        Key=key,
        ResponseContentType="application/pdf",
        ResponseContentDisposition='attachment; filename="invoice.pdf"',
        ResponseContentEncoding="identity",
        ResponseContentLanguage="en-US",
        ResponseCacheControl="no-cache, max-age=0",
        ResponseExpires="Thu, 03 Dec 2026 16:00:00 GMT",
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    headers = resp["ResponseMetadata"]["HTTPHeaders"]
    assert headers.get("content-type") == "application/pdf"
    assert headers.get("content-disposition") == 'attachment; filename="invoice.pdf"'
    assert headers.get("content-encoding") == "identity"
    assert headers.get("content-language") == "en-US"
    assert headers.get("cache-control") == "no-cache, max-age=0"
    assert headers.get("expires") == "Thu, 03 Dec 2026 16:00:00 GMT"
    assert resp["Body"].read() == body


def test_head_object_with_response_content_disposition(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Presigned HEAD with ResponseContentDisposition returns the override header."""
    bucket_name = unique_bucket_name("rcd-head")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name)

    key = "file.bin"
    body = b"payload"
    boto3_client.put_object(Bucket=bucket_name, Key=key, Body=body, ContentType="application/octet-stream")

    resp = boto3_client.head_object(
        Bucket=bucket_name,
        Key=key,
        ResponseContentDisposition='attachment; filename="custom.bin"',
        ResponseContentType="application/pdf",
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    headers = resp["ResponseMetadata"]["HTTPHeaders"]
    assert headers.get("content-disposition") == 'attachment; filename="custom.bin"'
    assert headers.get("content-type") == "application/pdf"


def test_get_object_range_with_response_content_disposition(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Range GET (206) with override applies the header alongside Content-Range."""
    bucket_name = unique_bucket_name("rcd-range")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name)

    key = "file.bin"
    body = b"0123456789abcdef"
    boto3_client.put_object(Bucket=bucket_name, Key=key, Body=body, ContentType="application/octet-stream")

    resp = boto3_client.get_object(
        Bucket=bucket_name,
        Key=key,
        Range="bytes=0-4",
        ResponseContentDisposition='attachment; filename="slice.bin"',
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert resp["Body"].read() == b"01234"
    headers = resp["ResponseMetadata"]["HTTPHeaders"]
    assert headers.get("content-disposition") == 'attachment; filename="slice.bin"'
    assert headers.get("content-range") == f"bytes 0-4/{len(body)}"


def test_anonymous_public_get_ignores_overrides(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Anonymous GET on a public bucket must NOT apply response-* overrides."""
    if is_real_aws():
        return

    bucket_name = unique_bucket_name("rcd-anon")
    cleanup_buckets(bucket_name)
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

    key = "public.bin"
    body = b"public payload"
    boto3_client.put_object(Bucket=bucket_name, Key=key, Body=body, ContentType="text/plain")

    url = f"http://localhost:8080/{bucket_name}/{key}"
    params = {
        "response-content-disposition": 'attachment; filename="should-be-ignored.bin"',
        "response-content-type": "application/pdf",
    }
    response = requests.get(url, params=params, timeout=10)
    assert response.status_code == 200
    assert response.content == body
    assert response.headers.get("content-type") == "text/plain"
    assert "content-disposition" not in {k.lower() for k in response.headers}


def test_get_object_with_crlf_override_rejected(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """A signed GET with CRLF in a response-* value must return 400 InvalidArgument."""
    if is_real_aws():
        return

    bucket_name = unique_bucket_name("rcd-crlf")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name)

    key = "file.bin"
    body = b"abc"
    boto3_client.put_object(Bucket=bucket_name, Key=key, Body=body, ContentType="application/octet-stream")

    presigned_url = boto3_client.generate_presigned_url(
        ClientMethod="get_object",
        Params={
            "Bucket": bucket_name,
            "Key": key,
            "ResponseContentDisposition": "evil\r\nX-Injected: yes",
        },
        ExpiresIn=300,
    )
    response = httpx.get(presigned_url, timeout=10)
    assert response.status_code == 400
    assert b"InvalidArgument" in response.content
