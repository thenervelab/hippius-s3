"""Integration tests for the S3 response-header override query parameters.

The integration tier verifies that presigned URLs carrying response-* overrides
are accepted by the gateway's SigV4 verification (i.e. our parser doesn't break
canonical query string construction). Signature tampering protection is covered
generically by tests/unit/gateway/test_access_key_presigned.py. Header
application against a real GET/HEAD response is verified in the e2e tier.
"""

from typing import Any
from urllib.parse import urlparse

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_presigned_get_with_response_content_disposition_signature_accepted(
    boto3_client: Any,
    gateway_client: AsyncClient,
    unique_bucket_name: Any,
) -> None:
    """Presigned GET carrying response-content-disposition must verify cleanly."""
    bucket_name = unique_bucket_name("presigned-rcd")
    key = "test-object.txt"

    presigned_url = boto3_client.generate_presigned_url(
        ClientMethod="get_object",
        Params={
            "Bucket": bucket_name,
            "Key": key,
            "ResponseContentDisposition": 'attachment; filename="custom.bin"',
            "ResponseContentType": "application/octet-stream",
        },
        ExpiresIn=3600,
    )
    parsed = urlparse(presigned_url)
    response = await gateway_client.get(parsed.path + ("?" + parsed.query if parsed.query else ""))

    assert response.status_code in (200, 403, 404)
    if response.status_code == 403:
        assert b"SignatureDoesNotMatch" not in response.content


@pytest.mark.asyncio
async def test_presigned_head_with_response_content_disposition_signature_accepted(
    boto3_client: Any,
    gateway_client: AsyncClient,
    unique_bucket_name: Any,
) -> None:
    """Presigned HEAD with response-content-disposition must verify cleanly."""
    bucket_name = unique_bucket_name("presigned-rcd-head")
    key = "test-object.txt"

    presigned_url = boto3_client.generate_presigned_url(
        ClientMethod="head_object",
        Params={
            "Bucket": bucket_name,
            "Key": key,
            "ResponseContentDisposition": 'attachment; filename="custom.bin"',
        },
        ExpiresIn=3600,
    )
    parsed = urlparse(presigned_url)
    response = await gateway_client.head(parsed.path + ("?" + parsed.query if parsed.query else ""))

    assert response.status_code in (200, 403, 404)
    if response.status_code == 403:
        assert b"SignatureDoesNotMatch" not in response.content


@pytest.mark.asyncio
async def test_presigned_get_with_all_six_overrides_signature_accepted(
    boto3_client: Any,
    gateway_client: AsyncClient,
    unique_bucket_name: Any,
) -> None:
    """All six override params together must verify cleanly."""
    bucket_name = unique_bucket_name("presigned-all6")
    key = "test-object.txt"

    presigned_url = boto3_client.generate_presigned_url(
        ClientMethod="get_object",
        Params={
            "Bucket": bucket_name,
            "Key": key,
            "ResponseContentType": "application/pdf",
            "ResponseContentDisposition": 'attachment; filename="invoice.pdf"',
            "ResponseContentEncoding": "identity",
            "ResponseContentLanguage": "en-US",
            "ResponseCacheControl": "no-cache, max-age=0",
            "ResponseExpires": "Thu, 01 Dec 2026 16:00:00 GMT",
        },
        ExpiresIn=3600,
    )
    parsed = urlparse(presigned_url)
    response = await gateway_client.get(parsed.path + ("?" + parsed.query if parsed.query else ""))

    assert response.status_code in (200, 403, 404)
    if response.status_code == 403:
        assert b"SignatureDoesNotMatch" not in response.content
