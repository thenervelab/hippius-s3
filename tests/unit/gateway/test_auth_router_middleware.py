"""Unit tests for auth_router middleware"""

import base64
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi import Request
from httpx import ASGITransport
from httpx import AsyncClient
from nacl.secret import SecretBox


@pytest.fixture  # type: ignore[misc]
def auth_router_app() -> Any:
    from gateway.middlewares.auth_router import auth_router_middleware

    app = FastAPI()
    app.state.redis_client = AsyncMock()

    @app.get("/test")
    async def test_endpoint(request: Request) -> dict[str, Any]:
        auth_method = getattr(request.state, "auth_method", None)
        if auth_method == "access_key":
            return {
                "auth_method": auth_method,
                "access_key": request.state.access_key,
                "account_address": request.state.account_address,
                "token_type": request.state.token_type,
            }
        elif auth_method == "seed_phrase":
            return {
                "auth_method": auth_method,
                "seed_phrase": request.state.seed_phrase,
            }
        else:
            return {"auth_method": auth_method}

    @app.put("/test")
    async def put_test_endpoint(request: Request) -> dict[str, str]:
        return {"message": "ok"}

    @app.get("/health")
    async def health_endpoint() -> dict[str, str]:
        return {"status": "healthy"}

    app.middleware("http")(auth_router_middleware)

    return app


@pytest.mark.asyncio
async def test_exempt_paths_bypass_auth(auth_router_app: Any) -> None:
    """Test that exempt paths bypass authentication"""
    async with AsyncClient(transport=ASGITransport(app=auth_router_app), base_url="http://test") as client:
        response = await client.get("/health")

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_options_request_bypasses_auth(auth_router_app: Any) -> None:
    """Test that OPTIONS requests bypass authentication"""
    async with AsyncClient(transport=ASGITransport(app=auth_router_app), base_url="http://test") as client:
        response = await client.options("/test")

    assert response.status_code == 405


@pytest.mark.asyncio
async def test_missing_auth_header_returns_403(auth_router_app: Any) -> None:
    """Test that missing auth header returns 403"""
    async with AsyncClient(transport=ASGITransport(app=auth_router_app), base_url="http://test") as client:
        response = await client.put("/test", content=b"test data")

    assert response.status_code == 403
    assert b"InvalidAccessKeyId" in response.content


@pytest.mark.asyncio
async def test_anonymous_get_request_allowed(auth_router_app: Any) -> None:
    """Test that GET requests without auth header are allowed (anonymous)"""
    async with AsyncClient(transport=ASGITransport(app=auth_router_app), base_url="http://test") as client:
        response = await client.get("/test")

    assert response.status_code == 200
    data = response.json()
    assert data["auth_method"] == "anonymous"


@pytest.mark.asyncio
async def test_root_get_without_auth_requires_access_key(auth_router_app: Any) -> None:
    """GET / without auth should still require an access key (no anonymous listing)."""
    async with AsyncClient(transport=ASGITransport(app=auth_router_app), base_url="http://test") as client:
        response = await client.get("/")

    assert response.status_code == 403
    assert b"InvalidAccessKeyId" in response.content


@pytest.mark.asyncio
async def test_access_key_detection_and_routing(auth_router_app: Any) -> None:
    """Test that access keys starting with hip_ are detected and routed correctly"""
    test_access_key = "hip_test_key_12345"
    test_account_address = "5FH2aQUbix3nNatzST4mPM8iuebGvSMFerZLdwvDmAwRDFep"
    test_token_type = "master"
    test_secret = "decrypted_secret_key"

    key_hex = "a" * 64
    key_bytes = bytes.fromhex(key_hex)
    box = SecretBox(key_bytes)
    encrypted = box.encrypt(test_secret.encode("utf-8"))
    encrypted_b64 = base64.b64encode(encrypted).decode("utf-8")
    nonce_b64 = base64.b64encode(encrypted.nonce).decode("utf-8")

    mock_token_response = MagicMock()
    mock_token_response.valid = True
    mock_token_response.status = "active"
    mock_token_response.account_address = test_account_address
    mock_token_response.token_type = test_token_type
    mock_token_response.encrypted_secret = encrypted_b64
    mock_token_response.nonce = nonce_b64

    auth_header = f"AWS4-HMAC-SHA256 Credential={test_access_key}/20250101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123def456"

    with patch(
        "gateway.middlewares.access_key_auth.cached_auth", new_callable=AsyncMock, return_value=mock_token_response
    ):
        with patch("gateway.middlewares.access_key_auth.config") as mock_config:
            mock_config.hippius_secret_decryption_material = key_hex

            with patch("gateway.middlewares.access_key_auth.calculate_signature", return_value="abc123def456"):
                with patch("gateway.middlewares.access_key_auth.create_canonical_request", return_value="canonical"):
                    async with AsyncClient(
                        transport=ASGITransport(app=auth_router_app), base_url="http://test"
                    ) as client:
                        response = await client.get(
                            "/test",
                            headers={
                                "Authorization": auth_header,
                                "x-amz-date": "20250101T000000Z",
                                "x-amz-content-sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                            },
                        )

    assert response.status_code == 200
    data = response.json()
    assert data["auth_method"] == "access_key"
    assert data["access_key"] == test_access_key
    assert data["account_address"] == test_account_address
    assert data["token_type"] == test_token_type


@pytest.mark.asyncio
async def test_invalid_credential_format_returns_403(auth_router_app: Any) -> None:
    """Test that invalid credential format returns 403"""
    auth_header = "AWS4-HMAC-SHA256 InvalidFormat"

    async with AsyncClient(transport=ASGITransport(app=auth_router_app), base_url="http://test") as client:
        response = await client.put(
            "/test",
            headers={
                "Authorization": auth_header,
            },
            content=b"test data",
        )

    assert response.status_code == 403
    assert b"InvalidAccessKeyId" in response.content


@pytest.mark.asyncio
async def test_access_key_with_invalid_signature_returns_403(auth_router_app: Any) -> None:
    """Test that access key with invalid signature returns 403"""
    test_access_key = "hip_test_key_12345"
    test_secret = "decrypted_secret_key"

    key_hex = "a" * 64
    key_bytes = bytes.fromhex(key_hex)
    box = SecretBox(key_bytes)
    encrypted = box.encrypt(test_secret.encode("utf-8"))
    encrypted_b64 = base64.b64encode(encrypted).decode("utf-8")
    nonce_b64 = base64.b64encode(encrypted.nonce).decode("utf-8")

    mock_token_response = MagicMock()
    mock_token_response.valid = True
    mock_token_response.status = "active"
    mock_token_response.account_address = "5FH2aQUbix3nNatzST4mPM8iuebGvSMFerZLdwvDmAwRDFep"
    mock_token_response.token_type = "master"
    mock_token_response.encrypted_secret = encrypted_b64
    mock_token_response.nonce = nonce_b64

    auth_header = f"AWS4-HMAC-SHA256 Credential={test_access_key}/20250101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

    with patch(
        "gateway.middlewares.access_key_auth.cached_auth", new_callable=AsyncMock, return_value=mock_token_response
    ):
        with patch("gateway.middlewares.access_key_auth.config") as mock_config:
            mock_config.hippius_secret_decryption_material = key_hex

            with patch("gateway.middlewares.access_key_auth.calculate_signature", return_value="correct_signature"):
                with patch("gateway.middlewares.access_key_auth.create_canonical_request", return_value="canonical"):
                    async with AsyncClient(
                        transport=ASGITransport(app=auth_router_app), base_url="http://test"
                    ) as client:
                        response = await client.put(
                            "/test",
                            headers={
                                "Authorization": auth_header,
                                "x-amz-date": "20250101T000000Z",
                                "x-amz-content-sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                            },
                            content=b"test data",
                        )

    assert response.status_code == 403
    assert b"SignatureDoesNotMatch" in response.content


@pytest.mark.asyncio
async def test_presigned_get_with_access_key_uses_access_key_auth(auth_router_app: Any) -> None:
    """Presigned GET with hip_ access key should route through access key auth and set state."""
    test_access_key = "hip_presigned_key_12345"
    test_account_address = "5FH2aQUbix3nNatzST4mPM8iuebGvSMFerZLdwvDmAwRDFep"
    test_token_type = "sub"

    # Patch the presigned URL verifier so we don't depend on its implementation here
    mock_verify = AsyncMock(return_value=(True, test_account_address, test_token_type))

    query_params = {
        "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
        "X-Amz-Credential": f"{test_access_key}/20250101/us-east-1/s3/aws4_request",
        "X-Amz-Date": "20250101T000000Z",
        "X-Amz-Expires": "3600",
        "X-Amz-SignedHeaders": "host",
        "X-Amz-Signature": "deadbeef",
    }

    with patch("gateway.services.auth_orchestrator.verify_access_key_presigned_url", mock_verify):
        async with AsyncClient(transport=ASGITransport(app=auth_router_app), base_url="http://test") as client:
            response = await client.get("/test", params=query_params)

    # Once implemented, we expect presigned URLs with hip_ keys to authenticate as access keys
    assert response.status_code == 200
    data = response.json()
    assert data["auth_method"] == "access_key"
    assert data["access_key"] == test_access_key
    assert data["account_address"] == test_account_address
    assert data["token_type"] == test_token_type


@pytest.mark.asyncio
async def test_presigned_get_with_non_hip_credential_rejected(auth_router_app: Any) -> None:
    """Presigned GET with non-hip credential should be rejected with InvalidAccessKeyId."""
    # Credential that does not start with hip_
    bad_credential = "not_hip_key_123"

    query_params = {
        "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
        "X-Amz-Credential": f"{bad_credential}/20250101/us-east-1/s3/aws4_request",
        "X-Amz-Date": "20250101T000000Z",
        "X-Amz-Expires": "3600",
        "X-Amz-SignedHeaders": "host",
        "X-Amz-Signature": "deadbeef",
    }

    async with AsyncClient(transport=ASGITransport(app=auth_router_app), base_url="http://test") as client:
        response = await client.get("/test", params=query_params)

    # v1 behavior: non-hip credentials in presigned URLs should be treated as invalid access keys
    assert response.status_code == 403
    assert b"InvalidAccessKeyId" in response.content
