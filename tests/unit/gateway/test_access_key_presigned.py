"""Unit tests for verify_access_key_presigned_url and related helpers."""

import datetime
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch
from urllib.parse import urlencode

import pytest
from fastapi import Request

from gateway.middlewares.access_key_auth import AccessKeyAuthError
from gateway.middlewares.access_key_auth import verify_access_key_presigned_url


def make_request(
    method: str = "GET",
    path: str = "/bucket/key",
    query_params: dict[str, str] | None = None,
    headers: dict[str, str] | None = None,
    raw_path: bytes | None = None,
) -> Request:
    """Create a minimal FastAPI Request suitable for presigned URL verification tests."""
    headers = headers or {}
    query_params = query_params or {}

    scope: dict[str, Any] = {
        "type": "http",
        "method": method,
        "path": path,
        "scheme": "http",
        "server": ("testserver", 80),
        "headers": [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in headers.items()],
        "query_string": urlencode(query_params).encode("latin-1"),
        # By default, mirror the path as raw_path so canonical_path_from_scope
        # can operate; individual tests can override this to simulate percent-
        # encoded wire paths.
        "raw_path": raw_path if raw_path is not None else path.encode("latin-1"),
    }
    return Request(scope)


mock_redis = AsyncMock()


def _make_token_response(
    account_address: str = "5FH2aQUbix3nNatzST4mPM8iuebGvSMFerZLdwvDmAwRDFep",
    token_type: str = "sub",
    credits: float = 100.0,
) -> MagicMock:
    """Build a mock TokenAuthResponse with all required fields."""
    mock = MagicMock()
    mock.valid = True
    mock.status = "active"
    mock.account_address = account_address
    mock.token_type = token_type
    mock.encrypted_secret = "enc"
    mock.nonce = "nonce"
    mock.credits = credits
    return mock


def _make_presigned_query_params(access_key: str, offset: datetime.timedelta = datetime.timedelta()) -> dict[str, str]:
    """Build presigned URL query params with configurable signing time offset from now."""
    signed_at = datetime.datetime.now(datetime.timezone.utc) + offset
    amz_date = signed_at.strftime("%Y%m%dT%H%M%SZ")
    date_scope = amz_date[:8]
    return {
        "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
        "X-Amz-Credential": f"{access_key}/{date_scope}/us-east-1/s3/aws4_request",
        "X-Amz-Date": amz_date,
        "X-Amz-Expires": "3600",
        "X-Amz-SignedHeaders": "host",
        "X-Amz-Signature": "deadbeef",
    }


@pytest.mark.asyncio
async def test_presigned_url_expired_raises() -> None:
    """Expired presigned URL should raise AccessKeyAuthError without hitting Hippius API."""
    access_key = "hip_presigned_key_12345"
    query_params = _make_presigned_query_params(access_key, offset=datetime.timedelta(hours=-2))
    request = make_request(query_params=query_params)

    mock_cached_auth = AsyncMock()

    with patch("gateway.middlewares.access_key_auth.cached_auth", mock_cached_auth):
        with pytest.raises(AccessKeyAuthError, match="expired"):
            await verify_access_key_presigned_url(request, access_key, mock_redis)

    mock_cached_auth.assert_not_awaited()


@pytest.mark.asyncio
async def test_presigned_url_valid_returns_verified_key() -> None:
    """Valid, unexpired presigned URL should return VerifiedAccessKey."""
    access_key = "hip_presigned_key_12345"
    query_params = _make_presigned_query_params(access_key, offset=datetime.timedelta(minutes=-1))
    request = make_request(query_params=query_params)

    with patch("gateway.middlewares.access_key_auth.cached_auth", AsyncMock(return_value=_make_token_response())):
        with patch("gateway.middlewares.access_key_auth.decrypt_secret", return_value="secret"):
            with patch(
                "gateway.middlewares.access_key_auth.create_canonical_request",
                new_callable=AsyncMock,
                return_value="canonical",
            ):
                with patch("gateway.middlewares.access_key_auth.calculate_signature", return_value="deadbeef"):
                    result = await verify_access_key_presigned_url(request, access_key, mock_redis)

    assert result.account_address == "5FH2aQUbix3nNatzST4mPM8iuebGvSMFerZLdwvDmAwRDFep"
    assert result.token_type == "sub"
    assert result.has_credits is True


@pytest.mark.asyncio
async def test_canonical_query_for_presigned_excludes_signature() -> None:
    """Canonical query string for presigned URLs must exclude X-Amz-Signature."""
    access_key = "hip_presigned_key_12345"
    query_params = _make_presigned_query_params(access_key)
    query_params["foo"] = "bar"
    request = make_request(query_params=query_params)

    captured_query_string: str | None = None

    async def fake_create_canonical_request(
        request: Request,
        signed_headers: list[str],
        method: str,
        path: str,
        query_string: str,
    ) -> str:
        nonlocal captured_query_string
        captured_query_string = query_string
        return "canonical"

    with patch("gateway.middlewares.access_key_auth.cached_auth", AsyncMock(return_value=_make_token_response())):
        with patch("gateway.middlewares.access_key_auth.decrypt_secret", return_value="secret"):
            with patch(
                "gateway.middlewares.access_key_auth.create_canonical_request",
                new=fake_create_canonical_request,
            ):
                with patch("gateway.middlewares.access_key_auth.calculate_signature", return_value="deadbeef"):
                    await verify_access_key_presigned_url(request, access_key, mock_redis)

    assert captured_query_string is not None
    assert "X-Amz-Signature" not in captured_query_string
    assert "foo=bar" in captured_query_string


@pytest.mark.asyncio
async def test_presigned_url_uses_raw_path_for_canonical_path() -> None:
    """
    Presigned URL verification must use the raw_path bytes from the ASGI
    scope when building the canonical request path, so that percent-encoding
    of spaces and other characters exactly matches what the client signed.
    """
    access_key = "hip_presigned_key_12345"
    query_params = _make_presigned_query_params(access_key)

    logical_path = "/bucket/conflict65 (3).jpg"
    wire_path = b"/bucket/conflict65%20(3).jpg"
    request = make_request(path=logical_path, query_params=query_params, raw_path=wire_path)

    captured_path: str | None = None

    async def fake_create_canonical_request(
        request: Request,
        signed_headers: list[str],
        method: str,
        path: str,
        query_string: str,
    ) -> str:
        nonlocal captured_path
        captured_path = path
        return "canonical"

    with patch("gateway.middlewares.access_key_auth.cached_auth", AsyncMock(return_value=_make_token_response())):
        with patch("gateway.middlewares.access_key_auth.decrypt_secret", return_value="secret"):
            with patch(
                "gateway.middlewares.access_key_auth.create_canonical_request",
                new=fake_create_canonical_request,
            ):
                with patch("gateway.middlewares.access_key_auth.calculate_signature", return_value="deadbeef"):
                    await verify_access_key_presigned_url(request, access_key, mock_redis)

    assert captured_path == "/bucket/conflict65%20(3).jpg"


@pytest.mark.asyncio
async def test_presigned_url_credential_id_mismatch_rejected() -> None:
    """Mismatch between access_key argument and X-Amz-Credential ID should raise and skip API call."""
    access_key = "hip_presigned_key_12345"
    other_key = "hip_other_key_99999"

    now = datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_scope = amz_date[:8]

    query_params = {
        "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
        "X-Amz-Credential": f"{other_key}/{date_scope}/us-east-1/s3/aws4_request",
        "X-Amz-Date": amz_date,
        "X-Amz-Expires": "3600",
        "X-Amz-SignedHeaders": "host",
        "X-Amz-Signature": "deadbeef",
    }

    request = make_request(query_params=query_params)

    mock_cached_auth = AsyncMock()

    with patch("gateway.middlewares.access_key_auth.cached_auth", mock_cached_auth):
        with pytest.raises(AccessKeyAuthError):
            await verify_access_key_presigned_url(request, access_key, mock_redis)

    mock_cached_auth.assert_not_awaited()
