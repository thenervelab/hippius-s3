"""Unit tests for verify_access_key_signature and canonical path handling."""

import datetime
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from fastapi import Request

from gateway.middlewares.access_key_auth import verify_access_key_signature


def make_request(
    method: str = "PUT",
    path: str = "/bucket/conflict65 (3).jpg",
    raw_path: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> Request:
    """Create a minimal FastAPI Request suitable for header-signed access key tests."""
    headers = headers or {}

    scope: dict[str, Any] = {
        "type": "http",
        "method": method,
        "path": path,
        "scheme": "http",
        "server": ("testserver", 80),
        "headers": [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in headers.items()],
        "query_string": b"",
    }
    if raw_path is not None:
        scope["raw_path"] = raw_path
    return Request(scope)


mock_redis = AsyncMock()


@pytest.mark.asyncio
async def test_access_key_signature_uses_raw_path_for_canonical_path() -> None:
    """
    Header-signed access key verification must use raw_path from the ASGI scope
    when building the canonical request path, so that percent-encoding matches
    what the AWS client signed.
    """
    access_key = "hip_test_key_12345"
    account_address = "5FH2aQUbix3nNatzST4mPM8iuebGvSMFerZLdwvDmAwRDFep"
    token_type = "sub"

    amz_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    date_scope = amz_date[:8]

    logical_path = "/aff/conflict65 (3).jpg"
    wire_path = b"/aff/conflict65%20(3).jpg"

    auth_header = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{date_scope}/us-east-1/s3/aws4_request, "
        "SignedHeaders=host;x-amz-date, Signature=deadbeef"
    )

    headers = {
        "Host": "s3-staging.hippius.com",
        "Authorization": auth_header,
        "x-amz-date": amz_date,
        # Content SHA is required by create_canonical_request but we stub that out
        "x-amz-content-sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    }

    request = make_request(path=logical_path, raw_path=wire_path, headers=headers)

    mock_token_response = MagicMock()
    mock_token_response.valid = True
    mock_token_response.status = "active"
    mock_token_response.account_address = account_address
    mock_token_response.token_type = token_type
    mock_token_response.encrypted_secret = "enc"
    mock_token_response.nonce = "nonce"

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

    with patch(
        "gateway.middlewares.access_key_auth.cached_auth", new_callable=AsyncMock, return_value=mock_token_response
    ):
        with patch("gateway.middlewares.access_key_auth.decrypt_secret", return_value="secret"):
            with patch(
                "gateway.middlewares.access_key_auth.create_canonical_request",
                new=fake_create_canonical_request,
            ):
                with patch("gateway.middlewares.access_key_auth.calculate_signature", return_value="deadbeef"):
                    is_valid, out_account, out_token_type = await verify_access_key_signature(
                        request, access_key, mock_redis
                    )

    assert is_valid is True
    assert out_account == account_address
    assert out_token_type == token_type
    # Path used for signing must be the percent-encoded wire path
    assert captured_path == "/aff/conflict65%20(3).jpg"


@pytest.mark.asyncio
async def test_access_key_signature_raises_when_raw_path_missing() -> None:
    """
    If raw_path is not available in the ASGI scope, access key verification
    should fail fast rather than attempting to re-encode the logical URL path.
    """
    access_key = "hip_test_key_12345"

    amz_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    date_scope = amz_date[:8]

    logical_path = "/aff/conflict65 (3).jpg"

    auth_header = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{date_scope}/us-east-1/s3/aws4_request, "
        "SignedHeaders=host;x-amz-date, Signature=deadbeef"
    )

    headers = {
        "Host": "s3-staging.hippius.com",
        "Authorization": auth_header,
        "x-amz-date": amz_date,
        "x-amz-content-sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    }

    # Note: no raw_path in scope -> canonical_path_from_scope should raise
    request = make_request(path=logical_path, raw_path=None, headers=headers)

    mock_token_response = MagicMock()
    mock_token_response.valid = True
    mock_token_response.status = "active"
    mock_token_response.account_address = "5FH2aQUbix3nNatzST4mPM8iuebGvSMFerZLdwvDmAwRDFep"
    mock_token_response.token_type = "sub"
    mock_token_response.encrypted_secret = "enc"
    mock_token_response.nonce = "nonce"

    with patch(
        "gateway.middlewares.access_key_auth.cached_auth", new_callable=AsyncMock, return_value=mock_token_response
    ):
        with patch("gateway.middlewares.access_key_auth.decrypt_secret", return_value="secret"):
            with pytest.raises(RuntimeError):
                await verify_access_key_signature(request, access_key, mock_redis)
