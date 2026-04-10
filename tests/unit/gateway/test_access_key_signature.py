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


def _make_token_response(
    account_address: str = "5FH2aQUbix3nNatzST4mPM8iuebGvSMFerZLdwvDmAwRDFep",
    token_type: str = "sub",
    credits: float = 100.0,
) -> MagicMock:
    mock = MagicMock()
    mock.valid = True
    mock.status = "active"
    mock.account_address = account_address
    mock.token_type = token_type
    mock.encrypted_secret = "enc"
    mock.nonce = "nonce"
    mock.credits = credits
    return mock


def _make_auth_headers(access_key: str) -> dict[str, str]:
    amz_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    date_scope = amz_date[:8]
    auth_header = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{date_scope}/us-east-1/s3/aws4_request, "
        "SignedHeaders=host;x-amz-date, Signature=deadbeef"
    )
    return {
        "Host": "s3-staging.hippius.com",
        "Authorization": auth_header,
        "x-amz-date": amz_date,
        "x-amz-content-sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    }


@pytest.mark.asyncio
async def test_access_key_signature_uses_raw_path_for_canonical_path() -> None:
    """
    Header-signed access key verification must use raw_path from the ASGI scope
    when building the canonical request path, so that percent-encoding matches
    what the AWS client signed.
    """
    access_key = "hip_test_key_12345"
    logical_path = "/aff/conflict65 (3).jpg"
    wire_path = b"/aff/conflict65%20(3).jpg"

    headers = _make_auth_headers(access_key)
    request = make_request(path=logical_path, raw_path=wire_path, headers=headers)

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
                    result = await verify_access_key_signature(request, access_key, mock_redis)

    assert result.account_address == "5FH2aQUbix3nNatzST4mPM8iuebGvSMFerZLdwvDmAwRDFep"
    assert result.token_type == "sub"
    # Path used for signing must be the percent-encoded wire path
    assert captured_path == "/aff/conflict65%20(3).jpg"


@pytest.mark.asyncio
async def test_access_key_signature_raises_when_raw_path_missing() -> None:
    """
    If raw_path is not available in the ASGI scope, access key verification
    should fail fast rather than attempting to re-encode the logical URL path.
    """
    access_key = "hip_test_key_12345"
    logical_path = "/aff/conflict65 (3).jpg"

    headers = _make_auth_headers(access_key)
    # Note: no raw_path in scope -> canonical_path_from_scope should raise
    request = make_request(path=logical_path, raw_path=None, headers=headers)

    with patch("gateway.middlewares.access_key_auth.cached_auth", AsyncMock(return_value=_make_token_response())):
        with patch("gateway.middlewares.access_key_auth.decrypt_secret", return_value="secret"):
            with pytest.raises(RuntimeError):
                await verify_access_key_signature(request, access_key, mock_redis)
