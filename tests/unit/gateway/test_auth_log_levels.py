"""Log-level regression tests for client-driven auth rejections.

Client-driven auth rejections (malformed credentials, bad/expired presigned
params, missing signing headers) must log at WARNING so they don't pollute the
ERROR dashboards — these are expected 4xx rejections, not server faults. Genuine
upstream failures (HippiusAPIError) must STAY at ERROR. These tests assert on
record.levelname explicitly and are deliberately failure-path only.
"""

import datetime
import logging
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch
from urllib.parse import urlencode

import pytest
from fastapi import Request

from hippius_s3.gateway.middlewares.access_key_auth import AccessKeyAuthError
from hippius_s3.gateway.middlewares.access_key_auth import verify_access_key_presigned_url
from hippius_s3.gateway.middlewares.sigv4 import AuthParsingError
from hippius_s3.gateway.middlewares.sigv4 import create_canonical_request
from hippius_s3.gateway.services.auth_orchestrator import _authenticate_access_key_header
from hippius_s3.gateway.services.auth_orchestrator import authenticate_request
from hippius_s3.services.hippius_api_service import HippiusAPIError


def make_request(
    method: str = "GET",
    path: str = "/bucket/key",
    query_params: dict[str, str] | None = None,
    headers: dict[str, str] | None = None,
) -> Request:
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
        "raw_path": path.encode("latin-1"),
        "state": {},
        "app": MagicMock(),
    }
    return Request(scope)


def _records(caplog: pytest.LogCaptureFixture, needle: str) -> list[logging.LogRecord]:
    return [r for r in caplog.records if needle in r.getMessage()]


@pytest.mark.asyncio
async def test_malformed_credential_logs_warning_not_error(caplog: pytest.LogCaptureFixture) -> None:
    """A SigV4 header that fails credential extraction (client input) -> WARNING, never ERROR."""
    caplog.set_level(logging.DEBUG, logger="hippius_s3.gateway.services.auth_orchestrator")

    # Well-formed enough to be treated as a SigV4 header, but no parseable Credential=.
    request = make_request(
        method="PUT",
        headers={"authorization": "AWS4-HMAC-SHA256 this-is-not-a-valid-credential-line"},
    )

    result = await authenticate_request(request)

    assert result.is_valid is False
    matched = _records(caplog, "Failed to extract credential")
    assert matched, "expected the credential-extraction failure to be logged"
    assert all(r.levelname == "WARNING" for r in matched)
    assert not any(r.levelname == "ERROR" for r in matched)


@pytest.mark.asyncio
async def test_presigned_invalid_credential_format_logs_warning(caplog: pytest.LogCaptureFixture) -> None:
    """A presigned URL with a malformed X-Amz-Credential (client input) -> WARNING, never ERROR."""
    caplog.set_level(logging.DEBUG, logger="hippius_s3.gateway.middlewares.access_key_auth")

    access_key = "hip_presigned_key_12345"
    query_params = {
        "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
        "X-Amz-Credential": access_key,  # missing /date/region/service/aws4_request -> <5 parts
        "X-Amz-Date": "20260101T000000Z",
        "X-Amz-Expires": "3600",
        "X-Amz-SignedHeaders": "host",
        "X-Amz-Signature": "deadbeef",
    }
    request = make_request(query_params=query_params)

    with pytest.raises(AccessKeyAuthError):
        await verify_access_key_presigned_url(request, access_key, AsyncMock())

    matched = _records(caplog, "Invalid X-Amz-Credential format")
    assert matched, "expected the invalid-credential-format rejection to be logged"
    assert all(r.levelname == "WARNING" for r in matched)
    assert not any(r.levelname == "ERROR" for r in matched)


@pytest.mark.asyncio
async def test_presigned_expired_signature_mismatch_logs_warning(caplog: pytest.LogCaptureFixture) -> None:
    """A presigned URL whose signature doesn't match (client input) -> WARNING, never ERROR."""
    caplog.set_level(logging.DEBUG, logger="hippius_s3.gateway.middlewares.access_key_auth")

    access_key = "hip_presigned_key_67890"
    now = datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_scope = amz_date[:8]
    query_params = {
        "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
        "X-Amz-Credential": f"{access_key}/{date_scope}/us-east-1/s3/aws4_request",
        "X-Amz-Date": amz_date,
        "X-Amz-Expires": "3600",
        "X-Amz-SignedHeaders": "host",
        "X-Amz-Signature": "clientprovidedsig",
    }
    request = make_request(query_params=query_params, headers={"host": "s3.hippius.com"})

    token_response = AsyncMock()
    token_response.valid = True
    token_response.status = "active"
    token_response.account_address = "5FH2aQUbix3nNatzST4mPM8iuebGvSMFerZLdwvDmAwRDFep"
    token_response.token_type = "sub"
    token_response.encrypted_secret = "enc"
    token_response.nonce = "nonce"

    with patch("hippius_s3.gateway.middlewares.access_key_auth.cached_auth", new_callable=AsyncMock, return_value=token_response):
        with patch("hippius_s3.gateway.middlewares.access_key_auth.decrypt_secret", return_value="secret"):
            with patch(
                "hippius_s3.gateway.middlewares.access_key_auth.create_canonical_request",
                new_callable=AsyncMock,
                return_value="canonical",
            ):
                with patch("hippius_s3.gateway.middlewares.access_key_auth.calculate_signature", return_value="serversidesig"):
                    with pytest.raises(AccessKeyAuthError):
                        await verify_access_key_presigned_url(request, access_key, AsyncMock())

    matched = _records(caplog, "Presigned URL signature mismatch")
    assert matched, "expected the signature-mismatch rejection to be logged"
    assert all(r.levelname == "WARNING" for r in matched)
    assert not any(r.levelname == "ERROR" for r in matched)


@pytest.mark.asyncio
async def test_upstream_hippius_api_error_stays_error(caplog: pytest.LogCaptureFixture) -> None:
    """A genuine upstream failure (HippiusAPIError) must STILL log at ERROR — not downgraded."""
    caplog.set_level(logging.DEBUG, logger="hippius_s3.gateway.services.auth_orchestrator")

    request = make_request(method="PUT")
    logger = logging.getLogger("hippius_s3.gateway.services.auth_orchestrator")

    with patch(
        "hippius_s3.gateway.services.auth_orchestrator.verify_access_key_signature",
        new_callable=AsyncMock,
        side_effect=HippiusAPIError("arion down"),
    ):
        result = await _authenticate_access_key_header(request, "hip_some_key_12345", logger)

    assert result.is_valid is False
    matched = _records(caplog, "Hippius API error during auth")
    assert matched, "expected the upstream failure to be logged"
    assert all(r.levelname == "ERROR" for r in matched)
    assert not any(r.levelname == "WARNING" for r in matched)


@pytest.mark.asyncio
async def test_sigv4_missing_payload_hash_logs_warning(caplog: pytest.LogCaptureFixture) -> None:
    """A non-presigned request missing x-amz-content-sha256 (client input) -> WARNING, never ERROR."""
    caplog.set_level(logging.DEBUG, logger="hippius_s3.gateway.middlewares.sigv4")

    request = make_request(method="PUT", headers={"host": "s3.hippius.com", "x-amz-date": "20260101T000000Z"})

    with pytest.raises(AuthParsingError):
        await create_canonical_request(
            request=request,
            signed_headers=["host", "x-amz-date"],
            method="PUT",
            path="/bucket/key",
            query_string="",
        )

    matched = _records(caplog, "Missing x-amz-content-sha256 header")
    assert matched, "expected the missing-payload-hash rejection to be logged"
    assert all(r.levelname == "WARNING" for r in matched)
    assert not any(r.levelname == "ERROR" for r in matched)
