"""Bearer access-key auth is refused, and header SigV4 enforces the x-amz-date skew.

An access key ID is public (X-Amz-Credential in every presigned URL and SigV4
header), so it must never authenticate on its own. A header-signed request is
only valid within 15 minutes of the gateway clock (AWS RequestTimeTooSkewed), so
a captured signed request cannot be replayed later.
"""

import datetime
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from fastapi import Request

from hippius_s3.gateway.middlewares.access_key_auth import AccessKeyAuthError
from hippius_s3.gateway.middlewares.access_key_auth import RequestTimeTooSkewedError
from hippius_s3.gateway.middlewares.access_key_auth import check_request_time
from hippius_s3.gateway.middlewares.access_key_auth import verify_access_key_signature
from hippius_s3.gateway.services.auth_orchestrator import authenticate_request


ACCOUNT = "5FH2aQUbix3nNatzST4mPM8iuebGvSMFerZLdwvDmAwRDFep"
KEY = "hip_master_key_123"
NOW = datetime.datetime(2026, 11, 9, 12, 0, 0, tzinfo=datetime.timezone.utc)


def _amz(moment: datetime.datetime) -> str:
    return moment.strftime("%Y%m%dT%H%M%SZ")


def _request(headers: dict[str, str], method: str = "GET", path: str = "/bucket/key") -> Request:
    scope: dict[str, Any] = {
        "type": "http",
        "method": method,
        "path": path,
        "raw_path": path.encode(),
        "scheme": "http",
        "server": ("testserver", 80),
        "headers": [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in headers.items()],
        "query_string": b"",
        "state": {"ray_id": "test"},
    }
    request = Request(scope)
    app = MagicMock()
    app.state.redis_client = AsyncMock()
    app.state.hippius_api_client = None
    scope["app"] = app
    return request


def _active_master() -> MagicMock:
    token = MagicMock()
    token.valid = True
    token.status = "active"
    token.account_address = ACCOUNT
    token.token_type = "master"
    token.encrypted_secret = "enc"
    token.nonce = "nonce"
    return token


def _signed_headers(amz_date: str, scope_date: str | None = None) -> dict[str, str]:
    scope_date = scope_date or amz_date[:8]
    return {
        "host": "s3.hippius.com",
        "authorization": (
            f"AWS4-HMAC-SHA256 Credential={KEY}/{scope_date}/us-east-1/s3/aws4_request, "
            f"SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={'a' * 64}"
        ),
        "x-amz-date": amz_date,
        "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
    }


# --- Bearer -------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["GET", "PUT", "DELETE"])
async def test_bearer_master_key_id_is_refused_without_any_lookup(method: str) -> None:
    """The reported attack: `Authorization: Bearer <master key ID>` used to give owner
    access. It is now refused before any key lookup."""
    lookup = AsyncMock(return_value=_active_master())
    with (
        patch("hippius_s3.gateway.services.auth_cache.cached_auth", lookup),
        patch("hippius_s3.gateway.middlewares.access_key_auth.cached_auth", lookup),
    ):
        result = await authenticate_request(_request({"authorization": f"Bearer {KEY}"}, method=method))

    assert result.is_valid is False
    assert result.error_response is not None
    assert result.error_response.status_code == 403
    assert b"InvalidRequest" in result.error_response.body
    lookup.assert_not_called()


@pytest.mark.asyncio
async def test_bearer_non_hip_token_is_refused() -> None:
    result = await authenticate_request(_request({"authorization": "Bearer whatever"}))
    assert result.is_valid is False
    assert result.error_response is not None
    assert result.error_response.status_code == 403


# --- x-amz-date skew ----------------------------------------------------------


@pytest.mark.parametrize(
    "delta", [datetime.timedelta(0), datetime.timedelta(minutes=14, seconds=59), -datetime.timedelta(minutes=15)]
)
def test_request_time_within_15_minutes_passes(delta: datetime.timedelta) -> None:
    check_request_time(_amz(NOW + delta), now=NOW)


@pytest.mark.parametrize(
    "delta", [datetime.timedelta(minutes=15, seconds=1), -datetime.timedelta(minutes=16), datetime.timedelta(days=365)]
)
def test_request_time_outside_15_minutes_is_skewed(delta: datetime.timedelta) -> None:
    with pytest.raises(RequestTimeTooSkewedError):
        check_request_time(_amz(NOW + delta), now=NOW)


@pytest.mark.parametrize("value", ["", "2026-11-09T12:00:00Z", "20261109", "garbage"])
def test_malformed_request_time_is_refused(value: str) -> None:
    with pytest.raises(AccessKeyAuthError):
        check_request_time(value, now=NOW)


@pytest.mark.asyncio
async def test_replayed_signed_request_is_refused_before_the_key_lookup() -> None:
    """A captured header-signed request from yesterday: refused on its date alone."""
    stale = _amz(datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1))
    lookup = AsyncMock(return_value=_active_master())
    with patch("hippius_s3.gateway.middlewares.access_key_auth.cached_auth", lookup):
        with pytest.raises(RequestTimeTooSkewedError):
            await verify_access_key_signature(_request(_signed_headers(stale)), KEY, AsyncMock())
    lookup.assert_not_called()


@pytest.mark.asyncio
async def test_orchestrator_answers_request_time_too_skewed() -> None:
    stale = _amz(datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=2))
    with patch("hippius_s3.gateway.middlewares.access_key_auth.cached_auth", AsyncMock(return_value=_active_master())):
        result = await authenticate_request(_request(_signed_headers(stale)))
    assert result.is_valid is False
    assert result.error_response is not None
    assert result.error_response.status_code == 403
    assert b"RequestTimeTooSkewed" in result.error_response.body


@pytest.mark.asyncio
async def test_credential_scope_date_must_match_amz_date() -> None:
    now = datetime.datetime.now(datetime.timezone.utc)
    headers = _signed_headers(_amz(now), scope_date=_amz(now - datetime.timedelta(days=2))[:8])
    with (
        patch("hippius_s3.gateway.middlewares.access_key_auth.cached_auth", AsyncMock(return_value=_active_master())),
        patch("hippius_s3.gateway.middlewares.access_key_auth.decrypt_secret", return_value="secret"),
    ):
        with pytest.raises(AccessKeyAuthError, match="scope date"):
            await verify_access_key_signature(_request(headers), KEY, AsyncMock())


@pytest.mark.asyncio
async def test_current_signed_request_still_verifies() -> None:
    """Regression guard: a fresh, correctly signed request is unaffected."""
    now = _amz(datetime.datetime.now(datetime.timezone.utc))
    with (
        patch("hippius_s3.gateway.middlewares.access_key_auth.cached_auth", AsyncMock(return_value=_active_master())),
        patch("hippius_s3.gateway.middlewares.access_key_auth.decrypt_secret", return_value="secret"),
        patch(
            "hippius_s3.gateway.middlewares.access_key_auth.create_canonical_request",
            AsyncMock(return_value="canonical"),
        ),
        patch("hippius_s3.gateway.middlewares.access_key_auth.calculate_signature", return_value="a" * 64),
    ):
        auth = await verify_access_key_signature(_request(_signed_headers(now)), KEY, AsyncMock())
    assert auth.account_address == ACCOUNT
    assert auth.token_type == "master"
