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

from hippius_s3.gateway.middlewares.auth_router import ALL_EXEMPT_SEGMENTS
from hippius_s3.gateway.middlewares.input_validation import RESERVED_BUCKET_SEGMENTS
from hippius_s3.gateway.utils.paths import first_path_segment


@pytest.fixture  # type: ignore[misc]
def auth_router_app() -> Any:
    from hippius_s3.gateway.middlewares.auth_router import auth_router_middleware

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
        else:
            return {"auth_method": auth_method}

    @app.put("/test")
    async def put_test_endpoint(request: Request) -> dict[str, str]:
        return {"message": "ok"}

    @app.api_route("/purge-target/{bucket}/{key:path}", methods=["PURGE"])
    async def purge_endpoint(request: Request, bucket: str, key: str) -> dict[str, str]:
        return {"message": "purged", "bucket": bucket, "key": key}

    @app.get("/health")
    async def health_endpoint() -> dict[str, str]:
        return {"status": "healthy"}

    @app.api_route("/{path:path}", methods=["GET", "PUT"])
    async def catch_all(request: Request, path: str) -> dict[str, Any]:
        return {"auth_method": getattr(request.state, "auth_method", None)}

    app.middleware("http")(auth_router_middleware)

    return app


@pytest.mark.asyncio
async def test_exempt_paths_bypass_auth(auth_router_app: Any) -> None:
    """Test that exempt paths bypass authentication"""
    async with AsyncClient(transport=ASGITransport(app=auth_router_app), base_url="http://test") as client:
        response = await client.get("/health")

    assert response.status_code == 200


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    ["/docs2", "/docsite", "/metrics-test", "/healthz", "/robots.txt.bak", "/openapi.json2", "/userdata"],
)
async def test_reserved_prefixed_bucket_names_still_require_auth(auth_router_app: Any, path: str) -> None:
    """A bucket name that merely STARTS with a reserved segment must still be
    authenticated. The old bare startswith() match let PUT /docs2 skip SigV4
    entirely and land as an anonymous-owned bucket (prod incident 2026-08-03)."""
    async with AsyncClient(transport=ASGITransport(app=auth_router_app), base_url="http://test") as client:
        response = await client.put(path, content=b"test data")

    assert response.status_code == 403
    assert b"InvalidAccessKeyId" in response.content


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    ["/docs", "/docs/cache", "/user/profile", "/metrics", "/health", "/openapi.json", "/robots.txt"],
)
async def test_reserved_paths_and_subpaths_bypass_auth(auth_router_app: Any, path: str) -> None:
    """Exact reserved segments (and their subpaths) stay auth-exempt."""
    async with AsyncClient(transport=ASGITransport(app=auth_router_app), base_url="http://test") as client:
        response = await client.put(path, content=b"test data")

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_bare_user_path_still_requires_auth(auth_router_app: Any) -> None:
    """Only `/user/...` is a frontend route; a bare `/user` is bucket-shaped. acl.py and
    account.py both special-case `/user/` WITH the slash, so exempting the bare segment here
    would make auth_router the only layer treating it as non-S3 — a widening, in a change
    whose whole point is narrowing."""
    async with AsyncClient(transport=ASGITransport(app=auth_router_app), base_url="http://test") as client:
        response = await client.put("/user", content=b"test data")

    assert response.status_code == 403
    assert b"InvalidAccessKeyId" in response.content


def test_every_auth_exempt_segment_is_a_reserved_bucket_name() -> None:
    """Any segment auth_router skips authentication for MUST be unusable as a bucket name:
    a bucket created under it is stamped with no owner, is invisible to its creator, and
    permanently locks the globally-unique name (prod incident 2026-08-03). These two sets
    live in different modules, so nothing but this test keeps them honest."""
    assert ALL_EXEMPT_SEGMENTS <= RESERVED_BUCKET_SEGMENTS


@pytest.mark.parametrize(
    ("raw_path", "api_segment"),
    [
        # httpx truncates the forwarded target at the `#`, so the api routes on `/docs` — and
        # judging this as the bucket `docs#x` (which is what reading the raw path gave) meant
        # auth_router and the api disagreed about which request was being let through.
        (b"/docs%23x", "docs"),
        # The traversal, both spellings: exempt as sent, an ordinary S3 request as forwarded.
        (b"/docs/%2E%2E/anybucket/key.txt", "anybucket"),
        (b"/docs/../anybucket/key.txt", "anybucket"),
        # Prefix is not segment: the `docs2` hole.
        (b"/docs2", "docs2"),
    ],
)
def test_exempt_segments_are_matched_on_the_path_the_api_will_receive(raw_path: bytes, api_segment: str) -> None:
    """auth_router, input_validation and the api must agree on where a path starts.

    They now agree on the api's own answer rather than on a shared-but-arbitrary lens: whatever
    httpx forwards is what actually routes, so it is the only view that cannot be wrong. Neither
    `request.url.path` (truncates at `#`) nor the raw decoded path (keeps dot segments httpx
    removes) is that view.
    """
    request = MagicMock()
    request.scope = {"raw_path": raw_path}

    assert first_path_segment(request) == api_segment


def test_a_traversal_out_of_an_exempt_route_is_not_exempt() -> None:
    """The consequence of the above, stated on the exempt set itself.

    `/docs/../anybucket/key.txt` is served from `anybucket`; treating it as the `docs` route skipped
    authentication and processed it as anonymous.
    """
    request = MagicMock()
    request.scope = {"raw_path": b"/docs/%2E%2E/anybucket/key.txt"}

    assert first_path_segment(request) not in ALL_EXEMPT_SEGMENTS


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
        "hippius_s3.gateway.middlewares.access_key_auth.cached_auth",
        new_callable=AsyncMock,
        return_value=mock_token_response,
    ):
        with patch("hippius_s3.gateway.middlewares.access_key_auth.config") as mock_config:
            mock_config.hippius_secret_decryption_material = key_hex

            with patch(
                "hippius_s3.gateway.middlewares.access_key_auth.calculate_signature", return_value="abc123def456"
            ):
                with patch(
                    "hippius_s3.gateway.middlewares.access_key_auth.create_canonical_request", return_value="canonical"
                ):
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
async def test_seed_phrase_credential_returns_deprecation_message(auth_router_app: Any) -> None:
    """A well-formed SigV4 header with a non-hip_ credential (the removed seed-phrase shape)
    is rejected with a deprecation pointer to token docs, not a bare invalid-key error."""
    # base64-ish non-hip_ credential — the shape seed-phrase auth used as the access key id
    auth_header = (
        "AWS4-HMAC-SHA256 Credential=d29yZCB3b3JkIHdvcmQ/20250101/us-east-1/s3/aws4_request, "
        "SignedHeaders=host;x-amz-date, Signature=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    )

    async with AsyncClient(transport=ASGITransport(app=auth_router_app), base_url="http://test") as client:
        response = await client.put("/test", headers={"Authorization": auth_header}, content=b"test data")

    assert response.status_code == 403
    assert b"InvalidAccessKeyId" in response.content
    assert b"Seed phrase authentication is deprecated" in response.content
    assert b"docs.hippius.com/storage/s3/integration" in response.content


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
        "hippius_s3.gateway.middlewares.access_key_auth.cached_auth",
        new_callable=AsyncMock,
        return_value=mock_token_response,
    ):
        with patch("hippius_s3.gateway.middlewares.access_key_auth.config") as mock_config:
            mock_config.hippius_secret_decryption_material = key_hex

            with patch(
                "hippius_s3.gateway.middlewares.access_key_auth.calculate_signature", return_value="correct_signature"
            ):
                with patch(
                    "hippius_s3.gateway.middlewares.access_key_auth.create_canonical_request", return_value="canonical"
                ):
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
    from hippius_s3.gateway.middlewares.access_key_auth import TokenAuth

    mock_verify = AsyncMock(
        return_value=TokenAuth(
            access_key=test_access_key,
            account_address=test_account_address,
            token_type=test_token_type,
        )
    )

    query_params = {
        "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
        "X-Amz-Credential": f"{test_access_key}/20250101/us-east-1/s3/aws4_request",
        "X-Amz-Date": "20250101T000000Z",
        "X-Amz-Expires": "3600",
        "X-Amz-SignedHeaders": "host",
        "X-Amz-Signature": "deadbeef",
    }

    with patch("hippius_s3.gateway.services.auth_orchestrator.verify_access_key_presigned_url", mock_verify):
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


# ---------------------------------------------------------------------------
# PURGE-from-ATS-authproxy bypass
# ---------------------------------------------------------------------------
#
# When ATS authproxy is in front of the gateway, gateway-initiated PURGEs to
# ATS get bounced back to the gateway as auth subrequests carrying a stamped
# X-Hippius-Auth-Probe header. Those PURGEs have no Authorization header, so
# the regular auth_router rule (PUT/POST/DELETE/PURGE without Authorization →
# 403) would block them and ats_purge_middleware's invalidation-on-write
# would break. Bypass auth when method == PURGE AND the probe secret matches.
PURGE_PROBE_SECRET = "fake-test-probe-secret-not-real"


@pytest.fixture  # type: ignore[misc]
def configured_probe_secret(monkeypatch: pytest.MonkeyPatch) -> str:
    import dataclasses

    from hippius_s3.config import get_config
    from hippius_s3.gateway.middlewares import auth_probe as auth_probe_mod

    cfg = dataclasses.replace(get_config(), auth_probe_secret=PURGE_PROBE_SECRET)
    monkeypatch.setattr(auth_probe_mod, "get_config", lambda: cfg)
    return PURGE_PROBE_SECRET


@pytest.mark.asyncio
async def test_purge_with_valid_probe_secret_bypasses_auth(auth_router_app: Any, configured_probe_secret: str) -> None:
    """PURGE with the matching probe secret skips auth_router validation
    (no Authorization header needed)."""
    async with AsyncClient(transport=ASGITransport(app=auth_router_app), base_url="http://test") as client:
        response = await client.request(
            "PURGE",
            "/purge-target/some-bucket/some/key.txt",
            headers={"x-hippius-auth-probe": configured_probe_secret},
        )
    assert response.status_code == 200
    assert response.json()["message"] == "purged"


@pytest.mark.asyncio
async def test_purge_without_probe_header_returns_403(auth_router_app: Any, configured_probe_secret: str) -> None:
    """PURGE without the probe header still gets the existing 403 — only the
    ATS authproxy bounce-back can bypass."""
    async with AsyncClient(transport=ASGITransport(app=auth_router_app), base_url="http://test") as client:
        response = await client.request("PURGE", "/purge-target/some-bucket/some/key.txt")
    assert response.status_code == 403
    assert b"InvalidAccessKeyId" in response.content


@pytest.mark.asyncio
async def test_purge_with_wrong_probe_value_returns_403(auth_router_app: Any, configured_probe_secret: str) -> None:
    """Constant-time compare protects against guessed probe values."""
    for bad in ("", "1", "wrong-secret", PURGE_PROBE_SECRET[:-1], PURGE_PROBE_SECRET + "x"):
        async with AsyncClient(transport=ASGITransport(app=auth_router_app), base_url="http://test") as client:
            response = await client.request(
                "PURGE",
                "/purge-target/some-bucket/some/key.txt",
                headers={"x-hippius-auth-probe": bad},
            )
        assert response.status_code == 403, f"value={bad!r}"


@pytest.mark.asyncio
async def test_purge_with_probe_when_secret_unset_returns_403(auth_router_app: Any) -> None:
    """Fail-closed: if HIPPIUS_AUTH_PROBE_SECRET is unset, the bypass is
    disabled and PURGE without auth gets the standard 403."""
    async with AsyncClient(transport=ASGITransport(app=auth_router_app), base_url="http://test") as client:
        response = await client.request(
            "PURGE",
            "/purge-target/some-bucket/some/key.txt",
            headers={"x-hippius-auth-probe": "anything"},
        )
    assert response.status_code == 403
