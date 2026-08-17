"""Unit tests for account middleware bypass mode and account ID derivation."""

from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import httpx
import pytest
from fastapi import FastAPI
from fastapi import Request
from httpx import ASGITransport
from httpx import AsyncClient

from hippius_s3.models.account import HippiusAccount
from tests.unit.mocks.mock_arion_service import MockArionService


@pytest.fixture  # type: ignore[misc]
def mock_config_bypass() -> Any:
    """Mock config with enable_bypass_credit_check enabled."""
    config = MagicMock()
    config.enable_bypass_credit_check = True
    return config


@pytest.fixture  # type: ignore[misc]
def mock_config_no_bypass() -> Any:
    """Mock config with enable_bypass_credit_check disabled."""
    config = MagicMock()
    config.enable_bypass_credit_check = False
    config.substrate_url = "ws://localhost:9944"
    config.can_upload_cache_ttl_seconds = 10
    config.can_upload_transient_retries = 2
    config.can_upload_transient_retry_delay_seconds = 0.0
    return config


@pytest.fixture  # type: ignore[misc]
def account_app_bypass(mock_config_bypass: Any, monkeypatch: Any) -> Any:
    """FastAPI app with account middleware in bypass mode."""
    from hippius_s3.gateway.middlewares.account import account_middleware

    monkeypatch.setattr("hippius_s3.gateway.middlewares.account.config", mock_config_bypass)

    app = FastAPI()

    @app.get("/test")
    async def test_endpoint(request: Request) -> dict[str, Any]:
        account_id = request.state.account_id if hasattr(request.state, "account_id") else None
        account = request.state.account if hasattr(request.state, "account") else None
        return {"account_id": account_id, "account": account}

    app.middleware("http")(account_middleware)

    return app


@pytest.mark.asyncio
async def test_bypass_mode_no_auth_returns_anonymous(mock_config_bypass: Any, monkeypatch: Any) -> None:
    """Test that requests without authentication get anonymous account ID."""
    from hippius_s3.gateway.middlewares.account import account_middleware

    monkeypatch.setattr("hippius_s3.gateway.middlewares.account.config", mock_config_bypass)

    app = FastAPI()

    @app.get("/test")
    async def test_endpoint(request: Request) -> dict[str, Any]:
        account_id = request.state.account_id
        account = request.state.account
        return {"account_id": account_id, "account": account}

    app.middleware("http")(account_middleware)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/test")

    assert response.status_code == 200
    data = response.json()
    assert data["account_id"] == "anonymous"
    assert data["account"]["id"] == "anonymous"


# ---------------------------------------------------------------------------
# can_upload integration tests (non-bypass mode, access_key auth)
# ---------------------------------------------------------------------------


def _make_can_upload_app(
    mock_config: Any,
    mock_arion: MockArionService,
    monkeypatch: Any,
) -> FastAPI:
    """Build a FastAPI app wired with account middleware, mock arion, and mock account fetching."""
    from hippius_s3.gateway.middlewares.account import account_middleware

    monkeypatch.setattr("hippius_s3.gateway.middlewares.account.config", mock_config)

    account = HippiusAccount(
        id="5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
        main_account="5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
        has_credits=True,
        upload=True,
        delete=True,
    )

    async def mock_fetch_account_by_main_address(address: str, redis_client: Any, substrate_url: str) -> HippiusAccount:
        return account

    monkeypatch.setattr(
        "hippius_s3.gateway.middlewares.account.fetch_account_by_main_address",
        mock_fetch_account_by_main_address,
    )

    app = FastAPI()
    mock_redis_accounts = AsyncMock()
    mock_redis_accounts.get = AsyncMock(return_value=None)
    mock_redis_accounts.set = AsyncMock()
    app.state.redis_accounts_client = mock_redis_accounts
    app.state.arion_client = mock_arion

    @app.api_route("/test-bucket/test-key", methods=["GET", "PUT", "POST", "DELETE", "HEAD"])
    async def test_endpoint(request: Request) -> dict[str, str]:
        return {"status": "ok"}

    async def inject_access_key(request: Request, call_next: Any) -> Any:
        request.state.auth_method = "access_key"
        request.state.account_address = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
        return await call_next(request)

    app.middleware("http")(account_middleware)
    app.middleware("http")(inject_access_key)

    return app


@pytest.mark.asyncio
async def test_can_upload_allows_when_result_true(mock_config_no_bypass: Any, monkeypatch: Any) -> None:
    mock_arion = MockArionService(allow_upload=True)
    app = _make_can_upload_app(mock_config_no_bypass, mock_arion, monkeypatch)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put("/test-bucket/test-key", content=b"hello", headers={"content-length": "5"})

    assert response.status_code == 200
    assert len(mock_arion.can_upload_calls) == 1


@pytest.mark.asyncio
async def test_can_upload_blocks_when_result_false(mock_config_no_bypass: Any, monkeypatch: Any) -> None:
    mock_arion = MockArionService(allow_upload=False, upload_error="Quota exceeded")
    app = _make_can_upload_app(mock_config_no_bypass, mock_arion, monkeypatch)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put("/test-bucket/test-key", content=b"hello", headers={"content-length": "5"})

    assert response.status_code == 402
    assert b"Quota exceeded" in response.content


@pytest.mark.asyncio
async def test_can_upload_skipped_for_get(mock_config_no_bypass: Any, monkeypatch: Any) -> None:
    mock_arion = MockArionService(allow_upload=True)
    app = _make_can_upload_app(mock_config_no_bypass, mock_arion, monkeypatch)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/test-bucket/test-key")

    assert response.status_code == 200
    assert len(mock_arion.can_upload_calls) == 0


@pytest.mark.asyncio
async def test_can_upload_skipped_for_delete(mock_config_no_bypass: Any, monkeypatch: Any) -> None:
    mock_arion = MockArionService(allow_upload=True)
    app = _make_can_upload_app(mock_config_no_bypass, mock_arion, monkeypatch)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.delete("/test-bucket/test-key")

    assert response.status_code == 200
    assert len(mock_arion.can_upload_calls) == 0


@pytest.mark.asyncio
async def test_can_upload_sends_correct_content_length(mock_config_no_bypass: Any, monkeypatch: Any) -> None:
    mock_arion = MockArionService(allow_upload=True)
    app = _make_can_upload_app(mock_config_no_bypass, mock_arion, monkeypatch)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put(
            "/test-bucket/test-key",
            content=b"x" * 1024,
            headers={"content-length": "1024"},
        )

    assert response.status_code == 200
    assert mock_arion.can_upload_calls[0] == (
        "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
        1024,
    )


@pytest.mark.asyncio
async def test_can_upload_fails_closed_on_arion_error(mock_config_no_bypass: Any, monkeypatch: Any) -> None:
    mock_arion = MockArionService(raise_on_can_upload=httpx.ConnectError("connection refused"))
    app = _make_can_upload_app(mock_config_no_bypass, mock_arion, monkeypatch)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put("/test-bucket/test-key", content=b"hello", headers={"content-length": "5"})

    assert response.status_code == 503


@pytest.mark.asyncio
async def test_can_upload_retries_transient_billing_then_succeeds(mock_config_no_bypass: Any, monkeypatch: Any) -> None:
    """A transient billing-service failure ('Failed to fetch billing balance') is retried; if a
    subsequent attempt succeeds the request proceeds (200), not a spurious 402."""
    from hippius_s3.services.arion_service import CanUploadResponse

    mock_arion = MockArionService(
        can_upload_results=[
            CanUploadResponse(result=False, error="Failed to fetch billing balance"),
            CanUploadResponse(result=True, error=None),
        ]
    )
    app = _make_can_upload_app(mock_config_no_bypass, mock_arion, monkeypatch)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put("/test-bucket/test-key", content=b"hello", headers={"content-length": "5"})

    assert response.status_code == 200
    assert len(mock_arion.can_upload_calls) == 2, "must retry the transient billing failure"


@pytest.mark.asyncio
async def test_can_upload_transient_billing_returns_503_not_402(mock_config_no_bypass: Any, monkeypatch: Any) -> None:
    """A persistent transient billing failure surfaces a retryable 503 SlowDown, never a hard 402
    that a client reads as 'insufficient funds'."""
    mock_arion = MockArionService(allow_upload=False, upload_error="Failed to fetch billing balance")
    app = _make_can_upload_app(mock_config_no_bypass, mock_arion, monkeypatch)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put("/test-bucket/test-key", content=b"hello", headers={"content-length": "5"})

    assert response.status_code == 503
    assert b"SlowDown" in response.content
    # initial attempt + can_upload_transient_retries
    assert len(mock_arion.can_upload_calls) == 3


@pytest.mark.asyncio
async def test_can_upload_genuine_denial_stays_402_and_is_not_retried(
    mock_config_no_bypass: Any, monkeypatch: Any
) -> None:
    """A genuine out-of-credit denial must return 402 and must NOT be retried — even when the
    message contains the words 'billing balance' (e.g. 'Insufficient billing balance'). Guards
    against widening the transient classifier into silently converting real 402s into 503s."""
    mock_arion = MockArionService(allow_upload=False, upload_error="Insufficient billing balance")
    app = _make_can_upload_app(mock_config_no_bypass, mock_arion, monkeypatch)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put("/test-bucket/test-key", content=b"hello", headers={"content-length": "5"})

    assert response.status_code == 402
    assert b"UploadNotPermitted" in response.content
    assert len(mock_arion.can_upload_calls) == 1, "a genuine denial must not be retried"


@pytest.mark.asyncio
async def test_can_upload_skipped_in_bypass_mode(mock_config_bypass: Any, monkeypatch: Any) -> None:
    mock_arion = MockArionService(allow_upload=False, upload_error="should not be called")

    from hippius_s3.gateway.middlewares.account import account_middleware

    monkeypatch.setattr("hippius_s3.gateway.middlewares.account.config", mock_config_bypass)

    app = FastAPI()
    app.state.arion_client = mock_arion

    @app.put("/test-bucket/test-key")
    async def test_endpoint(request: Request) -> dict[str, str]:
        return {"status": "ok"}

    async def inject_access_key(request: Request, call_next: Any) -> Any:
        request.state.auth_method = "access_key"
        request.state.account_address = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
        return await call_next(request)

    app.middleware("http")(account_middleware)
    app.middleware("http")(inject_access_key)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put("/test-bucket/test-key", content=b"hello", headers={"content-length": "5"})

    assert response.status_code == 200
    assert len(mock_arion.can_upload_calls) == 0


# ---------------------------------------------------------------------------
# can_upload Redis cache tests
# ---------------------------------------------------------------------------


def _make_can_upload_app_with_redis(
    mock_config: Any,
    mock_arion: MockArionService,
    mock_redis_accounts: AsyncMock,
    monkeypatch: Any,
) -> FastAPI:
    """Build a FastAPI app with explicit control over the redis_accounts mock."""
    from hippius_s3.gateway.middlewares.account import account_middleware

    monkeypatch.setattr("hippius_s3.gateway.middlewares.account.config", mock_config)

    account = HippiusAccount(
        id="5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
        main_account="5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
        has_credits=True,
        upload=True,
        delete=True,
    )

    async def mock_fetch_account_by_main_address(address: str, redis_client: Any, substrate_url: str) -> HippiusAccount:
        return account

    monkeypatch.setattr(
        "hippius_s3.gateway.middlewares.account.fetch_account_by_main_address",
        mock_fetch_account_by_main_address,
    )

    app = FastAPI()
    app.state.redis_accounts_client = mock_redis_accounts
    app.state.arion_client = mock_arion

    @app.api_route("/test-bucket/test-key", methods=["GET", "PUT", "POST", "DELETE", "HEAD"])
    async def test_endpoint(request: Request) -> dict[str, str]:
        return {"status": "ok"}

    async def inject_access_key(request: Request, call_next: Any) -> Any:
        request.state.auth_method = "access_key"
        request.state.account_address = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
        return await call_next(request)

    app.middleware("http")(account_middleware)
    app.middleware("http")(inject_access_key)

    return app


@pytest.mark.asyncio
async def test_can_upload_cache_hit_skips_arion_call(mock_config_no_bypass: Any, monkeypatch: Any) -> None:
    """When Redis has a cached can_upload result, Arion should not be called."""
    mock_arion = MockArionService(allow_upload=True)
    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=b"1")
    mock_redis.set = AsyncMock()

    app = _make_can_upload_app_with_redis(mock_config_no_bypass, mock_arion, mock_redis, monkeypatch)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put("/test-bucket/test-key", content=b"hello", headers={"content-length": "5"})

    assert response.status_code == 200
    mock_redis.get.assert_called_once_with("can_upload:5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY")
    assert len(mock_arion.can_upload_calls) == 0


@pytest.mark.asyncio
async def test_can_upload_cache_miss_calls_arion_and_caches(mock_config_no_bypass: Any, monkeypatch: Any) -> None:
    """On cache miss, Arion is called and a successful result is cached with the configured TTL."""
    mock_arion = MockArionService(allow_upload=True)
    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=None)
    mock_redis.set = AsyncMock()

    app = _make_can_upload_app_with_redis(mock_config_no_bypass, mock_arion, mock_redis, monkeypatch)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put("/test-bucket/test-key", content=b"hello", headers={"content-length": "5"})

    assert response.status_code == 200
    assert len(mock_arion.can_upload_calls) == 1
    mock_redis.set.assert_called_once_with(
        "can_upload:5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
        b"1",
        ex=10,
    )


@pytest.mark.asyncio
async def test_can_upload_denial_is_not_cached(mock_config_no_bypass: Any, monkeypatch: Any) -> None:
    """When Arion denies an upload, the result must NOT be cached so users can retry after topping up."""
    mock_arion = MockArionService(allow_upload=False, upload_error="Quota exceeded")
    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=None)
    mock_redis.set = AsyncMock()

    app = _make_can_upload_app_with_redis(mock_config_no_bypass, mock_arion, mock_redis, monkeypatch)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put("/test-bucket/test-key", content=b"hello", headers={"content-length": "5"})

    assert response.status_code == 402
    assert len(mock_arion.can_upload_calls) == 1
    mock_redis.set.assert_not_called()


@pytest.mark.asyncio
async def test_can_upload_cache_simulates_multipart_upload(mock_config_no_bypass: Any, monkeypatch: Any) -> None:
    """Simulate 8-part multipart upload: only the first part should call Arion, rest hit cache."""
    mock_arion = MockArionService(allow_upload=True)

    call_count = 0

    async def mock_get(key: str) -> bytes | None:
        nonlocal call_count
        call_count += 1
        # First call returns miss, subsequent calls return hit (simulating the cache being populated)
        if call_count == 1:
            return None
        return b"1"

    mock_redis = AsyncMock()
    mock_redis.get = mock_get
    mock_redis.set = AsyncMock()

    app = _make_can_upload_app_with_redis(mock_config_no_bypass, mock_arion, mock_redis, monkeypatch)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        for i in range(8):
            response = await client.put(
                "/test-bucket/test-key",
                content=b"x" * 16384,
                headers={"content-length": "16384"},
            )
            assert response.status_code == 200

    # Only 1 Arion call (first part), the other 7 hit cache
    assert len(mock_arion.can_upload_calls) == 1


@pytest.mark.asyncio
async def test_can_upload_cache_key_is_per_account(mock_config_no_bypass: Any, monkeypatch: Any) -> None:
    """The cache key includes the account address, so different accounts don't share cache."""
    mock_arion = MockArionService(allow_upload=True)
    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=None)
    mock_redis.set = AsyncMock()

    app = _make_can_upload_app_with_redis(mock_config_no_bypass, mock_arion, mock_redis, monkeypatch)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await client.put("/test-bucket/test-key", content=b"hello", headers={"content-length": "5"})

    mock_redis.get.assert_called_with("can_upload:5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY")


@pytest.mark.asyncio
async def test_gw4_get_skips_account_fetch_but_put_fetches(mock_config_no_bypass: Any, monkeypatch: Any) -> None:
    """GW-4: access-key GET/HEAD carry no credit gate and the API ignores the credit fields, so the
    redis-accounts fetch is skipped on reads; mutating methods still fetch and gate."""
    from hippius_s3.gateway.middlewares import account as account_mod

    monkeypatch.setattr("hippius_s3.gateway.middlewares.account.config", mock_config_no_bypass)
    addr = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
    calls = {"n": 0}

    async def spy_fetch(address: str, redis_client: Any, substrate_url: str) -> HippiusAccount:
        calls["n"] += 1
        return HippiusAccount(id=addr, main_account=addr, has_credits=True, upload=True, delete=True)

    monkeypatch.setattr("hippius_s3.gateway.middlewares.account.fetch_account_by_main_address", spy_fetch)
    monkeypatch.setattr(account_mod, "_check_can_upload", AsyncMock(return_value=None))

    app = FastAPI()
    app.state.redis_accounts_client = AsyncMock()

    @app.api_route("/b/k", methods=["GET", "PUT"])
    async def ep(request: Request) -> dict[str, str]:
        return {"account_id": request.state.account_id}

    async def inject(request: Request, call_next: Any) -> Any:
        request.state.auth_method = "access_key"
        request.state.account_address = addr
        return await call_next(request)

    app.middleware("http")(account_mod.account_middleware)
    app.middleware("http")(inject)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r_get = await client.get("/b/k")
        assert r_get.status_code == 200
        assert r_get.json()["account_id"] == addr
        assert calls["n"] == 0, "GET must not fetch the account (GW-4)"

        r_put = await client.put("/b/k", content=b"x", headers={"content-length": "1"})
        assert r_put.status_code == 200
        assert calls["n"] == 1, "PUT must still fetch the account for the credit gate"


# ---------------------------------------------------------------------------
# Arion unreachable => transient, not a hard verification failure
# ---------------------------------------------------------------------------


def _http_status_error(status: int) -> httpx.HTTPStatusError:
    """What ArionClient.can_upload's raise_for_status() produces on an upstream 5xx."""
    request = httpx.Request("POST", "https://arion.hippius.com/can_upload")
    response = httpx.Response(status, request=request, text="Next Hop Connection Failed")
    return httpx.HTTPStatusError("server error", request=request, response=response)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [500, 502, 503, 504])
async def test_arion_5xx_surfaces_slowdown_not_account_verification_error(
    mock_config_no_bypass: Any, monkeypatch: Any, status: int
) -> None:
    """ArionClient.can_upload ends in raise_for_status(), so an upstream 5xx leaves as an
    exception rather than a CanUploadResponse. That used to skip the transient ladder entirely
    and land in the blanket `except Exception`, answering AccountVerificationError — a code no
    S3 SDK knows to retry. Observed on prod when the ATS edge in front of Arion had all next
    hops down: 5948 such failures in 48h on 2026-07-26..28.
    """
    mock_arion = MockArionService(raise_on_can_upload=_http_status_error(status))
    app = _make_can_upload_app(mock_config_no_bypass, mock_arion, monkeypatch)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put("/test-bucket/test-key", content=b"hello", headers={"content-length": "5"})

    assert response.status_code == 503
    assert b"SlowDown" in response.content
    assert b"AccountVerificationError" not in response.content


@pytest.mark.asyncio
async def test_arion_5xx_is_retried_before_giving_up(mock_config_no_bypass: Any, monkeypatch: Any) -> None:
    mock_arion = MockArionService(raise_on_can_upload=_http_status_error(502))
    app = _make_can_upload_app(mock_config_no_bypass, mock_arion, monkeypatch)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await client.put("/test-bucket/test-key", content=b"hello", headers={"content-length": "5"})

    # initial attempt + can_upload_transient_retries
    assert len(mock_arion.can_upload_calls) == 3


@pytest.mark.asyncio
async def test_arion_transport_failure_is_also_transient(mock_config_no_bypass: Any, monkeypatch: Any) -> None:
    """'Billing backend is unreachable' is not a credit verdict however it manifests."""
    mock_arion = MockArionService(raise_on_can_upload=httpx.ConnectError("connection refused"))
    app = _make_can_upload_app(mock_config_no_bypass, mock_arion, monkeypatch)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put("/test-bucket/test-key", content=b"hello", headers={"content-length": "5"})

    assert response.status_code == 503
    assert b"SlowDown" in response.content


@pytest.mark.asyncio
async def test_genuine_denial_still_hard_fails(mock_config_no_bypass: Any, monkeypatch: Any) -> None:
    """Blast-radius guard: only unreachability became transient. A real out-of-credit verdict
    must stay a 402, never get softened into a retry-forever SlowDown."""
    mock_arion = MockArionService(allow_upload=False, upload_error="insufficient billing balance")
    app = _make_can_upload_app(mock_config_no_bypass, mock_arion, monkeypatch)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put("/test-bucket/test-key", content=b"hello", headers={"content-length": "5"})

    assert response.status_code == 402
    assert len(mock_arion.can_upload_calls) == 1, "a genuine denial must not be retried"


@pytest.mark.asyncio
async def test_arion_4xx_is_not_treated_as_transient(mock_config_no_bypass: Any, monkeypatch: Any) -> None:
    """A 4xx from Arion is a bad request on our side, not a blip — it must not be retried into
    a SlowDown that hides the bug."""
    mock_arion = MockArionService(raise_on_can_upload=_http_status_error(400))
    app = _make_can_upload_app(mock_config_no_bypass, mock_arion, monkeypatch)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put("/test-bucket/test-key", content=b"hello", headers={"content-length": "5"})

    assert response.status_code == 503
    assert b"AccountVerificationError" in response.content


@pytest.mark.asyncio
async def test_arion_429_is_transient_but_not_hammered(mock_config_no_bypass: Any, monkeypatch: Any) -> None:
    """429 is transient, but re-driving it is the one response guaranteed to be wrong: Arion just
    said 'too many requests'. Surface the SlowDown without running the retry ladder."""
    mock_arion = MockArionService(raise_on_can_upload=_http_status_error(429))
    app = _make_can_upload_app(mock_config_no_bypass, mock_arion, monkeypatch)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put("/test-bucket/test-key", content=b"hello", headers={"content-length": "5"})

    assert response.status_code == 503
    assert b"SlowDown" in response.content
    assert len(mock_arion.can_upload_calls) == 1, "must not re-drive a backend that asked us to back off"


@pytest.mark.asyncio
async def test_arion_507_stays_non_retryable(mock_config_no_bypass: Any, monkeypatch: Any) -> None:
    """retry_on_error re-raises 507 precisely because 'server is full' cannot be retried away.
    Treating it as a 5xx blip here would silently undo that."""
    mock_arion = MockArionService(raise_on_can_upload=_http_status_error(507))
    app = _make_can_upload_app(mock_config_no_bypass, mock_arion, monkeypatch)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put("/test-bucket/test-key", content=b"hello", headers={"content-length": "5"})

    assert b"AccountVerificationError" in response.content
    assert len(mock_arion.can_upload_calls) == 1


@pytest.mark.asyncio
async def test_a_broken_base_url_stays_loud(mock_config_no_bypass: Any, monkeypatch: Any) -> None:
    """A malformed HIPPIUS_ARION_BASE_URL is a config bug, not a blip. If it were folded into the
    transient set it would become a fleet-wide 'please retry' that never resolves."""
    mock_arion = MockArionService(raise_on_can_upload=httpx.UnsupportedProtocol("missing scheme"))
    app = _make_can_upload_app(mock_config_no_bypass, mock_arion, monkeypatch)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put("/test-bucket/test-key", content=b"hello", headers={"content-length": "5"})

    assert b"AccountVerificationError" in response.content
    assert b"SlowDown" not in response.content


# ---------------------------------------------------------------------------
# Real ArionClient: what actually lands on Arion, and the latency bound
#
# MockArionService raises the injected exception directly, so every test above
# measures middleware-level attempts and never exercises @retry_on_error. These
# drive the real client over a MockTransport so the numbers are the real ones.
# ---------------------------------------------------------------------------


def _real_arion_over(handler: Any, monkeypatch: Any) -> Any:
    from hippius_s3.services import arion_service as arion_mod

    async def _no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(arion_mod.asyncio, "sleep", _no_sleep)

    client = arion_mod.ArionClient(base_url="http://arion.test", service_key="k")
    client._client = httpx.AsyncClient(
        base_url="http://arion.test",
        transport=httpx.MockTransport(handler),
    )
    return client


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,expected_requests",
    [
        (502, 6),  # decorator 2 x middleware 3
        (429, 2),  # decorator 2, ladder skipped
        (400, 2),  # decorator 2, then re-raised to the blanket handler
        (507, 1),  # decorator re-raises immediately
    ],
)
async def test_real_request_amplification_against_arion(
    mock_config_no_bypass: Any, monkeypatch: Any, status: int, expected_requests: int
) -> None:
    """Pins how many requests a single client PUT actually costs a struggling billing backend."""
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(status)

    app = _make_can_upload_app(mock_config_no_bypass, _real_arion_over(handler, monkeypatch), monkeypatch)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await client.put("/test-bucket/test-key", content=b"hello", headers={"content-length": "5"})

    assert len(seen) == expected_requests


@pytest.mark.asyncio
async def test_can_upload_attempt_is_time_bounded(mock_config_no_bypass: Any, monkeypatch: Any) -> None:
    """Regression guard: retry COUNTS cannot bound latency when the per-attempt cost is unbounded.

    A blackholed Arion (TCP accepted, nothing returned) would otherwise cost the client-wide 60s
    read timeout per attempt, and the transient ladder re-drives it — minutes on a single PUT,
    holding a gateway worker the whole time. can_upload must carry its own short cap.
    """
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(502)

    app = _make_can_upload_app(mock_config_no_bypass, _real_arion_over(handler, monkeypatch), monkeypatch)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await client.put("/test-bucket/test-key", content=b"hello", headers={"content-length": "5"})

    read_timeouts = {r.extensions["timeout"]["read"] for r in seen}
    assert read_timeouts == {3.0}, "can_upload must not inherit the client-wide 60s bulk-upload timeout"
