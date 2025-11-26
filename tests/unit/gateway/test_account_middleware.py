"""Unit tests for account middleware bypass mode and account ID derivation."""

import hashlib
from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi import Request
from httpx import ASGITransport
from httpx import AsyncClient


@pytest.fixture  # type: ignore[misc]
def mock_config_bypass() -> Any:
    """Mock config with bypass_credit_check enabled."""
    config = MagicMock()
    config.bypass_credit_check = True
    return config


@pytest.fixture  # type: ignore[misc]
def mock_config_no_bypass() -> Any:
    """Mock config with bypass_credit_check disabled."""
    config = MagicMock()
    config.bypass_credit_check = False
    config.substrate_url = "ws://localhost:9944"
    return config


@pytest.fixture  # type: ignore[misc]
def account_app_bypass(mock_config_bypass: Any, monkeypatch: Any) -> Any:
    """FastAPI app with account middleware in bypass mode."""
    from gateway.middlewares.account import account_middleware

    monkeypatch.setattr("gateway.middlewares.account.config", mock_config_bypass)

    app = FastAPI()

    @app.get("/test")
    async def test_endpoint(request: Request) -> dict[str, Any]:
        account_id = request.state.account_id if hasattr(request.state, "account_id") else None
        account = request.state.account if hasattr(request.state, "account") else None
        return {"account_id": account_id, "account": account}

    app.middleware("http")(account_middleware)

    return app


@pytest.mark.asyncio
async def test_bypass_mode_derives_unique_id_from_seed_phrase(mock_config_bypass: Any, monkeypatch: Any) -> None:
    """Test that bypass mode derives unique account IDs from different seed phrases."""
    from gateway.middlewares.account import account_middleware

    monkeypatch.setattr("gateway.middlewares.account.config", mock_config_bypass)

    seed_phrase_a = "about acid actor absent action able actual abandon abstract above ability achieve"
    seed_phrase_b = "dream letter onion wreck return glove canal easy letter render wear bright"

    expected_id_a = hashlib.sha256(seed_phrase_a.encode()).digest().hex()
    expected_id_b = hashlib.sha256(seed_phrase_b.encode()).digest().hex()

    # Test with seed phrase A
    app_a = FastAPI()

    @app_a.get("/test")
    async def test_endpoint(request: Request) -> dict[str, Any]:
        account_id = request.state.account_id
        return {"account_id": account_id}

    # Inject seed phrase A before account middleware
    async def inject_seed_a(request: Request, call_next: Any) -> Any:
        request.state.seed_phrase = seed_phrase_a
        return await call_next(request)

    app_a.middleware("http")(account_middleware)
    app_a.middleware("http")(inject_seed_a)

    async with AsyncClient(transport=ASGITransport(app=app_a), base_url="http://test") as client:
        response_a = await client.get("/test")

    assert response_a.status_code == 200
    assert response_a.json()["account_id"] == expected_id_a

    # Test with seed phrase B
    app_b = FastAPI()

    @app_b.get("/test")
    async def test_endpoint_b(request: Request) -> dict[str, Any]:
        account_id = request.state.account_id
        return {"account_id": account_id}

    async def inject_seed_b(request: Request, call_next: Any) -> Any:
        request.state.seed_phrase = seed_phrase_b
        return await call_next(request)

    app_b.middleware("http")(account_middleware)
    app_b.middleware("http")(inject_seed_b)

    async with AsyncClient(transport=ASGITransport(app=app_b), base_url="http://test") as client:
        response_b = await client.get("/test")

    assert response_b.status_code == 200
    assert response_b.json()["account_id"] == expected_id_b

    # Verify they're different
    assert expected_id_a != expected_id_b


@pytest.mark.asyncio
async def test_bypass_mode_no_seed_phrase_returns_anonymous(mock_config_bypass: Any, monkeypatch: Any) -> None:
    """Test that requests without seed phrase get anonymous account ID."""
    from gateway.middlewares.account import account_middleware

    monkeypatch.setattr("gateway.middlewares.account.config", mock_config_bypass)

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


@pytest.mark.asyncio
async def test_account_id_format_matches_ss58_pattern(mock_config_bypass: Any, monkeypatch: Any) -> None:
    """Test that derived account IDs match AWS canonical ID format (64 hex chars)."""
    from gateway.middlewares.account import account_middleware

    monkeypatch.setattr("gateway.middlewares.account.config", mock_config_bypass)

    seed_phrase = "about acid actor absent action able actual abandon abstract above ability achieve"

    app = FastAPI()

    @app.get("/test")
    async def test_endpoint(request: Request) -> dict[str, Any]:
        account_id = request.state.account_id
        return {"account_id": account_id}

    async def inject_seed(request: Request, call_next: Any) -> Any:
        request.state.seed_phrase = seed_phrase
        return await call_next(request)

    app.middleware("http")(account_middleware)
    app.middleware("http")(inject_seed)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/test")

    data = response.json()
    account_id = data["account_id"]

    assert len(account_id) == 64  # Full SHA256 hex
    assert all(c in "0123456789abcdef" for c in account_id)
