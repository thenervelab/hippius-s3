"""Smoke tests for Frontend HMAC middleware."""

import hashlib
import hmac
from typing import Any

import pytest
from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient


@pytest.fixture  # type: ignore[misc]
def frontend_hmac_app() -> Any:
    from gateway.middlewares.frontend_hmac import verify_frontend_hmac_middleware

    app = FastAPI()

    @app.get("/user/profile")
    async def user_profile() -> dict[str, str]:
        return {"user": "test"}

    @app.get("/other/endpoint")
    async def other_endpoint() -> dict[str, str]:
        return {"message": "ok"}

    app.middleware("http")(verify_frontend_hmac_middleware)

    return app


@pytest.mark.asyncio
async def test_non_user_endpoints_skip_hmac(frontend_hmac_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=frontend_hmac_app), base_url="http://test") as client:
        response = await client.get("/other/endpoint")

    assert response.status_code == 200
    assert response.json() == {"message": "ok"}


@pytest.mark.asyncio
async def test_missing_hmac_header_returns_401_json(frontend_hmac_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=frontend_hmac_app), base_url="http://test") as client:
        response = await client.get("/user/profile")

    assert response.status_code == 401
    assert response.json() == {"detail": "Missing X-HMAC-Signature header"}


@pytest.mark.asyncio
async def test_invalid_hmac_signature_returns_403_json(frontend_hmac_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=frontend_hmac_app), base_url="http://test") as client:
        response = await client.get("/user/profile", headers={"X-HMAC-Signature": "deadbeef"})

    assert response.status_code == 403
    assert response.json() == {"detail": "Invalid HMAC signature"}


@pytest.mark.asyncio
async def test_valid_hmac_signature_passes(frontend_hmac_app: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    from gateway.middlewares import frontend_hmac as fh

    monkeypatch.setattr(fh.config, "frontend_hmac_secret", "unit-test-secret")

    message = "GET/user/profile"
    signature = hmac.new(
        b"unit-test-secret",
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    async with AsyncClient(transport=ASGITransport(app=frontend_hmac_app), base_url="http://test") as client:
        response = await client.get("/user/profile", headers={"X-HMAC-Signature": signature})

    assert response.status_code == 200
    assert response.json() == {"user": "test"}


@pytest.mark.skip(reason="Config caching prevents dynamic env var setting in unit tests - tested in integration")
@pytest.mark.asyncio
async def test_user_endpoint_missing_hmac_returns_401() -> None:
    import os

    os.environ["FRONTEND_HMAC_SECRET"] = "test-secret"

    from gateway.middlewares.frontend_hmac import verify_frontend_hmac_middleware

    app = FastAPI()

    @app.get("/user/profile")
    async def user_profile() -> dict[str, str]:
        return {"user": "test"}

    app.middleware("http")(verify_frontend_hmac_middleware)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/user/profile")

    assert response.status_code == 401


@pytest.mark.skip(reason="Config caching prevents dynamic env var setting in unit tests - tested in integration")
@pytest.mark.asyncio
async def test_user_endpoint_valid_hmac_passes() -> None:
    import os

    secret = "test-secret"
    os.environ["FRONTEND_HMAC_SECRET"] = secret

    from gateway.middlewares.frontend_hmac import verify_frontend_hmac_middleware

    app = FastAPI()

    @app.get("/user/profile")
    async def user_profile() -> dict[str, str]:
        return {"user": "test"}

    app.middleware("http")(verify_frontend_hmac_middleware)

    message = "GET/user/profile"
    signature = hmac.new(secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/user/profile", headers={"X-HMAC-Signature": signature})

    assert response.status_code == 200
    assert response.json() == {"user": "test"}
