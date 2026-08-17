"""Unit tests for the admin HMAC middleware (issue #421)."""

import hashlib
import hmac
from typing import Any

import pytest
from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient

from gateway.middlewares.admin_hmac import verify_admin_hmac_middleware


SECRET = "unit-test-admin-secret"


def _signature(secret: str, method: str, path: str, query: str = "") -> str:
    message = f"{method}{path}?{query}" if query else f"{method}{path}"
    return hmac.new(secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()


@pytest.fixture  # type: ignore[misc]
def admin_app(monkeypatch: pytest.MonkeyPatch) -> Any:
    from gateway.middlewares import admin_hmac as ah

    monkeypatch.setattr(ah.config, "admin_hmac_secret", SECRET)

    app = FastAPI()

    @app.post("/admin/accounts/{account_id}/suspend")
    async def suspend(account_id: str) -> dict[str, str]:
        return {"account_id": account_id}

    @app.get("/admin/accounts/{account_id}/status")
    async def status(account_id: str) -> dict[str, str]:
        return {"account_id": account_id}

    @app.get("/other/endpoint")
    async def other() -> dict[str, str]:
        return {"message": "ok"}

    app.middleware("http")(verify_admin_hmac_middleware)
    return app


@pytest.mark.asyncio
async def test_non_admin_endpoints_skip_admin_hmac(admin_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=admin_app), base_url="http://test") as client:
        response = await client.get("/other/endpoint")

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_missing_signature_returns_401(admin_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=admin_app), base_url="http://test") as client:
        response = await client.post("/admin/accounts/5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty/suspend")

    assert response.status_code == 401
    assert response.json() == {"detail": "Missing X-HMAC-Signature header"}


@pytest.mark.asyncio
async def test_invalid_signature_returns_403(admin_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=admin_app), base_url="http://test") as client:
        response = await client.post(
            "/admin/accounts/5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty/suspend",
            headers={"X-HMAC-Signature": "00" * 32},
        )

    assert response.status_code == 403
    assert response.json() == {"detail": "Invalid HMAC signature"}


@pytest.mark.asyncio
async def test_frontend_secret_does_not_verify_admin_routes(admin_app: Any) -> None:
    """The whole point of the separate secret: FRONTEND_HMAC_SECRET must not open /admin."""
    sig = _signature("test_secret", "POST", "/admin/accounts/5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty/suspend")
    async with AsyncClient(transport=ASGITransport(app=admin_app), base_url="http://test") as client:
        response = await client.post(
            "/admin/accounts/5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty/suspend",
            headers={"X-HMAC-Signature": sig},
        )

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_valid_signature_passes(admin_app: Any) -> None:
    path = "/admin/accounts/5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty/suspend"
    sig = _signature(SECRET, "POST", path)
    async with AsyncClient(transport=ASGITransport(app=admin_app), base_url="http://test") as client:
        response = await client.post(path, headers={"X-HMAC-Signature": sig})

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_query_string_is_part_of_signed_message(admin_app: Any) -> None:
    path = "/admin/accounts/5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty/status"
    sig_without_query = _signature(SECRET, "GET", path)
    async with AsyncClient(transport=ASGITransport(app=admin_app), base_url="http://test") as client:
        response = await client.get(path, params={"x": "1"}, headers={"X-HMAC-Signature": sig_without_query})
    assert response.status_code == 403

    sig_with_query = _signature(SECRET, "GET", path, "x=1")
    async with AsyncClient(transport=ASGITransport(app=admin_app), base_url="http://test") as client:
        response = await client.get(path, params={"x": "1"}, headers={"X-HMAC-Signature": sig_with_query})
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_empty_secret_fail_closes(admin_app: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Unset secret must disable the admin API entirely, even for a ''-signed request."""
    from gateway.middlewares import admin_hmac as ah

    monkeypatch.setattr(ah.config, "admin_hmac_secret", "")

    path = "/admin/accounts/5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty/suspend"
    sig_of_empty = _signature("", "POST", path)
    async with AsyncClient(transport=ASGITransport(app=admin_app), base_url="http://test") as client:
        response = await client.post(path, headers={"X-HMAC-Signature": sig_of_empty})

    assert response.status_code == 403
    assert response.json() == {"detail": "Admin API is not enabled"}


@pytest.mark.asyncio
async def test_options_skips_hmac(admin_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=admin_app), base_url="http://test") as client:
        response = await client.options("/admin/accounts/x/suspend")

    assert response.status_code != 401
    assert response.status_code != 403
