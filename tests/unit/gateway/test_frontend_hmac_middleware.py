"""Unit tests for the frontend HMAC middleware."""

import hashlib
import hmac
from typing import Any

import pytest
from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient

from gateway.config import get_config
from gateway.middlewares.frontend_hmac import verify_frontend_hmac_middleware


def _signature(secret: str, method: str, path: str, query: str = "") -> str:
    message = f"{method}{path}?{query}" if query else f"{method}{path}"
    return hmac.new(secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()


@pytest.fixture  # type: ignore[misc]
def frontend_hmac_app() -> Any:
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
async def test_user_endpoint_missing_hmac_returns_401(frontend_hmac_app: Any) -> None:
    """Regression: the middleware must return 401 as a proper Response.

    It used to `raise HTTPException(401)`, but Starlette's http-level middleware
    doesn't invoke FastAPI's exception handler, so that bubbled up as 500.
    """
    async with AsyncClient(transport=ASGITransport(app=frontend_hmac_app), base_url="http://test") as client:
        response = await client.get("/user/profile")

    assert response.status_code == 401
    assert response.json() == {"detail": "Missing X-HMAC-Signature header"}


@pytest.mark.asyncio
async def test_user_endpoint_invalid_hmac_returns_403(frontend_hmac_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=frontend_hmac_app), base_url="http://test") as client:
        response = await client.get("/user/profile", headers={"X-HMAC-Signature": "00" * 32})

    assert response.status_code == 403
    assert response.json() == {"detail": "Invalid HMAC signature"}


@pytest.mark.asyncio
async def test_user_endpoint_valid_hmac_passes(frontend_hmac_app: Any) -> None:
    secret = get_config().frontend_hmac_secret
    sig = _signature(secret, "GET", "/user/profile")

    async with AsyncClient(transport=ASGITransport(app=frontend_hmac_app), base_url="http://test") as client:
        response = await client.get("/user/profile", headers={"X-HMAC-Signature": sig})

    assert response.status_code == 200
    assert response.json() == {"user": "test"}


@pytest.mark.asyncio
async def test_user_endpoint_valid_hmac_with_query_string(frontend_hmac_app: Any) -> None:
    """Query string must be included in the signed message."""
    secret = get_config().frontend_hmac_secret
    sig = _signature(secret, "GET", "/user/profile", "foo=bar&x=1")

    # Add the query handler so the route resolves.
    @frontend_hmac_app.get("/user/search")
    async def search() -> dict[str, str]:
        return {"ok": "true"}

    sig_search = _signature(secret, "GET", "/user/search", "q=hello")
    async with AsyncClient(transport=ASGITransport(app=frontend_hmac_app), base_url="http://test") as client:
        response = await client.get("/user/search", params={"q": "hello"}, headers={"X-HMAC-Signature": sig_search})

    assert response.status_code == 200
    # Smoke: sig for /user/profile would be different if query were misused above.
    assert sig != sig_search


@pytest.mark.asyncio
async def test_options_skips_hmac(frontend_hmac_app: Any) -> None:
    """CORS preflight OPTIONS must pass through without signature."""
    async with AsyncClient(transport=ASGITransport(app=frontend_hmac_app), base_url="http://test") as client:
        response = await client.options("/user/profile")

    # Either 200 (if FastAPI routes it) or 405 (no OPTIONS handler); crucially NOT 401.
    assert response.status_code != 401
