"""Smoke tests for BanHammer middleware."""

from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from starlette.responses import Response
from httpx import ASGITransport
from httpx import AsyncClient


@pytest.fixture  # type: ignore[misc]
def banhammer_app() -> Any:
    from gateway.middlewares.banhammer import banhammer_middleware
    from gateway.middlewares.banhammer import BanHammerService

    app = FastAPI()

    @app.get("/ok")
    async def ok_endpoint() -> dict[str, str]:
        return {"message": "ok"}

    @app.get("/missing")
    async def missing_endpoint() -> Response:
        return Response(status_code=404)

    @app.get("/deny")
    async def deny_endpoint() -> Response:
        return Response(status_code=403)

    @app.get("/bad")
    async def bad_endpoint() -> Response:
        return Response(status_code=400)

    @app.get("/oops")
    async def oops_endpoint() -> Response:
        return Response(status_code=500)

    @app.get("/unknown-ip")
    async def unknown_ip_endpoint() -> Response:
        return Response(status_code=403)

    @app.get("/unauth-401")
    async def unauth_401_endpoint() -> Response:
        return Response(status_code=401)

    @app.get("/unauth-405")
    async def unauth_405_endpoint() -> Response:
        return Response(status_code=405)

    # Use a real BanHammerService for classification behavior, but stub out Redis operations.
    service = BanHammerService(redis=MagicMock(), allowlist_ips=set())
    service.is_blocked = AsyncMock(return_value=None)
    service.add_infringement = AsyncMock(return_value=None)

    app.state.banhammer_service = service

    async def banhammer_wrapper(request: Any, call_next: Any) -> Any:
        return await banhammer_middleware(request, call_next, app.state.banhammer_service)

    app.middleware("http")(banhammer_wrapper)

    async def test_auth_middleware(request: Any, call_next: Any) -> Any:
        # This runs *before* banhammer (registered after), so banhammer can see auth state.
        if request.headers.get("X-Test-Auth") == "1":
            request.state.account = object()
        return await call_next(request)

    app.middleware("http")(test_auth_middleware)

    return app


@pytest.mark.asyncio
async def test_allowed_ip_passes(banhammer_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=banhammer_app), base_url="http://test") as client:
        response = await client.get("/ok")

    assert response.status_code == 200
    assert response.json() == {"message": "ok"}


@pytest.mark.asyncio
async def test_blocked_ip_returns_403(banhammer_app: Any) -> None:
    banhammer_app.state.banhammer_service.is_blocked = AsyncMock(return_value=300)

    async with AsyncClient(transport=ASGITransport(app=banhammer_app), base_url="http://test") as client:
        response = await client.get("/ok", headers={"X-Real-IP": "1.2.3.4"})

    assert response.status_code == 403
    assert b"banned" in response.content or b"AccessDenied" in response.content


@pytest.mark.asyncio
async def test_allowlisted_ip_bypasses_banhammer(banhammer_app: Any) -> None:
    banhammer_app.state.banhammer_service.allowlist_ips = {"1.2.3.4"}

    async with AsyncClient(transport=ASGITransport(app=banhammer_app), base_url="http://test") as client:
        response = await client.get("/deny", headers={"X-Real-IP": "1.2.3.4"})

    assert response.status_code == 403
    banhammer_app.state.banhammer_service.is_blocked.assert_not_called()
    banhammer_app.state.banhammer_service.add_infringement.assert_not_called()


@pytest.mark.asyncio
async def test_unauthenticated_404_get_counts_as_infringement(banhammer_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=banhammer_app), base_url="http://test") as client:
        response = await client.get("/missing", headers={"X-Real-IP": "1.2.3.4"})

    assert response.status_code == 404
    banhammer_app.state.banhammer_service.add_infringement.assert_called()


@pytest.mark.asyncio
async def test_authenticated_404_get_does_not_count_as_infringement(banhammer_app: Any) -> None:
    banhammer_app.state.banhammer_service.add_infringement.reset_mock()

    async with AsyncClient(transport=ASGITransport(app=banhammer_app), base_url="http://test") as client:
        response = await client.get("/missing", headers={"X-Real-IP": "1.2.3.4", "X-Test-Auth": "1"})

    assert response.status_code == 404
    banhammer_app.state.banhammer_service.add_infringement.assert_not_called()


@pytest.mark.asyncio
async def test_authenticated_403_does_not_count_as_infringement(banhammer_app: Any) -> None:
    banhammer_app.state.banhammer_service.add_infringement.reset_mock()

    async with AsyncClient(transport=ASGITransport(app=banhammer_app), base_url="http://test") as client:
        response = await client.get("/deny", headers={"X-Real-IP": "1.2.3.4", "X-Test-Auth": "1"})

    assert response.status_code == 403
    banhammer_app.state.banhammer_service.add_infringement.assert_not_called()


@pytest.mark.asyncio
async def test_unauthenticated_400_counts_as_infringement(banhammer_app: Any) -> None:
    banhammer_app.state.banhammer_service.add_infringement.reset_mock()

    async with AsyncClient(transport=ASGITransport(app=banhammer_app), base_url="http://test") as client:
        response = await client.get("/bad", headers={"X-Real-IP": "1.2.3.4"})

    assert response.status_code == 400
    banhammer_app.state.banhammer_service.add_infringement.assert_called()


@pytest.mark.asyncio
async def test_5xx_does_not_count_as_infringement(banhammer_app: Any) -> None:
    banhammer_app.state.banhammer_service.add_infringement.reset_mock()

    async with AsyncClient(transport=ASGITransport(app=banhammer_app), base_url="http://test") as client:
        response = await client.get("/oops", headers={"X-Real-IP": "1.2.3.4"})

    assert response.status_code == 500
    banhammer_app.state.banhammer_service.add_infringement.assert_not_called()


@pytest.mark.asyncio
async def test_unknown_ip_fails_open_and_does_not_count(banhammer_app: Any, monkeypatch: Any) -> None:
    from gateway.middlewares import banhammer as banhammer_mod

    monkeypatch.setattr(banhammer_mod, "get_client_ip", lambda _req: "unknown")

    banhammer_app.state.banhammer_service.add_infringement.reset_mock()
    banhammer_app.state.banhammer_service.is_blocked.reset_mock()

    async with AsyncClient(transport=ASGITransport(app=banhammer_app), base_url="http://test") as client:
        response = await client.get("/unknown-ip")

    assert response.status_code == 403
    banhammer_app.state.banhammer_service.is_blocked.assert_not_called()
    banhammer_app.state.banhammer_service.add_infringement.assert_not_called()


@pytest.mark.asyncio
async def test_unauth_404_method_filter_is_respected(banhammer_app: Any) -> None:
    banhammer_app.state.banhammer_service.add_infringement.reset_mock()
    banhammer_app.state.banhammer_service.unauth_404_methods = {"POST"}

    async with AsyncClient(transport=ASGITransport(app=banhammer_app), base_url="http://test") as client:
        response = await client.get("/missing", headers={"X-Real-IP": "1.2.3.4"})

    assert response.status_code == 404
    banhammer_app.state.banhammer_service.add_infringement.assert_not_called()


@pytest.mark.asyncio
async def test_shared_block_applies_to_authenticated_and_unauthenticated(banhammer_app: Any) -> None:
    # Shared block key means the service checks the same ban regardless of auth state.
    banhammer_app.state.banhammer_service.is_blocked = AsyncMock(return_value=60)

    async with AsyncClient(transport=ASGITransport(app=banhammer_app), base_url="http://test") as client:
        resp_unauth = await client.get("/ok", headers={"X-Real-IP": "1.2.3.4"})
        resp_auth = await client.get("/ok", headers={"X-Real-IP": "1.2.3.4", "X-Test-Auth": "1"})

    assert resp_unauth.status_code == 403
    assert resp_auth.status_code == 403


@pytest.mark.asyncio
async def test_unauth_401_counts_as_infringement(banhammer_app: Any) -> None:
    banhammer_app.state.banhammer_service.add_infringement.reset_mock()

    async with AsyncClient(transport=ASGITransport(app=banhammer_app), base_url="http://test") as client:
        response = await client.get("/unauth-401", headers={"X-Real-IP": "1.2.3.4"})

    assert response.status_code == 401
    banhammer_app.state.banhammer_service.add_infringement.assert_called()


@pytest.mark.asyncio
async def test_unauth_405_counts_as_infringement(banhammer_app: Any) -> None:
    banhammer_app.state.banhammer_service.add_infringement.reset_mock()

    async with AsyncClient(transport=ASGITransport(app=banhammer_app), base_url="http://test") as client:
        response = await client.get("/unauth-405", headers={"X-Real-IP": "1.2.3.4"})

    assert response.status_code == 405
    banhammer_app.state.banhammer_service.add_infringement.assert_called()
