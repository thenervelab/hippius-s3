"""Smoke tests for BanHammer middleware."""

from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient


@pytest.fixture  # type: ignore[misc]
def banhammer_app() -> Any:
    from gateway.middlewares.banhammer import banhammer_middleware

    app = FastAPI()

    @app.get("/test")
    async def test_endpoint() -> dict[str, str]:
        return {"message": "ok"}

    mock_service = MagicMock()
    mock_service.is_blocked = AsyncMock(return_value=None)
    mock_service.add_infringement = AsyncMock()

    app.state.banhammer_service = mock_service

    async def banhammer_wrapper(request: Any, call_next: Any) -> Any:
        return await banhammer_middleware(request, call_next, app.state.banhammer_service)

    app.middleware("http")(banhammer_wrapper)

    return app


@pytest.mark.asyncio
async def test_allowed_ip_passes(banhammer_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=banhammer_app), base_url="http://test") as client:
        response = await client.get("/test")

    assert response.status_code == 200
    assert response.json() == {"message": "ok"}


@pytest.mark.asyncio
async def test_blocked_ip_returns_403(banhammer_app: Any) -> None:
    banhammer_app.state.banhammer_service.is_blocked = AsyncMock(return_value=300)

    async with AsyncClient(transport=ASGITransport(app=banhammer_app), base_url="http://test") as client:
        response = await client.get("/test", headers={"X-Real-IP": "1.2.3.4"})

    assert response.status_code == 403
    assert b"banned" in response.content or b"AccessDenied" in response.content
