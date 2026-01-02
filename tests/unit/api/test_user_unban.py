"""Unit tests for /user/unban endpoint."""

from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient


@pytest.fixture
def unban_app() -> Any:
    from hippius_s3.api.user import router

    app = FastAPI()
    app.include_router(router, prefix="/user")

    mock_redis = MagicMock()
    mock_redis.delete = AsyncMock(return_value=1)
    mock_redis.scan_iter = AsyncMock()

    app.state.redis_rate_limiting_client = mock_redis

    return app


@pytest.mark.asyncio
async def test_unban_valid_ipv4(unban_app: Any) -> None:
    async def mock_scan_iter(match: str) -> Any:
        for key in [
            b"hippius_banhammer:infringements:1.2.3.4:1000",
            b"hippius_banhammer:infringements:1.2.3.4:1001",
        ]:
            yield key

    unban_app.state.redis_rate_limiting_client.scan_iter = mock_scan_iter

    async with AsyncClient(transport=ASGITransport(app=unban_app), base_url="http://test") as client:
        response = await client.post("/user/unban?ip=1.2.3.4")

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["ip"] == "1.2.3.4"

    assert unban_app.state.redis_rate_limiting_client.delete.call_count == 2


@pytest.mark.asyncio
async def test_unban_valid_ipv6(unban_app: Any) -> None:
    async def mock_scan_iter(match: str) -> Any:
        for key in [b"hippius_banhammer:infringements:2001:db8::1:1000"]:
            yield key

    unban_app.state.redis_rate_limiting_client.scan_iter = mock_scan_iter

    async with AsyncClient(transport=ASGITransport(app=unban_app), base_url="http://test") as client:
        response = await client.post("/user/unban?ip=2001:db8::1")

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["ip"] == "2001:db8::1"


@pytest.mark.asyncio
async def test_unban_no_counters(unban_app: Any) -> None:
    async def mock_scan_iter(match: str) -> Any:
        return
        yield

    unban_app.state.redis_rate_limiting_client.scan_iter = mock_scan_iter

    async with AsyncClient(transport=ASGITransport(app=unban_app), base_url="http://test") as client:
        response = await client.post("/user/unban?ip=192.168.1.1")

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["ip"] == "192.168.1.1"

    assert unban_app.state.redis_rate_limiting_client.delete.call_count == 1


@pytest.mark.asyncio
async def test_unban_invalid_ip(unban_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=unban_app), base_url="http://test") as client:
        response = await client.post("/user/unban?ip=not-an-ip")

    assert response.status_code == 400
    data = response.json()
    assert "Invalid IP address format" in data["detail"]


@pytest.mark.asyncio
async def test_unban_missing_ip_parameter(unban_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=unban_app), base_url="http://test") as client:
        response = await client.post("/user/unban")

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_unban_redis_error(unban_app: Any) -> None:
    unban_app.state.redis_rate_limiting_client.delete = AsyncMock(
        side_effect=Exception("Redis connection error")
    )

    async with AsyncClient(transport=ASGITransport(app=unban_app), base_url="http://test") as client:
        response = await client.post("/user/unban?ip=1.2.3.4")

    assert response.status_code == 500
    data = response.json()
    assert data["detail"] == "Failed to unban IP"


@pytest.mark.asyncio
async def test_unban_deletes_correct_keys(unban_app: Any) -> None:
    deleted_keys = []

    async def mock_delete(*keys: str) -> int:
        deleted_keys.extend(keys)
        return len(keys)

    async def mock_scan_iter(match: str) -> Any:
        assert match == "hippius_banhammer:infringements:*:10.0.0.1:*"
        for key in [
            b"hippius_banhammer:infringements:unauth:10.0.0.1:1000",
            b"hippius_banhammer:infringements:auth:10.0.0.1:1001",
            b"hippius_banhammer:infringements:unauth:10.0.0.1:1002",
        ]:
            yield key

    unban_app.state.redis_rate_limiting_client.delete = mock_delete
    unban_app.state.redis_rate_limiting_client.scan_iter = mock_scan_iter

    async with AsyncClient(transport=ASGITransport(app=unban_app), base_url="http://test") as client:
        response = await client.post("/user/unban?ip=10.0.0.1")

    assert response.status_code == 200

    assert "hippius_banhammer:block:10.0.0.1" in deleted_keys
    assert b"hippius_banhammer:infringements:unauth:10.0.0.1:1000" in deleted_keys
    assert b"hippius_banhammer:infringements:auth:10.0.0.1:1001" in deleted_keys
    assert b"hippius_banhammer:infringements:unauth:10.0.0.1:1002" in deleted_keys
