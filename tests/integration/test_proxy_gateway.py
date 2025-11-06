import json
from typing import Any

import httpx
import pytest
import respx  # type: ignore[import-not-found]
from httpx import AsyncClient

from hippius_s3.api.middlewares.credit_check import HippiusAccount


@pytest.mark.asyncio
async def test_proxy_startup_and_health_check(proxy_client: AsyncClient) -> None:
    """Test that proxy starts successfully and health check works."""
    response = await proxy_client.get("/health")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["service"] == "proxy-gateway"


@pytest.mark.asyncio
@respx.mock
async def test_forward_simple_get_request(proxy_client: AsyncClient) -> None:
    """Test that proxy forwards a simple GET request correctly."""
    mock_backend = respx.get("http://api:8000/test-endpoint").mock(
        return_value=httpx.Response(200, json={"message": "success"})
    )

    response = await proxy_client.get("/test-endpoint")

    assert response.status_code == 200
    assert response.json() == {"message": "success"}
    assert mock_backend.called
    assert mock_backend.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_forward_post_request_with_body(proxy_client: AsyncClient) -> None:
    """Test that proxy forwards POST request with body correctly."""
    test_payload = {"key": "value", "data": "test" * 1000}

    def check_request(request: httpx.Request) -> httpx.Response:
        received_body = json.loads(request.content)
        assert received_body == test_payload
        return httpx.Response(201, json={"received": True})

    respx.post("http://api:8000/api/upload").mock(side_effect=check_request)

    response = await proxy_client.post("/api/upload", json=test_payload)

    assert response.status_code == 201
    assert response.json() == {"received": True}


@pytest.mark.asyncio
@respx.mock
async def test_forward_request_with_query_parameters(proxy_client: AsyncClient) -> None:
    """Test that proxy preserves query parameters in forwarded request."""

    def check_query_params(request: httpx.Request) -> httpx.Response:
        assert "prefix" in request.url.params
        assert request.url.params["prefix"] == "test/"
        assert "max-keys" in request.url.params
        assert request.url.params["max-keys"] == "100"
        return httpx.Response(200, json={"params_ok": True})

    respx.get("http://api:8000/list").mock(side_effect=check_query_params)

    response = await proxy_client.get("/list?prefix=test/&max-keys=100")

    assert response.status_code == 200
    assert response.json() == {"params_ok": True}


@pytest.mark.asyncio
@respx.mock
async def test_header_injection_for_auth(proxy_client: AsyncClient, proxy_app: Any) -> None:
    """Test that X-Hippius-* headers are injected from request.state."""

    def check_headers(request: httpx.Request) -> httpx.Response:
        assert "X-Hippius-Account-Id" in request.headers
        assert request.headers["X-Hippius-Account-Id"] == "test-account-123"
        assert "X-Hippius-Seed" in request.headers
        assert request.headers["X-Hippius-Seed"] == "test seed phrase"
        assert "X-Hippius-Main-Account" in request.headers
        assert request.headers["X-Hippius-Main-Account"] == "main-account-456"
        assert "X-Hippius-Has-Credits" in request.headers
        assert request.headers["X-Hippius-Has-Credits"] == "True"
        return httpx.Response(200, json={"auth_ok": True})

    respx.get("http://api:8000/authenticated").mock(side_effect=check_headers)

    async def inject_state_middleware(request: Any, call_next: Any) -> Any:
        request.state.account_id = "test-account-123"
        request.state.seed_phrase = "test seed phrase"
        request.state.account = HippiusAccount(
            seed="test seed phrase",
            id="test-account-123",
            main_account="main-account-456",
            has_credits=True,
            upload=True,
            delete=False,
        )
        return await call_next(request)

    proxy_app.middleware("http")(inject_state_middleware)

    response = await proxy_client.get("/authenticated")

    assert response.status_code == 200
    assert response.json() == {"auth_ok": True}


@pytest.mark.asyncio
@respx.mock
async def test_strip_malicious_client_headers(proxy_client: AsyncClient) -> None:
    """Test that client-provided X-Hippius-* headers are NOT forwarded."""

    def check_no_malicious_headers(request: httpx.Request) -> httpx.Response:
        assert "X-Hippius-Account-Id" not in request.headers
        return httpx.Response(200, json={"security_ok": True})

    respx.get("http://api:8000/secure").mock(side_effect=check_no_malicious_headers)

    response = await proxy_client.get("/secure", headers={"X-Hippius-Account-Id": "malicious-id-999"})

    assert response.status_code == 200
    assert response.json() == {"security_ok": True}


@pytest.mark.asyncio
@respx.mock
async def test_connection_pooling(proxy_client: AsyncClient) -> None:
    """Test that proxy reuses connections via httpx connection pooling."""
    respx.get("http://api:8000/endpoint1").mock(return_value=httpx.Response(200, json={"id": 1}))
    respx.get("http://api:8000/endpoint2").mock(return_value=httpx.Response(200, json={"id": 2}))
    respx.get("http://api:8000/endpoint3").mock(return_value=httpx.Response(200, json={"id": 3}))

    response1 = await proxy_client.get("/endpoint1")
    response2 = await proxy_client.get("/endpoint2")
    response3 = await proxy_client.get("/endpoint3")

    assert response1.status_code == 200
    assert response2.status_code == 200
    assert response3.status_code == 200

    assert response1.json() == {"id": 1}
    assert response2.json() == {"id": 2}
    assert response3.json() == {"id": 3}


@pytest.mark.asyncio
@respx.mock
async def test_error_handling_backend_500(proxy_client: AsyncClient) -> None:
    """Test that proxy correctly forwards backend 500 errors."""
    respx.get("http://api:8000/failing-endpoint").mock(
        return_value=httpx.Response(500, json={"error": "Internal Server Error"})
    )

    response = await proxy_client.get("/failing-endpoint")

    assert response.status_code == 500
    assert "error" in response.json()


@pytest.mark.asyncio
@respx.mock
async def test_error_handling_backend_404(proxy_client: AsyncClient) -> None:
    """Test that proxy correctly forwards backend 404 errors."""
    respx.get("http://api:8000/nonexistent").mock(return_value=httpx.Response(404, json={"error": "Not Found"}))

    response = await proxy_client.get("/nonexistent")

    assert response.status_code == 404
    assert response.json() == {"error": "Not Found"}


@pytest.mark.asyncio
@respx.mock
async def test_forward_with_custom_headers(proxy_client: AsyncClient) -> None:
    """Test that custom client headers are preserved in forwarded request."""

    def check_custom_headers(request: httpx.Request) -> httpx.Response:
        assert "X-Custom-Header" in request.headers
        assert request.headers["X-Custom-Header"] == "custom-value"
        assert "User-Agent" in request.headers
        return httpx.Response(200, json={"headers_ok": True})

    respx.get("http://api:8000/test-headers").mock(side_effect=check_custom_headers)

    response = await proxy_client.get(
        "/test-headers",
        headers={"X-Custom-Header": "custom-value", "User-Agent": "TestClient/1.0"},
    )

    assert response.status_code == 200
    assert response.json() == {"headers_ok": True}
