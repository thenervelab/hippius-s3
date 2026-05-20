"""Tests for auth_probe_middleware — short-circuits ATS authproxy subrequests with 200 OK
when the request carries X-Hippius-Auth-Probe = <shared secret>."""

from typing import Any
from typing import Generator

import pytest
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from httpx import ASGITransport
from httpx import AsyncClient

from gateway.config import GatewayConfig
from gateway.middlewares import auth_probe as auth_probe_mod
from gateway.middlewares.auth_probe import AUTH_PROBE_HEADER
from gateway.middlewares.auth_probe import auth_probe_middleware


TEST_SECRET = "fake-test-probe-secret-not-real"


@pytest.fixture  # type: ignore[misc]
def configured_secret(monkeypatch: pytest.MonkeyPatch) -> Generator[str, None, None]:
    """Force the middleware's get_config() to return a config with the test secret.
    Patching the cached singleton avoids any os.environ-vs-cache races."""
    cfg = GatewayConfig(auth_probe_secret=TEST_SECRET)
    monkeypatch.setattr(auth_probe_mod, "get_config", lambda: cfg)
    yield TEST_SECRET


@pytest.fixture  # type: ignore[misc]
def empty_secret(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    """get_config().auth_probe_secret == "" → middleware disabled (fail-closed)."""
    cfg = GatewayConfig(auth_probe_secret="")
    monkeypatch.setattr(auth_probe_mod, "get_config", lambda: cfg)
    yield


@pytest.fixture  # type: ignore[misc]
def app() -> Any:
    """An app where the catch-all handler counts how many times it ran."""
    app = FastAPI()
    app.state.handler_calls = 0
    app.state.handler_body = b"forwarded-payload"

    app.middleware("http")(auth_probe_middleware)

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "HEAD", "PATCH"])
    async def catch_all(request: Request) -> Response:
        request.app.state.handler_calls += 1
        return Response(status_code=200, content=request.app.state.handler_body)

    return app


@pytest.mark.asyncio
async def test_correct_secret_short_circuits_with_200(app: Any, configured_secret: str) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/warm-private/foo.txt", headers={AUTH_PROBE_HEADER: configured_secret})
    assert r.status_code == 200
    assert r.content == b""
    assert app.state.handler_calls == 0


@pytest.mark.asyncio
async def test_correct_secret_short_circuits_head(app: Any, configured_secret: str) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.head("/warm-private/foo.txt", headers={AUTH_PROBE_HEADER: configured_secret})
    assert r.status_code == 200
    assert app.state.handler_calls == 0


@pytest.mark.asyncio
async def test_no_probe_header_runs_handler(app: Any, configured_secret: str) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/regular/foo.txt")
    assert r.status_code == 200
    assert r.content == b"forwarded-payload"
    assert app.state.handler_calls == 1


@pytest.mark.asyncio
async def test_wrong_secret_does_not_short_circuit(app: Any, configured_secret: str) -> None:
    """A client guessing or smuggling the header must NOT trigger the short-circuit.
    The legacy literal '1' value is rejected — this is the main security property."""
    for value in ("1", "0", "true", "", TEST_SECRET[:-1], TEST_SECRET + "x", "guess"):
        app.state.handler_calls = 0
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            r = await client.get("/regular/foo.txt", headers={AUTH_PROBE_HEADER: value})
        assert r.status_code == 200, f"value={value!r}"
        assert r.content == b"forwarded-payload", f"value={value!r}"
        assert app.state.handler_calls == 1, f"value={value!r}"


@pytest.mark.asyncio
async def test_empty_secret_disables_middleware(app: Any, empty_secret: None) -> None:
    """When HIPPIUS_AUTH_PROBE_SECRET is unset, even a client sending the header
    (regardless of value, including '') must go through the handler — the middleware
    is fully disabled. Fail-closed default for un-rolled-out environments."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/regular/foo.txt", headers={AUTH_PROBE_HEADER: "any-value"})
    assert r.status_code == 200
    assert r.content == b"forwarded-payload"
    assert app.state.handler_calls == 1


@pytest.mark.asyncio
async def test_empty_secret_disables_middleware_even_with_empty_value(app: Any, empty_secret: None) -> None:
    """Guard against the trap of '' == '' short-circuiting when secret is empty."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/regular/foo.txt", headers={AUTH_PROBE_HEADER: ""})
    assert r.status_code == 200
    assert r.content == b"forwarded-payload"
    assert app.state.handler_calls == 1


@pytest.mark.asyncio
async def test_probe_header_is_case_insensitive(app: Any, configured_secret: str) -> None:
    """HTTP headers are case-insensitive — Starlette lowercases them on lookup,
    so an ATS rule that emits 'X-Hippius-Auth-Probe' must work the same as the
    lowercase form."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/warm-private/foo.txt", headers={"X-Hippius-Auth-Probe": configured_secret})
    assert r.status_code == 200
    assert r.content == b""
    assert app.state.handler_calls == 0


@pytest.mark.asyncio
async def test_probe_response_has_empty_body_for_all_methods(app: Any, configured_secret: str) -> None:
    """authproxy subrequest is HEAD by default but REDIRECT mode preserves the
    method. Either way the gateway should return an empty 200; ATS only consults
    the status code."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        for method in ("GET", "HEAD", "PUT", "POST", "DELETE"):
            r = await client.request(method, "/any/path", headers={AUTH_PROBE_HEADER: configured_secret})
            assert r.status_code == 200, method
            assert r.content == b"", method
    assert app.state.handler_calls == 0
