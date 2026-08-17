"""The request_context middleware replaced the forward_service → parse_internal_headers
header round-trip when the gateway merged into the api. These pin the two properties the
round-trip used to provide:

1. the exact state mapping S3 handlers rely on (main_account = bucket owner, caller
   fallback, flag carriage), and
2. header inertness — client-supplied X-Hippius-* headers must have zero effect on
   request.state, which is what the gateway's strip-loop used to enforce.
"""

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi import Request
from httpx import ASGITransport
from httpx import AsyncClient

from hippius_s3.api.middlewares.request_context import request_context_middleware
from hippius_s3.models.account import HippiusAccount


def _app_with_upstream_state(**state: Any) -> FastAPI:
    app = FastAPI()
    captured: dict[str, Any] = {}
    app.state.captured = captured

    @app.get("/{bucket}/{key:path}")
    async def probe(request: Request, bucket: str, key: str = "") -> dict[str, str]:
        captured["request_user_id"] = request.state.request_user_id
        captured["bucket_owner_id"] = request.state.bucket_owner_id
        captured["bucket_id"] = request.state.bucket_id
        captured["account"] = request.state.account
        return {"ok": "1"}

    app.middleware("http")(request_context_middleware)

    # Outermost: plays the auth/account/acl middlewares that populate state upstream.
    @app.middleware("http")
    async def upstream(request: Request, call_next: Any) -> Any:
        for k, v in state.items():
            setattr(request.state, k, v)
        return await call_next(request)

    return app


async def _get(app: FastAPI, path: str = "/bucket/key", headers: dict[str, str] | None = None) -> dict[str, Any]:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get(path, headers=headers)
    assert response.status_code == 200
    return app.state.captured


@pytest.mark.asyncio
async def test_account_maps_bucket_owner_to_main_account() -> None:
    caller = HippiusAccount(id="sub-1", main_account="caller-main", has_credits=True, upload=True, delete=False)
    captured = await _get(
        _app_with_upstream_state(account_id="sub-1", bucket_owner_id="owner-9", bucket_id="b-uuid", account=caller)
    )
    assert captured["request_user_id"] == "sub-1"
    assert captured["bucket_owner_id"] == "owner-9"
    assert captured["bucket_id"] == "b-uuid"
    # main_account is the BUCKET OWNER (storage attribution), never the caller's own main.
    assert captured["account"].main_account == "owner-9"
    assert captured["account"].id == "sub-1"
    assert captured["account"].has_credits is True
    assert captured["account"].upload is True
    assert captured["account"].delete is False


@pytest.mark.asyncio
async def test_bucket_owner_falls_back_to_caller() -> None:
    caller = HippiusAccount(id="sub-1", main_account="caller-main", has_credits=True, upload=True, delete=True)
    captured = await _get(_app_with_upstream_state(account_id="sub-1", account=caller))
    assert captured["bucket_owner_id"] == "sub-1"
    assert captured["account"].main_account == "sub-1"


@pytest.mark.asyncio
async def test_anonymous_request_gets_empty_account_with_no_flags() -> None:
    captured = await _get(_app_with_upstream_state())
    assert captured["request_user_id"] == ""
    assert captured["account"].id == ""
    assert captured["account"].has_credits is False
    assert captured["account"].upload is False
    assert captured["account"].delete is False


@pytest.mark.asyncio
async def test_client_supplied_x_hippius_headers_are_inert() -> None:
    captured = await _get(
        _app_with_upstream_state(),
        headers={
            "X-Hippius-Request-User": "attacker",
            "X-Hippius-Bucket-Owner": "attacker",
            "X-Hippius-Main-Account": "attacker",
            "X-Hippius-Has-Credits": "True",
            "X-Hippius-Can-Upload": "True",
            "X-Hippius-Can-Delete": "True",
        },
    )
    assert captured["request_user_id"] == ""
    assert captured["bucket_owner_id"] == ""
    assert captured["account"].main_account == ""
    assert captured["account"].upload is False


def test_no_source_reads_trusted_headers_anymore() -> None:
    """The header contract is dead; nothing may resurrect it. A read of any of the
    trust-carrying X-Hippius-* request headers would silently re-open the injection
    surface the merge closed."""
    import pathlib
    import re

    repo = pathlib.Path(__file__).resolve().parents[2]
    pattern = re.compile(
        r"X-Hippius-(Request-User|Bucket-Owner|Bucket-Id|Main-Account|Has-Credits|Can-Upload|Can-Delete)",
        re.IGNORECASE,
    )
    offenders = []
    for package in ("hippius_s3", "gateway"):
        for path in (repo / package).rglob("*.py"):
            for i, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if pattern.search(line) and "headers" in line:
                    offenders.append(f"{path.relative_to(repo)}:{i}")
    assert offenders == [], f"trusted X-Hippius-* header reads found: {offenders}"


class TestPeerFetchExemption:
    """The merged app exempts /internal/parts from the S3 pipeline ONLY for requests
    presenting the valid peer secret — fail-closed in every other case."""

    def _request(self, path: str, secret: str | None, method: str = "GET") -> Any:
        from starlette.requests import Request as StarletteRequest

        headers = []
        if secret is not None:
            headers.append((b"x-hippius-peer-auth", secret.encode()))
        scope = {
            "type": "http",
            "method": method,
            "path": path,
            "raw_path": path.encode(),
            "query_string": b"",
            "headers": headers,
            "client": ("10.0.0.9", 4242),
        }
        return StarletteRequest(scope)

    def test_valid_secret_on_internal_path_is_exempt(self, monkeypatch: Any) -> None:
        from hippius_s3 import config as config_mod
        from hippius_s3.peer_auth import is_authorized_peer_fetch

        secret = "ab" * 32
        monkeypatch.setattr(config_mod.get_config(), "internal_peer_secret", secret)
        monkeypatch.setattr(config_mod.get_config(), "peer_serve_enabled", True)
        assert is_authorized_peer_fetch(self._request("/internal/parts/obj/1/1/chunks/0", secret)) is True

    def test_wrong_secret_and_wrong_path_fail_closed(self, monkeypatch: Any) -> None:
        from hippius_s3 import config as config_mod
        from hippius_s3.peer_auth import is_authorized_peer_fetch

        secret = "ab" * 32
        monkeypatch.setattr(config_mod.get_config(), "internal_peer_secret", secret)
        monkeypatch.setattr(config_mod.get_config(), "peer_serve_enabled", True)
        assert is_authorized_peer_fetch(self._request("/internal/parts/obj/1/1/chunks/0", "cd" * 32)) is False
        assert is_authorized_peer_fetch(self._request("/internal/parts/obj/1/1/chunks/0", None)) is False
        assert is_authorized_peer_fetch(self._request("/some-bucket/key", secret)) is False
        # dot-segment smuggling into the internal prefix is judged on the routing view
        assert is_authorized_peer_fetch(self._request("/bucket/../internal/parts/x/1/1/chunks/0", secret)) is True

    def test_non_get_and_serve_disabled_fail_closed(self, monkeypatch: Any) -> None:
        """The exemption is scoped to what the peer tier actually does: GET chunk reads,
        on pods that opted into serving. A valid secret must not unlock write methods or
        pods where the route is not mounted."""
        from hippius_s3 import config as config_mod
        from hippius_s3.peer_auth import is_authorized_peer_fetch

        secret = "ab" * 32
        monkeypatch.setattr(config_mod.get_config(), "internal_peer_secret", secret)
        monkeypatch.setattr(config_mod.get_config(), "peer_serve_enabled", True)
        for method in ("PUT", "POST", "DELETE", "HEAD"):
            assert is_authorized_peer_fetch(self._request("/internal/parts/obj/1/1/chunks/0", secret, method)) is False

        monkeypatch.setattr(config_mod.get_config(), "peer_serve_enabled", False)
        assert is_authorized_peer_fetch(self._request("/internal/parts/obj/1/1/chunks/0", secret)) is False

    def test_no_configured_secret_never_authorizes(self, monkeypatch: Any) -> None:
        from hippius_s3 import config as config_mod
        from hippius_s3.peer_auth import is_authorized_peer_fetch

        monkeypatch.setattr(config_mod.get_config(), "internal_peer_secret", "")
        assert is_authorized_peer_fetch(self._request("/internal/parts/obj/1/1/chunks/0", "")) is False
