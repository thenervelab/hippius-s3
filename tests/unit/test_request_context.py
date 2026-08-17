"""The request_context middleware replaced the forward_service → parse_internal_headers
header round-trip when the gateway merged into the api. These pin the two properties the
round-trip used to provide:

1. the exact state mapping S3 handlers rely on (state.main_account_id = bucket owner
   with caller fallback; state.account stays the CALLER's, never rebound), and
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
        captured["main_account_id"] = request.state.main_account_id
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
    # Storage attribution is the BUCKET OWNER, under its own explicit key; the caller's
    # account object is untouched, so its main_account stays the caller's own.
    assert captured["main_account_id"] == "owner-9"
    assert captured["account"] is caller
    assert captured["account"].main_account == "caller-main"


@pytest.mark.asyncio
async def test_bucket_owner_falls_back_to_caller() -> None:
    caller = HippiusAccount(id="sub-1", main_account="caller-main", has_credits=True, upload=True, delete=True)
    captured = await _get(_app_with_upstream_state(account_id="sub-1", account=caller))
    assert captured["bucket_owner_id"] == "sub-1"
    assert captured["main_account_id"] == "sub-1"


@pytest.mark.asyncio
async def test_anonymous_request_gets_empty_account_with_no_flags() -> None:
    captured = await _get(_app_with_upstream_state())
    assert captured["request_user_id"] == ""
    assert captured["main_account_id"] == ""
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
    assert captured["main_account_id"] == ""
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
