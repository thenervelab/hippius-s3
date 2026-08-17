"""The audit log attributes operations to the CALLER. state.account carries caller
semantics only (request_context binds an empty stand-in for anonymous requests and
never rebinds an existing account; bucket-owner attribution lives under the separate
state.main_account_id), so these pin that the audit line books to the caller's main
account and that anonymous requests stay "unknown".
"""

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi import Request
from httpx import ASGITransport
from httpx import AsyncClient

from gateway.middlewares.audit_log import audit_log_middleware
from hippius_s3.api.middlewares.request_context import request_context_middleware
from hippius_s3.models.account import HippiusAccount


def _app(with_caller: bool) -> FastAPI:
    app = FastAPI()

    @app.get("/{bucket}/{key:path}")
    async def probe(request: Request, bucket: str, key: str = "") -> dict[str, str]:
        return {"ok": "1"}

    # The real inner middleware, so a regression back to account-rebinding fails here.
    app.middleware("http")(request_context_middleware)
    app.middleware("http")(audit_log_middleware)

    if with_caller:

        @app.middleware("http")
        async def fake_account(request: Request, call_next: Any) -> Any:
            request.state.account = HippiusAccount(
                id="caller-sub", main_account="caller-main", has_credits=True, upload=True, delete=True
            )
            request.state.bucket_owner_id = "bucket-owner"
            return await call_next(request)

    return app


async def _logged(app: FastAPI, monkeypatch: Any) -> dict[str, Any]:
    logged: dict[str, Any] = {}

    def capture_log_request(self: Any, **kwargs: Any) -> None:
        logged.update(kwargs)

    monkeypatch.setattr("hippius_s3.services.audit_service.AuditLogger.log_request", capture_log_request)
    monkeypatch.setattr("hippius_s3.services.audit_service.AuditLogger.should_skip", lambda self, path, ip: False)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/some-bucket/key")
    assert response.status_code == 200
    return logged


@pytest.mark.asyncio
async def test_audit_attributes_caller_not_bucket_owner(monkeypatch: Any) -> None:
    logged = await _logged(_app(with_caller=True), monkeypatch)
    assert logged["account_id"] == "caller-main"


@pytest.mark.asyncio
async def test_audit_attributes_anonymous_as_unknown(monkeypatch: Any) -> None:
    """No caller account upstream: request_context binds the empty stand-in, and the
    audit line must log "unknown", not the empty string and not any bucket owner."""
    logged = await _logged(_app(with_caller=False), monkeypatch)
    assert logged["account_id"] == "unknown"
