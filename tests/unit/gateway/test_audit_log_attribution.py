"""The audit log attributes operations to the CALLER. Post-merge, request_context
(inner to audit_log) rebinds request.state.account with bucket-owner semantics for the
S3 handlers — and request.state is one shared object, so a read after call_next sees
the rebound value. This pins that the audit middleware snapshots the caller BEFORE the
inner stack runs; a middleware-order test alone cannot catch a regression here.
"""

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi import Request
from httpx import ASGITransport
from httpx import AsyncClient

from gateway.middlewares.audit_log import audit_log_middleware
from hippius_s3.models.account import HippiusAccount


@pytest.mark.asyncio
async def test_audit_attributes_caller_not_rebound_bucket_owner(monkeypatch: Any) -> None:
    logged: dict[str, Any] = {}

    def capture_log_request(self: Any, **kwargs: Any) -> None:
        logged.update(kwargs)

    monkeypatch.setattr("hippius_s3.services.audit_service.AuditLogger.log_request", capture_log_request)
    monkeypatch.setattr("hippius_s3.services.audit_service.AuditLogger.should_skip", lambda self, path, ip: False)

    app = FastAPI()

    @app.get("/{bucket}/{key:path}")
    async def probe(request: Request, bucket: str, key: str = "") -> dict[str, str]:
        return {"ok": "1"}

    # Innermost: plays request_context — rebinds state.account to bucket-owner semantics.
    @app.middleware("http")
    async def fake_request_context(request: Request, call_next: Any) -> Any:
        request.state.account = HippiusAccount(
            id="caller-sub", main_account="bucket-owner", has_credits=True, upload=True, delete=True
        )
        return await call_next(request)

    app.middleware("http")(audit_log_middleware)

    # Outermost: plays the account middleware — the caller's own account.
    @app.middleware("http")
    async def fake_account(request: Request, call_next: Any) -> Any:
        request.state.account = HippiusAccount(
            id="caller-sub", main_account="caller-main", has_credits=True, upload=True, delete=True
        )
        return await call_next(request)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/victim-bucket/key")

    assert response.status_code == 200
    assert logged["account_id"] == "caller-main"


@pytest.mark.asyncio
async def test_audit_attributes_anonymous_as_unknown_despite_owner_rebind(monkeypatch: Any) -> None:
    """Anonymous public read: no account upstream of audit, but request_context still
    binds one (bucket-owner attribution) for the handlers. The audit line must stay
    'unknown', not book the read to the bucket owner."""
    logged: dict[str, Any] = {}

    def capture_log_request(self: Any, **kwargs: Any) -> None:
        logged.update(kwargs)

    monkeypatch.setattr("hippius_s3.services.audit_service.AuditLogger.log_request", capture_log_request)
    monkeypatch.setattr("hippius_s3.services.audit_service.AuditLogger.should_skip", lambda self, path, ip: False)

    app = FastAPI()

    @app.get("/{bucket}/{key:path}")
    async def probe(request: Request, bucket: str, key: str = "") -> dict[str, str]:
        return {"ok": "1"}

    @app.middleware("http")
    async def fake_request_context(request: Request, call_next: Any) -> Any:
        request.state.account = HippiusAccount(
            id="", main_account="bucket-owner", has_credits=False, upload=False, delete=False
        )
        return await call_next(request)

    app.middleware("http")(audit_log_middleware)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/public-bucket/key")

    assert response.status_code == 200
    assert logged["account_id"] == "unknown"
