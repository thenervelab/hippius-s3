"""The audit line's `service_account` field.

This field is the durable record that an operation ran unbilled — the thing an auditor greps
for months later. It must be present on every line, must be a real bool, and must never read
true for traffic the account middleware did not itself mark.
"""

import json
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi import Request
from httpx import ASGITransport
from httpx import AsyncClient

from hippius_s3.api.middlewares.request_context import request_context_middleware
from hippius_s3.gateway.middlewares.audit_log import audit_log_middleware
from hippius_s3.models.account import HippiusAccount
from hippius_s3.services.audit_service import AuditLogger


def _app(state: dict[str, Any] | None) -> FastAPI:
    app = FastAPI()

    @app.get("/{bucket}/{key:path}")
    async def probe(request: Request, bucket: str, key: str = "") -> dict[str, str]:
        return {"ok": "1"}

    app.middleware("http")(request_context_middleware)
    app.middleware("http")(audit_log_middleware)

    if state is not None:

        @app.middleware("http")
        async def fake_account(request: Request, call_next: Any) -> Any:
            request.state.account = HippiusAccount(
                id="caller", main_account="caller", has_credits=True, upload=True, delete=True
            )
            for key_, value in state.items():
                setattr(request.state, key_, value)
            return await call_next(request)

    return app


async def _logged(app: FastAPI, monkeypatch: Any) -> dict[str, Any]:
    logged: dict[str, Any] = {}

    def capture(self: Any, **kwargs: Any) -> None:
        logged.update(kwargs)

    monkeypatch.setattr("hippius_s3.services.audit_service.AuditLogger.log_request", capture)
    monkeypatch.setattr("hippius_s3.services.audit_service.AuditLogger.should_skip", lambda self, p, ip: False)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        assert (await client.get("/bucket/key")).status_code == 200
    return logged


@pytest.mark.asyncio
async def test_service_account_request_is_recorded_as_such(monkeypatch: Any) -> None:
    logged = await _logged(_app({"service_account": True}), monkeypatch)
    assert logged["service_account"] is True


@pytest.mark.asyncio
async def test_regular_request_is_recorded_as_not_a_service_account(monkeypatch: Any) -> None:
    logged = await _logged(_app({"service_account": False}), monkeypatch)
    assert logged["service_account"] is False


@pytest.mark.asyncio
async def test_field_is_present_even_when_the_middleware_never_ran(monkeypatch: Any) -> None:
    """Paths that skip account_middleware (docs, /admin, peer fetch) leave the flag unset. The
    field must still be emitted as false — an absent field is indistinguishable from a log line
    written before this feature existed, which makes the audit query unsound."""
    logged = await _logged(_app(None), monkeypatch)
    assert logged["service_account"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("truthy", ["true", "1", 1, ["yes"], object()])
async def test_a_truthy_non_bool_does_not_become_a_service_account_claim(monkeypatch: Any, truthy: Any) -> None:
    """The audit log must only ever report the exemption the account middleware actually granted,
    which it signals with a literal True. Anything else is not that claim."""
    logged = await _logged(_app({"service_account": truthy}), monkeypatch)
    assert logged["service_account"] is False


# ---------------------------------------------------------------------------
# Emitted shape — what Loki actually receives
# ---------------------------------------------------------------------------


def _emit(service_account: bool) -> dict[str, Any]:
    lines: list[str] = []

    class CaptureLogger:
        def info(self, msg: str) -> None:
            lines.append(msg)

        def warning(self, msg: str) -> None:
            lines.append(msg)

        def error(self, msg: str) -> None:
            lines.append(msg)

    AuditLogger("audit", logger=CaptureLogger()).log_request(  # ty: ignore[invalid-argument-type]
        client_ip="1.2.3.4",
        user_agent="aws-cli",
        account_id="5Grw",
        method="PUT",
        path="/bucket/key",
        query_params={},
        status_code=200,
        processing_time_ms=1.0,
        content_length=5,
        timestamp=0.0,
        service_account=service_account,
    )
    assert len(lines) == 1
    return json.loads(lines[0].split("S3_OPERATION_SUCCESS: ", 1)[1])


def test_emitted_json_carries_a_json_bool_not_a_string() -> None:
    """`|= "\\"service_account\\": true"` is the Loki query operators will write. A stringified
    "True" would silently match nothing."""
    assert _emit(True)["service_account"] is True
    assert _emit(False)["service_account"] is False


def test_defaults_to_false_when_the_caller_omits_it() -> None:
    """Every other log_request call site in the tree omits the new kwarg. None of them may start
    reporting unbilled traffic."""
    lines: list[str] = []

    class CaptureLogger:
        def info(self, msg: str) -> None:
            lines.append(msg)

    AuditLogger("audit", logger=CaptureLogger()).log_request(  # ty: ignore[invalid-argument-type]
        client_ip="1.2.3.4",
        user_agent="aws-cli",
        account_id="5Grw",
        method="GET",
        path="/bucket/key",
        query_params={},
        status_code=200,
        processing_time_ms=1.0,
        content_length=5,
        timestamp=0.0,
    )

    assert json.loads(lines[0].split("S3_OPERATION_SUCCESS: ", 1)[1])["service_account"] is False
