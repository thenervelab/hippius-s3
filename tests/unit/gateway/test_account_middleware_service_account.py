"""account_middleware's service-account branch.

The bypass is only ever keyed on request.state.account_address, which auth_router derives from
a VERIFIED signature or token. These tests pin that: the exempt path is reachable by exactly one
input, and every adjacent input a client can influence leaves the billing gates in force.
"""

from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi import Request
from httpx import ASGITransport
from httpx import AsyncClient

from hippius_s3.models.account import HippiusAccount
from tests.unit.mocks.mock_arion_service import MockArionService


SERVICE_ACCOUNT = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
REGULAR_ACCOUNT = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"


def _config(service_account_ids: frozenset[str]) -> Any:
    config = MagicMock()
    config.enable_bypass_credit_check = False
    config.substrate_url = "ws://localhost:9944"
    config.can_upload_cache_ttl_seconds = 10
    config.can_upload_transient_retries = 2
    config.can_upload_transient_retry_delay_seconds = 0.0
    config.service_account_ids = service_account_ids
    return config


def _build_app(
    monkeypatch: Any,
    *,
    caller: str,
    allowlist: frozenset[str],
    has_credits: bool = True,
    inject: Any = None,
) -> tuple[FastAPI, MockArionService, dict[str, int]]:
    """App wired with account_middleware. Returns (app, arion mock, account-fetch call counter).

    `has_credits=False` models an account the cacher says is broke — the state a service account
    is expected to write through.
    """
    from hippius_s3.gateway.middlewares import account as account_mod

    monkeypatch.setattr(account_mod, "config", _config(allowlist))

    fetch_calls = {"n": 0}

    async def spy_fetch(address: str, redis_client: Any, substrate_url: str) -> HippiusAccount:
        fetch_calls["n"] += 1
        return HippiusAccount(
            id=address, main_account=address, has_credits=has_credits, upload=True, delete=True
        )

    monkeypatch.setattr(account_mod, "fetch_account_by_main_address", spy_fetch)

    mock_arion = MockArionService(allow_upload=True)

    app = FastAPI()
    redis_accounts = AsyncMock()
    redis_accounts.get = AsyncMock(return_value=None)
    redis_accounts.set = AsyncMock()
    app.state.redis_accounts_client = redis_accounts
    app.state.arion_client = mock_arion

    @app.api_route("/test-bucket/test-key", methods=["GET", "PUT", "POST", "DELETE", "HEAD"])
    async def endpoint(request: Request) -> dict[str, Any]:
        account = request.state.account
        return {
            "service_account": getattr(request.state, "service_account", "UNSET"),
            "has_credits": account.has_credits,
            "main_account": account.main_account,
        }

    async def default_inject(request: Request, call_next: Any) -> Any:
        request.state.auth_method = "access_key"
        request.state.account_address = caller
        return await call_next(request)

    app.middleware("http")(account_mod.account_middleware)
    app.middleware("http")(inject or default_inject)

    return app, mock_arion, fetch_calls


async def _put(app: FastAPI, headers: dict[str, str] | None = None) -> Any:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        return await client.put(
            "/test-bucket/test-key",
            content=b"hello",
            headers={"content-length": "5", **(headers or {})},
        )


# ---------------------------------------------------------------------------
# The feature
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_service_account_put_skips_both_billing_gates(monkeypatch: Any) -> None:
    app, arion, fetch_calls = _build_app(
        monkeypatch, caller=SERVICE_ACCOUNT, allowlist=frozenset({SERVICE_ACCOUNT})
    )

    response = await _put(app)

    assert response.status_code == 200
    assert arion.can_upload_calls == [], "a service account must not consult the billing service"
    assert fetch_calls["n"] == 0, "and must not depend on the account-credit cache either"
    assert response.json()["service_account"] is True


@pytest.mark.asyncio
async def test_service_account_writes_through_a_zero_credit_verdict(monkeypatch: Any) -> None:
    """The point of the feature: our own uploads must not stop because the account-cacher says
    an internal account has no credit."""
    app, _arion, _fetch = _build_app(
        monkeypatch,
        caller=SERVICE_ACCOUNT,
        allowlist=frozenset({SERVICE_ACCOUNT}),
        has_credits=False,
    )

    response = await _put(app)

    assert response.status_code == 200
    assert response.json()["has_credits"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["PUT", "POST", "DELETE"])
async def test_bypass_covers_every_mutating_method(monkeypatch: Any, method: str) -> None:
    app, arion, fetch_calls = _build_app(
        monkeypatch, caller=SERVICE_ACCOUNT, allowlist=frozenset({SERVICE_ACCOUNT}), has_credits=False
    )

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.request(
            method, "/test-bucket/test-key", content=b"hello", headers={"content-length": "5"}
        )

    assert response.status_code == 200
    assert fetch_calls["n"] == 0
    assert arion.can_upload_calls == []


@pytest.mark.asyncio
async def test_multiple_accounts_on_the_allowlist_are_all_exempt(monkeypatch: Any) -> None:
    app, arion, _fetch = _build_app(
        monkeypatch,
        caller=REGULAR_ACCOUNT,
        allowlist=frozenset({SERVICE_ACCOUNT, REGULAR_ACCOUNT}),
        has_credits=False,
    )

    assert (await _put(app)).status_code == 200
    assert arion.can_upload_calls == []


# ---------------------------------------------------------------------------
# Nobody else gets in
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_regular_account_is_still_fully_gated(monkeypatch: Any) -> None:
    """Regression guard on the whole existing billing path."""
    app, arion, fetch_calls = _build_app(
        monkeypatch, caller=REGULAR_ACCOUNT, allowlist=frozenset({SERVICE_ACCOUNT})
    )

    response = await _put(app)

    assert response.status_code == 200
    assert len(arion.can_upload_calls) == 1, "an unlisted account must still hit can_upload"
    assert fetch_calls["n"] == 1, "and must still have its credit fetched"
    assert response.json()["service_account"] is False


@pytest.mark.asyncio
async def test_regular_account_without_credit_still_gets_402(monkeypatch: Any) -> None:
    app, _arion, _fetch = _build_app(
        monkeypatch,
        caller=REGULAR_ACCOUNT,
        allowlist=frozenset({SERVICE_ACCOUNT}),
        has_credits=False,
    )

    response = await _put(app)

    assert response.status_code == 402
    assert b"InsufficientAccountCredit" in response.content


@pytest.mark.asyncio
async def test_empty_allowlist_exempts_nobody(monkeypatch: Any) -> None:
    """The unconfigured deployment must behave exactly as it did before this feature."""
    app, arion, fetch_calls = _build_app(monkeypatch, caller=SERVICE_ACCOUNT, allowlist=frozenset())

    response = await _put(app)

    assert response.status_code == 200
    assert len(arion.can_upload_calls) == 1
    assert fetch_calls["n"] == 1
    assert response.json()["service_account"] is False


@pytest.mark.asyncio
async def test_case_flipped_address_is_not_exempt(monkeypatch: Any) -> None:
    """Two SS58 addresses differing only in case are two different accounts."""
    near_miss = SERVICE_ACCOUNT[0] + SERVICE_ACCOUNT[1].swapcase() + SERVICE_ACCOUNT[2:]
    assert near_miss != SERVICE_ACCOUNT

    app, arion, _fetch = _build_app(
        monkeypatch, caller=near_miss, allowlist=frozenset({SERVICE_ACCOUNT}), has_credits=False
    )

    response = await _put(app)

    assert response.status_code == 402
    assert arion.can_upload_calls == []  # rejected at the credit gate, before can_upload


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers",
    [
        {"x-hippius-service-account": "true"},
        {"x-hippius-account-address": SERVICE_ACCOUNT},
        {"x-hippius-main-account": SERVICE_ACCOUNT},
        {"x-hippius-request-user": SERVICE_ACCOUNT},
        {"x-amz-meta-service-account": "true"},
        {"authorization": f"Bearer {SERVICE_ACCOUNT}"},
    ],
)
async def test_client_headers_cannot_claim_the_exemption(monkeypatch: Any, headers: dict[str, str]) -> None:
    """The exemption is derived from verified auth state, never from anything on the wire.
    A regular caller naming the service account in any header stays billed."""
    app, arion, fetch_calls = _build_app(
        monkeypatch, caller=REGULAR_ACCOUNT, allowlist=frozenset({SERVICE_ACCOUNT})
    )

    response = await _put(app, headers=headers)

    assert response.json()["service_account"] is False
    assert len(arion.can_upload_calls) == 1
    assert fetch_calls["n"] == 1


@pytest.mark.asyncio
async def test_a_service_account_bucket_owner_does_not_exempt_the_caller(monkeypatch: Any) -> None:
    """The gateway gate bills the CALLER. Writing into a service account's bucket must not
    launder a regular user's write past the credit check — otherwise anyone granted write
    access to one of our buckets would upload for free."""
    from hippius_s3.gateway.middlewares import account as account_mod

    async def inject(request: Request, call_next: Any) -> Any:
        request.state.auth_method = "access_key"
        request.state.account_address = REGULAR_ACCOUNT
        # what acl_middleware would resolve for a bucket owned by the service account
        request.state.bucket_owner_id = SERVICE_ACCOUNT
        request.state.main_account_id = SERVICE_ACCOUNT
        return await call_next(request)

    app, arion, fetch_calls = _build_app(
        monkeypatch,
        caller=REGULAR_ACCOUNT,
        allowlist=frozenset({SERVICE_ACCOUNT}),
        has_credits=False,
        inject=inject,
    )
    assert account_mod  # imported for the monkeypatch target above

    response = await _put(app)

    assert response.status_code == 402
    assert fetch_calls["n"] == 1


@pytest.mark.asyncio
async def test_anonymous_request_is_never_a_service_account(monkeypatch: Any) -> None:
    """Anonymous callers land on account_id='anonymous'. That string can never be allowlisted
    (config rejects non-SS58), but the flag must read False regardless."""
    from hippius_s3.gateway.middlewares import account as account_mod

    monkeypatch.setattr(account_mod, "config", _config(frozenset({SERVICE_ACCOUNT})))

    app = FastAPI()

    @app.get("/test-bucket/test-key")
    async def endpoint(request: Request) -> dict[str, Any]:
        return {
            "service_account": getattr(request.state, "service_account", "UNSET"),
            "account_id": request.state.account_id,
        }

    app.middleware("http")(account_mod.account_middleware)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/test-bucket/test-key")

    assert response.json() == {"service_account": False, "account_id": "anonymous"}


@pytest.mark.asyncio
async def test_bypass_survives_an_uninitialised_metrics_collector(monkeypatch: Any) -> None:
    """The bypass branch records a metric from inside account_middleware's blanket
    `except Exception`, which answers 503 AccountVerificationError. A collector missing the
    method — NullMetricsCollector before this feature added it, or any pod that has not
    finished its lifespan — would turn every internal write into a 503. Pin the null path.
    """
    from hippius_s3.gateway.middlewares import account as account_mod
    from hippius_s3.monitoring import NullMetricsCollector

    app, arion, _fetch = _build_app(
        monkeypatch, caller=SERVICE_ACCOUNT, allowlist=frozenset({SERVICE_ACCOUNT}), has_credits=False
    )
    monkeypatch.setattr(account_mod, "get_metrics_collector", lambda: NullMetricsCollector())

    response = await _put(app)

    assert response.status_code == 200
    assert b"AccountVerificationError" not in response.content
    assert arion.can_upload_calls == []


@pytest.mark.asyncio
async def test_bypass_is_recorded_on_the_metrics_collector(monkeypatch: Any) -> None:
    from hippius_s3.gateway.middlewares import account as account_mod

    app, _arion, _fetch = _build_app(
        monkeypatch, caller=SERVICE_ACCOUNT, allowlist=frozenset({SERVICE_ACCOUNT})
    )
    collector = MagicMock()
    monkeypatch.setattr(account_mod, "get_metrics_collector", lambda: collector)

    await _put(app)

    collector.record_billing_bypass.assert_called_once_with(surface="gateway")


@pytest.mark.asyncio
async def test_no_bypass_metric_for_a_regular_account(monkeypatch: Any) -> None:
    from hippius_s3.gateway.middlewares import account as account_mod

    app, _arion, _fetch = _build_app(
        monkeypatch, caller=REGULAR_ACCOUNT, allowlist=frozenset({SERVICE_ACCOUNT})
    )
    collector = MagicMock()
    monkeypatch.setattr(account_mod, "get_metrics_collector", lambda: collector)

    await _put(app)

    collector.record_billing_bypass.assert_not_called()


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_service_account_read_is_flagged_but_gains_no_privilege(monkeypatch: Any) -> None:
    """Reads are not billed, so there is nothing to bypass. The flag is still stamped — the audit
    log should show everything an internal account did — but the account object must stay the
    same lightweight read stand-in every other caller gets (GW-4), not a credited one."""
    app, arion, fetch_calls = _build_app(
        monkeypatch, caller=SERVICE_ACCOUNT, allowlist=frozenset({SERVICE_ACCOUNT})
    )

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/test-bucket/test-key")

    assert response.status_code == 200
    body = response.json()
    assert body["service_account"] is True
    assert body["has_credits"] is False, "reads must not mint credit flags for anyone"
    assert fetch_calls["n"] == 0
    assert arion.can_upload_calls == []
