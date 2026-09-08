"""The billing-plan branch of account_middleware.

Two things are being pinned here that nothing else pins:

  1. A plan account skips BOTH pay-as-you-go gates -- the substrate has_credits check AND Arion
     can_upload. Plan customers hold no substrate credits, so leaving either in place 402s every one
     of them. The `can_upload_calls == 0` assertions are the load-bearing part.
  2. An account with no plan takes the OLD path, unchanged. Plans run in parallel with PAYG; this
     feature must be invisible to every account that is not on a plan.
"""

import json
import logging
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


GB = 1_000_000_000
ACCOUNT = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"


@pytest.fixture  # type: ignore[misc]
def plan_config() -> Any:
    config = MagicMock()
    config.enable_bypass_credit_check = False
    config.service_account_ids = frozenset()
    config.substrate_url = "ws://localhost:9944"
    config.can_upload_cache_ttl_seconds = 10
    config.can_upload_transient_retries = 2
    config.can_upload_transient_retry_delay_seconds = 0.0
    config.enable_billing_plans = True
    config.usage_cache_ttl_seconds = 30
    config.usage_authoritative_timeout_seconds = 5.0
    return config


class PlanRedis:
    """redis-accounts stand-in serving the plan hash plus the usage/can_upload string keys."""

    def __init__(self, plan_id: str | None, storage_bytes: int | None, cached_usage: int | None) -> None:
        self._plan_id = plan_id
        self._storage_bytes = storage_bytes
        self._cached_usage = cached_usage

    async def hget(self, key: str, field: str) -> bytes | None:
        if key == "hippius_s3_plan_accounts" and self._plan_id:
            return json.dumps({"plan": self._plan_id, "storage_bytes": self._storage_bytes}).encode()
        return None

    async def get(self, key: str) -> bytes | None:
        if key.startswith("hippius_s3_usage:") and self._cached_usage is not None:
            return str(self._cached_usage).encode()
        return None

    async def setex(self, *args: object, **kwargs: object) -> None:
        return None

    async def set(self, *args: object, **kwargs: object) -> None:
        return None

    async def delete(self, *args: object) -> None:
        return None


def build_app(
    config: Any,
    monkeypatch: Any,
    *,
    plan_id: str | None,
    storage_bytes: int | None = None,
    cached_usage: int | None = 0,
    authoritative_usage: int = 0,
    has_credits: bool = True,
) -> tuple[FastAPI, MockArionService]:
    from hippius_s3.gateway.middlewares import account as account_module
    from hippius_s3.gateway.middlewares.account import account_middleware

    monkeypatch.setattr("hippius_s3.gateway.middlewares.account.config", config)
    monkeypatch.setattr(
        "hippius_s3.gateway.services.plan_gate.usage_service.get_account_bytes_authoritative",
        AsyncMock(return_value=authoritative_usage),
    )

    async def fake_fetch(address: str, redis_client: Any, substrate_url: str) -> HippiusAccount:
        return HippiusAccount(id=address, main_account=address, has_credits=has_credits, upload=True, delete=True)

    monkeypatch.setattr(account_module, "fetch_account_by_main_address", fake_fetch)

    mock_arion = MockArionService(allow_upload=True)

    app = FastAPI()
    app.state.redis_accounts_client = PlanRedis(plan_id, storage_bytes, cached_usage)
    app.state.arion_client = mock_arion
    app.state.postgres_pool = MagicMock()

    @app.api_route("/test-bucket/test-key", methods=["GET", "PUT", "POST", "DELETE", "HEAD"])
    async def endpoint(request: Request) -> dict[str, Any]:
        return {"plan_id": getattr(request.state, "plan_id", None)}

    async def inject_access_key(request: Request, call_next: Any) -> Any:
        request.state.auth_method = "access_key"
        request.state.account_address = ACCOUNT
        return await call_next(request)

    app.middleware("http")(account_middleware)
    app.middleware("http")(inject_access_key)
    return app, mock_arion


async def put(app: FastAPI, size: int = 5) -> Any:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        return await client.put("/test-bucket/test-key", content=b"x" * size, headers={"content-length": str(size)})


@pytest.mark.asyncio
async def test_a_plan_account_under_quota_uploads_without_touching_arion(plan_config: Any, monkeypatch: Any) -> None:
    app, arion = build_app(plan_config, monkeypatch, plan_id="pro", storage_bytes=10 * GB, cached_usage=1 * GB)

    response = await put(app)

    assert response.status_code == 200
    assert response.json()["plan_id"] == "pro"
    assert len(arion.can_upload_calls) == 0


@pytest.mark.asyncio
async def test_a_plan_account_over_quota_is_refused_with_a_useful_message(plan_config: Any, monkeypatch: Any) -> None:
    app, arion = build_app(
        plan_config,
        monkeypatch,
        plan_id="pro",
        storage_bytes=10 * GB,
        cached_usage=11 * GB,
        authoritative_usage=11 * GB,
    )

    response = await put(app)

    assert response.status_code == 402
    body = response.content.decode()
    assert "QuotaExceeded" in body
    assert "10.00 GB" in body
    assert "upgrade" in body.lower()
    assert len(arion.can_upload_calls) == 0


@pytest.mark.asyncio
async def test_a_plan_account_with_no_substrate_credits_still_uploads(plan_config: Any, monkeypatch: Any) -> None:
    """The regression this feature would otherwise ship: a plan customer holds no substrate
    credits, so the has_credits gate would 402 them before the quota check ever ran."""
    app, _ = build_app(
        plan_config,
        monkeypatch,
        plan_id="pro",
        storage_bytes=10 * GB,
        cached_usage=0,
        has_credits=False,
    )

    assert (await put(app)).status_code == 200


@pytest.mark.asyncio
async def test_an_over_quota_plan_account_can_still_delete(plan_config: Any, monkeypatch: Any) -> None:
    """A customer who downgraded below their usage must be able to dig themselves out."""
    app, _ = build_app(
        plan_config,
        monkeypatch,
        plan_id="pro",
        storage_bytes=1 * GB,
        cached_usage=500 * GB,
        authoritative_usage=500 * GB,
    )

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.delete("/test-bucket/test-key")

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_an_account_with_no_plan_takes_the_pay_as_you_go_path(plan_config: Any, monkeypatch: Any) -> None:
    """The parallel-path guarantee. Everything about PAYG must be unchanged."""
    app, arion = build_app(plan_config, monkeypatch, plan_id=None)

    response = await put(app)

    assert response.status_code == 200
    assert response.json()["plan_id"] is None
    assert len(arion.can_upload_calls) == 1


@pytest.mark.asyncio
async def test_the_master_switch_off_returns_every_account_to_pay_as_you_go(plan_config: Any, monkeypatch: Any) -> None:
    """HIPPIUS_ENABLE_BILLING_PLANS=false: a wildly over-quota plan account is still billed
    pay-as-you-go and still reaches Arion, exactly as before this feature existed."""
    plan_config.enable_billing_plans = False
    app, arion = build_app(plan_config, monkeypatch, plan_id="pro", storage_bytes=1, cached_usage=999 * GB)

    assert (await put(app)).status_code == 200
    assert len(arion.can_upload_calls) == 1


@pytest.mark.asyncio
async def test_the_master_switch_off_logs_what_it_would_have_done(
    plan_config: Any, monkeypatch: Any, caplog: Any
) -> None:
    """The point of shipping disabled: prove the whole chain works in prod logs before it can cost
    anyone an upload."""
    plan_config.enable_billing_plans = False
    app, arion = build_app(plan_config, monkeypatch, plan_id="business", storage_bytes=10 * GB, cached_usage=99 * GB)

    with caplog.at_level(logging.INFO):
        assert (await put(app)).status_code == 200

    shadow = [r.getMessage() for r in caplog.records if "BILLING_PLAN_SHADOW" in r.getMessage()]
    assert len(shadow) == 1, "exactly one shadow line per write"
    line = shadow[0]
    assert "enforcement=disabled" in line
    assert "plan=business" in line
    assert "would=would_deny" in line
    assert f"used_bytes={99 * GB}" in line
    assert f"limit_bytes={10 * GB}" in line
    assert "HIPPIUS_ENABLE_BILLING_PLANS=true" in line, "the line must say how to act on it"
    # ...and the request was still billed the old way.
    assert len(arion.can_upload_calls) == 1


@pytest.mark.asyncio
async def test_the_master_switch_off_logs_allow_for_an_account_under_quota(
    plan_config: Any, monkeypatch: Any, caplog: Any
) -> None:
    plan_config.enable_billing_plans = False
    app, _ = build_app(plan_config, monkeypatch, plan_id="pro", storage_bytes=10 * GB, cached_usage=1 * GB)

    with caplog.at_level(logging.INFO):
        await put(app)

    assert any("would=allow" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_the_master_switch_off_logs_nothing_for_an_account_with_no_plan(
    plan_config: Any, monkeypatch: Any, caplog: Any
) -> None:
    """A line per pay-as-you-go write would bury the signal in the volume it exists to be found in."""
    plan_config.enable_billing_plans = False
    app, _ = build_app(plan_config, monkeypatch, plan_id=None)

    with caplog.at_level(logging.INFO):
        await put(app)

    assert not any("BILLING_PLAN_SHADOW" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_a_failure_inside_the_shadow_path_cannot_break_the_upload(
    plan_config: Any, monkeypatch: Any, caplog: Any
) -> None:
    """Pure observation. A diagnostic feature must never 500 a live upload."""
    plan_config.enable_billing_plans = False
    app, arion = build_app(plan_config, monkeypatch, plan_id="pro", storage_bytes=10 * GB, cached_usage=1 * GB)
    monkeypatch.setattr(
        "hippius_s3.gateway.services.plan_gate.usage_service.get_account_bytes",
        AsyncMock(side_effect=RuntimeError("postgres is down")),
    )

    with caplog.at_level(logging.INFO):
        response = await put(app)

    assert response.status_code == 200
    assert len(arion.can_upload_calls) == 1
    assert any("BILLING_PLAN_SHADOW" in r.getMessage() and "error=" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_no_shadow_line_is_emitted_when_the_feature_is_on(
    plan_config: Any, monkeypatch: Any, caplog: Any
) -> None:
    app, _ = build_app(plan_config, monkeypatch, plan_id="pro", storage_bytes=10 * GB, cached_usage=1 * GB)

    with caplog.at_level(logging.INFO):
        assert (await put(app)).status_code == 200

    assert not any("BILLING_PLAN_SHADOW" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_a_cold_catalog_allows_the_upload(plan_config: Any, monkeypatch: Any) -> None:
    """Positively on a plan, but the catalog cannot price it. Allow -- never block a paying
    customer because the plans-cacher has not warmed up."""
    app, arion = build_app(plan_config, monkeypatch, plan_id="pro", storage_bytes=None, cached_usage=999 * GB)

    assert (await put(app)).status_code == 200
    assert len(arion.can_upload_calls) == 0


@pytest.mark.asyncio
async def test_a_redis_failure_falls_back_to_pay_as_you_go(plan_config: Any, monkeypatch: Any) -> None:
    """Not a new failure mode: this is exactly what the code did before plans existed."""
    app, arion = build_app(plan_config, monkeypatch, plan_id="pro", storage_bytes=10 * GB)

    class ExplodingRedis(PlanRedis):
        async def hget(self, key: str, field: str) -> bytes | None:
            raise ConnectionError("redis-accounts is down")

    app.state.redis_accounts_client = ExplodingRedis("pro", None, 0)

    assert (await put(app)).status_code == 200
    assert len(arion.can_upload_calls) == 1


@pytest.mark.asyncio
async def test_a_drifted_counter_cannot_deny(plan_config: Any, monkeypatch: Any) -> None:
    """Cached counter says wildly over; ground truth says fine. Asymmetric verification wins."""
    app, _ = build_app(
        plan_config,
        monkeypatch,
        plan_id="pro",
        storage_bytes=10 * GB,
        cached_usage=900 * GB,
        authoritative_usage=1 * GB,
    )

    assert (await put(app)).status_code == 200


@pytest.mark.asyncio
async def test_reads_never_consult_the_plan_caches(plan_config: Any, monkeypatch: Any) -> None:
    """GET/HEAD take the lightweight branch above the plan branch; the hot read path stays clean."""
    app, _ = build_app(plan_config, monkeypatch, plan_id="pro", storage_bytes=10 * GB)

    calls: list[str] = []

    class CountingRedis(PlanRedis):
        async def hget(self, key: str, field: str) -> bytes | None:
            calls.append(key)
            return await super().hget(key, field)

    app.state.redis_accounts_client = CountingRedis("pro", 10 * GB, 0)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        assert (await client.get("/test-bucket/test-key")).status_code == 200

    assert calls == []


@pytest.mark.asyncio
async def test_a_service_account_is_still_exempt_before_the_plan_branch(plan_config: Any, monkeypatch: Any) -> None:
    plan_config.service_account_ids = frozenset({ACCOUNT})
    app, arion = build_app(
        plan_config,
        monkeypatch,
        plan_id="pro",
        storage_bytes=1,
        cached_usage=999 * GB,
        authoritative_usage=999 * GB,
    )

    assert (await put(app)).status_code == 200
    assert len(arion.can_upload_calls) == 0
