"""Quota-gate decision logic.

The property under test throughout is the asymmetry: the cheap cached counter may ALLOW, only the
authoritative SUM may DENY. Every path where ground truth is unavailable must resolve to "allow",
because a drifted or slow counter blocking a paying customer's upload is a support incident, while
letting one over-quota upload through is a rounding error the reconciler catches.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest

from hippius_s3.gateway.services import plan_gate
from hippius_s3.gateway.services.plan_gate import PlanLookupUnavailable
from hippius_s3.services.plans_cache import PlanQuota


GB = 1_000_000_000


def make_config(**overrides: object) -> SimpleNamespace:
    base = dict(
        enable_billing_plans=True,
        usage_cache_ttl_seconds=30,
        usage_authoritative_timeout_seconds=5.0,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


class StubRedis:
    """redis-accounts stand-in: the account hash carries plan + allowance, the catalog is fallback."""

    def __init__(self, plan_id: str | None = None, storage_bytes: int | None = None) -> None:
        self._plan_id = plan_id
        self._storage_bytes = storage_bytes

    async def hget(self, key: str, field: str):
        import json

        if key == "hippius_s3_plan_accounts":
            if not self._plan_id:
                return None
            return json.dumps({"plan": self._plan_id, "storage_bytes": self._storage_bytes}).encode()
        return None


# --------------------------------------------------------------------------- resolve_plan


@pytest.mark.asyncio
async def test_resolve_plan_still_answers_when_the_feature_is_off() -> None:
    """resolve_plan is deliberately NOT gated on enable_billing_plans.

    With the feature off we still want to know the account is on a plan, so the middleware can emit
    the BILLING_PLAN_SHADOW line. The flag decides what is DONE with the answer, not whether the
    question is asked.
    """
    redis = StubRedis(plan_id="pro", storage_bytes=GB)
    resolved = await plan_gate.resolve_plan(redis, "acct", make_config(enable_billing_plans=False))
    assert resolved is not None and resolved.plan_id == "pro"


@pytest.mark.asyncio
async def test_an_account_with_no_plan_row_is_pay_as_you_go() -> None:
    assert await plan_gate.resolve_plan(StubRedis(plan_id=None), "acct", make_config()) is None


@pytest.mark.asyncio
async def test_a_plan_with_a_known_quota_resolves() -> None:
    redis = StubRedis(plan_id="business", storage_bytes=5 * GB)
    quota = await plan_gate.resolve_plan(redis, "acct", make_config())

    assert quota == PlanQuota(plan_id="business", storage_bytes=5 * GB)
    assert quota.enforceable


@pytest.mark.asyncio
async def test_a_plan_whose_quota_is_unknown_resolves_with_no_quota() -> None:
    """Cold catalog. The account is positively on a plan, so it must NOT fall to pay-as-you-go
    (which would 402 them on the credit check) -- it resolves with quota None and is allowed."""
    redis = StubRedis(plan_id="pro", storage_bytes=None)
    quota = await plan_gate.resolve_plan(redis, "acct", make_config())

    assert quota is not None and quota.plan_id == "pro"
    assert not quota.enforceable, "on a plan, allowance unknown -> callers ALLOW"


@pytest.mark.asyncio
async def test_a_nonpositive_allowance_resolves_with_no_quota() -> None:
    redis = StubRedis(plan_id="pro", storage_bytes=0)
    quota = await plan_gate.resolve_plan(redis, "acct", make_config())
    assert quota is not None and not quota.enforceable


@pytest.mark.asyncio
async def test_a_redis_error_raises_lookup_unavailable() -> None:
    """The caller falls back to the pay-as-you-go path on this, which is exactly today's behaviour
    -- so a Redis blip can never be a NEW failure mode introduced by plans."""

    class ExplodingRedis:
        async def hget(self, *args: object) -> None:
            raise ConnectionError("redis is down")

    with pytest.raises(PlanLookupUnavailable):
        await plan_gate.resolve_plan(ExplodingRedis(), "acct", make_config())


@pytest.mark.asyncio
async def test_malformed_cached_quota_json_raises_lookup_unavailable() -> None:
    class GarbageRedis:
        async def hget(self, key: str, field: str) -> bytes:
            return b"{not json"

    with pytest.raises(PlanLookupUnavailable):
        await plan_gate.resolve_plan(GarbageRedis(), "acct", make_config())


# --------------------------------------------------------------------------- evaluate_quota


async def evaluate(cached_used: int, authoritative: int | None, incoming: int, limit: int | None, **cfg):
    quota = PlanQuota(plan_id="pro", storage_bytes=limit)
    with (
        patch.object(plan_gate.usage_service, "get_account_bytes", AsyncMock(return_value=cached_used)),
        patch.object(plan_gate, "_authoritative_bytes", AsyncMock(return_value=authoritative)),
    ):
        return await plan_gate.evaluate_quota(
            db=object(),
            redis_accounts_client=object(),
            main_account_id="acct",
            quota=quota,
            incoming_bytes=incoming,
            config=make_config(**cfg),
        )


@pytest.mark.asyncio
async def test_under_quota_allows_without_consulting_ground_truth() -> None:
    """The whole point of the cheap counter: the common case costs one Redis GET."""
    with (
        patch.object(plan_gate.usage_service, "get_account_bytes", AsyncMock(return_value=1 * GB)),
        patch.object(plan_gate, "_authoritative_bytes", AsyncMock()) as authoritative,
    ):
        decision = await plan_gate.evaluate_quota(
            db=object(),
            redis_accounts_client=object(),
            main_account_id="acct",
            quota=PlanQuota(plan_id="pro", storage_bytes=10 * GB),
            incoming_bytes=1 * GB,
            config=make_config(),
        )

    assert decision.outcome == "allow"
    authoritative.assert_not_called()


@pytest.mark.asyncio
async def test_exactly_at_the_limit_is_allowed() -> None:
    decision = await evaluate(cached_used=9 * GB, authoritative=None, incoming=1 * GB, limit=10 * GB)
    assert decision.outcome == "allow"


@pytest.mark.asyncio
async def test_one_byte_over_the_limit_is_denied() -> None:
    decision = await evaluate(cached_used=10 * GB, authoritative=10 * GB, incoming=1, limit=10 * GB)
    assert decision.outcome == "deny"


@pytest.mark.asyncio
async def test_a_denial_is_verified_against_ground_truth_before_it_is_returned() -> None:
    """A drifted counter must never 402 a paying customer.

    The cached counter says 20 GB (over a 10 GB plan); ground truth says 1 GB. The upload proceeds.
    """
    decision = await evaluate(cached_used=20 * GB, authoritative=1 * GB, incoming=1 * GB, limit=10 * GB)

    assert decision.outcome == "allow"
    assert decision.used_bytes == 1 * GB


@pytest.mark.asyncio
async def test_an_unavailable_ground_truth_allows_rather_than_denies() -> None:
    """Statement timeout on the authoritative SUM. A slow query must not become a 402."""
    decision = await evaluate(cached_used=20 * GB, authoritative=None, incoming=1 * GB, limit=10 * GB)
    assert decision.outcome == "allow"


# --------------------------------------------------------------------------- shadow_evaluate


async def shadow(cached_used: int, incoming: int, limit: int | None):
    quota = PlanQuota(plan_id="pro", storage_bytes=limit)
    with (
        patch.object(plan_gate.usage_service, "get_account_bytes", AsyncMock(return_value=cached_used)),
        patch.object(plan_gate.usage_service, "get_account_bytes_authoritative", AsyncMock()) as authoritative,
    ):
        decision = await plan_gate.shadow_evaluate(
            db=object(),
            redis_accounts_client=object(),
            main_account_id="acct",
            quota=quota,
            incoming_bytes=incoming,
            config=make_config(enable_billing_plans=False),
        )
    return decision, authoritative


@pytest.mark.asyncio
async def test_shadow_reports_would_deny_without_denying_anything() -> None:
    decision, _ = await shadow(cached_used=20 * GB, incoming=1 * GB, limit=10 * GB)
    assert decision.outcome == "would_deny"
    assert decision.allowed, "shadow mode must never produce a denial"


@pytest.mark.asyncio
async def test_shadow_reports_allow_when_under_quota() -> None:
    decision, _ = await shadow(cached_used=1 * GB, incoming=1 * GB, limit=10 * GB)
    assert decision.outcome == "allow"


@pytest.mark.asyncio
async def test_shadow_never_runs_the_expensive_authoritative_query() -> None:
    """Shadow runs on the pay-as-you-go path of every write. The authoritative SUM can take seconds
    on a large account; adding it to a live upload for the sake of a log line would be a
    self-inflicted latency regression."""
    _, authoritative = await shadow(cached_used=999 * GB, incoming=1 * GB, limit=1 * GB)
    authoritative.assert_not_called()


@pytest.mark.asyncio
async def test_shadow_reports_catalog_miss_when_the_plan_has_no_known_quota() -> None:
    decision, _ = await shadow(cached_used=1 * GB, incoming=1, limit=None)
    assert decision.outcome == "catalog_miss"


@pytest.mark.asyncio
async def test_an_unknown_quota_allows_and_reports_catalog_miss() -> None:
    decision = await evaluate(cached_used=999 * GB, authoritative=999 * GB, incoming=1 * GB, limit=None)
    assert decision.outcome == "catalog_miss"
    assert decision.allowed


@pytest.mark.asyncio
async def test_a_zero_byte_request_is_allowed_when_already_at_the_limit() -> None:
    """A request that declares no size reads as 0 bytes and is admitted, exactly as can_upload
    already admits it. Pinned so the plan gate's precision is understood to be identical to the
    gate it sits beside — see _declared_content_length."""
    decision = await evaluate(cached_used=10 * GB, authoritative=10 * GB, incoming=0, limit=10 * GB)
    assert decision.outcome == "allow"


# --------------------------------------------------------------------------- the 402 body


def test_the_denial_message_carries_the_real_numbers_and_an_action() -> None:
    decision = plan_gate.PlanDecision(plan_id="pro", outcome="deny", quota_bytes=10 * GB, used_bytes=12 * GB)
    message = plan_gate.quota_exceeded_message(decision)

    assert "10.00 GB" in message
    assert "12.00 GB" in message
    assert "upgrade" in message.lower()


def test_the_denial_message_cannot_be_misread_as_a_transient_billing_error() -> None:
    """account.py substring-matches billing error text to decide what is retryable. A quota message
    containing one of those phrases would turn a hard denial into a retry loop."""
    from hippius_s3.gateway.middlewares.account import _TRANSIENT_BILLING_ERROR_MARKERS

    decision = plan_gate.PlanDecision(plan_id="pro", outcome="deny", quota_bytes=10 * GB, used_bytes=12 * GB)
    message = plan_gate.quota_exceeded_message(decision).lower()

    for marker in _TRANSIENT_BILLING_ERROR_MARKERS:
        assert marker not in message
