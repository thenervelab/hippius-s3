"""Quota-gate decision logic.

The property under test throughout is the asymmetry: the cheap cached counter may ALLOW, only the
authoritative SUM may DENY. Every path where ground truth is unavailable must resolve to "allow",
because a drifted or slow counter blocking a paying customer's upload is a support incident, while
letting one over-quota upload through is a rounding error the reconciler catches.
"""

from types import SimpleNamespace

import pytest

from hippius_s3.gateway.services import plan_gate
from hippius_s3.gateway.services.plan_gate import PlanLookupUnavailable
from hippius_s3.services.plans_cache import PlanQuota


GB = 1_000_000_000


def make_config(**overrides: object) -> SimpleNamespace:
    base = dict(
        enable_billing_plans=True,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


class StubRedis:
    """redis-accounts stand-in: the account hash carries plan + allowance, the catalog is fallback."""

    def __init__(self, plan_id: str | None = None, storage_bytes: int | None = None, used: int = 0) -> None:
        self._plan_id = plan_id
        self._storage_bytes = storage_bytes
        self._used = used

    async def hget(self, key: str, field: str):
        import json

        if key == "hippius_s3_plan_accounts" and self._plan_id:
            return json.dumps(
                {"plan": self._plan_id, "storage_limit_bytes": self._storage_bytes, "used_bytes": self._used}
            ).encode()
        return None


# --------------------------------------------------------------------------- resolve_plan


@pytest.mark.asyncio
async def test_resolve_plan_does_not_consult_the_feature_flag_at_all() -> None:
    """resolve_plan takes no config: with the feature off the middleware still needs the answer to
    emit the BILLING_PLAN_SHADOW line. The flag decides what is DONE with it, not whether the
    question is asked."""
    redis = StubRedis(plan_id="pro", storage_bytes=GB)
    resolved = await plan_gate.resolve_plan(redis, "acct")
    assert resolved is not None and resolved.plan_id == "pro"


@pytest.mark.asyncio
async def test_an_account_with_no_plan_row_is_pay_as_you_go() -> None:
    assert await plan_gate.resolve_plan(StubRedis(plan_id=None), "acct") is None


@pytest.mark.asyncio
async def test_a_plan_with_a_known_quota_resolves() -> None:
    redis = StubRedis(plan_id="business", storage_bytes=5 * GB)
    quota = await plan_gate.resolve_plan(redis, "acct")

    assert quota == PlanQuota(plan_id="business", storage_bytes=5 * GB, used_bytes=0)
    assert quota.enforceable


@pytest.mark.asyncio
async def test_a_plan_whose_quota_is_unknown_resolves_with_no_quota() -> None:
    """Cold catalog. The account is positively on a plan, so it must NOT fall to pay-as-you-go
    (which would 402 them on the credit check) -- it resolves with quota None and is allowed."""
    redis = StubRedis(plan_id="pro", storage_bytes=None)
    quota = await plan_gate.resolve_plan(redis, "acct")

    assert quota is not None and quota.plan_id == "pro"
    assert not quota.enforceable, "on a plan, allowance unknown -> callers ALLOW"


@pytest.mark.asyncio
async def test_a_nonpositive_allowance_resolves_with_no_quota() -> None:
    redis = StubRedis(plan_id="pro", storage_bytes=0)
    quota = await plan_gate.resolve_plan(redis, "acct")
    assert quota is not None and not quota.enforceable


@pytest.mark.asyncio
async def test_a_redis_error_raises_lookup_unavailable() -> None:
    """The caller falls back to the pay-as-you-go path on this, which is exactly today's behaviour
    -- so a Redis blip can never be a NEW failure mode introduced by plans."""

    class ExplodingRedis:
        async def hget(self, *args: object) -> None:
            raise ConnectionError("redis is down")

    with pytest.raises(PlanLookupUnavailable):
        await plan_gate.resolve_plan(ExplodingRedis(), "acct")


@pytest.mark.asyncio
async def test_malformed_cached_quota_json_raises_lookup_unavailable() -> None:
    class GarbageRedis:
        async def hget(self, key: str, field: str) -> bytes:
            return b"{not json"

    with pytest.raises(PlanLookupUnavailable):
        await plan_gate.resolve_plan(GarbageRedis(), "acct")


# --------------------------------------------------------------------------- evaluate_quota


def decide(used: int, incoming: int, limit: int | None, *, enforcing: bool = True):
    return plan_gate.evaluate_quota(
        PlanQuota(plan_id="pro", storage_bytes=limit, used_bytes=used), incoming, enforcing=enforcing
    )


def test_under_quota_allows() -> None:
    decision = decide(used=1 * GB, incoming=1 * GB, limit=10 * GB)
    assert decision.outcome == "allow"


def test_exactly_at_the_limit_is_allowed() -> None:
    assert decide(used=9 * GB, incoming=1 * GB, limit=10 * GB).outcome == "allow"


def test_one_byte_over_the_limit_is_denied() -> None:
    assert decide(used=10 * GB, incoming=1, limit=10 * GB).outcome == "deny"


def test_shadow_mode_reports_would_deny_and_never_denies() -> None:
    decision = decide(used=99 * GB, incoming=1 * GB, limit=10 * GB, enforcing=False)
    assert decision.outcome == "would_deny"
    assert decision.outcome != "deny", "shadow mode must never produce a denial"


def test_shadow_and_enforced_share_the_arithmetic() -> None:
    """The one property shadow mode cannot self-check: it must agree with enforcement on everything
    except the name of the over-quota verdict."""
    for used, incoming, limit in ((0, 1, 10 * GB), (10 * GB, 1, 10 * GB), (5 * GB, 5 * GB, 10 * GB)):
        enforced = decide(used=used, incoming=incoming, limit=limit)
        shadow = decide(used=used, incoming=incoming, limit=limit, enforcing=False)
        assert (enforced.outcome == "deny") == (shadow.outcome == "would_deny")
        assert enforced.used_bytes == shadow.used_bytes
        assert enforced.quota_bytes == shadow.quota_bytes


def test_an_unknown_quota_allows_and_reports_catalog_miss() -> None:
    decision = decide(used=999 * GB, incoming=1 * GB, limit=None)
    assert decision.outcome == "catalog_miss"


def test_a_zero_byte_request_is_allowed_when_already_at_the_limit() -> None:
    """A request that declares no size reads as 0 bytes and is admitted, exactly as can_upload
    already admits it -- see _declared_content_length."""
    assert decide(used=10 * GB, incoming=0, limit=10 * GB).outcome == "allow"


# --------------------------------------------------------------------------- the 402 body


def test_the_denial_message_carries_the_real_numbers_and_an_action() -> None:
    TiB = 1 << 40
    decision = plan_gate.PlanDecision(outcome="deny", quota_bytes=10 * TiB, used_bytes=12 * TiB)
    message = plan_gate.quota_exceeded_message(decision)

    assert "10.00 TiB" in message
    assert "12.00 TiB" in message
    assert "upgrade" in message.lower()


@pytest.mark.parametrize(
    "value,expected",
    [
        (10995116277760, "10.00 TiB"),  # the real "10 TB" plan, as the endpoint denominates it
        (54975581388800, "50.00 TiB"),
        (5 * (1 << 30), "5.00 GiB"),
        (0, "0 bytes"),
    ],
)
def test_sizes_are_rendered_in_binary_units(value: int, expected: str) -> None:
    """Dividing by 1e9 would render the 10 TiB plan as "10995.12 GB" — a number no customer can
    reconcile with what they were sold, and a unit mismatch against the console."""
    assert plan_gate.format_bytes(value) == expected


def test_the_denial_message_cannot_be_misread_as_a_transient_billing_error() -> None:
    """account.py substring-matches billing error text to decide what is retryable. A quota message
    containing one of those phrases would turn a hard denial into a retry loop."""
    from hippius_s3.gateway.middlewares.account import _TRANSIENT_BILLING_ERROR_MARKERS

    decision = plan_gate.PlanDecision(outcome="deny", quota_bytes=10 * GB, used_bytes=12 * GB)
    message = plan_gate.quota_exceeded_message(decision).lower()

    for marker in _TRANSIENT_BILLING_ERROR_MARKERS:
        assert marker not in message
