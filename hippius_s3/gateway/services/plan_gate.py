"""Storage-quota gate for accounts on an S3 billing plan.

Runs inside account_middleware, in a branch that sits between the service-account bypass and the
pay-as-you-go path. An account on a plan is not credit-metered: it skips BOTH the substrate
has_credits check and Arion can_upload, and is gated on its total stored bytes instead.

FAILURE POSTURE -- the two cases are deliberately different:

  * The lookup itself fails (Redis down, malformed catalog JSON): raise PlanLookupUnavailable and
    let the caller fall through to the pay-as-you-go path. That is exactly today's behaviour, so a
    Redis blip can never be a NEW failure mode introduced by this feature.

  * The lookup succeeds and says "this account is on plan X", but X's allowance is unknown (cold
    catalog, unknown plan id): ALLOW, loudly. A positively identified paying customer must never be
    blocked because our cache has not warmed up.

ASYMMETRIC VERIFICATION. The cached rollup may only ALLOW. Every denial is re-checked against the
authoritative SUM before it is returned, under a timeout, allowing on timeout -- so counter drift
can cost us an over-quota upload, but can never 402 a paying customer. Denials are rare (only
accounts genuinely near their limit reach that branch), so the expensive query is affordable there.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any
from typing import Literal

from hippius_s3.config import Config
from hippius_s3.services import usage_service
from hippius_s3.services.plans_cache import PlanQuota
from hippius_s3.services.plans_cache import get_plan_id_for_account
from hippius_s3.services.plans_cache import get_plan_quota


logger = logging.getLogger(__name__)

Outcome = Literal["allow", "deny", "would_deny", "catalog_miss"]
# The plan_gate_total metric axis: the decision outcomes above plus "unavailable", which the
# middleware records when the caches could not be consulted at all and it fell back to
# pay-as-you-go. Closed set of five, fixed here -- never plan- or account-derived.
GateMetricOutcome = Literal["allow", "deny", "would_deny", "catalog_miss", "unavailable"]


class PlanLookupUnavailable(Exception):
    """The plan caches could not be consulted. Caller must fall back to pay-as-you-go."""


@dataclass(frozen=True)
class PlanDecision:
    plan_id: str
    outcome: Outcome
    quota_bytes: int | None = None
    used_bytes: int | None = None

    @property
    def allowed(self) -> bool:
        return self.outcome != "deny"


async def resolve_plan(
    redis_accounts_client: Any,
    main_account_id: str,
    config: Config,
) -> tuple[str, PlanQuota | None] | None:
    """Resolve an account's plan.

    Returns None when the account is pay-as-you-go (the common case -- no plan row).
    Returns (plan_id, quota) when it is on a plan; `quota` is None when the catalog cannot price it.
    Raises PlanLookupUnavailable when the caches could not be read at all.
    """
    if not config.plans_enforcement_enabled:
        return None

    try:
        plan_id = await get_plan_id_for_account(redis_accounts_client, main_account_id)
        if plan_id is None:
            return None
        quota = await get_plan_quota(redis_accounts_client, plan_id)
    except Exception as e:
        # Narrow by intent, broad by necessity: a Redis transport error and a malformed cached
        # payload must both degrade to "we don't know", never to a 500 on a user's upload.
        raise PlanLookupUnavailable(str(e)) from e

    if quota is not None and not quota.enforceable:
        return plan_id, None

    return plan_id, quota


async def evaluate_quota(
    db: Any,
    redis_accounts_client: Any,
    main_account_id: str,
    plan_id: str,
    quota: PlanQuota | None,
    incoming_bytes: int,
    config: Config,
) -> PlanDecision:
    """Decide whether this write fits inside the account's plan allowance."""
    if quota is None or quota.storage_bytes is None:
        return PlanDecision(plan_id=plan_id, outcome="catalog_miss")

    limit = quota.storage_bytes
    used = await usage_service.get_account_bytes(
        db,
        redis_accounts_client,
        main_account_id,
        config.usage_cache_ttl_seconds,
    )

    if used + incoming_bytes <= limit:
        return PlanDecision(plan_id=plan_id, outcome="allow", quota_bytes=limit, used_bytes=used)

    # The cheap counter says "over". It is a cache, and a stale or drifted cache must not be able to
    # reject a paying customer's upload -- so confirm against ground truth before denying.
    verified = await _authoritative_bytes(db, main_account_id, config)
    if verified is None:
        logger.warning(
            f"PLAN_QUOTA verification unavailable account={main_account_id} plan={plan_id} "
            f"cached_used={used} limit={limit}; allowing"
        )
        return PlanDecision(plan_id=plan_id, outcome="allow", quota_bytes=limit, used_bytes=used)

    if verified + incoming_bytes <= limit:
        logger.warning(
            f"PLAN_QUOTA counter drift account={main_account_id} plan={plan_id} "
            f"cached_used={used} authoritative_used={verified}; allowing"
        )
        return PlanDecision(plan_id=plan_id, outcome="allow", quota_bytes=limit, used_bytes=verified)

    outcome: Outcome = "deny" if config.plans_enforcement_mode == "enforce" else "would_deny"
    return PlanDecision(plan_id=plan_id, outcome=outcome, quota_bytes=limit, used_bytes=verified)


async def _authoritative_bytes(db: Any, main_account_id: str, config: Config) -> int | None:
    """Ground-truth usage, or None if it could not be produced in time."""
    try:
        return await asyncio.wait_for(
            usage_service.get_account_bytes_authoritative(db, main_account_id),
            timeout=config.usage_authoritative_timeout_seconds,
        )
    except Exception:
        # Timeout, statement cancellation, pool exhaustion -- any failure to establish ground truth
        # means we must not deny. The account keeps uploading and the reconciler surfaces the gap.
        return None


def quota_exceeded_message(decision: PlanDecision) -> str:
    """The 402 body. Clients surface the S3 <Message> verbatim, so this is the entire UX.

    Must not contain any phrase from _TRANSIENT_BILLING_ERROR_MARKERS in account.py -- those are
    substring-matched against billing error text to decide what is retryable.
    """
    limit_gb = (decision.quota_bytes or 0) / 1_000_000_000
    used_gb = (decision.used_bytes or 0) / 1_000_000_000
    return (
        f"Your plan includes {limit_gb:.2f} GB of storage and you are currently using "
        f"{used_gb:.2f} GB. This upload would exceed the storage quota allowed by your plan. "
        f"Delete objects you no longer need, or upgrade to a plan with more storage."
    )
