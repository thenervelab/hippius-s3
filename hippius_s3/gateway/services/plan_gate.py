"""Storage-quota gate for accounts on an S3 billing plan.

Runs inside account_middleware, in a branch between the service-account bypass and the
pay-as-you-go path. An account on a plan is not credit-metered: it skips BOTH the substrate
has_credits check and Arion can_upload, and is gated on its total stored bytes instead.

Both numbers come from the same cached row, published by the plans-cacher from one upstream page:
the plan's allowance and the account's current S3 usage, the latter computed on chain. So the whole
gate is one Redis HGET and a comparison -- no database work on the request path.

WHAT THAT COSTS. Usage is as fresh as the last poll (HIPPIUS_PLANS_LOOP_SLEEP, 60s), not as fresh
as the last write. An account can exceed its allowance by up to one poll interval's worth of
uploads before the gate notices, and there is no second source to check a denial against. The poll
interval IS the enforcement lag, and shortening it is the only lever.

FAILURE POSTURE -- the two cases are deliberately different:

  * The lookup itself fails (Redis down, malformed cached payload): raise PlanLookupUnavailable and
    let the caller fall through to the pay-as-you-go path. That is exactly today's behaviour, so a
    Redis blip can never be a NEW failure mode introduced by this feature.

  * The lookup succeeds and says "this account is on plan X", but X's allowance is unknown (cold
    catalog, unknown plan): ALLOW, loudly. A positively identified paying customer must never be
    blocked because our cache has not warmed up.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any
from typing import Literal

from hippius_s3.services.plans_cache import PlanQuota
from hippius_s3.services.plans_cache import get_plan_for_account


logger = logging.getLogger(__name__)

Outcome = Literal["allow", "deny", "would_deny", "catalog_miss"]


class PlanLookupUnavailable(Exception):
    """The plan cache could not be consulted. Caller must fall back to pay-as-you-go."""


@dataclass(frozen=True)
class PlanDecision:
    outcome: Outcome
    quota_bytes: int | None = None
    used_bytes: int | None = None


async def resolve_plan(redis_accounts_client: Any, main_account_id: str) -> PlanQuota | None:
    """The account's plan, or None when it is pay-as-you-go. Raises PlanLookupUnavailable.

    Deliberately not gated on config.enable_billing_plans: with the feature off the caller still
    needs the answer for the shadow log line. The flag decides what is DONE with it, not whether
    the question is asked.
    """
    try:
        return await get_plan_for_account(redis_accounts_client, main_account_id)
    except Exception as e:
        # Narrow by intent, broad by necessity: a Redis transport error and a malformed cached
        # payload must both degrade to "we don't know", never to a 500 on a user's upload.
        raise PlanLookupUnavailable(str(e)) from e


def evaluate_quota(quota: PlanQuota, incoming_bytes: int, *, enforcing: bool = True) -> PlanDecision:
    """Decide whether this write fits inside the account's plan allowance.

    `enforcing=False` is shadow mode, used while HIPPIUS_ENABLE_BILLING_PLANS is off: identical
    arithmetic, but an over-quota verdict is reported as `would_deny` so the caller logs it and lets
    the request through. One function rather than two on purpose -- the arithmetic is exactly what
    the shadow period exists to validate, so a second copy could drift from what the shadow log
    claims would have happened, which is the one failure shadow mode cannot catch.
    """
    if not quota.enforceable:
        return PlanDecision(outcome="catalog_miss")

    limit = quota.storage_bytes or 0
    if quota.used_bytes + incoming_bytes <= limit:
        return PlanDecision(outcome="allow", quota_bytes=limit, used_bytes=quota.used_bytes)

    return PlanDecision(
        outcome="deny" if enforcing else "would_deny",
        quota_bytes=limit,
        used_bytes=quota.used_bytes,
    )


def format_bytes(value: int) -> str:
    """Human-readable size in BINARY units.

    Plan allowances arrive as powers of two (10995116277760 is 10 TiB), so dividing by 1e9 renders
    a "10 TB" plan as "10995.12 GB" -- a number no customer can reconcile with anything they were
    sold. Units drifting between what we enforce and what we display is a known past bug in this
    ecosystem.
    """
    for unit, size in (("TiB", 1 << 40), ("GiB", 1 << 30), ("MiB", 1 << 20)):
        if abs(value) >= size:
            return f"{value / size:.2f} {unit}"
    return f"{value} bytes"


def quota_exceeded_message(decision: PlanDecision) -> str:
    """The 402 body. Clients surface the S3 <Message> verbatim, so this is the entire UX.

    Must not contain any phrase from _TRANSIENT_BILLING_ERROR_MARKERS in account.py -- those are
    substring-matched against billing error text to decide what is retryable.
    """
    return (
        f"Your plan includes {format_bytes(decision.quota_bytes or 0)} of storage and you are "
        f"currently using {format_bytes(decision.used_bytes or 0)}. This upload would exceed the "
        f"storage quota allowed by your plan. Delete objects you no longer need, or upgrade to a "
        f"plan with more storage."
    )
