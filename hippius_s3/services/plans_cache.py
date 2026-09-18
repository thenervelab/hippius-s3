"""Redis layer for the S3 billing-plan caches.

Fed by one upstream page — GET /api/s3/plans/accounts/ carries both the catalog and the account
roll — and split across hashes on redis-accounts:

    hippius_s3_plan_accounts       field = account SS58   value = {"plan", "storage_limit_bytes", "used_bytes"}
    hippius_s3_billing_inactive    field = account SS58   value = {"reason": "expired_plan"|"payg_inactive", ...}
    hippius_s3_plans               field = plan name      value = {"h256", "storage_bytes"}   (see below)
    hippius_s3_plans:meta          JSON {fetched_at, counts}

The quota hash is on the serving path (one HGET for an active plan). The inactive hash is consulted
only after a quota miss, on the PAYG path. `hippius_s3_plans` is published for operators -- it
answers "what does plan X allow" when someone is debugging a quota decision -- and nothing reads
it in code: the per-account row already carries the resolved limit.

The account row carries everything the quota gate needs, so an active-plan write is one `HGET` and a
comparison -- no catalog lookup, no database. The two halves come from different places and it
matters which is which:

  * `storage_limit_bytes` is the account's MAX QUOTA, from upstream (its own value if it has a
    bespoke one, otherwise its plan's list price).
  * `used_bytes` is what the account actually stores, computed BY US in the plans-cacher. Upstream
    does not report usage.

Deliberately NO TTL on either hash. A TTL would delete the last-known-good mapping in the middle of
an api.hippius.com outage -- exactly the failure the cache exists to survive -- and silently demote
every plan customer to pay-as-you-go. redis-accounts is `noeviction` + AOF, so the maps also survive
a Redis restart. Staleness is surfaced by the meta key and a metric, never by data disappearing.

Publication is a whole-hash atomic swap (build into `<key>:building`, then RENAME). Three properties
depend on that and none are optional:

  1. A partial scrape is never published. If page 7 of 20 fails, the live hash is untouched.
  2. Accounts that LEFT a plan disappear on the next swap. A per-key SETEX layout would keep serving
     their old allowance until the TTL expired.
  3. Readers never observe a half-built map.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any
from typing import Literal
from typing import Mapping


PLAN_ACCOUNTS_KEY = "hippius_s3_plan_accounts"
PLAN_CATALOG_KEY = "hippius_s3_plans"
PLANS_META_KEY = "hippius_s3_plans:meta"
# Accounts that are not on an active plan but must not be treated as a silent PAYG miss:
# expired subscriptions (try PAYG, refuse with PlanExpired if that also fails) and PAYG
# accounts upstream marked inactive (refuse without trying credits).
PLAN_INACTIVE_KEY = "hippius_s3_billing_inactive"

InactiveReason = Literal["expired_plan", "payg_inactive"]

_BUILDING_SUFFIX = ":building"
_HSET_BATCH = 1000

# Refuse to publish an account map that lost more than this fraction of its entries. One bad
# upstream deploy that returns a truncated (but syntactically valid) roll would otherwise demote
# most plan customers to PAYG and 402 them, with nothing in the logs but a successful cycle.
MAX_ACCOUNT_MAP_SHRINK_RATIO = 0.5


class PlanMapShrankTooMuch(Exception):
    """Raised instead of publishing an account map that lost too many entries."""


@dataclass(frozen=True)
class PlanQuota:
    plan_id: str
    # The account's max quota. `storage_bytes` keeps the upstream field name.
    storage_bytes: int | None
    # What the account actually stores, as of the last refresh.
    used_bytes: int

    @property
    def enforceable(self) -> bool:
        # A missing, zero or negative allowance means "we do not know this plan's limit", never
        # "this plan permits nothing". Treating 0 as a real quota would deny every upload for a
        # paying customer on the strength of a malformed payload.
        return self.storage_bytes is not None and self.storage_bytes > 0


@dataclass(frozen=True)
class InactiveBilling:
    reason: InactiveReason
    plan_id: str | None = None


def _decode(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


async def _publish_hash(redis_client: Any, key: str, entries: Mapping[str, str]) -> int:
    building = key + _BUILDING_SUFFIX
    await redis_client.delete(building)

    if not entries:
        # RENAME on a key that was never written raises "no such key", and because accounts publish
        # before the catalog that would abort the whole cycle -- silently, since run_cycle swallows
        # it, and permanently, since touch_meta never runs so the staleness alarm has no timestamp
        # to fire on. Reaching here with nothing is legitimate: publish_plan_roll has already
        # refused an empty roll over a non-empty live hash, so this is the genuinely-zero case
        # (a fresh deploy before the first subscriber, and the default e2e state). Clear and return.
        await redis_client.delete(key)
        return 0

    items = list(entries.items())
    for start in range(0, len(items), _HSET_BATCH):
        await redis_client.hset(building, mapping=dict(items[start : start + _HSET_BATCH]))

    await redis_client.rename(building, key)
    return len(items)


def _live_plan_keys(raw_keys: Any) -> set[str]:
    keys: set[str] = set()
    for key in raw_keys or []:
        decoded = _decode(key)
        if decoded:
            keys.add(decoded)
    return keys


async def publish_plan_roll(
    redis_client: Any,
    accounts: Mapping[str, dict[str, Any]],
    catalog: Mapping[str, dict[str, Any]],
    inactive: Mapping[str, dict[str, Any]] | None = None,
) -> tuple[int, int]:
    """Atomically replace both hashes. Returns (accounts_published, plans_published).

    Refuses outright rather than publishing a roll that would strip most accounts of their plan --
    an empty result set, or one that shrank past MAX_ACCOUNT_MAP_SHRINK_RATIO. Both are far more
    likely to be an upstream bug than most of the roll cancelling at once, and the cost of being
    wrong in the other direction (a fleet-wide 402) is not symmetric.

    Expired subscriptions are not a shrink. An account that left the plan hash because this scrape
    classified it `expired_plan` is accounted for, so a real cancellation (or every subscriber
    lapsing) is published rather than wedging the cacher on the old allowance. A truncated scrape
    that simply omits them still trips the guard. Run `DEL hippius_s3_plan_accounts` on
    redis-accounts to clear a wedged roll; an empty live hash short-circuits both checks.
    """
    inactive = inactive or {}
    live_keys = _live_plan_keys(await redis_client.hkeys(PLAN_ACCOUNTS_KEY))
    live_size = len(live_keys)
    expired_ss58s = {ss58 for ss58, row in inactive.items() if row.get("reason") == "expired_plan"}
    accounted = set(accounts) | (expired_ss58s & live_keys)

    if not accounts and live_size and not (expired_ss58s & live_keys):
        raise PlanMapShrankTooMuch(
            f"refusing to publish an empty account plan map over {live_size} live entries; keeping last known good"
        )
    if (
        live_size
        and len(accounts) < live_size * (1 - MAX_ACCOUNT_MAP_SHRINK_RATIO)
        and len(accounted) < live_size * (1 - MAX_ACCOUNT_MAP_SHRINK_RATIO)
    ):
        raise PlanMapShrankTooMuch(
            f"refusing to publish account plan map: {len(accounts)} entries vs {live_size} live "
            f"(shrink > {MAX_ACCOUNT_MAP_SHRINK_RATIO:.0%}); keeping last known good"
        )

    # Catalog first, accounts second. Nothing on the request path reads the catalog -- it is an
    # operator-facing record of what each plan allows -- so publishing it first means a failure
    # there aborts before the accounts hash moves. The other order can leave a freshly-published
    # accounts map with a stale meta timestamp, so the staleness alarm fires against current data.
    published_plans = 0
    if catalog:
        published_plans = await _publish_hash(
            redis_client, PLAN_CATALOG_KEY, {name: json.dumps(entry) for name, entry in catalog.items()}
        )

    published_accounts = await _publish_hash(
        redis_client, PLAN_ACCOUNTS_KEY, {ss58: json.dumps(row) for ss58, row in accounts.items()}
    )

    await _publish_hash(redis_client, PLAN_INACTIVE_KEY, {ss58: json.dumps(row) for ss58, row in inactive.items()})

    return published_accounts, published_plans


async def touch_meta(redis_client: Any, accounts: int, plans: int) -> None:
    """Record a successful publish. Feeds the staleness metric; the hashes themselves never expire."""
    await redis_client.set(
        PLANS_META_KEY,
        json.dumps({"fetched_at": int(time.time()), "accounts": accounts, "plans": plans}),
    )


async def get_meta(redis_client: Any) -> dict[str, Any]:
    raw = _decode(await redis_client.get(PLANS_META_KEY))
    return json.loads(raw) if raw else {}


async def get_inactive_billing(redis_client: Any, account_id: str) -> InactiveBilling | None:
    """Why this account is not on an active plan, or None when that is just a PAYG miss.

    Consulted only after a plan-hash miss, on the pay-as-you-go path. A miss here is the common
    case (an ordinary PAYG account) and must not become a new failure mode.
    """
    raw = _decode(await redis_client.hget(PLAN_INACTIVE_KEY, account_id))
    if raw is None:
        return None

    row = json.loads(raw)
    reason = row.get("reason")
    if reason == "expired_plan":
        plan_id = row.get("plan")
        return InactiveBilling(reason="expired_plan", plan_id=str(plan_id) if plan_id else None)
    if reason == "payg_inactive":
        return InactiveBilling(reason="payg_inactive")
    return None


async def get_plan_for_account(redis_client: Any, account_id: str) -> PlanQuota | None:
    """The account's plan, quota and usage, in one HGET, or None when they are pay-as-you-go.

    Only accounts with an *active* plan are in the hash (see _is_enforceable_plan_row in the
    plans-cacher). A miss covers pay-as-you-go, an expired plan, and an account upstream does not
    know about. Expired / inactive PAYG rows live on PLAN_INACTIVE_KEY and are consulted by the
    request path after this miss.
    """
    raw = _decode(await redis_client.hget(PLAN_ACCOUNTS_KEY, account_id))
    if raw is None:
        return None

    row = json.loads(raw)
    plan_id = row.get("plan")
    if not plan_id:
        return None

    limit = row.get("storage_limit_bytes")
    return PlanQuota(
        plan_id=str(plan_id),
        storage_bytes=int(limit) if limit is not None else None,
        used_bytes=int(row.get("used_bytes") or 0),
    )
