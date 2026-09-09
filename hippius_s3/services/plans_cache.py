"""Redis layer for the S3 billing-plan caches.

Fed by one upstream page — GET /api/s3/plans/accounts/ carries both the catalog and the account
roll — and split across two hashes on redis-accounts:

    hippius_s3_plan_accounts   field = account SS58   value = {"plan", "storage_limit_bytes", "used_bytes"}
    hippius_s3_plans           field = plan name      value = {"h256", "storage_bytes"}   (see below)
    hippius_s3_plans:meta      JSON {fetched_at, counts}

Only the FIRST of those is on the serving path. `hippius_s3_plans` is published for operators --
it answers "what does plan X allow" when someone is debugging a quota decision -- and nothing reads
it in code: the per-account row already carries the resolved limit.

The account row carries everything the quota gate needs, so the request path is ONE `HGET` and a
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
from typing import Mapping


PLAN_ACCOUNTS_KEY = "hippius_s3_plan_accounts"
PLAN_CATALOG_KEY = "hippius_s3_plans"
PLANS_META_KEY = "hippius_s3_plans:meta"

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


async def publish_plan_roll(
    redis_client: Any,
    accounts: Mapping[str, dict[str, Any]],
    catalog: Mapping[str, dict[str, Any]],
) -> tuple[int, int]:
    """Atomically replace both hashes. Returns (accounts_published, plans_published).

    Refuses outright rather than publishing a roll that would strip most accounts of their plan --
    an empty result set, or one that shrank past MAX_ACCOUNT_MAP_SHRINK_RATIO. Both are far more
    likely to be an upstream bug than most of the roll cancelling at once, and the cost of being
    wrong in the other direction (a fleet-wide 402) is not symmetric.

    ⚠️ THIS GUARD ALSO BLOCKS AN INTENTIONAL SHRINK, INCLUDING A ROLLBACK. Restoring the `active`
    check in _is_enforceable_plan_row (or any change that legitimately admits far fewer accounts)
    computes a much smaller roll, trips this, and run_cycle swallows the raise -- so the OLD wide
    roll keeps serving, with used_bytes frozen at the moment of the revert. The deploy looks clean
    and nothing changes. Run `DEL hippius_s3_plan_accounts` on redis-accounts as part of any such
    rollback; an empty live hash short-circuits both checks.
    """
    live_size = int(await redis_client.hlen(PLAN_ACCOUNTS_KEY) or 0)

    if not accounts and live_size:
        raise PlanMapShrankTooMuch(
            f"refusing to publish an empty account plan map over {live_size} live entries; keeping last known good"
        )
    if live_size and len(accounts) < live_size * (1 - MAX_ACCOUNT_MAP_SHRINK_RATIO):
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


async def get_plan_for_account(redis_client: Any, account_id: str) -> PlanQuota | None:
    """The account's plan, quota and usage, in one HGET, or None when they are pay-as-you-go.

    Only accounts upstream BILLS as a plan are in the hash at all (see _is_enforceable_plan_row in
    the plans-cacher), so a miss here covers the pay-as-you-go cases: no subscription, or an account
    upstream does not know about.

    A lapsed subscription is NOT a miss. `active` is not consulted, so a cancelled subscriber whose
    row still carries billing="plan" and its old plan name is present here and keeps its allowance.
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
