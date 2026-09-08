"""Redis layer for the S3 billing-plan caches.

Two hashes on redis-accounts, both written only by the plans-cacher worker and read on the request
path by account_middleware:

    hippius_s3_plan_accounts   field = account SS58   value = plan_id
    hippius_s3_plans           field = plan_id        value = quota JSON
    hippius_s3_plans:meta      JSON {plans_fetched_at, accounts_fetched_at, ...}

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
import logging
import time
from dataclasses import dataclass
from typing import Any
from typing import Mapping


logger = logging.getLogger(__name__)

PLAN_ACCOUNTS_KEY = "hippius_s3_plan_accounts"
PLAN_CATALOG_KEY = "hippius_s3_plans"
PLANS_META_KEY = "hippius_s3_plans:meta"

_BUILDING_SUFFIX = ":building"
_HSET_BATCH = 1000

# Refuse to publish an account map that lost more than this fraction of its entries. One bad
# upstream deploy that returns a truncated (but syntactically valid) list would otherwise demote
# most plan customers to PAYG and 402 them, with nothing in the logs but a successful cycle.
MAX_ACCOUNT_MAP_SHRINK_RATIO = 0.5


class PlanMapShrankTooMuch(Exception):
    """Raised instead of publishing an account map that lost too many entries."""


@dataclass(frozen=True)
class PlanQuota:
    plan_id: str
    storage_bytes: int | None
    name: str | None = None

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


async def publish_account_plans(redis_client: Any, mapping: Mapping[str, str]) -> int:
    """Atomically replace the account -> plan_id map. Returns the number of entries published."""
    building = PLAN_ACCOUNTS_KEY + _BUILDING_SUFFIX

    live_size = int(await redis_client.hlen(PLAN_ACCOUNTS_KEY) or 0)
    if live_size and len(mapping) < live_size * (1 - MAX_ACCOUNT_MAP_SHRINK_RATIO):
        raise PlanMapShrankTooMuch(
            f"refusing to publish account plan map: {len(mapping)} entries vs {live_size} live "
            f"(shrink > {MAX_ACCOUNT_MAP_SHRINK_RATIO:.0%}); keeping last known good"
        )

    await redis_client.delete(building)

    if not mapping:
        # An empty upstream response is never a legitimate reason to wipe the live map. Bail before
        # the RENAME so the previous map keeps serving.
        raise PlanMapShrankTooMuch("refusing to publish an empty account plan map")

    pipe = redis_client.pipeline()
    pending = 0
    for account_id, plan_id in mapping.items():
        await pipe.hset(building, account_id, plan_id)
        pending += 1
        if pending % _HSET_BATCH == 0:
            await pipe.execute()
            pipe = redis_client.pipeline()
    if pending % _HSET_BATCH != 0:
        await pipe.execute()

    await redis_client.rename(building, PLAN_ACCOUNTS_KEY)
    return pending


async def publish_plan_catalog(redis_client: Any, quotas: Mapping[str, dict[str, Any]]) -> int:
    """Atomically replace the plan_id -> quota catalog. Returns the number of plans published."""
    building = PLAN_CATALOG_KEY + _BUILDING_SUFFIX
    await redis_client.delete(building)

    if not quotas:
        raise PlanMapShrankTooMuch("refusing to publish an empty plan catalog")

    pipe = redis_client.pipeline()
    for plan_id, quota in quotas.items():
        await pipe.hset(building, plan_id, json.dumps(quota))
    await pipe.execute()

    await redis_client.rename(building, PLAN_CATALOG_KEY)
    return len(quotas)


async def touch_meta(redis_client: Any, field: str, count: int) -> None:
    """Record a successful publish. Best-effort: the meta key feeds staleness metrics only."""
    raw = await redis_client.get(PLANS_META_KEY)
    meta: dict[str, Any] = json.loads(raw) if raw else {}
    meta[field] = int(time.time())
    meta[f"{field}_count"] = count
    await redis_client.set(PLANS_META_KEY, json.dumps(meta))


async def get_meta(redis_client: Any) -> dict[str, Any]:
    raw = await redis_client.get(PLANS_META_KEY)
    return json.loads(raw) if raw else {}


async def get_plan_id_for_account(redis_client: Any, account_id: str) -> str | None:
    """The account's plan_id, or None when they are pay-as-you-go."""
    return _decode(await redis_client.hget(PLAN_ACCOUNTS_KEY, account_id))


async def get_plan_quota(redis_client: Any, plan_id: str) -> PlanQuota | None:
    """The plan's quota, or None when the catalog is cold or does not know this plan."""
    raw = _decode(await redis_client.hget(PLAN_CATALOG_KEY, plan_id))
    if raw is None:
        return None

    payload = json.loads(raw)
    storage_bytes = payload.get("storage_bytes")
    return PlanQuota(
        plan_id=plan_id,
        storage_bytes=int(storage_bytes) if storage_bytes is not None else None,
        name=payload.get("name"),
    )
