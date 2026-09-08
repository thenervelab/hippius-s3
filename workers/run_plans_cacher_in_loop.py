#!/usr/bin/env python3
"""Scrapes the S3 billing-plan roll from api.hippius.com into redis-accounts.

    GET /api/s3/plans/accounts/?page=1&page_size=500

One endpoint carries both halves — `plans` is the catalog, `results` is the paginated per-account
roll — so this is a single loop, polled every HIPPIUS_PLANS_LOOP_SLEEP seconds (default 300).

Caching is UNCONDITIONAL. This worker does not read HIPPIUS_ENABLE_BILLING_PLANS and is not deployed
with it: the maps stay warm and observably correct long before enforcement is switched on, so
flipping the flag on the api is a config change rather than a cold-cache event.

This worker failing is not an outage. Neither cache has a TTL and neither is ever published
partially — see the module docstring of hippius_s3/services/plans_cache.py for why both of those are
correctness requirements rather than optimisations. The short version: a failure here must degrade
to "serve the last known good map", never to "every plan customer silently becomes pay-as-you-go and
gets 402'd on their next upload".
"""

import asyncio
import logging
import sys
import time
from pathlib import Path
from typing import Any

from redis.asyncio import Redis


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.monitoring import initialize_metrics_collector
from hippius_s3.sentry import init_sentry
from hippius_s3.services import plans_cache
from hippius_s3.services.hippius_api_service import HippiusApiClient
from hippius_s3.services.hippius_api_service import S3PlanAccountRow
from hippius_s3.services.hippius_api_service import S3PlanAccountsResponse
from hippius_s3.workers.shutdown import run_worker


config = get_config()

setup_loki_logging(config, "plans-cacher")
logger = logging.getLogger(__name__)
init_sentry("plans-cacher", is_worker=True)

# Bound on how many pages we will follow before declaring the upstream pagination broken. Without
# it a `next` pointer that loops back on itself spins this worker forever, holding the scrape open
# and never publishing. 500 pages x 500 rows = 250k accounts, well clear of the current ~1.3k.
MAX_PAGES = 500

# Upstream's own name for "this account is on a subscription". Anything else in `billing` — today
# only "pay_as_you_go" — means exactly that.
_PLAN_BILLING = "plan"


def _is_enforceable_plan_row(row: S3PlanAccountRow) -> bool:
    """Whether this account should be gated on a plan quota rather than billed pay-as-you-go.

    Requires all three of billing == "plan", a plan name, and active is true.

    `active: false` is the interesting one. A lapsed or cancelled subscription still comes back with
    billing="plan" and its old plan name, and honouring it would hand a free allowance to someone
    who has stopped paying. Dropping the row instead sends them down the pay-as-you-go path, which
    is where a non-subscriber belongs — Arion then decides on credit, exactly as it does for every
    other PAYG account. It is deliberately NOT a quota denial: their storage is not over any limit,
    their subscription simply is not in force.
    """
    return bool(row.billing == _PLAN_BILLING and row.plan and row.active)


def _parse_page(page: S3PlanAccountsResponse) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """Wire shape -> what we cache. The single place to change if the payload moves.

    Accounts that are not on an active plan are simply ABSENT from the map: an absent field is
    exactly what the request path already treats as pay-as-you-go, so there is nothing to encode
    for them and nothing to keep in sync.

    THE TWO `storage_bytes` FIELDS MEAN OPPOSITE THINGS, and confusing them is catastrophic:

      * `plans.<name>.storage_bytes` is the plan's ALLOWANCE.
      * `results[].storage_bytes` is that account's CURRENT TOTAL S3 USAGE, computed on chain.

    Reading an account's usage as its allowance would hand every plan customer a limit exactly equal
    to what they already store, and deny their very next upload. They are stored here under names
    that cannot be mixed up: `storage_limit_bytes` and `used_bytes`.

    Both are resolved at publish time, so the request path is a single HGET against one key, with no
    catalog lookup and no database work.
    """
    accounts: dict[str, dict[str, Any]] = {}
    for row in page.results:
        if not row.ss58 or not _is_enforceable_plan_row(row):
            continue
        plan = page.plans.get(row.plan or "")
        accounts[row.ss58] = {
            "plan": row.plan,
            "storage_limit_bytes": plan.storage_bytes if plan else None,
            "used_bytes": row.storage_bytes or 0,
        }

    catalog = {name: {"h256": entry.h256, "storage_bytes": entry.storage_bytes} for name, entry in page.plans.items()}
    return accounts, catalog


async def refresh_plan_roll_once(redis_client: Redis) -> tuple[int, int]:
    """Fetch EVERY page, then publish. Returns (accounts, plans).

    The all-or-nothing shape is the load-bearing part of this worker. If page 7 of 20 fails, the
    exception propagates out of here before publish_plan_roll is reached, the live hashes are left
    untouched, and the cycle is recorded as a failure. Publishing what we had so far would drop the
    accounts on the unfetched pages to pay-as-you-go and 402 them on their next upload.
    """
    accounts: dict[str, dict[str, Any]] = {}
    catalog: dict[str, dict[str, Any]] = {}
    next_url: str | None = None
    pages = 0
    rows_seen = 0

    async with HippiusApiClient() as api_client:
        for _ in range(MAX_PAGES):
            page = await api_client.get_s3_plan_accounts(next_url=next_url)
            pages += 1
            rows_seen += len(page.results)

            page_accounts, page_catalog = _parse_page(page)
            accounts.update(page_accounts)
            # The catalog is repeated on every page; later pages simply confirm it.
            catalog.update(page_catalog)

            next_url = page.next
            if not next_url:
                break
        else:
            raise RuntimeError(
                f"account plan pagination exceeded {MAX_PAGES} pages; refusing to publish a map "
                f"built from a possibly looping cursor"
            )

    published_accounts, published_plans = await plans_cache.publish_plan_roll(redis_client, accounts, catalog)
    await plans_cache.touch_meta(redis_client, published_accounts, published_plans)

    logger.info(
        f"Published plan roll: {published_accounts} accounts on an active plan out of {rows_seen} rows, "
        f"{published_plans} plans, over {pages} page(s)"
    )
    return published_accounts, published_plans


async def _report_cache_age(redis_client: Redis) -> None:
    """The caches never expire, so staleness has to be measured explicitly or it is invisible."""
    meta = await plans_cache.get_meta(redis_client)
    fetched_at = meta.get("fetched_at")
    if not fetched_at:
        return

    age = int(time.time()) - int(fetched_at)
    get_metrics_collector().record_plans_cache_age(age_seconds=age)
    if age > config.plans_stale_after_seconds:
        logger.error(
            f"PLANS_CACHE_STALE age={age}s exceeds {config.plans_stale_after_seconds}s. Still serving "
            f"the last known good map; uploads are unaffected."
        )


async def run_cycle(redis_client: Redis) -> bool:
    """Run one refresh. Never raises -- a failed cycle must leave the previous cache serving."""
    collector = get_metrics_collector()
    started = time.monotonic()
    try:
        accounts, _ = await refresh_plan_roll_once(redis_client)
        collector.record_plans_cacher_cycle(success=True, entries=accounts, duration=time.monotonic() - started)
        return True
    except Exception as e:
        logger.error(f"plans-cacher cycle failed: {e}; keeping last known good cache", exc_info=True)
        collector.record_plans_cacher_cycle(success=False, entries=0, duration=time.monotonic() - started)
        return False


async def run_plans_cacher_loop() -> None:
    redis_client = Redis.from_url(config.redis_accounts_url)
    initialize_metrics_collector()

    logger.info(
        f"Starting plans-cacher: polling every {config.plans_loop_sleep}s. Caching is unconditional "
        f"— it does not depend on whether plan enforcement is enabled."
    )

    # Closing on the way out is what tells Redis this client is gone; a cancelled worker otherwise
    # leaves its connection behind.
    try:
        while True:
            ok = await run_cycle(redis_client)
            await _report_cache_age(redis_client)
            sleep_for = config.plans_loop_sleep if ok else 60
            logger.info(f"plans-cacher: sleeping {sleep_for}s")
            await asyncio.sleep(sleep_for)
    finally:
        await redis_client.aclose()


if __name__ == "__main__":
    # Default (no restart_on_crash): a crash exits the process and the kubelet restarts the pod, so
    # a crash-looping cacher shows up in pod-restart alerting instead of hiding behind
    # restart_count: 0. Individual cycle failures never reach here -- run_cycle swallows them and
    # the previous cache keeps serving -- so anything that gets this far is a real defect.
    run_worker(run_plans_cacher_loop, "plans-cacher")
