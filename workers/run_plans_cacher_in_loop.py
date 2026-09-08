#!/usr/bin/env python3
"""Scrapes the S3 billing-plan endpoints on api.hippius.com into redis-accounts.

Two independent loops in one process:

    catalog   GET /s3-plans           every 10 min   plan_id -> allowance
    accounts  GET /s3-plans/accounts  every  5 min   account SS58 -> plan_id

They are independent on purpose: the catalog is a handful of rows that change when someone edits a
pricing page, the account map is ~40k rows that change whenever anyone subscribes. A failure in one
must not stall the other, so each has its own cycle, its own backoff and its own metrics label.

Neither cache has a TTL, and neither is ever published partially -- see the module docstring of
hippius_s3/services/plans_cache.py for why both of those are correctness requirements rather than
optimisations. The short version: this worker failing must degrade to "serve the last known good
map", never to "every plan customer silently becomes pay-as-you-go and gets 402'd".
"""

import asyncio
import logging
import sys
import time
from pathlib import Path

from redis.asyncio import Redis


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.monitoring import initialize_metrics_collector
from hippius_s3.sentry import init_sentry
from hippius_s3.services import plans_cache
from hippius_s3.services.hippius_api_service import AccountPlansResponse
from hippius_s3.services.hippius_api_service import HippiusApiClient
from hippius_s3.services.hippius_api_service import S3PlansResponse
from hippius_s3.workers.shutdown import run_worker


config = get_config()

setup_loki_logging(config, "plans-cacher")
logger = logging.getLogger(__name__)
init_sentry("plans-cacher", is_worker=True)

# Bound on how many pages we will follow before declaring the upstream pagination broken. Without
# it a `next` pointer that loops back on itself spins this worker forever, holding the scrape open
# and never publishing.
MAX_ACCOUNT_PAGES = 500


def _parse_plan_catalog(response: S3PlansResponse) -> dict[str, dict[str, object]]:
    """Wire shape -> what we cache. The single place to change when the real payload lands."""
    return {
        plan.plan_id: {"name": plan.name, "storage_bytes": plan.storage_bytes}
        for plan in response.plans
        if plan.plan_id
    }


def _parse_account_plans(pages: list[AccountPlansResponse]) -> dict[str, str]:
    """Wire shape -> what we cache. Accounts with a null plan_id are pay-as-you-go and are simply
    absent from the map -- an absent field is exactly what the request path treats as PAYG, so
    there is nothing to encode for them."""
    mapping: dict[str, str] = {}
    for page in pages:
        for entry in page.accounts:
            if entry.account_id and entry.plan_id:
                mapping[entry.account_id] = entry.plan_id
    return mapping


async def refresh_plan_catalog_once(redis_client: Redis) -> int:
    async with HippiusApiClient() as api_client:
        response = await api_client.get_s3_plans()

    quotas = _parse_plan_catalog(response)
    published = await plans_cache.publish_plan_catalog(redis_client, quotas)
    await plans_cache.touch_meta(redis_client, "plans_fetched_at", published)
    logger.info(f"Published plan catalog: {published} plans")
    return published


async def refresh_account_plans_once(redis_client: Redis) -> int:
    """Fetch EVERY page before publishing anything.

    This is the load-bearing part of the whole worker. If page 7 of 20 fails, the exception
    propagates out of this function before publish_account_plans is reached, the live hash is left
    untouched, and the cycle is recorded as a failure. Publishing what we had so far would drop
    ~65% of plan customers to pay-as-you-go and 402 them on their next upload.
    """
    pages: list[AccountPlansResponse] = []
    next_page: str | None = None

    async with HippiusApiClient() as api_client:
        for _ in range(MAX_ACCOUNT_PAGES):
            response = await api_client.get_account_plans(page=next_page)
            pages.append(response)
            next_page = response.next
            if not next_page:
                break
        else:
            raise RuntimeError(
                f"account plan pagination exceeded {MAX_ACCOUNT_PAGES} pages; refusing to publish a "
                f"map built from a possibly looping cursor"
            )

    mapping = _parse_account_plans(pages)
    published = await plans_cache.publish_account_plans(redis_client, mapping)
    await plans_cache.touch_meta(redis_client, "accounts_fetched_at", published)
    logger.info(f"Published account plan map: {published} accounts over {len(pages)} page(s)")
    return published


async def _report_cache_age(redis_client: Redis) -> None:
    """The caches never expire, so staleness has to be measured explicitly or it is invisible."""
    meta = await plans_cache.get_meta(redis_client)
    now = int(time.time())
    collector = get_metrics_collector()
    for loop, field in (("catalog", "plans_fetched_at"), ("accounts", "accounts_fetched_at")):
        fetched_at = meta.get(field)
        if not fetched_at:
            continue
        age = now - int(fetched_at)
        collector.record_plans_cache_age(loop=loop, age_seconds=age)
        if age > config.plans_stale_after_seconds:
            logger.error(
                f"PLANS_CACHE_STALE loop={loop} age={age}s exceeds {config.plans_stale_after_seconds}s. "
                f"Still serving the last known good map; uploads are unaffected."
            )


async def run_cycle(loop_name: str, redis_client: Redis) -> bool:
    """Run one refresh. Never raises -- a failed cycle must leave the previous cache serving."""
    collector = get_metrics_collector()
    started = time.monotonic()
    try:
        if loop_name == "catalog":
            entries = await refresh_plan_catalog_once(redis_client)
        else:
            entries = await refresh_account_plans_once(redis_client)
        collector.record_plans_cacher_cycle(
            loop=loop_name, success=True, entries=entries, duration=time.monotonic() - started
        )
        return True
    except Exception as e:
        logger.error(f"plans-cacher {loop_name} cycle failed: {e}; keeping last known good cache", exc_info=True)
        collector.record_plans_cacher_cycle(
            loop=loop_name, success=False, entries=0, duration=time.monotonic() - started
        )
        return False


async def _loop(loop_name: str, redis_client: Redis, interval: int) -> None:
    while True:
        ok = await run_cycle(loop_name, redis_client)
        await _report_cache_age(redis_client)
        sleep_for = interval if ok else 60
        logger.info(f"plans-cacher {loop_name}: sleeping {sleep_for}s")
        await asyncio.sleep(sleep_for)


async def run_plans_cacher_loop() -> None:
    redis_client = Redis.from_url(config.redis_accounts_url)
    initialize_metrics_collector()

    logger.info(
        f"Starting plans-cacher: catalog every {config.plans_catalog_loop_sleep}s, "
        f"accounts every {config.plans_accounts_loop_sleep}s, "
        f"enforcement={'on' if config.plans_enforcement_enabled else 'off'} "
        f"mode={config.plans_enforcement_mode}"
    )

    # Closing on the way out is what tells Redis this client is gone; a cancelled worker otherwise
    # leaves its connection behind.
    try:
        await asyncio.gather(
            _loop("catalog", redis_client, config.plans_catalog_loop_sleep),
            _loop("accounts", redis_client, config.plans_accounts_loop_sleep),
        )
    finally:
        await redis_client.aclose()


if __name__ == "__main__":
    # Default (no restart_on_crash): a crash exits the process and the kubelet restarts the pod, so
    # a crash-looping cacher shows up in pod-restart alerting instead of hiding behind
    # restart_count: 0. Individual cycle failures never reach here -- run_cycle swallows them and
    # the previous cache keeps serving -- so anything that gets this far is a real defect.
    run_worker(run_plans_cacher_loop, "plans-cacher")
