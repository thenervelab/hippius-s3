#!/usr/bin/env python3
"""Scrapes the S3 billing-plan roll from api.hippius.com into redis-accounts.

    GET /api/s3/plans/accounts/?page=1&page_size=500

One endpoint carries both halves — `plans` is the catalog, `results` is the paginated per-account
roll — so this is a single loop, polled every HIPPIUS_PLANS_LOOP_SLEEP seconds (default 600).

Upstream reports each account's MAX QUOTA but not its usage, so this worker also computes the usage
itself: one SUM per account that is actually on a plan, run here in the background rather than on
anyone's upload. There are only a few tens of such accounts, so a cycle is a few seconds of work
spread over HIPPIUS_PLANS_USAGE_CONCURRENCY connections. That cardinality is the whole reason there
is no rollup table and no triggers — a maintained counter would buy nothing here except a second
source of truth to keep in step.

The refresh interval IS the enforcement lag, in both directions: an account can overshoot by one
cycle's worth of uploads, and a customer who deletes data stays refused until the next cycle sees
it. Nothing on the request path recomputes.

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

import asyncpg
from redis.asyncio import Redis
from substrateinterface.utils.ss58 import is_valid_ss58_address


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import HIPPIUS_SS58_FORMAT
from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.monitoring import initialize_metrics_collector
from hippius_s3.sentry import init_sentry
from hippius_s3.services import plans_cache
from hippius_s3.services import usage_service
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
# and never publishing. 500 pages x 500 rows = 250k accounts, well clear of the current ~3.1k.
MAX_PAGES = 500

# Upstream's own name for "this account is on a subscription". Anything else in `billing` — today
# only "pay_as_you_go" — means exactly that.
_PLAN_BILLING = "plan"


def _is_enforceable_plan_row(row: S3PlanAccountRow) -> bool:
    """Whether this account should be gated on a plan quota rather than billed pay-as-you-go.

    Requires billing == "plan" and a plan name. `active` is deliberately NOT consulted.

    It used to be, on the reading that a lapsed subscription keeps billing="plan" and its old plan
    name and is distinguished only by active=false. The first real payload said otherwise: upstream
    returns active=false on EVERY row it serves — 3069 of them at the last check, with zero
    exceptions over two days — including the one genuine subscriber, whose row carried a real
    subscription id and a next_charge date in the FUTURE. A cancelled
    subscription does not have a future charge date, so the field is not carrying the meaning we
    assumed; on present evidence it is simply not populated.

    Honouring it therefore admitted nobody, which is worse than the failure it was guarding against:
    the gate could never engage, so the feature could not be observed even in shadow mode, and
    turning enforcement on would have been a silent no-op forever.

    MEASURED BLAST RADIUS: exactly ONE row in 3069 carries billing == "plan" (checked twice, a day
    apart). This admits that one account, not the lapsed population — re-measure before assuming
    otherwise, because every risk below scales with that number.

    THE RISKS THIS ACCEPTS, both directions — admission is not purely generous:

    1. A cancelled subscriber keeps their allowance until the check is restored.
    2. Admission also IMPOSES A CAP and removes the pay-as-you-go path. An admitted account that is
       over its plan size but has substrate credits used to upload fine via can_upload; with
       enforcement on it is refused 402 until it deletes data AND a cacher cycle re-counts.
    3. An admitted account whose quota is unknown (plan absent from the catalog, or a null/0/
       negative storage_bytes) resolves to catalog_miss, which allows the write AND skips
       has_credits and can_upload. That is unmetered storage, not merely an unenforced quota.

    None of the three is reachable while HIPPIUS_ENABLE_BILLING_PLANS is off, which is how prod
    ships. Staging has it ON, so staging is where 2 and 3 would first appear.

    When upstream confirms what `active` means, restore the check (or switch to `next_charge` in the
    future, which is the field that actually tracked reality here). See todo.md — and note that
    restoring it needs `DEL hippius_s3_plan_accounts` on redis-accounts first, or the shrink guard
    refuses the smaller roll and wedges the cacher on the stale one.
    """
    return bool(row.billing == _PLAN_BILLING and row.plan)


def _parse_page(page: S3PlanAccountsResponse) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """Wire shape -> what we cache. The single place to change if the payload moves.

    Accounts that are not on a plan are simply ABSENT from the map: an absent field is
    exactly what the request path already treats as pay-as-you-go, so there is nothing to encode
    for them and nothing to keep in sync.

    `results[].storage_bytes` is the account's own MAX QUOTA, and it wins over the plan's list price
    so a negotiated limit is not silently overwritten. Upstream reports NO usage figure -- that is
    filled in afterwards by _attach_usage, from our own count.
    """
    accounts: dict[str, dict[str, Any]] = {}
    for row in page.results:
        if not row.ss58 or not _is_enforceable_plan_row(row):
            continue
        if not is_valid_ss58_address(row.ss58, valid_ss58_format=HIPPIUS_SS58_FORMAT):
            # Every address we key on is network prefix 42 (config._parse_service_accounts pins the
            # same thing, for the same reason). An address in another prefix -- the likeliest real
            # upstream mistake -- would match no bucket, so its count would come back 0 and we would
            # publish it as "stores nothing", i.e. unlimited headroom, with nothing to distinguish
            # it from a genuinely empty account. Drop the row; the account falls back to PAYG.
            logger.error(f"PLANS_BAD_ADDRESS skipping row with non-network-42 ss58: {row.ss58!r}")
            continue
        plan = page.plans.get(row.plan or "")
        limit = row.storage_bytes if row.storage_bytes is not None else (plan.storage_bytes if plan else None)
        accounts[row.ss58] = {"plan": row.plan, "storage_limit_bytes": limit}

    catalog = {name: {"h256": entry.h256, "storage_bytes": entry.storage_bytes} for name, entry in page.plans.items()}
    return accounts, catalog


async def _attach_usage(pool: asyncpg.Pool, accounts: dict[str, dict[str, Any]]) -> None:
    """Fill in `used_bytes` for every account, in parallel, mutating `accounts` in place.

    Each count is O(objects the account owns) -- sub-second for a typical account, seconds for the
    largest. Bounded concurrency rather than one serial pass, so a single 11.8M-object account does
    not set the pace for the whole cycle; and bounded rather than unbounded, so a few tens of
    simultaneous aggregates cannot become the heaviest thing running on the primary.

    A failure for ANY account propagates. refresh_plan_roll_once then publishes nothing and the
    previous roll keeps serving, which is the right trade: publishing a partial answer would mean
    writing used_bytes=0 for the accounts we failed to count, silently handing them unlimited
    headroom until the next cycle.
    """

    async def count(account_id: str) -> tuple[str, int]:
        # The pool IS the concurrency limiter -- it is sized to plans_usage_concurrency, so
        # acquire() blocks past that many in flight. A semaphore in front of it would be a second
        # knob for one limit, and could only ever disagree with the pool.
        async with pool.acquire() as conn:
            return account_id, await usage_service.get_account_storage_bytes(
                conn,
                account_id,
                timeout=config.plans_usage_timeout_seconds,
                page_size=config.plans_usage_page_size,
            )

    for account_id, used in await asyncio.gather(*(count(a) for a in accounts)):
        accounts[account_id]["used_bytes"] = used


async def refresh_plan_roll_once(redis_client: Redis, pool: asyncpg.Pool) -> tuple[int, int]:
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
    # The ONLY signal that upstream has started populating `active`. Nothing else reads the field
    # any more, so without this the condition for restoring the check in _is_enforceable_plan_row
    # would never announce itself -- we would have to go and look. Alert on this going non-zero.
    active_seen = 0

    async with HippiusApiClient() as api_client:
        for _ in range(MAX_PAGES):
            page = await api_client.get_s3_plan_accounts(next_url=next_url)
            pages += 1
            rows_seen += len(page.results)
            active_seen += sum(1 for row in page.results if row.active)

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

    usage_started = time.monotonic()
    await _attach_usage(pool, accounts)
    usage_seconds = time.monotonic() - usage_started

    published_accounts, published_plans = await plans_cache.publish_plan_roll(redis_client, accounts, catalog)
    await plans_cache.touch_meta(redis_client, published_accounts, published_plans)

    logger.info(
        f"Published plan roll: {published_accounts} accounts on a plan out of {rows_seen} rows, "
        f"{published_plans} plans, over {pages} page(s); usage counted in {usage_seconds:.1f}s; "
        f"upstream_active={active_seen}"
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
            f"the last known good map. Plan accounts keep the quota they had at that timestamp, and "
            f"their used_bytes is frozen — so where enforcement is ON, a customer who deletes data "
            f"to get back under quota stays refused until this recovers."
        )


async def run_cycle(redis_client: Redis, pool: asyncpg.Pool) -> bool:
    """Run one refresh. Never raises -- a failed cycle must leave the previous cache serving."""
    collector = get_metrics_collector()
    started = time.monotonic()
    try:
        accounts, _ = await refresh_plan_roll_once(redis_client, pool)
        collector.record_plans_cacher_cycle(success=True, entries=accounts, duration=time.monotonic() - started)
        return True
    except Exception as e:
        logger.error(f"plans-cacher cycle failed: {e}; keeping last known good cache", exc_info=True)
        collector.record_plans_cacher_cycle(success=False, entries=0, duration=time.monotonic() - started)
        return False


async def run_plans_cacher_loop() -> None:
    redis_client = Redis.from_url(config.redis_accounts_url)
    # READ-ONLY DSN. These counts are aggregates over the largest tables in the schema, run on a
    # timer with nobody waiting, so they belong nowhere near the primary — on this cluster a
    # read-storm has stalled it before and triggered a failover. Falls back to DATABASE_URL when
    # unset, so local dev and e2e are unaffected. A replica may cancel a long query under recovery
    # conflict rather than let it lag replay; run_cycle already treats that as a failed cycle and
    # keeps the previous roll serving, which is the behaviour we want.
    #
    # Sized to the usage concurrency and no larger: this worker's only DB work is those counts, and
    # an oversized pool here is idle backends against Postgres max_connections for nothing.
    # jit=off for the whole pool. This worker runs nothing but the storage counts, and JIT is pure
    # overhead for them: it fires because the planner's cost estimate is inflated (see the
    # n_distinct migration), then spends 107ms of a 326ms count for a 1,300-object account compiling
    # expressions for a query that is index-probe bound, not expression bound. Measured 20-35% off
    # small accounts for free.
    pool = await asyncpg.create_pool(
        config.database_readonly_url,
        min_size=1,
        max_size=max(1, config.plans_usage_concurrency),
        server_settings={"jit": "off"},
    )
    initialize_metrics_collector()

    logger.info(
        f"Starting plans-cacher: polling every {config.plans_loop_sleep}s, counting usage over "
        f"{config.plans_usage_concurrency} connections. Caching is unconditional — it does not "
        f"depend on whether plan enforcement is enabled."
    )

    # Closing on the way out is what tells Postgres and Redis these clients are gone; a cancelled
    # worker otherwise leaves its backends behind.
    try:
        while True:
            ok = await run_cycle(redis_client, pool)
            await _report_cache_age(redis_client)
            sleep_for = config.plans_loop_sleep if ok else 60
            logger.info(f"plans-cacher: sleeping {sleep_for}s")
            await asyncio.sleep(sleep_for)
    finally:
        await pool.close()
        await redis_client.aclose()


if __name__ == "__main__":
    # Default (no restart_on_crash): a crash exits the process and the kubelet restarts the pod, so
    # a crash-looping cacher shows up in pod-restart alerting instead of hiding behind
    # restart_count: 0. Individual cycle failures never reach here -- run_cycle swallows them and
    # the previous cache keeps serving -- so anything that gets this far is a real defect.
    run_worker(run_plans_cacher_loop, "plans-cacher")
