import asyncio
import logging
import sys
import time


sys.path.insert(0, "/app")

from cacher.run_cacher import SubstrateCacher
from hippius_s3.config import get_config
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.monitoring import initialize_metrics_collector
from hippius_s3.sentry import init_sentry


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

config = get_config()
init_sentry("account-cacher", is_worker=True)


async def run_cacher_once() -> int:
    """Run one cache-update pass. Returns the number of main-account credit rows cached."""
    cacher = SubstrateCacher(
        substrate_url=config.substrate_url,
        redis_url=config.redis_accounts_url,
    )

    try:
        await cacher.connect()
        accounts_cached = await cacher.run_cache_update()
        logger.info("Cache update completed successfully")
        return accounts_cached
    finally:
        if cacher.redis_client:
            await cacher.redis_client.aclose()


async def run_account_cacher_cycle() -> bool:
    """Run one cacher pass, time it, and record metrics. Returns success."""
    collector = get_metrics_collector()
    started = time.monotonic()
    try:
        accounts_cached = await run_cacher_once()
        duration = time.monotonic() - started
        collector.record_account_cacher_cycle(success=True, accounts_cached=accounts_cached, duration=duration)
        logger.info(f"Cache update completed in {duration:.2f}s, sleeping for 5 minutes...")
        return True
    except Exception as e:
        logger.error(f"Cache update failed: {e}, will retry in 5 minutes")
        collector.record_account_cacher_cycle(success=False, accounts_cached=0, duration=time.monotonic() - started)
        return False


async def main():
    logger.info("Account cacher service started (runs every 5 minutes)")
    initialize_metrics_collector()

    while True:
        await run_account_cacher_cycle()
        await asyncio.sleep(300)


if __name__ == "__main__":
    asyncio.run(main())
