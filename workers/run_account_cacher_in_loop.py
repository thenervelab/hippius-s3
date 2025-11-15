import asyncio
import logging
import sys
import time


sys.path.insert(0, "/app")

from cacher.run_cacher import SubstrateCacher
from hippius_s3.config import get_config


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

config = get_config()


async def run_cacher_once():
    cacher = SubstrateCacher(
        substrate_url=config.substrate_url,
        redis_url=config.redis_accounts_url,
    )

    try:
        await cacher.connect()
        await cacher.run_cache_update()
        logger.info("Cache update completed successfully")
    finally:
        if cacher.redis_client:
            await cacher.redis_client.close()


async def main():
    logger.info("Account cacher service started (runs every 5 minutes)")

    while True:
        try:
            start_time = time.time()
            logger.info("Running account cache update...")

            await run_cacher_once()

            duration = time.time() - start_time
            logger.info(f"Cache update completed in {duration:.2f}s, sleeping for 5 minutes...")
        except Exception as e:
            logger.error(f"Cache update failed: {e}, will retry in 5 minutes")

        await asyncio.sleep(300)


if __name__ == "__main__":
    asyncio.run(main())
