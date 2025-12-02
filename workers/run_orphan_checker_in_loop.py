#!/usr/bin/env python3
import asyncio
import logging
import sys
import time
from pathlib import Path

import asyncpg
import redis.asyncio as async_redis


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.queue import UnpinChainRequest
from hippius_s3.queue import enqueue_unpin_request
from hippius_s3.queue import initialize_queue_client
from hippius_s3.services.hippius_api_service import HippiusApiClient


config = get_config()

setup_loki_logging(config, "orphan-checker")
logger = logging.getLogger(__name__)


async def check_for_orphans(
    db: asyncpg.Connection,
) -> None:
    """Check for orphaned files on chain that don't exist in local DB."""
    logger.info("Starting orphan check cycle...")

    async with HippiusApiClient() as api_client:
        page = 1
        total_checked = 0
        total_orphans = 0

        while True:
            response = await api_client.list_files(
                page=page,
                page_size=config.orphan_checker_batch_size,
            )

            results = response.get("results", [])
            if not results:
                break

            logger.info(f"Processing page {page} with {len(results)} files")

            for file_data in results:
                total_checked += 1

                cid = file_data.get("cid")
                original_name = file_data.get("original_name", "")
                account = file_data.get("account_ss58")

                if not original_name.startswith("s3-"):
                    continue

                if not cid or not account:
                    logger.warning(f"Skipping file with missing CID or account: {file_data}")
                    continue

                row = await db.fetchrow("SELECT EXISTS(SELECT 1 FROM cids WHERE cid = $1) AS exists", cid)

                if not row["exists"]:
                    logger.warning(
                        f"Found orphaned file: CID={cid}, account={account}, "
                        f"filename={original_name}, size={file_data.get('size_bytes')}"
                    )
                    total_orphans += 1

                    await enqueue_unpin_request(
                        payload=UnpinChainRequest(
                            address=account,
                            object_id="00000000-0000-0000-0000-000000000000",
                            object_version=0,
                            cid=cid,
                        ),
                    )


            next_page = response.get("next")
            if not next_page:
                break

            page += 1

    logger.info(f"Orphan check complete: checked {total_checked} files, found {total_orphans} orphans")
    get_metrics_collector().record_orphan_checker_operation(
        orphans_found=total_orphans, files_checked=total_checked, success=True
    )


async def run_orphan_checker_loop() -> None:
    """Main loop for orphan checker worker."""
    redis_client = async_redis.from_url(config.redis_url)
    redis_queues_client = async_redis.from_url(config.redis_queues_url)
    db = await asyncpg.connect(config.database_url)

    from hippius_s3.redis_cache import initialize_cache_client

    initialize_queue_client(redis_queues_client)
    initialize_cache_client(redis_client)

    logger.info("Starting orphan checker service...")
    logger.info(f"Database URL: {config.database_url}")
    logger.info(f"Redis URL: {config.redis_url}")
    logger.info(f"Redis Queues URL: {config.redis_queues_url}")
    logger.info(f"Check interval: {config.orphan_checker_loop_sleep} seconds")
    logger.info(f"Batch size: {config.orphan_checker_batch_size} files")

    while True:
        try:
            await check_for_orphans(db)
            logger.info(f"Sleeping for {config.orphan_checker_loop_sleep} seconds...")
            await asyncio.sleep(config.orphan_checker_loop_sleep)
        except Exception as e:
            logger.error(f"Orphan checker error: {e}", exc_info=True)
            get_metrics_collector().record_orphan_checker_operation(success=False)
            await asyncio.sleep(60)


if __name__ == "__main__":
    while True:
        try:
            asyncio.run(run_orphan_checker_loop())
        except KeyboardInterrupt:
            logger.info("Orphan checker service stopped by user")
            break
        except Exception as e:
            logger.error(f"Orphan checker crashed, restarting in 5 seconds: {e}", exc_info=True)
            time.sleep(5)
