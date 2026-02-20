import asyncio
import contextlib
import logging
from typing import Optional
from typing import Union

from redis.asyncio import Redis
from redis.asyncio.cluster import RedisCluster

from hippius_s3.monitoring import MetricsCollector


logger = logging.getLogger(__name__)


class BackgroundMetricsCollector:
    """Background task for collecting custom metrics from Redis and other sources."""

    def __init__(
        self,
        metrics_collector: MetricsCollector,
        redis_client: Union[Redis, RedisCluster],
        redis_accounts_client: Redis,
        redis_chain_client: Optional[Redis] = None,
        redis_rate_limiting_client: Optional[Redis] = None,
        redis_queues_client: Optional[Redis] = None,
    ):
        self.metrics_collector = metrics_collector
        self.redis_client = redis_client
        self.redis_accounts_client = redis_accounts_client
        self.redis_chain_client = redis_chain_client
        self.redis_rate_limiting_client = redis_rate_limiting_client
        self.redis_queues_client = redis_queues_client
        self.running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Start the background metrics collection task."""
        if self.running:
            return

        self.running = True
        self._task = asyncio.create_task(self._collect_metrics_loop())
        logger.info("Background metrics collection started")

    async def stop(self) -> None:
        """Stop the background metrics collection task."""
        self.running = False
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        logger.info("Background metrics collection stopped")

    async def _collect_metrics_loop(self) -> None:
        """Main loop for collecting metrics."""
        while self.running:
            try:
                await self._collect_redis_metrics()
                await asyncio.sleep(10)  # Collect metrics every 10 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error collecting metrics: {e}")
                await asyncio.sleep(5)  # Wait 5 seconds on error

    # All per-backend queues to track (lists use LLEN, ZSETs use ZCARD)
    LIST_QUEUES = [
        "ipfs_upload_requests",
        "arion_upload_requests",
        "ovh_upload_requests",
        "ipfs_download_requests",
        "arion_download_requests",
        "ovh_download_requests",
        "ipfs_unpin_requests",
        "arion_unpin_requests",
        "ovh_unpin_requests",
        "substrate_requests",
        "ipfs_upload_requests:dlq",
        "arion_upload_requests:dlq",
    ]
    ZSET_QUEUES = [
        "ipfs_upload_retries",
        "arion_upload_retries",
    ]

    async def _collect_redis_metrics(self) -> None:
        try:
            rc = self.redis_queues_client or self.redis_client

            for queue_name in self.LIST_QUEUES:
                length = int(await rc.llen(queue_name) or 0)  # ty: ignore
                self.metrics_collector.set_queue_length(queue_name, length)

            for queue_name in self.ZSET_QUEUES:
                length = int(await rc.zcard(queue_name) or 0)  # ty: ignore
                self.metrics_collector.set_queue_length(queue_name, length)

            info = await self.redis_client.info("memory")  # ty: ignore
            self.metrics_collector._used_mem = info.get("used_memory", 0)
            self.metrics_collector._max_mem = info.get("maxmemory", 0)

            logger.debug("Redis metrics collected successfully")

        except Exception as e:
            logger.error(f"Failed to collect Redis metrics: {e}")
