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

    async def _collect_redis_metrics(self) -> None:
        try:
            if self.redis_queues_client:
                self.metrics_collector._upload_len = int(await self.redis_queues_client.llen("upload_requests") or 0)
                self.metrics_collector._unpin_len = int(await self.redis_queues_client.llen("unpin_requests") or 0)
                self.metrics_collector._substrate_len = int(
                    await self.redis_queues_client.llen("substrate_requests") or 0
                )
                self.metrics_collector._download_len = int(
                    await self.redis_queues_client.llen("download_requests") or 0
                )
            else:
                self.metrics_collector._upload_len = int(await self.redis_client.llen("upload_requests") or 0)  # ty: ignore
                self.metrics_collector._unpin_len = int(await self.redis_client.llen("unpin_requests") or 0)  # ty: ignore
                self.metrics_collector._substrate_len = int(await self.redis_client.llen("substrate_requests") or 0)  # ty: ignore
                self.metrics_collector._download_len = int(await self.redis_client.llen("download_requests") or 0)  # ty: ignore

            self.metrics_collector._main_db_size = int(await self.redis_client.dbsize() or 0)  # ty: ignore
            self.metrics_collector._accounts_db_size = int(await self.redis_accounts_client.dbsize() or 0)

            if self.redis_chain_client:
                self.metrics_collector._chain_db_size = int(await self.redis_chain_client.dbsize() or 0)

            if self.redis_rate_limiting_client:
                self.metrics_collector._rate_limiting_db_size = int(await self.redis_rate_limiting_client.dbsize() or 0)

            info = await self.redis_client.info("memory")  # ty: ignore
            self.metrics_collector._used_mem = info.get("used_memory", 0)
            self.metrics_collector._max_mem = info.get("maxmemory", 0)

            logger.debug("Redis metrics collected successfully")

        except Exception as e:
            logger.error(f"Failed to collect Redis metrics: {e}")
