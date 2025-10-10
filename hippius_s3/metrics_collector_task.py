import asyncio
import contextlib
import logging
from typing import Optional

import redis.asyncio as async_redis

from hippius_s3.monitoring import MetricsCollector


logger = logging.getLogger(__name__)


class BackgroundMetricsCollector:
    """Background task for collecting custom metrics from Redis and other sources."""

    def __init__(self, metrics_collector: MetricsCollector, redis_client: async_redis.Redis):
        self.metrics_collector = metrics_collector
        self.redis_client = redis_client
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
        """Collect metrics from Redis."""
        try:
            # Update queue metrics
            await self.metrics_collector.update_queue_metrics()

            # Update multipart chunks metric
            await self.metrics_collector.update_multipart_chunks_metric()

            # Collect Redis memory info
            info = await self.redis_client.info("memory")
            used_memory = info.get("used_memory", 0)
            max_memory = info.get("maxmemory", 0)

            # Record Redis memory metrics
            self.metrics_collector.meter.create_gauge(
                name="redis_memory_used_bytes", description="Redis memory usage in bytes"
            ).set(used_memory, attributes={"type": "used_memory"})

            if max_memory > 0:
                self.metrics_collector.meter.create_gauge(
                    name="redis_memory_max_bytes", description="Redis max memory in bytes"
                ).set(max_memory, attributes={"type": "max_memory"})

            logger.debug("Redis metrics collected successfully")

        except Exception as e:
            logger.error(f"Failed to collect Redis metrics: {e}")

    async def collect_multipart_chunks_count(self) -> int:
        """Collect count of multipart chunks in Redis."""
        try:
            count = 0
            pattern = "multipart:*:part:*"
            async for _key in self.redis_client.scan_iter(match=pattern):
                count += 1
            return count
        except Exception as e:
            logger.error(f"Failed to count multipart chunks: {e}")
            return 0
