import asyncio
import contextlib
import logging
from typing import Any
from typing import Optional
from typing import Union

from redis.asyncio import Redis
from redis.asyncio.cluster import RedisCluster

from hippius_s3.config import get_config
from hippius_s3.monitoring import MetricsCollector


logger = logging.getLogger(__name__)


def _redis_memory(info: dict) -> tuple[int, int]:
    """`(used_memory, maxmemory)` from an INFO reply, for a single node OR a cluster.

    A single `Redis` returns a flat mapping. A `RedisCluster` fans INFO out to every primary and
    returns one mapping PER NODE, keyed by node name — so the flat `info["used_memory"]` lookup
    silently yielded 0 and the redis-memory gauge read empty for every cluster-backed pod. Sum the
    per-node values so the gauge reports the cluster as a whole; `maxmemory` sums too, since the
    cap that matters is the cluster's aggregate headroom. Nodes missing the field contribute 0.
    """
    per_node = bool(info) and all(isinstance(v, dict) for v in info.values())
    nodes = list(info.values()) if per_node else [info]
    used = sum(int(n.get("used_memory", 0) or 0) for n in nodes)
    cap = sum(int(n.get("maxmemory", 0) or 0) for n in nodes)
    return used, cap


class BackgroundMetricsCollector:
    """Background task for collecting custom metrics from Redis and other sources."""

    def __init__(
        self,
        metrics_collector: MetricsCollector,
        redis_client: Union[Redis, RedisCluster],
        redis_accounts_client: Redis,
        redis_rate_limiting_client: Optional[Redis] = None,
        redis_queues_client: Optional[Redis] = None,
    ):
        self.metrics_collector = metrics_collector
        self.redis_client = redis_client
        self.redis_accounts_client = redis_accounts_client
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
        "arion_upload_requests",
        "ovh_upload_requests",
        "arion_unpin_requests",
        "ovh_unpin_requests",
        "substrate_requests",
    ]
    ZSET_QUEUES = [
        "arion_upload_retries",
    ]

    @staticmethod
    def _dlq_queues() -> list[str]:
        # Every DLQ, derived the same way the janitor derives its protection set — adding a backend
        # extends coverage automatically. Previously only arion_upload_requests:dlq was gauged, so a
        # full ovh or unpin DLQ was invisible until it caused a pipeline-wide redis-queues stall.
        config = get_config()
        return [f"{b}_upload_requests:dlq" for b in config.upload_backends] + ["unpin_requests:dlq"]

    @staticmethod
    async def _node_scoped_queues(rc: Any) -> tuple[list[str], list[str]]:
        # The drain publishes to `{b}_upload_requests:<node>` and the node's uploader moves its
        # retries through `{b}_upload_retries:<node>`; discovered per sample because the node set
        # is a cluster fact. A node queue nobody reads is the backlog this gauge exists to show.
        config = get_config()
        lists: list[str] = []
        zsets: list[str] = []
        for backend in config.upload_backends:
            async for raw in rc.scan_iter(match=f"{backend}_upload_requests:*", count=200):
                key = raw.decode() if isinstance(raw, bytes) else str(raw)
                if not key.endswith(":dlq"):
                    lists.append(key)
            zsets.extend(
                [
                    raw.decode() if isinstance(raw, bytes) else str(raw)
                    async for raw in rc.scan_iter(match=f"{backend}_upload_retries:*", count=200)
                ]
            )
        return sorted(lists), sorted(zsets)

    async def _collect_redis_metrics(self) -> None:
        try:
            rc = self.redis_queues_client or self.redis_client

            node_lists, node_zsets = await self._node_scoped_queues(rc)
            for queue_name in self.LIST_QUEUES + self._dlq_queues() + node_lists:
                length = int(await rc.llen(queue_name) or 0)  # ty: ignore
                self.metrics_collector.set_queue_length(queue_name, length)

            for queue_name in self.ZSET_QUEUES + node_zsets:
                length = int(await rc.zcard(queue_name) or 0)
                self.metrics_collector.set_queue_length(queue_name, length)

            # Gauge the REDIS-QUEUES instance memory, not the main cache: redis-queues is
            # `noeviction`, so once it fills EVERY write fails — the sole-producer upload LPUSH,
            # the chunk pub/sub, and the cephor:* coordination keys — a pipeline-wide cascade.
            # (The old code read the main redis, whose fullness is harmless — it just evicts.)
            used_mem, max_mem = _redis_memory(await rc.info("memory"))
            self.metrics_collector._used_mem = used_mem
            self.metrics_collector._max_mem = max_mem
            if max_mem > 0:
                fill = used_mem / max_mem
                if fill >= 0.85:
                    logger.error(
                        f"redis-queues at {fill:.0%} of its {max_mem} byte cap — a full noeviction "
                        f"instance fails EVERY write (upload LPUSH, pub/sub, cephor:*). Investigate DLQ/backlog growth."
                    )
                elif fill >= 0.70:
                    logger.warning(
                        f"redis-queues at {fill:.0%} of its memory cap (noeviction); watch for a fill trend."
                    )

            logger.debug("Redis metrics collected successfully")

        except Exception as e:
            logger.error(f"Failed to collect Redis metrics: {e}")
