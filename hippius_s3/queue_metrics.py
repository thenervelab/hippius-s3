"""Depth/age gauges for the Redis work queues.

The 2026-07-25 audit found 136k payloads silently accumulated in
`ovh_download_requests` (fan-out enabled, consumer scaled to 0) on a
`noeviction` Redis — invisible because nothing measured queue depth. These
gauges make every work queue, retry ZSET, and DLQ a first-class signal:

    queue_depth{queue=...}              LLEN / ZCARD per key
    queue_oldest_age_seconds{queue=...} age of the head payload, where knowable

Runs inside the janitor (single instance, already holds a queues-Redis client),
sampled on a fixed interval off the cycle path. Sampling is best-effort: a
Redis blip keeps the previous values rather than crashing the janitor or
zeroing gauges (a false "queue empty" during an outage would mask the exact
condition this exists to catch).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from opentelemetry import metrics as otel_metrics

from hippius_s3.config import Config


logger = logging.getLogger(__name__)

DEFAULT_SAMPLE_INTERVAL_SECONDS = 30.0

# OVERLAP NOTE: the api's BackgroundMetricsCollector also samples list depths
# into `hippius_queue_length{queue_name=...}` every 10s. This sampler is the
# intended single source of truth going forward (it adds retry ZSETs and
# oldest-age, and lives with the janitor rather than every api pod); retiring
# the api collector's depth loop is a planned follow-up — until then dashboards
# should prefer `queue_depth{queue=...}`.


def build_queue_key_sets(config: Config) -> tuple[list[str], list[str]]:
    """(list_keys, zset_keys) for every queue this deployment can produce.

    Derived from the backend config rather than hardcoded so a new backend is
    covered the moment it is configured. Naming mirrors hippius_s3.queue:
    `{b}_{kind}_requests` lists, `{b}_{kind}_retries` ZSETs, and the DLQ lists
    (`{b}_upload_requests:dlq`, shared `unpin_requests:dlq`).
    """
    kinds = (
        ("upload", config.upload_backends),
        ("download", config.download_backends),
        ("unpin", config.delete_backends),
    )
    lists: list[str] = []
    zsets: list[str] = []
    for kind, backends in kinds:
        lists.extend(f"{backend}_{kind}_requests" for backend in backends)
        zsets.extend(f"{backend}_{kind}_retries" for backend in backends)
    lists.extend(f"{backend}_upload_requests:dlq" for backend in config.upload_backends)
    lists.append("unpin_requests:dlq")
    return lists, zsets


def _payload_age_seconds(raw: Any, now: float) -> float | None:
    """Best-effort age of a queue payload from its own timestamps.

    `first_enqueued_at` is authoritative when set; `expire_at` minus the
    reader's cache TTL would be guesswork, so a payload without a usable
    timestamp reports no age rather than a wrong one.
    """
    try:
        data = json.loads(raw)
        enqueued = data.get("first_enqueued_at")
        if enqueued:
            return max(0.0, now - float(enqueued))
    except Exception:
        return None
    return None


class QueueDepthSampler:
    """Samples queue depths/ages into dicts served by OTel observable gauges."""

    def __init__(
        self,
        redis_client: Any,
        config: Config,
        *,
        register_metrics: bool = True,
    ) -> None:
        self._redis = redis_client
        self.list_keys, self.zset_keys = build_queue_key_sets(config)
        self.depths: dict[str, int] = {}
        self.oldest_age: dict[str, float] = {}
        if register_metrics:
            meter = otel_metrics.get_meter(__name__)
            meter.create_observable_gauge(
                name="queue_depth",
                callbacks=[self._obs_depth],
                description="Items in each Redis work queue / retry ZSET / DLQ",
            )
            meter.create_observable_gauge(
                name="queue_oldest_age_seconds",
                callbacks=[self._obs_age],
                description="Age of the oldest payload in each work queue, where knowable",
            )

    def _obs_depth(self, _: object) -> list[otel_metrics.Observation]:
        return [otel_metrics.Observation(v, {"queue": k}) for k, v in self.depths.items()]

    def _obs_age(self, _: object) -> list[otel_metrics.Observation]:
        return [otel_metrics.Observation(v, {"queue": k}) for k, v in self.oldest_age.items()]

    async def sample_once(self, now: float | None = None) -> None:
        now = time.time() if now is None else now
        # Build fresh dicts and atomically rebind at the end. The OTel exporter runs the
        # gauge callbacks on a BACKGROUND thread, so mutating the live dicts in place here
        # (on the event loop) can raise "dictionary changed size during iteration" in a
        # callback. A reference rebind is atomic under the GIL, so a callback always reads a
        # complete prior-or-next snapshot.
        new_depths: dict[str, int] = {}
        for key in self.list_keys:
            new_depths[key] = int(await self._redis.llen(key))
        for key in self.zset_keys:
            new_depths[key] = int(await self._redis.zcard(key))
        # Oldest-age only for plain request lists: BRPOP consumes from the
        # right, so index -1 is the next payload out and the oldest waiting.
        # Keys with unknowable age are simply omitted (fresh dict starts empty).
        new_oldest_age: dict[str, float] = {}
        for key in self.list_keys:
            if key.endswith(":dlq"):
                continue
            if new_depths.get(key, 0) > 0:
                raw = await self._redis.lindex(key, -1)
                if raw is not None:
                    age = _payload_age_seconds(raw, now)
                    if age is not None:
                        new_oldest_age[key] = age
        self.depths = new_depths
        self.oldest_age = new_oldest_age

    async def run(self, interval: float = DEFAULT_SAMPLE_INTERVAL_SECONDS) -> None:
        while True:
            try:
                await self.sample_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Keep prior values: zeroed gauges during a Redis outage would
                # read as "all queues drained" — the opposite of the truth.
                logger.warning("Queue depth sample failed (keeping last values): %s", exc)
            await asyncio.sleep(interval)


__all__ = [
    "DEFAULT_SAMPLE_INTERVAL_SECONDS",
    "QueueDepthSampler",
    "build_queue_key_sets",
]
