"""Shared FS-cache pressure signal: one authority, many consumers.

The janitor computes pressure as max(local statvfs, fullest Ceph pool %USED
from the mgr exporter) and publishes it here; every writer that must back off
(the api PUT throttle and every external bulk writer) reads the published
key instead of probing the mgr or guessing from its own mount. Independent
per-writer probes are how the api middleware ended up gating on the local NVMe
while the backing pool filled to the read-only cliff (2026-07-24 incident).

Contract (external consumers mirror this; keep the two in step):
    key   fs_cache:pressure                       (cache Redis)
    value {"mode": 0|1|2, "ratio": float, "source": "janitor", "ts": unix}
    SET every PUBLISH_INTERVAL_SECONDS with EX=PRESSURE_TTL_SECONDS
Consumers treat a missing/expired key as "signal unavailable" and fall back to
their own local check — never fail-open silently, never hard-stop on absence.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import shutil
import time
from pathlib import Path
from typing import Any
from typing import Callable

import httpx


logger = logging.getLogger(__name__)

PRESSURE_KEY = "fs_cache:pressure"
PUBLISH_INTERVAL_SECONDS = 30.0
# TTL covers several missed publishes before consumers fall back to local
# checks; a janitor outage degrades the gate rather than freezing a stale mode.
PRESSURE_TTL_SECONDS = 120

# Enter/exit watermark pairs (hysteresis): a disk hovering at a boundary must
# not flap the mode — and everything keyed off it — every sample.
PRESSURE_ELEVATED = 0.85
PRESSURE_ELEVATED_EXIT = 0.83
PRESSURE_CRITICAL = 0.95
PRESSURE_CRITICAL_EXIT = 0.93


def label_value(line: str, needle: str) -> str | None:
    """The prometheus label value following `needle` (e.g. `pool_id="`), up to its closing quote."""
    start = line.find(needle)
    if start < 0:
        return None
    start += len(needle)
    end = line.find('"', start)
    return line[start:end] if end >= 0 else None


def parse_pool_percent_used(body: str, pools: list[str]) -> float | None:
    """The fullest of `pools` from a mgr prometheus scrape, resolving each name to its id via
    `ceph_pool_metadata` then reading `ceph_pool_percent_used`. Returns None if ANY pool is absent
    — a missing pool must fail safe (fall back to statvfs), never silently shrink the gate. Pure so
    the parse is unit-testable without a live exporter."""
    name_to_id: dict[str, str] = {}
    id_to_pct: dict[str, float] = {}
    for line in body.splitlines():
        if line.startswith("ceph_pool_metadata{"):
            name = label_value(line, 'name="')
            pool_id = label_value(line, 'pool_id="')
            if name is not None and pool_id is not None and name in pools:
                name_to_id[name] = pool_id
        elif line.startswith("ceph_pool_percent_used{"):
            pool_id = label_value(line, 'pool_id="')
            if pool_id is None:
                continue
            try:
                value = float(line.rsplit(" ", 1)[1])
            except (ValueError, IndexError):
                continue
            if math.isfinite(value):  # a NaN/inf is not a ratio — drop it so the pool reads missing
                id_to_pct[pool_id] = value

    fullest: float | None = None
    for pool in pools:
        pool_id = name_to_id.get(pool)
        pct = id_to_pct.get(pool_id) if pool_id is not None else None
        if pct is None:
            logger.warning("pool-fullness probe: pool %r absent from mgr output; falling back to statvfs", pool)
            return None
        pct = min(max(pct, 0.0), 1.0)  # exporter rounding can nudge just past 1.0
        fullest = pct if fullest is None else max(fullest, pct)
    return fullest


def compute_mode(ratio: float, prev_mode: int) -> int:
    """Hysteresis step: enter at the higher threshold, exit below the lower one."""
    if ratio >= PRESSURE_CRITICAL or (prev_mode == 2 and ratio >= PRESSURE_CRITICAL_EXIT):
        return 2
    if ratio >= PRESSURE_ELEVATED or (prev_mode >= 1 and ratio >= PRESSURE_ELEVATED_EXIT):
        return 1
    return 0


class PressurePublisher:
    """Publishes the pressure mode to the cache Redis on a fixed interval.

    Owns its own hysteresis state — sampled every 30s rather than once per
    janitor cycle (~20min), because a mass writer (a bulk restore, a drain
    burst) can move the pool percent materially inside one cycle.
    """

    def __init__(
        self,
        redis_client: Any,
        root: Path,
        *,
        mgr_metrics_url: str,
        pools: list[str],
        probe_timeout_seconds: float,
        on_publish: Callable[[], None] | None = None,
    ) -> None:
        self._redis = redis_client
        self._root = root
        self._mgr_metrics_url = mgr_metrics_url
        self._pools = [p.strip() for p in pools if p.strip()]
        self._probe_timeout = probe_timeout_seconds
        self._prev_mode = 0
        # Called after each SUCCESSFUL publish so the janitor can gauge signal freshness — the
        # published key is invisible to monitoring on its own (it expires silently on the TTL).
        self._on_publish = on_publish

    async def _pool_ratio(self) -> float | None:
        if not self._mgr_metrics_url or not self._pools:
            return None
        try:
            async with httpx.AsyncClient(timeout=self._probe_timeout) as client:
                resp = await client.get(self._mgr_metrics_url)
                resp.raise_for_status()
                body = resp.text
        except httpx.HTTPError as exc:
            logger.warning("pool-fullness probe failed (%s); publishing statvfs-only pressure", exc)
            return None
        return parse_pool_percent_used(body, self._pools)

    async def compute_payload(self) -> dict[str, Any] | None:
        """Current pressure payload, or None when even statvfs is unreadable
        (publishing a made-up mode would be worse than letting the TTL lapse)."""

        def _local_ratio() -> float:
            usage = shutil.disk_usage(self._root)
            return usage.used / usage.total if usage.total else 0.0

        try:
            local_ratio = await asyncio.to_thread(_local_ratio)
        except OSError as exc:
            logger.warning("statvfs failed (%s); skipping pressure publish", exc)
            return None
        pool_ratio = await self._pool_ratio()
        ratio = max(local_ratio, pool_ratio or 0.0)
        self._prev_mode = compute_mode(ratio, self._prev_mode)
        return {"mode": self._prev_mode, "ratio": round(ratio, 4), "source": "janitor", "ts": time.time()}

    async def publish_once(self) -> None:
        payload = await self.compute_payload()
        if payload is None:
            return
        await self._redis.set(PRESSURE_KEY, json.dumps(payload), ex=PRESSURE_TTL_SECONDS)
        if self._on_publish is not None:
            self._on_publish()

    async def run(self, interval: float = PUBLISH_INTERVAL_SECONDS) -> None:
        while True:
            try:
                await self.publish_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # The TTL is the fail-safe: consumers fall back to local checks
                # once the key lapses, so a publish failure must not kill the janitor.
                logger.warning("pressure publish failed (consumers fall back on TTL lapse): %s", exc)
            await asyncio.sleep(interval)


# Consumer-side memo: one Redis read per _PUBLISHED_MEMO_SECONDS per process,
# not one per request. (mode, ts) — ts=0 forces the first read. _last_good
# survives READ ERRORS (a Redis blip during genuine mode-2 must not open the
# gate) but never key ABSENCE (absence is the publisher's honest "signal off"),
# and only within the publish TTL so a dead Redis can't pin a stale mode.
_published_cache: tuple[int | None, float] = (None, 0.0)
_last_good: tuple[int | None, float] = (None, 0.0)
_PUBLISHED_MEMO_SECONDS = 5.0


async def get_published_pressure_mode(redis_client: Any) -> int | None:
    """The published mode, or None when the signal is unavailable (consumer
    then falls back to its own local check per the contract)."""
    global _published_cache, _last_good
    cached_mode, cached_at = _published_cache
    now = time.monotonic()
    if now - cached_at < _PUBLISHED_MEMO_SECONDS:
        return cached_mode

    mode: int | None = None
    if redis_client is not None:
        try:
            raw = await redis_client.get(PRESSURE_KEY)
            if raw:
                parsed = int(json.loads(raw)["mode"])
                if parsed in (0, 1, 2):
                    mode = parsed
                    _last_good = (mode, now)
        except Exception as exc:
            logger.debug("published pressure read failed: %s", exc)
            good_mode, good_at = _last_good
            if good_mode is not None and now - good_at < PRESSURE_TTL_SECONDS:
                mode = good_mode
    _published_cache = (mode, now)
    return mode


__all__ = [
    "PRESSURE_CRITICAL",
    "PRESSURE_CRITICAL_EXIT",
    "PRESSURE_ELEVATED",
    "PRESSURE_ELEVATED_EXIT",
    "PRESSURE_KEY",
    "PRESSURE_TTL_SECONDS",
    "PUBLISH_INTERVAL_SECONDS",
    "PressurePublisher",
    "compute_mode",
    "get_published_pressure_mode",
    "label_value",
    "parse_pool_percent_used",
]
