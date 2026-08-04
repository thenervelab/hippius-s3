from __future__ import annotations

import random
import shutil
from dataclasses import dataclass

from hippius_s3.config import Config


@dataclass(frozen=True)
class FsCachePressure:
    """Snapshot of filesystem cache pressure state."""

    free_bytes: int
    total_bytes: int
    free_ratio: float


def get_fs_cache_pressure(config: Config) -> FsCachePressure:
    usage = shutil.disk_usage(config.object_cache_dir)
    total = int(usage.total)
    free = int(usage.free)
    free_ratio = float(free / total) if total > 0 else 0.0
    return FsCachePressure(free_bytes=free, total_bytes=total, free_ratio=free_ratio)


def should_reject_fs_cache_write(
    *,
    config: Config,
    published_mode: int | None = None,
) -> tuple[bool, float, FsCachePressure, str]:
    """Reject when local free-space OR the published pool signal says stop.

    The local statvfs check sees only the mount under `object_cache_dir` — on
    prod api-local that is the node NVMe, which stayed green on 2026-07-24
    while the backing Ceph pool filled to the read-only cliff. The janitor's
    published `fs_cache:pressure` mode (pressure_signal.py) closes that blind
    spot; `None` (signal unavailable) preserves the local-only behavior.
    """
    pressure = get_fs_cache_pressure(config)

    threshold_hit = pressure.free_bytes <= int(config.fs_cache_min_free_bytes) or pressure.free_ratio <= float(
        config.fs_cache_min_free_ratio
    )
    reason = "threshold"
    if not threshold_hit and published_mode == 2:
        threshold_hit = True
        reason = "pool"
    if threshold_hit:
        # C2: jitter Retry-After ±25% so a fleet of throttled clients doesn't retry in a
        # synchronized wave (thundering herd) the instant a shared window elapses — which would
        # re-spike disk pressure and re-trigger the gate. Floor at 1s.
        base = float(config.fs_cache_retry_after_seconds)
        jittered = max(1.0, base * random.uniform(0.75, 1.25))
        return True, jittered, pressure, reason

    return False, 0.0, pressure, "ok"
