from __future__ import annotations

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
) -> tuple[bool, float, FsCachePressure, str]:
    """Reject when current free-space is below configured thresholds."""
    pressure = get_fs_cache_pressure(config)

    threshold_hit = pressure.free_bytes <= int(config.fs_cache_min_free_bytes) or pressure.free_ratio <= float(
        config.fs_cache_min_free_ratio
    )
    if threshold_hit:
        return True, float(config.fs_cache_retry_after_seconds), pressure, "threshold"

    return False, 0.0, pressure, "ok"
