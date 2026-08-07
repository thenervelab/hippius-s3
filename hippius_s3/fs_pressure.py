from __future__ import annotations

import logging
import random
import shutil
import time
from dataclasses import dataclass
from typing import Callable
from typing import Optional

from hippius_s3.config import Config


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FsCachePressure:
    """Snapshot of filesystem cache pressure state."""

    free_bytes: int
    total_bytes: int
    free_ratio: float


# How long one free-space reading is reused. Read-through promotion asks per CHUNK, so an
# unmemoized probe would be a syscall per chunk on the read path; 5s is far shorter than the
# time it takes ingest to move the ratio meaningfully.
_GATE_MEMO_SECONDS = 5.0


class FreeSpaceGate:
    """Answers "is there room to promote onto this disk?", at most one probe per TTL.

    Read-through promotion is the only unthrottled writer to the ingest SSD, and it competes
    for the very disk `fs_cache_pressure` gates PUTs on — on an ingest node
    `HIPPIUS_OBJECT_CACHE_DIR` and the drain agent's `CEPHOR_SSD_ROOT` are the same mount. So
    promotion has to yield to ingest, and it must yield *before* the evictor is even armed:
    warming a cache is an optimisation, refusing a write is an outage.

    The threshold sits INSIDE the evictor's hysteresis band, which is the part that is easy to
    get wrong in both directions:

        fs_cache_min_free 0.08  <  evict_reserve 0.15  <  promote_floor  <  evict target 0.20

    A floor equal to the evictor's target chatters — promotion is live only in the instant a
    pass completes. A floor ABOVE the target deadlocks — promotion stops and the evictor, which
    only frees back to its target, can never restore enough for it to resume. Only a floor
    strictly between the reserve and the target leaves a band where eviction restores the disk
    past the point promotion resumes.
    """

    def __init__(
        self,
        path: str,
        min_free_ratio: float,
        *,
        ttl_seconds: float = _GATE_MEMO_SECONDS,
        probe: Optional[Callable[[str], float]] = None,
    ) -> None:
        self._path = path
        self._min_free_ratio = float(min_free_ratio)
        self._ttl = float(ttl_seconds)
        self._probe = probe or free_ratio_of
        self._expires_at = 0.0
        self._allowed = True

    def allows(self, now: Optional[float] = None) -> bool:
        """True when the disk has room to spare for a cache copy.

        Fails OPEN: a probe that raises leaves the previous verdict in place (initially
        "allowed"). Promotion is best-effort in every other respect too — a `statvfs` blip must
        not silently disable the read tier, and a genuinely full disk still stops the write with
        `ENOSPC`, which `_promote_chunk` already swallows.
        """
        stamp = now if now is not None else time.monotonic()
        if stamp < self._expires_at:
            return self._allowed
        try:
            self._allowed = self._probe(self._path) > self._min_free_ratio
        except OSError as exc:
            logger.debug("promotion free-space probe failed for %s: %s", self._path, exc)
        self._expires_at = stamp + self._ttl
        return self._allowed


def free_ratio_of(path: str) -> float:
    """Free share of the filesystem holding `path`, in 0.0..=1.0."""
    usage = shutil.disk_usage(path)
    return float(usage.free / usage.total) if usage.total > 0 else 0.0


# The drain agent's shipped eviction policy, as ratios. The evictor lives in Rust
# (`crates/hippius-drain-agent/src/config.rs`: DEFAULT_EVICT_RESERVE_PERMILLE = 150,
# DEFAULT_EVICT_HEADROOM_PERMILLE = 50) with no runtime coupling to this process, so the
# contract is mirrored here and pinned by a test on each side. If the Rust defaults move and
# these do not, `validate_promotion_band` stops describing the deployed system.
EVICT_RESERVE_RATIO = 0.150
EVICT_HEADROOM_RATIO = 0.050


class PromotionBandError(ValueError):
    """The promote floor cannot form a live control loop with the evictor."""


def validate_promotion_band(
    *,
    promote_min_free_ratio: float,
    fs_cache_min_free_ratio: float,
    evict_reserve_ratio: float = EVICT_RESERVE_RATIO,
    evict_headroom_ratio: float = EVICT_HEADROOM_RATIO,
) -> None:
    """Raise unless the four thresholds form a loop that can actually recover.

    Checked against the RESOLVED config at startup, not just against code defaults, because
    every one of these is settable per-deployment and a bad combination fails silently — the
    read tier simply stops warming, with nothing logging why.

    Required ordering::

        fs_cache_min_free  <  evict_reserve  <  promote_floor  <  evict_reserve + headroom

    Each inequality earns its place:

    - ``fs_cache_min_free < evict_reserve``: the evictor must be reclaiming before PUTs are
      refused, not racing the refusal.
    - ``evict_reserve < promote_floor``: promotion backs off before eviction is even armed, so
      the cheap lever is pulled first.
    - ``promote_floor < evict target``: the one that is easy to get backwards. The evictor
      never frees past ``reserve + headroom``; a floor at or above that point can never be
      restored, so promotion stops permanently the first time the disk dips. A floor exactly
      equal to the target chatters instead — live only in the instant a pass completes.

    Raises:
        PromotionBandError: with the offending values, so the fix is obvious from the log.
    """
    evict_target = evict_reserve_ratio + evict_headroom_ratio
    if not fs_cache_min_free_ratio < evict_reserve_ratio:
        raise PromotionBandError(
            f"the PUT-refusal threshold ({fs_cache_min_free_ratio}) must be below the evictor's "
            f"reserve ({evict_reserve_ratio}), or writes are refused before eviction arms"
        )
    if not evict_reserve_ratio < promote_min_free_ratio:
        raise PromotionBandError(
            f"the promote floor ({promote_min_free_ratio}) must be above the evictor's reserve "
            f"({evict_reserve_ratio}), so promotion yields before eviction is armed"
        )
    if not promote_min_free_ratio < evict_target:
        raise PromotionBandError(
            f"the promote floor ({promote_min_free_ratio}) must be below the evictor's target "
            f"({evict_target}); the evictor never frees past its target, so a floor at or above "
            "it can never be restored and promotion would stop permanently"
        )


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
