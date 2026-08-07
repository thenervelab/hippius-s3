from __future__ import annotations

import logging
import random
import shutil
import time
from dataclasses import dataclass
from typing import Awaitable
from typing import Callable
from typing import Literal
from typing import Optional

from hippius_s3.config import Config


logger = logging.getLogger(__name__)


# Yields the free-space floor the local evictor is actually holding, as a ratio, or `None` when
# the signal is unavailable. See hippius_s3/promote_floor.py for the wire contract.
PublishedFloorSource = Callable[[], Awaitable[Optional[float]]]


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


def _record_floor_divergence(direction: Literal["stricter", "looser"]) -> None:
    """Count a gate decision made on a published floor that differs from the configured one.

    Imported lazily and never allowed to raise, matching the other promotion counters: this
    sits on the read path and observability must not be able to fail a GET.
    """
    try:
        from hippius_s3.monitoring import get_metrics_collector

        collector = get_metrics_collector()
        if collector is not None:
            collector.record_promote_floor_divergence(direction)
    except Exception:  # noqa: BLE001 - a metrics failure must not fail a read
        pass


class FreeSpaceGate:
    """Answers "is there room to promote onto this disk?", at most one probe per TTL.

    Read-through promotion is the only unthrottled writer to the ingest SSD, and it competes
    for the very disk `fs_cache_pressure` gates PUTs on — on an ingest node
    `HIPPIUS_OBJECT_CACHE_DIR` and the drain agent's `CEPHOR_SSD_ROOT` are the same mount. So
    promotion has to yield to ingest, and it must yield *before* the evictor is even armed:
    warming a cache is an optimisation, refusing a write is an outage.

    The threshold sits INSIDE the evictor's hysteresis band, which is the part that is easy to
    get wrong in both directions:

        fs_cache_min_free  <  evict_reserve  <  promote_floor  <  evict_reserve + headroom

    A floor equal to the evictor's target chatters — promotion is live only in the instant a
    pass completes. A floor ABOVE the target deadlocks — promotion stops and the evictor, which
    only frees back to its target, can never restore enough for it to resume. Only a floor
    strictly between the reserve and the target leaves a band where eviction restores the disk
    past the point promotion resumes.

    That band is NOT fixed. The drain allocator publishes a per-node eviction reserve that
    overrides the agent's configured one (150..400 permille, by how stalled the drain is), so a
    constant on this side is only ever correct by accident. The agent therefore resolves the
    floor for the reserve in force and publishes it; `floor_source` reads that number and it
    WINS over `min_free_ratio`, which is now purely the fallback for when no floor is published.
    """

    def __init__(
        self,
        path: str,
        min_free_ratio: float,
        *,
        ttl_seconds: float = _GATE_MEMO_SECONDS,
        probe: Optional[Callable[[str], float]] = None,
        floor_source: Optional[PublishedFloorSource] = None,
    ) -> None:
        self._path = path
        self._min_free_ratio = float(min_free_ratio)
        self._ttl = float(ttl_seconds)
        self._probe = probe or free_ratio_of
        self._floor_source = floor_source
        self._expires_at = 0.0
        self._allowed = True
        # The last divergent floor warned about, so a sustained divergence logs on the
        # TRANSITION rather than every memo window (which on a stalled node would be 12 lines a
        # minute per pod, for hours). The counter below carries the rate; the log carries the event.
        self._warned_floor: Optional[float] = None

    async def allows(self, now: Optional[float] = None) -> bool:
        """True when the disk has room to spare for a cache copy.

        Fails OPEN: a probe that raises leaves the previous verdict in place (initially
        "allowed"). Promotion is best-effort in every other respect too — a `statvfs` blip must
        not silently disable the read tier, and a genuinely full disk still stops the write with
        `ENOSPC`, which `_promote_chunk` already swallows.

        The published floor is resolved inside the SAME memo window as the probe, so a decision
        asked per chunk costs at most one Redis round-trip and one `statvfs` per TTL.
        """
        stamp = now if now is not None else time.monotonic()
        if stamp < self._expires_at:
            return self._allowed
        floor = await self._resolve_floor()
        try:
            self._allowed = self._probe(self._path) > floor
        except OSError as exc:
            logger.debug("promotion free-space probe failed for %s: %s", self._path, exc)
        self._expires_at = stamp + self._ttl
        return self._allowed

    async def _resolve_floor(self) -> float:
        """The floor to gate on: the evictor's published one, else the static fallback.

        Prefers the published value because it is the only one that tracks the reserve actually
        in force — the allocator raises that reserve at runtime and a configured constant cannot
        follow it. Every failure mode lands on the static floor, which is today's behaviour: an
        unavailable signal must not disable the read tier.
        """
        if self._floor_source is None:
            return self._min_free_ratio
        try:
            published = await self._floor_source()
        except Exception as exc:  # noqa: BLE001 - promotion is awaited inline in a GET
            logger.debug("published promote-floor lookup failed for %s: %s", self._path, exc)
            return self._min_free_ratio
        if published is None:
            return self._min_free_ratio
        if published == self._min_free_ratio:
            self._warned_floor = None
            return published
        # Surfaced rather than merely handled: a divergence means the evictor is holding a band
        # this process was configured for the wrong side of. At the allocator's ceiling the
        # published floor is ~0.425, so promotion effectively stops on a stressed node — which
        # is the intent, since warming a cache while the drain is stalled is the failure this
        # prevents, but it must never look like the read tier quietly broke.
        _record_floor_divergence("stricter" if published > self._min_free_ratio else "looser")
        if published != self._warned_floor:
            logger.warning(
                "promote floor: using the evictor's published %.3f instead of the configured %.3f "
                "(the allocator has moved this node's eviction reserve)",
                published,
                self._min_free_ratio,
            )
            self._warned_floor = published
        return published


def free_ratio_of(path: str) -> float:
    """Free share of the filesystem holding `path`, in 0.0..=1.0."""
    usage = shutil.disk_usage(path)
    return float(usage.free / usage.total) if usage.total > 0 else 0.0


# The drain agent's DEFAULT eviction policy, as ratios (`crates/hippius-drain-agent/src/config.rs`:
# DEFAULT_EVICT_RESERVE_PERMILLE = 150, DEFAULT_EVICT_HEADROOM_PERMILLE = 50).
#
# These describe the band an agent holds when the allocator is publishing NO reserve for it —
# nothing more. The live band moves with the allocator's per-node reserve, and the agent
# publishes the resolved floor for it (see hippius_s3/promote_floor.py), so these constants no
# longer bound the running system. They remain here because `promote_min_free_ratio` is the
# fallback used when that signal is unavailable, and that fallback still has to be valid
# against the defaults it will be paired with.
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
    """Raise unless the STATIC FALLBACK thresholds form a loop that can actually recover.

    This no longer describes the live band. The drain allocator moves each node's eviction
    reserve at runtime, so no startup check in this process can bound where the evictor will
    be; the agent publishes the resolved floor per pass and `FreeSpaceGate` prefers it. What is
    validated here is the floor used when that signal is UNAVAILABLE, against the agent's
    default policy — the one configuration this side can still reason about locally.

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
