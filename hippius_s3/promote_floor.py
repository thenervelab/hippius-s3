"""Consumer for the drain-agent's published read-promotion floor.

The evictor that frees this node's ingest SSD does not hold a fixed reserve:
the drain allocator publishes a per-node reserve that overrides the agent's
configured one, interpolating 150..400 permille by how badly the drain is
stalled. Read-through promotion has to back off strictly inside that evictor's
hysteresis band, so its threshold cannot be a constant on this side — it moves
whenever the allocator moves the reserve.

The agent therefore resolves the floor and publishes the RESOLVED number; this
module only reads an integer. It deliberately does NOT re-derive the floor from
the reserve and headroom: mirroring the formula fails the same way mirroring the
constant did, just one level up.

Contract (agent side: `Coordinator::publish_promote_floor`, called once per
eviction pass from `evict_once`):
    key   cephor:promote_floor:{node_name}        (queues Redis, 6382)
    value {"floor_permille": int, "source": "drain-agent", "ts": unix}
    SET every eviction poll (30s default) with EX=PROMOTE_FLOOR_TTL_SECONDS
A missing, expired, or malformed key means "signal unavailable" and the consumer
falls back to its own statically configured floor — never to no floor, and never
to an exception on the read path.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any
from typing import Optional


logger = logging.getLogger(__name__)

PROMOTE_FLOOR_KEY_PREFIX = "cephor:promote_floor:"

# Mirrors PROMOTE_FLOOR_TTL in crates/hippius-drain-core/src/coordination.rs. Only used here to
# bound how long a last-known-good value may survive a Redis READ ERROR; the key's own expiry is
# what actually retires the signal.
PROMOTE_FLOOR_TTL_SECONDS = 120

# The floor lookup sits on the read path (promotion is decided per chunk, memoised), and the
# api's queues client is built with no socket timeout — so a reachable-but-blocked Redis would
# otherwise stall a GET rather than fail it.
_LOOKUP_TIMEOUT_SECONDS = 1.0


def promote_floor_key(node_name: str) -> str:
    return f"{PROMOTE_FLOOR_KEY_PREFIX}{node_name}"


def parse_floor_ratio(raw: object) -> Optional[float]:
    """The published floor as a 0..1 ratio, or `None` if the value is absent or unusable.

    Pure, so the wire form is testable without a Redis. Permille is rejected outside
    `1..=1000` because both ends parse as perfectly good integers and neither is a floor: zero
    would switch the gate off entirely, and a floor past the size of the disk would switch
    promotion off forever — the two silent failures this whole change exists to prevent.
    """
    if raw is None:
        return None
    if not isinstance(raw, (str, bytes, bytearray)):
        logger.warning("published promote floor is not a JSON payload (%.60r); falling back to the static floor", raw)
        return None
    try:
        permille = int(json.loads(raw)["floor_permille"])
    except (TypeError, ValueError, KeyError):
        logger.warning("published promote floor is malformed (%.60r); falling back to the static floor", raw)
        return None
    if not 0 < permille <= 1_000:
        logger.warning(
            "published promote floor %d permille is out of range; falling back to the static floor", permille
        )
        return None
    return permille / 1_000.0


class PublishedPromoteFloor:
    """Reads this node's published promote floor as a free-space ratio."""

    def __init__(
        self,
        queues_client: Any,
        node_name: str,
        *,
        timeout_seconds: float = _LOOKUP_TIMEOUT_SECONDS,
        ttl_seconds: float = PROMOTE_FLOOR_TTL_SECONDS,
    ) -> None:
        self._redis = queues_client
        self._key = promote_floor_key(node_name)
        self._timeout = float(timeout_seconds)
        self._ttl = float(ttl_seconds)
        self._last_good: Optional[float] = None
        self._last_good_at = 0.0

    async def ratio(self, now: Optional[float] = None) -> Optional[float]:
        """This node's published floor as a ratio, or `None` when the signal is unavailable.

        A read FAILURE holds the last good value for the publisher's TTL, while an absent or
        malformed key does not. The distinction matters in one direction only: the agent raises
        the floor precisely when the node is stressed, so a Redis blip must not silently hand
        promotion back its old, looser threshold. Absence, by contrast, IS the publisher saying
        it has nothing to say.
        """
        stamp = now if now is not None else time.monotonic()
        try:
            raw = await asyncio.wait_for(self._redis.get(self._key), timeout=self._timeout)
        except Exception as exc:  # noqa: BLE001 - the read path must not fail on a Redis fault
            logger.debug("published promote-floor read failed for %s: %s", self._key, exc)
            if self._last_good is not None and stamp - self._last_good_at < self._ttl:
                return self._last_good
            return None

        floor = parse_floor_ratio(raw)
        # A key that read cleanly is authoritative even when it says nothing usable, so it
        # retires the memo rather than letting it outlive the signal it came from.
        self._last_good = floor
        self._last_good_at = stamp
        return floor


def create_published_floor_source(queues_client: Any, node_name: str) -> Optional[PublishedPromoteFloor]:
    """The floor source for this node, or `None` when there is nothing to read it with.

    Without `NODE_NAME` there is no way to know WHICH node's floor applies, and gating this
    node's promotion on a peer's disk is worse than gating it on the static default — the same
    rule the residency recorder and the landed-part publisher already follow.
    """
    if queues_client is None or not node_name:
        return None
    return PublishedPromoteFloor(queues_client, node_name)


__all__ = [
    "PROMOTE_FLOOR_KEY_PREFIX",
    "PROMOTE_FLOOR_TTL_SECONDS",
    "PublishedPromoteFloor",
    "create_published_floor_source",
    "parse_floor_ratio",
    "promote_floor_key",
]
