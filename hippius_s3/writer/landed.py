"""Announce a freshly-written part to the node's drain agent.

The drain agent's reconciler used to be the sole way a new part was discovered: it walked the
whole SSD cache every 15s and asked the database which parts it had never seen. That was cheap
while the SSD held only the undrained backlog, because a drained part was unlinked. Retention
broke it — the disk now holds the node's entire replicated shard, measured on prod 2026-08-07 at
2.28M parts / ~930 GB, so the walk became millions of stat calls and thousands of batched
queries every 15s per node to find a handful of new parts.

Announcing turns discovery into O(new parts). The agent consumes this queue
(`crates/hippius-drain-agent/src/landed.rs`) and writes the `cephor_replication_status` row
itself — the api never touches that table, which belongs to the drain and to the drain's
migrations.

**Best-effort by construction.** A dropped announcement costs latency and nothing else: the
reconciler still finds the part on disk, exactly as it does today. That is what makes it safe to
swallow every failure here rather than fail a PUT that has already durably written its data.

Wiring follows the repo's module-singleton pattern (`get_access_tracker`):
`initialize_landed_publisher` in the api lifespan; processes that never initialize it — workers,
scripts, tests — get `None` and publishing is a no-op.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any
from typing import Optional


logger = logging.getLogger(__name__)


# Must match `landed_queue_key` in crates/hippius-drain-agent/src/landed.rs.
_LANDED_QUEUE_PREFIX = "cephor:landed:"

# Bounds the one await this puts on the client PUT path. Generous next to a healthy LPUSH
# (sub-millisecond) and short next to a client timeout: the announcement is an optimisation
# over the reconcile walk, so waiting on it is never worth a slow PUT.
_PUBLISH_TIMEOUT_SECONDS = 1.0

# Cap on a node's queue. The agent normally drains this within a poll, so depth is near zero;
# the bound only matters when the agent is down. Past it the OLDEST announcements are dropped,
# which is safe precisely because the reconciler backstop still finds those parts on disk — and
# is preferable to letting an unbounded list grow on a 1 GB Redis while the agent is away.
_DEFAULT_MAX_QUEUE_DEPTH = 200_000


def landed_queue_key(node_name: str) -> str:
    return f"{_LANDED_QUEUE_PREFIX}{node_name}"


class LandedPartPublisher:
    """Publishes "this node just finished writing a part" to its drain agent."""

    def __init__(self, queues_client: Any, node_name: str, max_depth: int = _DEFAULT_MAX_QUEUE_DEPTH) -> None:
        self._redis = queues_client
        self._key = landed_queue_key(node_name)
        self._max_depth = int(max_depth)

    async def publish(self, object_id: str, object_version: int, part_number: int) -> None:
        """Announce one completed part. Never raises.

        Field names are the wire contract with the agent's `LandedMessage`; the agent discards
        anything it cannot parse, so renaming one side alone silently disables the fast path
        rather than erroring. `drain_landed_dropped_total` is the alert for that.
        """
        payload = json.dumps(
            {
                "object_id": str(object_id),
                "version": int(object_version),
                "part_number": int(part_number),
            }
        )
        try:
            # LPUSH + LTRIM in one round-trip: the trim bounds the queue for the case where the
            # agent is down, and paying two round-trips per part on the write path to bound a
            # queue that is normally empty would be a poor trade.
            pipe = self._redis.pipeline()
            pipe.lpush(self._key, payload)
            pipe.ltrim(self._key, 0, self._max_depth - 1)
            # Bounded, because this await is on the CLIENT PUT PATH and the api's queues client is
            # built with no socket_timeout — so `except` below catches a redis-queues that ERRORS
            # and does nothing at all for one that merely goes SLOW. That is not hypothetical on
            # this queue: a 1.29M-entry list once made redis-queues slow enough to surface as
            # prod GET IncompleteRead. `promote_floor` bounds its own queues read for exactly this
            # reason; the write path is the one that needed it more. On timeout the part is
            # already durable and the agent's reconcile walk still finds it.
            await asyncio.wait_for(pipe.execute(), timeout=_PUBLISH_TIMEOUT_SECONDS)
        except Exception as exc:  # noqa: BLE001 - the part is already durable; the backstop covers this
            logger.debug(
                "announcing landed part %s v%s part %s failed: %s", object_id, object_version, part_number, exc
            )


_publisher: Optional[LandedPartPublisher] = None


def initialize_landed_publisher(queues_client: Any, node_name: str) -> Optional[LandedPartPublisher]:
    """Installs the process-wide publisher, or `None` without a node identity.

    No `NODE_NAME` means no way to say WHICH node's agent should drain the part, and an
    announcement on the wrong queue would be recorded against a node that does not hold the
    data — where the node-scoped `claim_part` would then never drain it. Publishing nothing is
    strictly better: the reconciler on the node that DOES hold it still finds it.
    """
    global _publisher
    _publisher = LandedPartPublisher(queues_client, node_name) if queues_client is not None and node_name else None
    return _publisher


def get_landed_publisher() -> Optional[LandedPartPublisher]:
    return _publisher


__all__ = [
    "LandedPartPublisher",
    "get_landed_publisher",
    "initialize_landed_publisher",
    "landed_queue_key",
]
