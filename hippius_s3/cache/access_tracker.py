"""Sampled read-recency tracking into fs_cache_inventory.last_access_at.

Replaces filesystem atime as the janitor's "recently read" hot-retention
signal. The old channel — os.utime() on every chunk read — was dead on
read-only mounts (prod api-local swallows EROFS, so read traffic protected
nothing) and turned every read into a CephFS MDS metadata write elsewhere.

Hot-path cost is one dict probe per chunk read (`note_read` is sync and
never awaits); DB writes are sampled (a part is re-recorded at most once per
quarter hot-window) and flushed as one batched UPDATE every 30s. Losing a
flush costs at most premature evictability of a part that is then re-hydrated
on next read — advisory, like the rest of the inventory.

Wiring follows the repo's module-singleton pattern (initialize_cache_client):
`initialize_access_tracker` in the api lifespan; worker processes that never
initialize it get a None tracker and note_read is a no-op.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any
from typing import Optional


logger = logging.getLogger(__name__)


def _parse_update_count(status: Any) -> int:
    """Affected-row count from an asyncpg command tag ("UPDATE N"); 0 if unparseable."""
    if not isinstance(status, str):
        return 0
    parts = status.split()
    if len(parts) >= 2 and parts[-1].isdigit():
        return int(parts[-1])
    return 0


FLUSH_INTERVAL_SECONDS = 30.0
# Bound on the sampling map so a pathological key scan can't grow it forever;
# pruning drops the oldest entries, which only means earlier re-sampling.
MAX_TRACKED_KEYS = 100_000
# One UPDATE's unnest arrays are capped so a storm-sized flush can't become a
# single giant statement.
FLUSH_CHUNK_SIZE = 10_000


class AccessTracker:
    """Buffers sampled (object_id, version, part) reads; flushes batched UPDATEs."""

    def __init__(
        self,
        db_pool: Any,
        *,
        hot_window_seconds: float,
        flush_interval: float = FLUSH_INTERVAL_SECONDS,
    ) -> None:
        self._pool = db_pool
        # A part needs at most one refresh per quarter hot-window to stay hot;
        # finer sampling is pure write amplification. Floored so a tiny window
        # can't turn every read into a DB write.
        self._sample_window = max(60.0, hot_window_seconds / 4)
        self._flush_interval = flush_interval
        self._last_noted: dict[tuple[str, int, int], float] = {}
        self._pending: set[tuple[str, int, int]] = set()

    def note_read(self, object_id: str, object_version: int, part_number: int) -> None:
        """Record a chunk read. Sync and allocation-light — called per chunk on the stream path."""
        key = (str(object_id), int(object_version), int(part_number))
        now = time.monotonic()
        last = self._last_noted.get(key)
        if last is not None and now - last < self._sample_window:
            return
        # _pending drains only on flush; a failing flush (DB outage) plus a
        # key-diverse read stream would otherwise grow it without bound. Cap it
        # at the sampling-map bound and drop-newest — leaving _last_noted
        # untouched so the very next read re-notes the dropped part (no lost
        # recency), same cheap outcome as a lost flush.
        if key not in self._pending and len(self._pending) >= MAX_TRACKED_KEYS:
            return
        self._last_noted[key] = now
        self._pending.add(key)
        if len(self._last_noted) > MAX_TRACKED_KEYS:
            self._enforce_bound(now)

    async def flush_once(self) -> int:
        """Write pending keys in chunked batched UPDATEs. Returns rows attempted."""
        if not self._pending:
            return 0
        # Sort by key tuple so every pod locks the same fs_cache_inventory rows
        # in one canonical order; set-iteration (hash) order let two pods with
        # overlapping keys acquire locks in opposite orders and deadlock.
        batch = sorted(self._pending)
        self._pending.clear()
        for start in range(0, len(batch), FLUSH_CHUNK_SIZE):
            chunk = batch[start : start + FLUSH_CHUNK_SIZE]
            try:
                status = await self._pool.execute(
                    """
                    UPDATE fs_cache_inventory AS f SET last_access_at = now()
                    FROM unnest($1::text[], $2::bigint[], $3::bigint[]) AS u(oid, ver, pnum)
                    WHERE f.object_id = u.oid AND f.object_version = u.ver AND f.part_number = u.pnum
                    """,
                    [k[0] for k in chunk],
                    [k[1] for k in chunk],
                    [k[2] for k in chunk],
                )
            except Exception as exc:
                # Advisory data, but a transient failure must not leave these
                # keys sampling-suppressed with their recency silently lost.
                # Re-queue them and drop their suppression so the next flush
                # retries and any continued read re-notes them; _pending stays
                # bounded (see note_read), so a sustained outage can't grow it
                # without limit.
                for k in chunk:
                    self._last_noted.pop(k, None)
                self._pending.update(chunk)
                logger.warning("last_access_at flush failed (%s keys requeued): %s", len(chunk), exc)
                continue
            affected = _parse_update_count(status)
            if affected < len(chunk):
                # A zero/partial UPDATE means inventory rows are missing for keys
                # we thought were hot — used to be silent. Surface it: warn when
                # the whole chunk missed, debug for the expected trickle of parts
                # evicted from inventory between note and flush.
                level = logging.WARNING if affected == 0 else logging.DEBUG
                logger.log(level, "last_access_at flush updated %s/%s rows", affected, len(chunk))
        return len(batch)

    def _enforce_bound(self, now: float) -> None:
        """Shrink the sampling map: window-prune first, then hard-evict oldest.

        The window prune alone cannot shrink a map whose keys were ALL read
        within the sample window (key-diverse storm) — that case drops the
        oldest quarter; the only cost is earlier re-sampling of those parts.
        """
        cutoff = now - self._sample_window
        self._last_noted = {k: ts for k, ts in self._last_noted.items() if ts >= cutoff}
        if len(self._last_noted) <= MAX_TRACKED_KEYS:
            return
        items = sorted(self._last_noted.items(), key=lambda kv: kv[1])
        self._last_noted = dict(items[len(items) // 4 :])

    async def run(self) -> None:
        while True:
            await asyncio.sleep(self._flush_interval)
            try:
                await self.flush_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("access-tracker flush loop error: %s", exc)


_tracker: Optional[AccessTracker] = None


def initialize_access_tracker(db_pool: Any, *, hot_window_seconds: float) -> AccessTracker:
    global _tracker
    _tracker = AccessTracker(db_pool, hot_window_seconds=hot_window_seconds)
    return _tracker


def get_access_tracker() -> Optional[AccessTracker]:
    return _tracker


__all__ = [
    "AccessTracker",
    "get_access_tracker",
    "initialize_access_tracker",
]
