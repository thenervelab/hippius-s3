"""A tiny bounded, TTL'd per-part memo.

Three things on the read path are per-PART facts that were being recomputed per CHUNK:
which peer holds a part, that part's `meta.json` on the local tier, and its residency row.
A 64-chunk part turned each into 64 Postgres round-trips, 64 Redis GETs, or 64 fsync pairs
— on a tier whose whole justification is shaving ~34 ms per chunk.

The TTL is what makes this safe to cache at all. A plain "already did it" set is wrong for
anything the evictor can undo: a part promoted, evicted, then read again must be re-recorded,
and a permanent memo would skip it forever, leaving a copy on disk that no evictor can see.
Bounding entries keeps a long-lived api pod from growing this without limit.
"""

from __future__ import annotations

import time
from typing import Generic
from typing import Hashable
from typing import Optional
from typing import TypeVar


K = TypeVar("K", bound=Hashable)
V = TypeVar("V")


class PartMemo(Generic[K, V]):
    """Remembers a per-part value for `ttl_seconds`, evicting oldest-first past `max_entries`."""

    def __init__(self, ttl_seconds: float, max_entries: int) -> None:
        self._ttl = ttl_seconds
        self._max = max_entries
        self._entries: dict[K, tuple[float, V]] = {}

    def get(self, key: K, now: Optional[float] = None) -> Optional[V]:
        """The remembered value, or None if absent or expired."""
        entry = self._entries.get(key)
        if entry is None:
            return None
        expires_at, value = entry
        if (now if now is not None else time.monotonic()) >= expires_at:
            # Drop on read rather than sweeping: expiry only matters when a key is looked up,
            # and this keeps the class free of any background task.
            del self._entries[key]
            return None
        return value

    def put(self, key: K, value: V, now: Optional[float] = None) -> None:
        stamp = now if now is not None else time.monotonic()
        if len(self._entries) >= self._max and key not in self._entries:
            # dicts preserve insertion order, so the first key is the oldest inserted. An
            # approximate LRU is enough: over-evicting costs a repeated lookup, never
            # correctness, because every caller can recompute what it lost.
            self._entries.pop(next(iter(self._entries)), None)
        self._entries[key] = (stamp + self._ttl, value)
