import contextlib
import json
import logging
import time
import uuid
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import redis.asyncio as async_redis


logger = logging.getLogger(__name__)


class PinnerDLQEntry:
    """Represents a CID entry in the pin checker DLQ."""

    def __init__(
        self,
        cid: str,
        user: str,
        object_id: str,
        object_version: int,
        reason: str,
        pin_attempts: int,
        last_pinned_at: Optional[float] = None,
        dlq_timestamp: Optional[float] = None,
    ):
        self.cid = cid
        self.user = user
        self.object_id = object_id
        self.object_version = object_version
        self.reason = reason
        self.pin_attempts = pin_attempts
        self.last_pinned_at = last_pinned_at
        self.dlq_timestamp = dlq_timestamp or time.time()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "cid": self.cid,
            "user": self.user,
            "object_id": self.object_id,
            "object_version": self.object_version,
            "reason": self.reason,
            "pin_attempts": self.pin_attempts,
            "last_pinned_at": self.last_pinned_at,
            "dlq_timestamp": self.dlq_timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PinnerDLQEntry":
        return cls(
            cid=data["cid"],
            user=data["user"],
            object_id=data["object_id"],
            object_version=data["object_version"],
            reason=data.get("reason", "unknown"),
            pin_attempts=data.get("pin_attempts", 0),
            last_pinned_at=data.get("last_pinned_at"),
            dlq_timestamp=data.get("dlq_timestamp"),
        )


class PinnerDLQManager:
    """Manages Dead-Letter Queue for pin checker CIDs."""

    DLQ_KEY = "pinner:dlq"

    def __init__(self, redis_client: async_redis.Redis):
        self.redis_client = redis_client
        self.lock_prefix = "pinner:dlq:lock:"

    def _lock_key(self, cid: str) -> str:
        return f"{self.lock_prefix}{cid}"

    async def _acquire_lock(self, cid: str, ttl_ms: int = 60000) -> Optional[str]:
        """Acquire a per-CID lock using Redis SET NX PX. Returns token or None."""
        token = uuid.uuid4().hex
        ok = await self.redis_client.set(self._lock_key(cid), token, nx=True, px=ttl_ms)  # type: ignore[misc]
        return token if ok else None

    async def _release_lock(self, cid: str, token: str) -> None:
        """Release lock only if token matches (atomic compare-and-delete)."""
        script = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end"
        with contextlib.suppress(Exception):
            await self.redis_client.eval(script, 1, self._lock_key(cid), token)  # type: ignore[misc]

    async def push(self, entry: PinnerDLQEntry) -> None:
        """Push a CID entry to the DLQ."""
        await self.redis_client.lpush(self.DLQ_KEY, json.dumps(entry.to_dict()))  # type: ignore[misc]
        logger.warning(f"Pushed to pinner DLQ: cid={entry.cid} user={entry.user} attempts={entry.pin_attempts}")

    async def peek(self, limit: int = 10) -> List[PinnerDLQEntry]:
        """Peek at DLQ entries without removing them."""
        raw_data = await self.redis_client.lrange(self.DLQ_KEY, -limit, -1)  # type: ignore[misc]
        raw: List[bytes] = list(raw_data) if raw_data else []
        entries = []
        for entry_json in raw:
            try:
                data = json.loads(entry_json)
                entries.append(PinnerDLQEntry.from_dict(data))
            except Exception as e:
                logger.warning(f"Invalid DLQ entry: {e}")
        return entries

    async def stats(self) -> Dict[str, Any]:
        """Get DLQ statistics."""
        count_result = await self.redis_client.llen(self.DLQ_KEY)  # type: ignore[misc]
        count: int = int(count_result) if count_result is not None else 0
        return {
            "total_entries": count,
            "queue_key": self.DLQ_KEY,
        }

    async def _find_and_remove_unlocked(self, cid: str) -> Optional[PinnerDLQEntry]:
        """Find and remove a specific CID entry without locking."""
        all_entries_data = await self.redis_client.lrange(self.DLQ_KEY, 0, -1)  # type: ignore[misc]
        all_entries: List[bytes] = list(all_entries_data) if all_entries_data else []
        for entry_json in all_entries:
            try:
                data = json.loads(entry_json)
                if data.get("cid") == cid:
                    entry_str = entry_json.decode("utf-8") if isinstance(entry_json, bytes) else entry_json
                    removed = await self.redis_client.lrem(self.DLQ_KEY, 1, entry_str)  # type: ignore[misc]
                    if removed:
                        return PinnerDLQEntry.from_dict(data)
            except Exception:
                continue
        return None

    async def find_and_remove(self, cid: str) -> Optional[PinnerDLQEntry]:
        """Find and remove a specific CID entry with distributed locking."""
        token = await self._acquire_lock(cid)
        if not token:
            logger.error(f"Requeue already in progress for CID: {cid}")
            return None

        try:
            return await self._find_and_remove_unlocked(cid)
        finally:
            with contextlib.suppress(Exception):
                await self._release_lock(cid, token)

    async def purge(self, cid: Optional[str] = None) -> int:
        """Purge entries from DLQ. If cid is specified, only that entry."""
        if cid:
            entry = await self.find_and_remove(cid)
            return 1 if entry else 0
        count_result = await self.redis_client.llen(self.DLQ_KEY)  # type: ignore[misc]
        count: int = int(count_result) if count_result is not None else 0
        await self.redis_client.delete(self.DLQ_KEY)  # type: ignore[misc]
        return count

    async def export_all(self) -> List[PinnerDLQEntry]:
        """Export all DLQ entries."""
        raw_data = await self.redis_client.lrange(self.DLQ_KEY, 0, -1)  # type: ignore[misc]
        raw: List[bytes] = list(raw_data) if raw_data else []
        entries = []
        for entry_json in raw:
            try:
                data = json.loads(entry_json)
                entries.append(PinnerDLQEntry.from_dict(data))
            except Exception as e:
                logger.warning(f"Invalid DLQ entry during export: {e}")
        return entries
