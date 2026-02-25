import asyncio
import contextlib
import json
import logging
import time
import uuid
from pathlib import Path
from typing import Any
from typing import Callable
from typing import Dict
from typing import Generic
from typing import List
from typing import Optional
from typing import TypeVar

import redis.asyncio as async_redis
from pydantic import BaseModel


logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class BaseDLQManager(Generic[T]):
    """Base class for Dead-Letter Queue management operations."""

    def __init__(
        self,
        redis_client: async_redis.Redis,
        dlq_key: str,
        enqueue_func: Callable[[T], Any],
        request_class: type[T],
    ):
        self.redis_client = redis_client
        self.dlq_key = dlq_key
        self.enqueue_func = enqueue_func
        self.request_class = request_class
        self.lock_prefix = f"dlq:requeue:lock:{dlq_key}:"

    def _lock_key(self, identifier: str) -> str:
        return f"{self.lock_prefix}{identifier}"

    async def _acquire_lock(self, identifier: str, ttl_ms: int = 60000) -> Optional[str]:
        """Acquire a per-identifier lock using Redis SET NX PX. Returns token or None."""
        token = uuid.uuid4().hex
        ok = await self.redis_client.set(self._lock_key(identifier), token, nx=True, px=ttl_ms)
        return token if ok else None

    async def _release_lock(self, identifier: str, token: str) -> None:
        """Release lock only if token matches (atomic compare-and-delete)."""
        script = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end"
        with contextlib.suppress(Exception):
            await self.redis_client.eval(script, 1, self._lock_key(identifier), token)

    async def push(self, payload: T, last_error: str, error_type: str) -> None:
        """Push a failed request to the Dead-Letter Queue."""
        etype = error_type
        if isinstance(etype, bool):
            etype = "transient" if etype else "permanent"

        dlq_entry = self._create_entry(payload, last_error, etype)

        await self.redis_client.lpush(self.dlq_key, json.dumps(dlq_entry))
        identifier = self._get_identifier(payload)
        logger.warning(
            f"Pushed to DLQ ({self.dlq_key}): identifier={identifier}, error_type={etype}, error={last_error}"
        )

    def _create_entry(self, payload: T, last_error: str, error_type: str) -> Dict[str, Any]:
        """Create DLQ entry. Override for worker-specific fields."""
        return {
            "payload": payload.model_dump(),
            "attempts": getattr(payload, "attempts", 0) or 0,
            "first_enqueued_at": getattr(payload, "first_enqueued_at", time.time()),
            "last_attempt_at": time.time(),
            "last_error": last_error,
            "error_type": error_type,
        }

    def _get_identifier(self, payload: T) -> str:
        """Get unique identifier from payload. Must be overridden."""
        raise NotImplementedError("Subclass must implement _get_identifier()")

    async def peek(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Peek at DLQ entries without removing them."""
        raw = await self.redis_client.lrange(self.dlq_key, -limit, -1)
        out: List[Dict[str, Any]] = []
        for entry_json in raw:
            try:
                out.append(json.loads(entry_json))
            except json.JSONDecodeError:
                logger.warning("Invalid JSON in DLQ: %r", entry_json)
        return out

    async def stats(self) -> Dict[str, Any]:
        """Get DLQ statistics."""
        count = await self.redis_client.llen(self.dlq_key)
        error_types = {"transient": 0, "permanent": 0, "unknown": 0}

        if count > 0:
            all_entries = await self.redis_client.lrange(self.dlq_key, 0, -1)
            for entry_json in all_entries:
                try:
                    entry = json.loads(entry_json)
                    error_type = entry.get("error_type", "unknown")
                    error_types[error_type] = error_types.get(error_type, 0) + 1
                except json.JSONDecodeError:
                    error_types["unknown"] += 1

        return {"total_entries": count, "error_types": error_types}

    async def _find_and_remove_entry(self, identifier: str) -> Optional[Dict[str, Any]]:
        """Find and remove a specific entry by identifier. Must be overridden."""
        raise NotImplementedError("Subclass must implement _find_and_remove_entry()")

    async def requeue(self, identifier: str, force: bool = False) -> bool:
        """Requeue a specific entry by identifier."""
        token = await self._acquire_lock(identifier)
        if not token:
            logger.error(f"Requeue already in progress for identifier: {identifier}")
            return False

        try:
            entry = await self._find_and_remove_entry(identifier)
            if not entry:
                logger.error(f"No DLQ entry found for identifier: {identifier}")
                return False

            if entry.get("error_type") == "permanent" and not force:
                logger.error(
                    f"Refusing to requeue permanent error for identifier: {identifier}. Use --force to override."
                )
                await self.redis_client.lpush(self.dlq_key, json.dumps(entry))
                return False

            payload_data = entry["payload"]
            payload = self.request_class.model_validate(payload_data)

            if not force and hasattr(payload, "attempts"):
                payload.attempts = 0  # ty: ignore[invalid-assignment]

            await self.enqueue_func(payload)
            logger.info(f"Successfully requeued identifier: {identifier}")
            return True

        except Exception as e:
            logger.error(f"Failed to requeue identifier: {identifier}, error: {e}")
            with contextlib.suppress(Exception):
                if "entry" in locals() and entry:
                    await self.redis_client.lpush(self.dlq_key, json.dumps(entry))
            return False
        finally:
            with contextlib.suppress(Exception):
                await self._release_lock(identifier, token)

    async def requeue_all(self, force: bool = False, batch_size: int = 500) -> int:
        """Requeue all DLQ entries using batched pipeline operations."""
        total_requeued = 0
        total_skipped = 0

        while True:
            # Pop a batch from the DLQ using rpop (FIFO order)
            pipe = self.redis_client.pipeline()
            for _ in range(batch_size):
                pipe.rpop(self.dlq_key)
            results = await pipe.execute()

            # Filter out None results (queue exhausted)
            raw_entries = [r for r in results if r is not None]
            if not raw_entries:
                break

            # Parse entries and build payloads
            payloads: list[T] = []
            for entry_json in raw_entries:
                entry = json.loads(entry_json)
                if entry.get("error_type") == "permanent" and not force:
                    # Put permanent errors back
                    await self.redis_client.lpush(self.dlq_key, entry_json)
                    total_skipped += 1
                    continue
                payload_data = entry["payload"]
                payload = self.request_class.model_validate(payload_data)
                if not force and hasattr(payload, "attempts"):
                    payload.attempts = 0  # ty: ignore[invalid-assignment]
                payloads.append(payload)

            # Bulk enqueue via pipeline
            if payloads:
                await self._bulk_enqueue(payloads)
                total_requeued += len(payloads)

            logger.info(f"Requeued batch: {len(payloads)} entries (total: {total_requeued}, skipped: {total_skipped})")

            # If we got fewer than batch_size, the queue is drained
            if len(raw_entries) < batch_size:
                break

        logger.info(f"Requeue complete: {total_requeued} requeued, {total_skipped} skipped")
        return total_requeued

    async def _bulk_enqueue(self, payloads: list[T]) -> None:
        """Bulk enqueue payloads. Override for pipeline support, falls back to sequential."""
        for payload in payloads:
            await self.enqueue_func(payload)

    def _extract_identifier_from_entry(self, entry: Dict[str, Any]) -> Optional[str]:
        """Extract identifier from DLQ entry. Must be overridden."""
        raise NotImplementedError("Subclass must implement _extract_identifier_from_entry()")

    async def purge(self, identifier: Optional[str] = None) -> int:
        """Purge entries from DLQ. If identifier is specified, only that entry."""
        if identifier:
            entry = await self._find_and_remove_entry(identifier)
            return 1 if entry else 0
        count = int(await self.redis_client.llen(self.dlq_key))
        await self.redis_client.delete(self.dlq_key)
        return count

    async def export(self, file_path: str) -> bool:
        """Export DLQ contents to a JSON file."""
        try:
            all_entries = await self.redis_client.lrange(self.dlq_key, 0, -1)
            entries = []
            for entry_json in all_entries:
                try:
                    entries.append(json.loads(entry_json))
                except json.JSONDecodeError:
                    logger.warning(f"Skipping invalid JSON: {entry_json}")

            def _write_json() -> None:
                with Path(file_path).open("w") as f:
                    json.dump(entries, f, indent=2, default=str)

            await asyncio.to_thread(_write_json)

            logger.info(f"Exported {len(entries)} entries to {file_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to export DLQ: {e}")
            return False

    async def import_dlq(self, file_path: str) -> bool:
        """Import DLQ contents from a JSON file."""
        try:

            def _read_json() -> Any:
                with Path(file_path).open("r") as f:
                    return json.load(f)

            entries = await asyncio.to_thread(_read_json)

            for entry in entries:
                await self.redis_client.lpush(self.dlq_key, json.dumps(entry))

            logger.info(f"Imported {len(entries)} entries from {file_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to import DLQ: {e}")
            return False
