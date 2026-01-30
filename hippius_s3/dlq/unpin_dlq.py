import json
from typing import Any
from typing import Dict
from typing import Optional

import redis.asyncio as async_redis

from hippius_s3.dlq.base import BaseDLQManager
from hippius_s3.queue import UnpinChainRequest
from hippius_s3.queue import enqueue_unpin_request


class UnpinDLQManager(BaseDLQManager[UnpinChainRequest]):
    """DLQ manager for unpin operations."""

    def __init__(self, redis_client: async_redis.Redis):
        super().__init__(
            redis_client=redis_client,
            dlq_key="unpin_requests:dlq",
            enqueue_func=enqueue_unpin_request,
            request_class=UnpinChainRequest,
        )

    def _get_identifier(self, payload: UnpinChainRequest) -> str:
        # cid is optional during transition; use object_id as a stable fallback.
        return payload.cid or payload.object_id

    def _create_entry(self, payload: UnpinChainRequest, last_error: str, error_type: str) -> Dict[str, Any]:
        entry = super()._create_entry(payload, last_error, error_type)
        entry["cid"] = payload.cid
        entry["object_id"] = payload.object_id
        entry["address"] = payload.address
        entry["object_version"] = payload.object_version
        return entry

    async def _find_and_remove_entry(self, identifier: str) -> Optional[Dict[str, Any]]:
        """Find and remove entry by CID or object_id."""
        all_entries = await self.redis_client.lrange(self.dlq_key, 0, -1)
        target_entry: Optional[Dict[str, Any]] = None

        for entry_json in all_entries:
            try:
                entry = json.loads(entry_json)
            except json.JSONDecodeError:
                continue

            if entry.get("cid") == identifier or entry.get("object_id") == identifier:
                removed = await self.redis_client.lrem(self.dlq_key, 1, entry_json)
                if removed:
                    target_entry = entry
                    break

        return target_entry

    def _extract_identifier_from_entry(self, entry: Dict[str, Any]) -> Optional[str]:
        return entry.get("cid") or entry.get("object_id")
