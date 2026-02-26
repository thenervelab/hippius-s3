import json
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import redis.asyncio as async_redis

from hippius_s3.dlq.base import BaseDLQManager
from hippius_s3.queue import UploadChainRequest
from hippius_s3.queue import get_queue_client


class UploadDLQManager(BaseDLQManager[UploadChainRequest]):
    """DLQ manager for upload operations."""

    def __init__(self, redis_client: async_redis.Redis, backend_name: str = "upload"):
        self.backend_name = backend_name
        self._queue_name = f"{backend_name}_upload_requests"

        async def enqueue_to_backend(payload: UploadChainRequest) -> None:
            """Enqueue upload request to specific backend queue."""
            client = get_queue_client()
            raw = payload.model_dump_json()
            await client.lpush(self._queue_name, raw)

        super().__init__(
            redis_client=redis_client,
            dlq_key=f"{backend_name}_upload_requests:dlq",
            enqueue_func=enqueue_to_backend,
            request_class=UploadChainRequest,
        )

    async def _bulk_enqueue(self, payloads: List[UploadChainRequest]) -> None:
        """Bulk enqueue upload requests via Redis pipeline."""
        client = get_queue_client()
        pipe = client.pipeline()
        for payload in payloads:
            pipe.lpush(self._queue_name, payload.model_dump_json())
        await pipe.execute()

    def _get_identifier(self, payload: UploadChainRequest) -> str:
        return payload.object_id

    def _create_entry(self, payload: UploadChainRequest, last_error: str, error_type: str) -> Dict[str, Any]:
        entry = super()._create_entry(payload, last_error, error_type)
        entry["object_id"] = payload.object_id
        entry["upload_id"] = payload.upload_id
        entry["bucket_name"] = payload.bucket_name
        entry["object_key"] = payload.object_key
        return entry

    async def _find_and_remove_entry(self, identifier: str) -> Optional[Dict[str, Any]]:
        """Find and remove entry by object_id."""
        all_entries = await self.redis_client.lrange(self.dlq_key, 0, -1)
        target_entry: Optional[Dict[str, Any]] = None

        for entry_json in all_entries:
            try:
                entry = json.loads(entry_json)
            except json.JSONDecodeError:
                continue
            if entry.get("object_id") == identifier:
                removed = await self.redis_client.lrem(self.dlq_key, 1, entry_json)
                if removed:
                    target_entry = entry
                    break

        return target_entry

    def _extract_identifier_from_entry(self, entry: Dict[str, Any]) -> Optional[str]:
        return entry.get("object_id")
