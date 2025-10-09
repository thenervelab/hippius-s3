"""
DLQ Logic Module

Handles rehydration and requeue operations for DLQ recovery.
"""

import logging
from typing import Any
from typing import Dict
from typing import Optional

from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.dlq.storage import DLQStorage
from hippius_s3.queue import UploadChainRequest
from hippius_s3.queue import enqueue_upload_request


logger = logging.getLogger(__name__)


class DLQLogic:
    """Business logic for DLQ rehydration and requeue operations."""

    def __init__(self, storage: Optional[DLQStorage] = None, redis_client: Any = None):
        self.storage = storage or DLQStorage()
        self.redis_client = redis_client

    def _get_redis_cache(self) -> type:
        """Lazy import to avoid circular dependencies."""
        return RedisObjectPartsCache

    async def hydrate_cache_from_dlq(self, object_id: str) -> bool:
        """Hydrate Redis cache with exactly the meta + per-chunk pieces from DLQ.

        Never infer chunk boundaries or slice concatenated files. Only restore from
        per-chunk piece files that were persisted directly from Redis.
        """
        if not self.storage.has_object(object_id):
            logger.warning(f"No DLQ data found for object {object_id}")
            return False

        available_parts = self.storage.list_chunks(object_id)
        if not available_parts:
            logger.warning(f"No DLQ parts found for object {object_id}")
            return False

        cache = self._get_redis_cache()(self.redis_client)
        ok = True

        for part_number in available_parts:
            meta = self.storage.load_meta(object_id, part_number)
            indices = self.storage.list_chunk_piece_indices(object_id, part_number)

            if not meta or not indices:
                logger.error(
                    f"Cannot hydrate object {object_id} part {part_number}: "
                    f"{'missing meta' if not meta else 'no chunk pieces'}"
                )
                ok = False
                continue

            # Load all pieces
            pieces: list[tuple[int, bytes]] = []
            total_bytes = 0
            for ci in indices:
                data = self.storage.load_chunk_piece(object_id, part_number, ci)
                if not isinstance(data, (bytes, bytearray)):
                    logger.error(f"Missing piece {ci} for object {object_id} part {part_number}")
                    ok = False
                    pieces = []
                    break
                b = bytes(data)
                pieces.append((ci, b))
                total_bytes += len(b)

            if not pieces:
                continue

            # Write meta first (use observed counts/size for correctness)
            try:
                chunk_size = int(meta.get("chunk_size", 4 * 1024 * 1024))
            except Exception:
                chunk_size = 4 * 1024 * 1024

            await cache.set_meta(
                object_id,
                part_number,
                chunk_size=chunk_size,
                num_chunks=len(pieces),
                size_bytes=total_bytes,
            )

            # Then write each chunk piece
            for ci, b in pieces:
                await cache.set_chunk(object_id, part_number, ci, b)

            logger.debug(
                f"Hydrated object {object_id} part {part_number} with {len(pieces)} pieces (bytes={total_bytes})"
            )

        return ok

    async def requeue_with_hydration(
        self, payload: UploadChainRequest, force: bool = False, redis_client: Any = None
    ) -> bool:
        """Hydrate cache from DLQ and requeue the payload."""
        redis_client = redis_client or self.redis_client
        if not redis_client:
            raise ValueError("redis_client required")

        object_id = payload.object_id

        # Check if we have DLQ data for this object
        if not self.storage.has_object(object_id):
            logger.warning(f"No DLQ data available for object {object_id}, proceeding without hydration")
            # Still requeue but don't archive since no DLQ data
            await enqueue_upload_request(payload, redis_client)
            return True

        # Hydrate cache from DLQ
        logger.info(f"Hydrating cache from DLQ for object {object_id}")
        hydration_success = await self.hydrate_cache_from_dlq(object_id)

        if not hydration_success:
            if force:
                logger.warning(f"Hydration failed but forcing requeue for object {object_id}")
            else:
                logger.error(f"Hydration failed for object {object_id}, refusing to requeue (use --force to override)")
                return False

        # Requeue the payload
        logger.info(f"Requeueing object {object_id}")
        await enqueue_upload_request(payload, redis_client)

        # Archive the DLQ data (move to archive directory)
        try:
            self.storage.archive_object(object_id)
            logger.info(f"Archived DLQ data for successfully requeued object {object_id}")
        except Exception as e:
            logger.error(f"Failed to archive DLQ data for object {object_id}: {e}")
            # Don't fail the requeue for archive failure

        return True

    async def validate_dlq_data_integrity(self, object_id: str) -> Dict[str, Any]:
        """Validate that DLQ data is complete and matches database expectations."""
        # This would need database access to compare with parts table
        # For now, just check filesystem integrity
        result = {
            "object_id": object_id,
            "has_dlq_data": self.storage.has_object(object_id),
            "available_parts": [],
            "total_size": 0,
            "valid": False,
        }

        if not result["has_dlq_data"]:
            return result

        available_parts = self.storage.list_chunks(object_id)
        result["available_parts"] = available_parts

        # Load each part and accumulate size
        for part_num in available_parts:
            chunk_data = self.storage.load_chunk(object_id, part_num)
            if chunk_data is not None:
                current_size = result["total_size"]
                if isinstance(current_size, int):
                    result["total_size"] = current_size + len(chunk_data)

        # Basic validation: at least one part exists
        result["valid"] = len(available_parts) > 0

        return result
