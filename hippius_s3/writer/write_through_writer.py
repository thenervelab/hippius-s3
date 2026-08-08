"""Parts writer: persists chunks and meta to the FS cache."""

from __future__ import annotations

from typing import Any

from hippius_s3.writer.landed import get_landed_publisher


class WriteThroughPartsWriter:
    """Writes object parts to the filesystem cache (mandatory, fatal on failure).

    Historically this also mirrored writes to a Redis download cache. Since the
    2026-04-21 FS-cache migration the Redis cache delegates straight back to the
    same FS store, so the mirror was a redundant second disk write. Meta is always
    written last to indicate completeness.
    """

    def __init__(self, fs_store: Any, redis_cache: Any, ttl_seconds: int) -> None:
        """Initialize the parts writer.

        Args:
            fs_store: FileSystemPartsStore instance
            redis_cache: retained for call-site compatibility; no longer written to
            ttl_seconds: TTL for cache keys
        """
        self.fs_store = fs_store
        self.redis_cache = redis_cache
        self.ttl_seconds = int(ttl_seconds)

    async def write_meta(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        *,
        chunk_size: int,
        num_chunks: int,
        plain_size: int,
    ) -> None:
        """Write metadata to the FS cache (fatal on failure).

        Args:
            object_id: Object UUID
            object_version: Object version number
            part_number: Part number
            chunk_size: Size of each chunk (bytes)
            num_chunks: Total number of chunks
            plain_size: Total plaintext size (bytes)

        Raises:
            Exception: If FS write fails (fatal to request)
        """
        await self.fs_store.set_meta(
            object_id,
            int(object_version),
            int(part_number),
            chunk_size=int(chunk_size),
            num_chunks=int(num_chunks),
            size_bytes=int(plain_size),
        )
        # Announce to this node's drain agent, strictly AFTER meta lands. Meta is the readiness
        # gate: a part is only complete once it exists, so announcing earlier could have the
        # drain claim a part whose chunks are still being written. It also has to be here rather
        # than at the call sites — this is the one choke point every upload path (simple PUT,
        # streamed PUT, MPU part, append) already funnels through, and a hook one of them
        # forgets is a part that falls back to the disk walk with nothing saying so.
        #
        # Best-effort: the bytes and meta are already durable, and the agent's reconciler still
        # discovers the part from disk if this never arrives.
        publisher = get_landed_publisher()
        if publisher is not None:
            await publisher.publish(object_id, int(object_version), int(part_number))

    async def publish_part(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        *,
        attempt_id: str,
        chunk_size: int,
        num_chunks: int,
        plain_size: int,
    ) -> None:
        """Promote one attempt's staged chunks to the part, then announce it.

        `write_meta`'s counterpart for the paths that stage (see `fs_store.stage_chunk`):
        publishing IS the meta write there, so the landed announcement has to hang off this
        method too or an MPU part would stop reaching the drain agent directly.

        Raises:
            Exception: If the publish fails (fatal to request)
        """
        await self.fs_store.publish_part(
            object_id,
            int(object_version),
            int(part_number),
            attempt_id=attempt_id,
            chunk_size=int(chunk_size),
            num_chunks=int(num_chunks),
            size_bytes=int(plain_size),
        )
        publisher = get_landed_publisher()
        if publisher is not None:
            await publisher.publish(object_id, int(object_version), int(part_number))

    async def write_chunks(self, object_id: str, object_version: int, part_number: int, chunks: list[bytes]) -> None:
        """Write chunks to the FS cache (fatal on failure).

        Args:
            object_id: Object UUID
            object_version: Object version number
            part_number: Part number
            chunks: List of ciphertext chunk bytes

        Raises:
            Exception: If any FS write fails (fatal to request)
        """
        for i, ct in enumerate(chunks):
            await self.fs_store.set_chunk(object_id, int(object_version), int(part_number), int(i), ct)
