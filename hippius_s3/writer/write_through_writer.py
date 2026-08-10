"""Parts writer: persists chunks and meta to the FS cache."""

from __future__ import annotations

from typing import Any

from hippius_s3.cache.read_recency import get_read_recency_recorder
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
        # Stamp the part's residency recency BEFORE announcing. A rewrite of an
        # already-replicated part touches nothing the drain evictor sorts on, so the only copy
        # of the new bytes would rank as the LRU's COLDEST candidate for the whole window until
        # the agent pops the announcement and runs its divergence check — an eviction in that
        # window destroys the rewrite and leaves the pool serving the superseded bytes forever.
        # Stamping first means that by the time the agent can even see the announcement, the
        # part is the hottest thing on the disk. A first-time part has no residency row and the
        # stamp is a no-op; best-effort, like the announcement below.
        recorder = get_read_recency_recorder()
        if recorder is not None:
            await recorder(object_id, int(object_version), int(part_number))
        # Announce to this node's drain agent, strictly AFTER meta lands. Meta is the readiness
        # gate: a part is only complete once it exists, so announcing earlier could have the
        # drain claim a part whose chunks are still being written. The hook lives on the writer
        # rather than at the call sites because a call site that forgets it is a part that falls
        # back to the disk walk with nothing saying so — but this is one of TWO choke points now,
        # not the only one: the simple-PUT path lands meta here, while the staging paths (MPU
        # part, append) land it inside `publish_part` below, which carries its own announcement.
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
        # Same stamp-then-announce order as `write_meta`, and for the same reason: between a
        # rewrite landing on SSD and the agent's divergence check, the rewritten part is the ONLY
        # copy of the client's new bytes, yet nothing on this path touches the columns the
        # evictor sorts on — so it ranks as the LRU's coldest candidate exactly when it is least
        # replaceable. This path carries it too because a re-uploaded MPU part is precisely the
        # case B-2 is about, so omitting it here would leave the shape it targets uncovered while
        # the simple-PUT path it cannot occur on is protected.
        recorder = get_read_recency_recorder()
        if recorder is not None:
            await recorder(object_id, int(object_version), int(part_number))
        publisher = get_landed_publisher()
        if publisher is not None:
            await publisher.publish(object_id, int(object_version), int(part_number))
