"""Fetch a ciphertext chunk straight from a storage backend into memory, for the read path.

The lowest tier of a GET. When a chunk is on neither this node's SSD, a peer's, nor the pool, the
streamer pulls it from the backend by the `chunk_backend.backend_identifier` the uploader recorded,
decrypts it in-process and yields it to the client. The bytes are NOT written anywhere on the way
through: a cold read of an object nobody else is reading warms nothing — the pool copy is gone
with the pool, and promoting Arion-served chunks onto the NVMe would put cache-fill write
amplification on every cold read, competing with ingest for the same disk. (Peer-served chunks
still promote, in `DualFileSystemPartsStore`; that decision is unchanged.)

This replaced the downloader round-trip: the api used to enqueue a download request, a worker
fetched the chunk into the pool, and the streamer waited on a pub/sub notification to re-read it.
Three hops and two copies for one 4 MiB read.

One `ArionClient` per process — `fetch` runs once per chunk, ~1280 times for a 5 GiB object, and
a client per call is a TCP+TLS handshake per chunk. A per-process semaphore bounds the backend
concurrency the pod exerts across every in-flight GET, the way the uploader bounds its POSTs.
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Iterable

from hippius_s3.config import get_config
from hippius_s3.workers.errors import classify_download_error


logger = logging.getLogger(__name__)

# (backend name, backend_identifier) — where one chunk can be fetched from.
BackendLocation = tuple[str, str]

# Downloads one identifier from one backend, returning the whole ciphertext chunk.
FetchOne = Callable[[str, str], Awaitable[bytes]]


class ChunkUnavailableError(Exception):
    """No tier can serve the chunk right now.

    Raised when the chunk is not on any local tier AND either no backend holds it yet (the part is
    still in its upload window and lives only on another node's SSD, or the object never finished)
    or every backend fetch failed. Retryable from the client's point of view: the first-chunk peek
    turns it into a 503 with Retry-After, a mid-stream one ends the stream.
    """


class BackendChunkFetcher:
    """Fetches chunks from backends in preference order, with bounded retries and concurrency."""

    def __init__(
        self,
        fetchers: dict[str, FetchOne],
        *,
        concurrency: int,
        attempts: int,
        base_sleep: float,
        jitter: float,
        queue_timeout: float | None = None,
    ) -> None:
        self._fetchers = fetchers
        self._semaphore = asyncio.Semaphore(max(1, int(concurrency)))
        self._attempts = max(1, int(attempts))
        self._base_sleep = float(base_sleep)
        self._jitter = float(jitter)
        self._queue_timeout = None if queue_timeout is None else float(queue_timeout)

    def can_serve(self, backend: str) -> bool:
        return backend in self._fetchers

    async def _acquire_slot(self) -> None:
        """Take a slot in the pod's budget, or give up: a saturated budget (a slow backend under
        many cold readers) must surface as a fast retryable failure on the reads that cannot be
        served, not as every queued read timing out in lockstep at the first-chunk bound."""
        if self._queue_timeout is None:
            await self._semaphore.acquire()
            return
        try:
            await asyncio.wait_for(self._semaphore.acquire(), timeout=self._queue_timeout)
        except TimeoutError as exc:
            raise ChunkUnavailableError(
                f"backend fetch budget saturated for {self._queue_timeout:.0f}s; the read cannot be served now"
            ) from exc

    async def fetch(self, locations: Iterable[BackendLocation], address: str) -> bytes:
        """Return the chunk's ciphertext from the first location that serves it.

        Locations are tried in the order given (the object's download-backend order). A transient
        failure is retried on the same location with exponential backoff (a 429/5xx is ridden
        out, not burnt through); a permanent one (a 404: the identifier is stale) moves on to the
        next. Exhausting every location is `ChunkUnavailableError`.
        """
        tried = 0
        for backend, identifier in locations:
            fetch_one = self._fetchers.get(backend)
            if fetch_one is None:
                continue
            tried += 1
            for attempt in range(1, self._attempts + 1):
                await self._acquire_slot()
                data: bytes | None = None
                retry = False
                try:
                    data = await fetch_one(identifier, address)
                except Exception as exc:  # noqa: BLE001 - every backend error is classified below
                    kind = classify_download_error(exc)
                    if kind == "transient" and attempt != self._attempts:
                        retry = True
                    else:
                        logger.warning(
                            "backend chunk fetch failed backend=%s id=%s attempt=%s/%s kind=%s: %s",
                            backend,
                            identifier,
                            attempt,
                            self._attempts,
                            kind,
                            exc,
                        )
                finally:
                    self._semaphore.release()
                if data is not None:
                    _record_backend_read()
                    return data
                if retry:
                    await asyncio.sleep(self._base_sleep * (2 ** (attempt - 1)) + random.uniform(0, self._jitter))
                    continue
                break
        raise ChunkUnavailableError(f"no backend served the chunk (locations tried: {tried})")


def _record_backend_read() -> None:
    """Count a chunk served by the backend tier, next to local/peer/pool. Never let observability
    fail a read."""
    try:
        from hippius_s3.monitoring import get_metrics_collector

        collector = get_metrics_collector()
        if collector is not None:
            collector.record_chunk_read_tier("backend")
    except Exception:  # noqa: BLE001 - a metrics failure must not fail a read
        pass


_fetcher: BackendChunkFetcher | None = None


def get_backend_fetcher() -> BackendChunkFetcher:
    """The process-wide fetcher: one Arion client, one concurrency budget, built on first use."""
    global _fetcher
    if _fetcher is None:
        _fetcher = _build_fetcher()
    return _fetcher


def set_backend_fetcher(fetcher: BackendChunkFetcher | None) -> None:
    """Replace the process-wide fetcher (tests; `None` rebuilds from config on next use)."""
    global _fetcher
    _fetcher = fetcher


def _build_fetcher() -> BackendChunkFetcher:
    from hippius_s3.services.arion_service import ArionClient

    cfg: Any = get_config()
    client = ArionClient()

    async def arion_fetch(identifier: str, address: str) -> bytes:
        return b"".join([piece async for piece in client.download_file(identifier, address)])

    return BackendChunkFetcher(
        {"arion": arion_fetch},
        concurrency=int(cfg.read_backend_fetch_concurrency),
        attempts=int(cfg.read_backend_fetch_attempts),
        base_sleep=float(cfg.read_backend_fetch_retry_base_seconds),
        jitter=float(cfg.read_backend_fetch_retry_jitter_seconds),
        queue_timeout=float(cfg.read_backend_fetch_queue_timeout_seconds),
    )
