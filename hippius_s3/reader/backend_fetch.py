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

One `ArionClient` per process, with the per-operation bounds and pool size from
`fetch_client_settings` — `fetch` runs once per chunk, ~1280 times for a 5 GiB object, and a client
per call is a TCP+TLS handshake per chunk. A per-process semaphore bounds the backend concurrency
the pod exerts across every in-flight GET, the way the uploader bounds its POSTs. A streak of pool
timeouts means that client's pool is wedged (connections held by nothing that is a live fetch), and
the fetcher replaces it rather than waiting for the pod to be deleted.
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Iterable

import httpx

from hippius_s3.config import get_config
from hippius_s3.monitoring import BackendFetchOutcome
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.workers.errors import classify_download_error


logger = logging.getLogger(__name__)

# (backend name, backend_identifier) — where one chunk can be fetched from.
BackendLocation = tuple[str, str]

# Downloads one identifier from one backend, returning the whole ciphertext chunk.
FetchOne = Callable[[str, str], Awaitable[bytes]]

# How long a replaced client's close() may take before it is abandoned. A wedged pool may not
# close promptly, and nothing waits on the old client once it is swapped out; the bound only
# keeps the reset from inheriting the stall it exists to escape.
_OLD_CLIENT_CLOSE_TIMEOUT_SECONDS = 5.0


class ChunkUnavailableError(Exception):
    """No tier can serve the chunk right now.

    Raised when the chunk is not on any local tier AND either no backend holds it yet (the part is
    still in its upload window and lives only on another node's SSD, or the object never finished)
    or every backend fetch failed. Retryable from the client's point of view: the first-chunk peek
    turns it into a 503 with Retry-After, a mid-stream one ends the stream.
    """


class BackendChunkFetcher:
    """Fetches chunks from backends in preference order, with bounded retries and concurrency, and
    replaces the backend client when a streak of pool timeouts says its pool is wedged."""

    def __init__(
        self,
        fetchers: dict[str, FetchOne],
        *,
        concurrency: int,
        attempts: int,
        base_sleep: float,
        jitter: float,
        queue_timeout: float | None = None,
        attempt_budget_seconds: float = 0.0,
        reset_fn: Callable[[], Awaitable[None]] | None = None,
        reset_after_pool_timeouts: int = 3,
    ) -> None:
        # reset_fn's contract: swap the client SYNCHRONOUSLY when called and return the deferred
        # cleanup (closing the old client) as an awaitable; see `_note_pool_timeout` for why.
        self._fetchers = fetchers
        self._semaphore = asyncio.Semaphore(max(1, int(concurrency)))
        # The semaphore exposes no occupancy, so the fetcher keeps its own count for the gauges.
        # The counts are per instance but the gauge is process-global: one fetcher per process.
        self._inflight = 0
        self._waiting = 0
        self._attempts = max(1, int(attempts))
        self._base_sleep = float(base_sleep)
        self._jitter = float(jitter)
        self._queue_timeout = None if queue_timeout is None else float(queue_timeout)
        # The worst case of one whole attempt (slot wait + pool wait + connect + read); a retry is
        # only started when this much time, plus its backoff, remains before the caller's deadline.
        self._attempt_budget = float(attempt_budget_seconds)

        self._reset_fn = reset_fn
        self._reset_after_pool_timeouts = max(1, int(reset_after_pool_timeouts))
        self._consecutive_pool_timeouts = 0
        self._reset_lock = asyncio.Lock()

        # Bumped once per rebuild (see `_note_pool_timeout`). Every pending close keeps a strong
        # reference here (asyncio holds tasks weakly); a set, not a slot, because a second streak
        # inside the close window would otherwise drop the first close on the floor.
        self._generation = 0
        self._reset_tasks: set[asyncio.Task[None]] = set()

    def can_serve(self, backend: str) -> bool:
        return backend in self._fetchers

    async def _acquire_slot(self) -> None:
        """Take a slot in the pod's budget, or give up: a saturated budget (a slow backend under
        many cold readers) must surface as a fast retryable failure on the reads that cannot be
        served, not as every queued read timing out in lockstep at the first-chunk bound."""
        self._waiting += 1
        _publish_slots(self._inflight, self._waiting)

        acquired = False
        try:
            if self._queue_timeout is None:
                await self._semaphore.acquire()
            else:
                await asyncio.wait_for(self._semaphore.acquire(), timeout=self._queue_timeout)
            acquired = True
        # Identical on 3.11+, but requires-python is >= 3.10 and there the bare form lets
        # asyncio.TimeoutError escape as a 500.
        except (TimeoutError, asyncio.TimeoutError) as exc:
            raise ChunkUnavailableError(
                f"backend fetch budget saturated for {self._queue_timeout:.0f}s; the read cannot be served now"
            ) from exc
        finally:
            # Slot taken, wait timed out, or cancelled while queued (a client that disconnected):
            # in every case this requester is no longer waiting. A phantom waiter would pin the
            # gauge above zero for the life of the process.
            self._waiting -= 1
            if acquired:
                self._inflight += 1
            _publish_slots(self._inflight, self._waiting)

    def _release_slot(self) -> None:
        self._semaphore.release()
        self._inflight -= 1
        _publish_slots(self._inflight, self._waiting)

    async def fetch(
        self, locations: Iterable[BackendLocation], address: str, *, deadline: float | None = None
    ) -> bytes:
        """Return the chunk's ciphertext from the first location that serves it.

        Locations are tried in the order given (the object's download-backend order). A transient
        failure — a 429/5xx, a connection reset, an httpx connect/read timeout — is retried on the
        same location with exponential backoff; a permanent one (a 404: the identifier is stale)
        moves on to the next. A pool timeout on the current client is terminal (see
        `_attempt_once`); if that timeout replaces the client, the attempt is retried
        once on the new one. Exhausting every location is `ChunkUnavailableError`.

        `deadline` is a loop-time instant (the caller's own bound on this chunk). No attempt after
        the first is started unless a whole attempt's worst case (slot wait + pool wait + connect
        + read, 24 s by default) plus its backoff still fits before it: a retry that the caller
        would cancel halfway is a silent stall, and the point of the bounds is that a stall leaves
        a line. With the 25 s first-chunk deadline that means the first chunk gets exactly one
        attempt by design; later chunks (300 s) retry.
        """
        tried = 0
        for backend, identifier in locations:
            fetch_one = self._fetchers.get(backend)
            if fetch_one is None:
                continue
            tried += 1
            if tried > 1:
                self._require_budget(deadline, 0.0, backend=backend, identifier=identifier, attempt=1)
            for attempt in range(1, self._attempts + 1):
                data, retry, generation = await self._attempt_once(
                    fetch_one, backend=backend, identifier=identifier, address=address, attempt=attempt
                )
                if data is not None:
                    # A success only says something about the client it ran on: a fetch that
                    # started under the old client must not clear the new client's streak.
                    if generation == self._generation:
                        self._consecutive_pool_timeouts = 0
                    _record_backend_read()
                    _record_backend_fetch_outcome("ok")
                    return data
                if not retry:
                    break
                backoff = self._base_sleep * (2 ** (attempt - 1)) + random.uniform(0, self._jitter)
                self._require_budget(deadline, backoff, backend=backend, identifier=identifier, attempt=attempt + 1)
                await asyncio.sleep(backoff)
        raise ChunkUnavailableError(f"no backend served the chunk (locations tried: {tried})")

    def _require_budget(
        self, deadline: float | None, backoff: float, *, backend: str, identifier: str, attempt: int
    ) -> None:
        """Refuse to start another attempt that cannot finish before the caller's deadline.

        The budget is the whole attempt, slot wait included: on the first chunk (25 s deadline,
        24 s budget) no retry ever fits, which is the intended single-attempt rule.
        """
        if deadline is None:
            return
        left = deadline - asyncio.get_running_loop().time()
        needed = self._attempt_budget + backoff
        if left > needed:
            return
        logger.warning(
            "backend chunk fetch failed backend=%s id=%s attempt=%s/%s kind=deadline retry=False: "
            "%.1fs left, attempt needs %.1fs",
            backend,
            identifier,
            attempt,
            self._attempts,
            left,
            needed,
        )
        raise ChunkUnavailableError(f"backend fetch out of time for {backend}: no budget for another attempt")

    async def _attempt_once(
        self,
        fetch_one: FetchOne,
        *,
        backend: str,
        identifier: str,
        address: str,
        attempt: int,
        allow_rebuild_retry: bool = True,
    ) -> tuple[bytes | None, bool, int]:
        """One attempt under a concurrency slot: `(data, False, generation)` on success,
        `(None, retry, generation)` on a classified failure, or `ChunkUnavailableError` when the
        failure is terminal for the request. `generation` is the client generation the attempt
        ran under.

        Every failed attempt is logged at WARNING, retried or not — the point of the bounds is that
        a stall leaves a line.
        """
        await self._acquire_slot()
        generation = self._generation
        try:
            data = await fetch_one(identifier, address)
        # PoolTimeout is a subclass of TimeoutException: this clause must come first, or pool
        # saturation becomes an ordinary retried timeout and the client is never replaced.
        except httpx.PoolTimeout as exc:
            # The pool is sized to the semaphore, so this cannot be "busy": connections are held by
            # something that is not a live fetch. Retrying the SAME client re-queues behind the
            # stuck pool. If this timeout is the one that replaces the client, retry this attempt
            # once on the new one — that is the point of the swap, and mid-stream it keeps a
            # committed 200 alive. A PoolTimeout that does not trip the rebuild is still terminal.
            self._release_slot()
            _record_backend_fetch_outcome("pool_timeout")
            if allow_rebuild_retry:
                await self._note_pool_timeout(generation)
            rebuilt = allow_rebuild_retry and self._generation != generation
            self._log_failed_attempt(
                backend=backend,
                identifier=identifier,
                attempt=attempt,
                kind="pool_saturated",
                retry=rebuilt,
                exc=exc,
            )
            if not rebuilt:
                raise ChunkUnavailableError(
                    f"backend connection pool saturated (pool wait exceeded) for {backend}"
                ) from exc
            return await self._attempt_once(
                fetch_one,
                backend=backend,
                identifier=identifier,
                address=address,
                attempt=attempt,
                allow_rebuild_retry=False,
            )
        except Exception as exc:  # noqa: BLE001 - every backend error is classified below
            self._release_slot()
            outcome = _outcome_of(exc)
            classified = classify_download_error(exc)
            retry = classified == "transient" and attempt != self._attempts
            # A timeout is logged under its outcome label (connect_timeout / read_timeout) so the
            # log line and the metric share one vocabulary; everything else under its class.
            kind = classified if outcome == "error" else outcome
            self._log_failed_attempt(
                backend=backend, identifier=identifier, attempt=attempt, kind=kind, retry=retry, exc=exc
            )
            _record_backend_fetch_outcome(outcome)
            return None, retry, generation
        except BaseException:
            # Cancellation (a client that disconnected mid-fetch) must give the slot back too.
            self._release_slot()
            raise

        self._release_slot()
        return data, False, generation

    def _log_failed_attempt(
        self, *, backend: str, identifier: str, attempt: int, kind: str, retry: bool, exc: Exception
    ) -> None:
        """One WARNING per failed attempt, the same shape whether or not it is retried, so a stall
        always leaves a greppable line (`kind=` and `retry=` are what the runbook keys on)."""
        logger.warning(
            "backend chunk fetch failed backend=%s id=%s attempt=%s/%s kind=%s retry=%s: %s",
            backend,
            identifier,
            attempt,
            self._attempts,
            kind,
            retry,
            exc,
        )

    async def _note_pool_timeout(self, generation: int) -> None:
        """Count a PoolTimeout against the client generation that produced it; at the threshold,
        replace the client exactly once per streak.

        The generation gate is what makes "once" true. A wedged pool fails its waiters in a burst,
        and the counter alone would turn a burst of N into N/threshold rebuilds while waiters still
        queued on the old pool kept timing out for up to `pool_timeout` and were counted against the
        new client. Every attempt carries the generation it started under; the one that reaches the
        threshold bumps it, and the rest of the burst says nothing about the replacement.
        """
        async with self._reset_lock:
            if generation != self._generation:
                return
            self._consecutive_pool_timeouts += 1
            if self._consecutive_pool_timeouts < self._reset_after_pool_timeouts or self._reset_fn is None:
                return
            self._consecutive_pool_timeouts = 0
            self._generation += 1

        # No await between the bump and the swap: reset_fn swaps synchronously, so every attempt
        # that starts after the bump runs on the new client under the new generation.
        try:
            pending = self._reset_fn()
        except Exception:  # noqa: BLE001 - a failed rebuild must not mask the fetch failure
            # The generation is already bumped, so the rest of this burst is ignored rather than
            # hammering a rebuild that just failed; the still-wedged client produces a fresh
            # streak within seconds, and that streak retries it.
            logger.exception("Arion client rebuild failed; the next PoolTimeout streak retries it")
            return

        logger.error(
            "backend fetch pool saturated %s times in a row; replaced the Arion client (inflight=%s waiting=%s)",
            self._reset_after_pool_timeouts,
            self._inflight,
            self._waiting,
        )
        self._detach_reset(pending)

    def _detach_reset(self, pending: Awaitable[None]) -> None:
        """Run the deferred half of a reset (closing the old client) as its own task. The requester
        that tripped it is a GET that may disconnect at any moment and whose 503 must not wait on
        a wedged pool's close, so nothing awaits the task; the done-callback reports on it."""

        async def run() -> None:
            # create_task wants a coroutine; reset_fn only promises an awaitable.
            await pending

        task = asyncio.create_task(run())
        self._reset_tasks.add(task)
        task.add_done_callback(self._reset_tasks.discard)
        task.add_done_callback(_log_reset_failure)


def _log_reset_failure(task: asyncio.Task[None]) -> None:
    """Done-callback for the detached reset: the requester that started it may be gone."""
    if task.cancelled():
        # Only the loop shutting down cancels this task (nothing else holds it), and the swap
        # already happened; there is no client left to recover.
        logger.info("Arion client close was cancelled (shutdown); nothing to retry")
        return
    exc = task.exception()
    if exc is not None:
        logger.error("Arion client reset failed; the next PoolTimeout streak retries it", exc_info=exc)


def _record_backend_read() -> None:
    """Count a chunk served by the backend tier, next to local/peer/pool. Never let observability
    fail a read."""
    try:
        collector = get_metrics_collector()
        if collector is not None:
            collector.record_chunk_read_tier("backend")
    except Exception:  # noqa: BLE001 - a metrics failure must not fail a read
        pass


def _record_backend_fetch_outcome(outcome: BackendFetchOutcome) -> None:
    """Count how one fetch attempt ended. Never let observability fail a read."""
    try:
        collector = get_metrics_collector()
        if collector is not None:
            collector.record_backend_fetch_outcome(outcome)
    except Exception:  # noqa: BLE001 - a metrics failure must not fail a read
        pass


def _publish_slots(inflight: int, waiting: int) -> None:
    """Push the fetcher's slot occupancy to the collector's gauges. Never let observability fail
    a read."""
    try:
        collector = get_metrics_collector()
        if collector is not None:
            collector.update_backend_fetch_slots(inflight, waiting)
    except Exception:  # noqa: BLE001 - a metrics failure must not fail a read
        pass


def _outcome_of(exc: Exception) -> BackendFetchOutcome:
    """Map a failed attempt's exception onto the bounded outcome label.

    PoolTimeout is checked first for the same reason the except clauses are ordered: it is a
    TimeoutException subclass, and folding it into a plain timeout would hide the one failure
    shape that means the client itself is wedged.
    """
    if isinstance(exc, httpx.PoolTimeout):
        return "pool_timeout"
    if isinstance(exc, httpx.ConnectTimeout):
        return "connect_timeout"
    # Write and read share one bound (`fetch_client_settings` sets write=read), so one outcome.
    if isinstance(exc, (httpx.ReadTimeout, httpx.WriteTimeout)):
        return "read_timeout"
    return "error"


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


def fetch_client_settings(cfg: Any) -> tuple[httpx.Timeout, httpx.Limits]:
    """The read-path ArionClient's per-operation bounds and pool size, derived from config.

    Pure so the derivation is testable without httpx internals. A misconfiguration raises when the
    fetcher is built, with the offending variable named; the app is expected to build the fetcher
    at startup so a bad value fails boot rather than the first cold read.
    """
    first = float(cfg.stream_first_chunk_timeout_seconds)
    queue = float(cfg.read_backend_fetch_queue_timeout_seconds)
    connect = float(cfg.read_backend_fetch_connect_timeout_seconds)
    read = float(cfg.read_backend_fetch_read_timeout_seconds)
    pool = float(cfg.read_backend_fetch_pool_timeout_seconds)

    # One attempt's worst case is the slot wait, then httpx's pool wait (a partially wedged pool
    # waits up to `pool` without raising), then connect + read; all of it must fail before the
    # reader's first-chunk bound cancels it silently.
    if queue + pool + connect + read >= first:
        raise ValueError(
            "HIPPIUS_READ_BACKEND_FETCH_QUEUE_TIMEOUT_SECONDS + "
            "HIPPIUS_READ_BACKEND_FETCH_POOL_TIMEOUT_SECONDS + "
            "HIPPIUS_READ_BACKEND_FETCH_CONNECT_TIMEOUT_SECONDS + "
            f"HIPPIUS_READ_BACKEND_FETCH_READ_TIMEOUT_SECONDS ({queue} + {pool} + {connect} + {read}) must be "
            f"below HIPPIUS_STREAM_FIRST_CHUNK_TIMEOUT_SECONDS ({first}); otherwise the reader cancels "
            "the fetch before httpx can fail it, and the failure is silent"
        )

    # Pool == semaphore: a healthy process never queues at the pool, so a PoolTimeout can only
    # mean connections are held by something that is not a live fetch.
    concurrency = max(1, int(cfg.read_backend_fetch_concurrency))
    return (
        httpx.Timeout(connect=connect, read=read, write=read, pool=pool),
        httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency),
    )


class _ReplaceableArionClient:
    """Holds the read path's ArionClient so the fetcher can swap it for a fresh one.

    `reset` builds the replacement FIRST and swaps it in synchronously (the fetcher's generation
    gate relies on that). Only the close of the old client is deferred: it drains first, then
    closes under a bound, because a wedged pool may not close promptly and waiting on it is the
    one thing the reset must not do.
    """

    def __init__(self, make: Callable[[], Any], *, drain_seconds: float = 17.0) -> None:
        self._make = make
        self._client = make()
        self._drain_seconds = float(drain_seconds)

    @property
    def client(self) -> Any:
        return self._client

    def reset(self) -> Awaitable[None]:
        """Swap in a fresh client now; return the deferred close of the old one.

        A failing `make()` raises here and leaves the previous client installed. The old pool's
        state is logged before the swap: it is the only evidence of what was holding the
        connections, and it goes away with the client.
        """
        old = self._client
        take_snapshot = getattr(old, "pool_snapshot", None)
        snapshot = take_snapshot() if take_snapshot is not None else "unavailable"
        self._client = self._make()
        logger.error("replacing the Arion client; old pool %s", snapshot)
        return self._close_old(old)

    async def _close_old(self, old: Any) -> None:
        # The old client still carries healthy in-flight fetches (only the pool is wedged, not
        # every connection). The drain covers a pool wait plus one wire attempt (pool + connect
        # + read + 1); a slow but healthy body can outlive it, which is acceptable: that fetch
        # fails as a transient error and retries on the new client.
        await asyncio.sleep(self._drain_seconds)
        try:
            await asyncio.wait_for(old.close(), timeout=_OLD_CLIENT_CLOSE_TIMEOUT_SECONDS)
        except Exception as exc:  # noqa: BLE001 - the old client is already unreferenced
            logger.warning("old Arion client did not close cleanly (%s); dropped", exc)


def _build_fetcher() -> BackendChunkFetcher:
    from hippius_s3.services.arion_service import ArionClient

    cfg: Any = get_config()
    timeout, limits = fetch_client_settings(cfg)
    queue = float(cfg.read_backend_fetch_queue_timeout_seconds)
    wire = (
        float(cfg.read_backend_fetch_pool_timeout_seconds)
        + float(cfg.read_backend_fetch_connect_timeout_seconds)
        + float(cfg.read_backend_fetch_read_timeout_seconds)
    )
    holder = _ReplaceableArionClient(lambda: ArionClient(timeout=timeout, limits=limits), drain_seconds=wire + 1.0)

    async def arion_fetch(identifier: str, address: str) -> bytes:
        return b"".join([piece async for piece in holder.client.download_file(identifier, address)])

    return BackendChunkFetcher(
        {"arion": arion_fetch},
        concurrency=int(cfg.read_backend_fetch_concurrency),
        attempts=int(cfg.read_backend_fetch_attempts),
        base_sleep=float(cfg.read_backend_fetch_retry_base_seconds),
        jitter=float(cfg.read_backend_fetch_retry_jitter_seconds),
        queue_timeout=float(cfg.read_backend_fetch_queue_timeout_seconds),
        attempt_budget_seconds=queue + wire,
        reset_fn=holder.reset,
        reset_after_pool_timeouts=int(cfg.read_backend_fetch_client_reset_after_pool_timeouts),
    )
