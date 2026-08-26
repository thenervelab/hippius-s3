"""Filesystem-backed parts store for object chunks and metadata.

Provides persistent storage for multipart upload chunks and metadata on a shared
volume, ensuring data availability beyond Redis TTL for long-running uploads.
Also serves as the download cache — when workers fetch chunks from a backend,
they write them here (not to Redis) for the streamer to read.
"""

from __future__ import annotations

import asyncio
import contextlib
import fcntl
import json
import logging
import os
import shutil
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from typing import AsyncIterator
from typing import Iterator
from typing import Optional
from uuid import UUID

from hippius_s3.cache.access_tracker import get_access_tracker


logger = logging.getLogger(__name__)

# Part-directory scans run here, NOT on asyncio's default executor. Two reasons, both precedent:
# crypto_pool keeps AES off the default pool because that pool "already carries FS md5 offload and
# blocking FS work" and must not be starved — and the default pool is exactly where `get_chunk`,
# `set_chunk` and `set_meta` do their blocking IO, so a fan-out of presence scans landing there
# would stall the chunk reads those same requests are about to make. The janitor's parallel walk
# reaches for a dedicated `janitor-walk` pool for the same reason.
#
# Sizing it here is also what makes the bound PROCESS-wide. A per-request semaphore only bounds one
# request: N concurrent multi-part GETs would still demand N x limit threads. max_workers is the
# real ceiling on how many metadata ops this process can have in flight against a shared MDS.
# Module-level rather than per-instance on purpose: DualFileSystemPartsStore holds two of these
# stores (local primary + pool fallback) and both inherit this method, so per-instance pools would
# quietly double the ceiling.
_SCAN_POOL: ThreadPoolExecutor | None = None
_SCAN_POOL_SIZE = 0


def _scan_pool_size() -> int:
    """Worker count of the scan pool; also the per-request in-flight cap. Pool-creating."""
    _scan_pool()
    return _SCAN_POOL_SIZE


def _scan_pool() -> ThreadPoolExecutor:
    """The process-wide part-scan pool, created on first use (config isn't loaded at import).

    Deliberately unlocked. The only caller is an `async def`, so this runs on the event loop
    thread and there is no `await` between the check and the assignment — a single loop cannot
    interleave two initialisers, and uvicorn's workers are separate processes with their own
    module state. Even if two ever did race, `ThreadPoolExecutor` spawns threads lazily on first
    submit, so the losing instance holds no threads and is simply collected. A lock here would
    defend an unreachable race at the cost of a synchronisation primitive on the read path.
    """
    global _SCAN_POOL, _SCAN_POOL_SIZE
    if _SCAN_POOL is None:
        # Deferred import: config imports the cache package, so a module-level import here is
        # circular (same reason object_parts.py defers it).
        from hippius_s3.config import get_config

        _SCAN_POOL_SIZE = max(1, int(get_config().fs_store_scan_concurrency))
        _SCAN_POOL = ThreadPoolExecutor(max_workers=_SCAN_POOL_SIZE, thread_name_prefix="fs-scan")
    return _SCAN_POOL


def _reset_scan_pool_for_tests() -> None:
    """Drop the memoised pool so a test can re-read the concurrency config. Tests only."""
    global _SCAN_POOL, _SCAN_POOL_SIZE
    if _SCAN_POOL is not None:
        _SCAN_POOL.shutdown(wait=False)
    _SCAN_POOL = None
    _SCAN_POOL_SIZE = 0


# Marks a chunk file as belonging to ONE in-flight upload attempt and not yet published.
#
# The infix deliberately does NOT contain ".tmp.". Both tmp sweepers assume a write temp lives
# for milliseconds, so anything older is a crash orphan: the drain agent reaps `*.tmp.*` on the
# ingest SSD after CEPHOR_RECLAIM_GRACE_SECS (1h), and the janitor does the same on the shared
# pool after TMP_FILE_MAX_AGE_SECONDS (30m). A staged chunk is held for the WHOLE of one
# UploadPart by design, and a multi-GB part on a slow link outlives both windows — tmp-shaped
# staging would be deleted out from under a live upload. The drain's sweep knows this name and
# gives it the (much longer) orphan grace instead; see `is_staged_name` in localfs.rs.
#
# Everything else ignores these files for free: the drain's `parse_chunk_index` accepts only
# exactly `chunk_<u32>.bin` so they never count toward its completeness gate, readers open
# exact names, and the evictor unlinks whole part dirs.
_STAGED_INFIX = ".staged."


class _PartPublishLocks:
    """Serializes publishing of one part directory within this process.

    Publishing swaps a SET of files into place, and a set-rename has no atomic primitive; two
    attempts publishing the same part concurrently would interleave their renames into a
    mixture belonging to neither.

    This is the in-process HALF of that exclusion, not the whole of it. The api runs uvicorn
    with UVICORN_WORKERS>1 (4 in both namespaces today), so a pod is several interpreters and
    this registry is per-worker: two attempts at one part are spread across workers by the
    shared listening socket and would miss each other here. `_part_dir_flock` supplies the
    cross-process half; this lock stays because it is far cheaper, and it means each worker
    takes the syscall once per part rather than once per waiter.

    Entries are refcounted and dropped when idle so the registry does not grow by one lock per
    part ever uploaded.
    """

    def __init__(self) -> None:
        self._entries: dict[str, tuple[asyncio.Lock, list[int]]] = {}

    @contextlib.asynccontextmanager
    async def acquire(self, key: str) -> AsyncIterator[None]:
        entry = self._entries.get(key)
        if entry is None:
            entry = (asyncio.Lock(), [0])
            self._entries[key] = entry
        lock, holders = entry
        # Counted BEFORE awaiting the lock, so a waiter keeps the entry alive and every
        # contender for one path shares one lock object.
        holders[0] += 1
        try:
            async with lock:
                yield
        finally:
            holders[0] -= 1
            if holders[0] == 0 and self._entries.get(key) is entry:
                del self._entries[key]


@contextlib.contextmanager
def _part_dir_flock(part_dir: Path) -> "Iterator[None]":
    """Exclude other PROCESSES from publishing this part. BLOCKING; call on the worker thread.

    Locks the part DIRECTORY's own descriptor rather than a lockfile, deliberately: everything
    that walks a part dir treats its contents as meaningful — the drain's completeness gate wants
    exactly `chunk_<i>.bin` plus `meta.json`, the tmp sweepers match on name shape, the janitor
    parses what it finds — so an extra entry is a way to strand a part, and there is nothing to
    clean up afterwards or leak if a pod is killed mid-publish.

    `flock` is keyed on the open file description, so two `open()` calls contend even inside one
    process. That is what makes this testable without spawning a second interpreter, and it is
    also why the in-process lock above is kept: without it, waiters in one worker would each burn
    an executor thread blocking here.

    `flock` on a local filesystem only. CLAUDE.md's "no flock (unreliable on CephFS)" applies to
    the shared object_cache PVC; this path is HIPPIUS_OBJECT_CACHE_DIR, which on an ingest node is
    the node-local SSD (/dev/md3 on staging). The lock is released by the kernel if the process
    dies, so a killed worker cannot wedge the part.
    """
    fd = os.open(part_dir, os.O_RDONLY)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


_publish_locks = _PartPublishLocks()


def _trim_chunk_tail(part_dir: Path, first_index: int, label: str) -> int:
    """Delete `chunk_<i>.bin` for i >= first_index; the publish-time exact-set trim. BLOCKING.

    The drain replicates a part only when the SSD chunk set is EXACTLY {0..num_chunks-1}
    (partdrain.rs completeness gate → IncompleteSource); a stale tail left by a LARGER earlier
    attempt would strand the part forever — never replicated, never evicted. Never touches
    meta.json or chunks below first_index.

    Per-file failures are logged at ERROR — a surviving tail IS the stranded-part risk and
    operators must see it — but never raised: the chunks themselves are durable on SSD and the
    client's success must not hinge on tail cleanup. Runs BEFORE meta.json is written, so the
    published (meta, chunk set) pair is exact from the moment it becomes visible.

    Returns:
        Number of chunk files removed.
    """
    removed = 0
    try:
        entries = list(part_dir.iterdir())
    except FileNotFoundError:
        return 0
    except OSError as e:
        # Same loudness as a per-file failure: an unscannable dir may hide a tail.
        logger.error(
            f"FS trim failed — could not scan part dir, a stale chunk tail may strand the "
            f"part as IncompleteSource in the drain: {label}: {e}"
        )
        return 0
    for entry in entries:
        name = entry.name
        if not (name.startswith("chunk_") and name.endswith(".bin")):
            continue
        try:
            idx = int(name[len("chunk_") : -len(".bin")])
        except ValueError:
            continue  # tmp files etc. — the janitor's orphan sweep owns those
        if idx < first_index:
            continue
        try:
            entry.unlink()
            removed += 1
        except OSError as e:
            logger.error(
                f"FS trim failed — stale chunk tail survives and the part may strand as "
                f"IncompleteSource in the drain: {label} chunk={idx}: {e}"
            )
    return removed


def _write_meta_file(meta_path: Path, payload: dict[str, int]) -> None:
    """Write meta.json atomically (unique tmp, fsync, rename). BLOCKING."""
    tmp_path = meta_path.with_name(f"{meta_path.name}.tmp.{uuid.uuid4().hex}")
    try:
        with tmp_path.open("w") as f:
            json.dump(payload, f)
            f.flush()
            os.fsync(f.fileno())
        tmp_path.replace(meta_path)
    except Exception:
        with contextlib.suppress(OSError):
            tmp_path.unlink(missing_ok=True)
        raise


class FileSystemPartsStore:
    """Filesystem-backed store for object parts with atomic writes.

    Layout: <root>/<object_id>/v<object_version>/part_<part_number>/
              chunk_<index>.bin
              chunk_<index>.bin.staged.<attempt>  (one attempt's unpublished chunk)
              meta.json (presence indicates part is complete)

    All writes are atomic (unique-tmp + rename) so concurrent writers from
    different worker pods don't corrupt files. Readers check for meta.json
    existence before reading chunks.

    Two write disciplines share the layout. `set_chunk` publishes a chunk immediately —
    right for the download path, where each chunk that lands should become readable at
    once and the only writers are workers filling identical content. `stage_chunk` +
    `publish_part` promotes a whole attempt's set at once — required for UploadPart,
    where two attempts of the SAME part carry DIFFERENT content and publishing chunk by
    chunk would let one overwrite the other's already-acknowledged bytes.
    """

    def __init__(self, root_dir: str) -> None:
        """Initialize the store with a root directory path.

        Args:
            root_dir: Root directory path for object cache storage
        """
        self.root = Path(root_dir)
        self.root.mkdir(parents=True, exist_ok=True)

    def _safe_object_id(self, object_id: Any) -> str:
        """Validate object_id as UUID to prevent path traversal.

        Accepts either a str (normalised via UUID parse) or a UUID instance
        (passed through via str()). Anything else raises ValueError. asyncpg
        may return UUID objects for UUID columns depending on codec setup.
        """
        if isinstance(object_id, UUID):
            return str(object_id)
        if not isinstance(object_id, str):
            raise ValueError(f"Invalid object_id type: {type(object_id).__name__}")
        try:
            return str(UUID(object_id.strip()))
        except (ValueError, AttributeError, TypeError) as e:
            raise ValueError(f"Invalid object_id: {object_id!r}") from e

    def part_path(self, object_id: str, object_version: int, part_number: int) -> str:
        """Return the directory path for a specific part.

        Args:
            object_id: Object UUID
            object_version: Object version number
            part_number: Part number

        Returns:
            Absolute path to part directory
        """
        safe_id = self._safe_object_id(object_id)
        return str(self.root / safe_id / f"v{int(object_version)}" / f"part_{int(part_number)}")

    def _chunk_file(self, part_dir: Path, chunk_index: int) -> Path:
        """Return the path for a chunk file."""
        return part_dir / f"chunk_{int(chunk_index)}.bin"

    def _meta_file(self, part_dir: Path) -> Path:
        """Return the path for the metadata file."""
        return part_dir / "meta.json"

    def _unique_tmp(self, target: Path) -> Path:
        """Build a unique temp filename alongside target.

        Using a uuid4 suffix guarantees two concurrent writers to the same
        final path never collide on the tempfile and interleave bytes.
        """
        return target.with_name(f"{target.name}.tmp.{uuid.uuid4().hex}")

    async def set_chunk(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int, data: bytes
    ) -> None:
        """Write a chunk to filesystem atomically.

        Concurrent writers to the same chunk path are safe: each uses a unique
        temp file, and the final `os.replace` is atomic. Last rename wins;
        content is deterministic per chunk so the "winner" doesn't matter.

        Args:
            object_id: Object UUID
            object_version: Object version number
            part_number: Part number
            chunk_index: Chunk index within the part
            data: Chunk bytes (ciphertext)

        Raises:
            OSError: If write fails (fatal to request)
        """
        # Basic validation
        if int(part_number) < 0 or int(chunk_index) < 0:
            raise ValueError("part_number and chunk_index must be non-negative")

        part_dir = Path(self.part_path(object_id, object_version, part_number))
        part_dir.mkdir(parents=True, exist_ok=True)

        chunk_path = self._chunk_file(part_dir, chunk_index)
        tmp_path = self._unique_tmp(chunk_path)

        try:
            # Write to temp file (off the event loop)
            def _write_chunk() -> None:
                with tmp_path.open("wb") as f:
                    f.write(data)
                    f.flush()
                tmp_path.replace(chunk_path)

            await asyncio.to_thread(_write_chunk)

            logger.debug(
                f"FS: wrote chunk object_id={object_id} v={object_version} part={part_number} chunk={chunk_index} size={len(data)}"
            )
        except Exception as e:
            # Clean up temp file if it exists
            with contextlib.suppress(OSError):
                if tmp_path.exists():
                    tmp_path.unlink()
            logger.error(
                f"FS write failed: object_id={object_id} v={object_version} part={part_number} chunk={chunk_index}: {e}"
            )
            raise

    def _staged_chunk_file(self, part_dir: Path, chunk_index: int, attempt_id: str) -> Path:
        """Path for a chunk that belongs to one unpublished upload attempt."""
        if not attempt_id.isalnum():
            # It is a path component; keep it incapable of carrying a separator even though
            # every caller passes a uuid4 hex.
            raise ValueError(f"Invalid attempt_id: {attempt_id!r}")
        return part_dir / f"chunk_{int(chunk_index)}.bin{_STAGED_INFIX}{attempt_id}"

    async def stage_chunk(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        chunk_index: int,
        data: bytes,
        *,
        attempt_id: str,
    ) -> None:
        """Write a chunk under a name private to ONE upload attempt.

        The counterpart of `set_chunk` for paths where duplicate attempts share a part dir
        (MPU UploadPart, and the append delta part that goes through it). Nothing is visible
        at `chunk_<i>.bin` until `publish_part` renames the whole set, so an attempt that dies
        mid-stream — or one still writing while another publishes — cannot overwrite chunks of
        an already-acknowledged attempt. That is not reachable by cancelling the writer: a
        chunk already inside the worker thread lands regardless, and it must land somewhere
        harmless rather than not land.

        Costs the same as `set_chunk`: one file created now, one rename later at publish,
        versus a create and an immediate rename. No extra copy of the bytes.

        WHO REAPS A STAGED FILE. Naming these so no existing sweeper touches them is what keeps
        a live 14 GB upload alive, and it means nothing reclaims them by accident either — so
        the disposal path is explicit at every exit:
          - Ordinary failure or cancellation: `discard_staged`, from the writer's `finally`.
          - Publish: the rename consumes them; a stale tail from a larger earlier attempt goes
            with the trim.
          - SIGKILL/OOM mid-upload: neither of the above runs. The part dir has no `meta.json`,
            so the drain's reclaim scan never discovers it, and with NEITHER a residency row nor
            a replication row (both are written only after publish) the evictor's INNER JOIN can
            never emit it — `remove_dir_all` is never called on it. The drain agent's
            `sweep_orphan_tmp` is therefore the only reaper that can reach it, which is why it
            was taught this name, on the orphan grace (24h) rather than the write-temp one.
            `sweep_empty_shells` then collects the emptied part/version/object dirs.

        Raises:
            OSError: If the write fails (fatal to request)
        """
        if int(part_number) < 0 or int(chunk_index) < 0:
            raise ValueError("part_number and chunk_index must be non-negative")

        part_dir = Path(self.part_path(object_id, object_version, part_number))
        part_dir.mkdir(parents=True, exist_ok=True)
        staged_path = self._staged_chunk_file(part_dir, chunk_index, attempt_id)

        def _write_staged() -> None:
            with staged_path.open("wb") as f:
                f.write(data)
                f.flush()

        try:
            await asyncio.to_thread(_write_staged)
        except Exception as e:
            with contextlib.suppress(OSError):
                staged_path.unlink(missing_ok=True)
            logger.error(
                f"FS staged write failed: object_id={object_id} v={object_version} "
                f"part={part_number} chunk={chunk_index}: {e}"
            )
            raise

    async def publish_part(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        *,
        attempt_id: str,
        chunk_size: int,
        num_chunks: int,
        size_bytes: int,
    ) -> None:
        """Make one attempt's staged chunks the part, as a single operation.

        Renames `chunk_<i>.bin.staged.<attempt>` onto `chunk_<i>.bin` for the whole set, drops
        any stale tail from a larger earlier attempt (the drain replicates only an EXACT
        {0..num_chunks-1} set), and writes `meta.json` last — all inside one thread call under
        a per-part lock, so no other attempt in this process can interleave into it and no
        Python-level cancellation can stop it half-way.

        A set-rename has no atomic primitive, so an FS error part-way through CAN leave two
        attempts' chunks side by side. That state is made invisible rather than served:
        `meta.json` is removed before the error propagates, which is the same "un-publish"
        the append CAS loser does, and the reader/drain then treat the part as absent rather
        than decrypting a mixture that AEAD-verifies as the wrong plaintext.

        The one window that survives is a process KILL inside this thread call, which would
        leave a partly-swapped set under the previous attempt's meta. It is bounded by the call
        itself — `num_chunks` stats, `num_chunks` renames, one dir scan and one small write,
        with no data copied — measured at 2.7 ms for a 100 MB part and 326 ms for a 14 GB one
        (3584 chunks). Un-publishing FIRST would make even that invisible, but a kill in the
        new window would then lose an acknowledged, not-yet-replicated part: that trades a
        narrow corruption window for a loss path, against the leak-beats-loss doctrine.

        Raises:
            FileNotFoundError: If a staged chunk is missing (a concurrent whole-part eviction
                is the realistic cause) — detected BEFORE any rename, so nothing is disturbed.
            OSError: If a rename or the meta write fails.
        """
        if int(num_chunks) <= 0:
            # Publishing an empty set would trim every chunk away and then mark the emptied
            # dir complete. No upload path produces one (zero-length parts are rejected
            # upstream), so treat it as a caller bug rather than an instruction.
            raise ValueError(f"num_chunks must be positive, got {num_chunks}")

        part_dir = Path(self.part_path(object_id, object_version, part_number))
        label = f"object_id={object_id} v={object_version} part={part_number}"
        payload = {
            "chunk_size": int(chunk_size),
            "num_chunks": int(num_chunks),
            "size_bytes": int(size_bytes),
        }
        staged = [self._staged_chunk_file(part_dir, i, attempt_id) for i in range(int(num_chunks))]
        targets = [self._chunk_file(part_dir, i) for i in range(int(num_chunks))]
        meta_path = self._meta_file(part_dir)

        def _publish() -> None:
            # The flock is taken HERE, on the worker thread, and wraps the whole swap: the
            # pre-flight, every rename, the trim and the meta write are one critical section
            # against other processes exactly as they already were against other coroutines.
            # Taking it outside `to_thread` would put the blocking acquire on the event loop.
            with _part_dir_flock(part_dir):
                _publish_locked()

        def _publish_locked() -> None:
            missing = [p.name for p in staged if not p.exists()]
            if missing:
                raise FileNotFoundError(f"staged chunks missing at publish: {label} missing={missing}")
            renamed = 0
            try:
                for src, dst in zip(staged, targets, strict=False):
                    src.replace(dst)
                    renamed += 1
                # Inside the same guard as the renames, not after it. meta.json is the readiness
                # gate, so the window this un-publish exists to close does not end at the last
                # rename — it ends when the NEW meta lands. An OSError from the trim or the meta
                # write left the PREVIOUS attempt's meta.json standing over a chunk set that is
                # now this attempt's and a different length: the part still reads as published,
                # with a wrong declared size and holes, and the drain's completeness gate strands
                # it as IncompleteSource. ENOSPC is the obvious way in, and a disk that retention
                # deliberately runs at 15-20% free is where it happens.
                _trim_chunk_tail(part_dir, int(num_chunks), label)
                _write_meta_file(meta_path, payload)
            except OSError:
                if renamed:
                    with contextlib.suppress(OSError):
                        meta_path.unlink(missing_ok=True)
                raise

        async with _publish_locks.acquire(str(part_dir)):
            await asyncio.to_thread(_publish)
            await self._fsync_dir_async(part_dir)

        logger.debug(f"FS: published part {label} num_chunks={num_chunks}")

    async def discard_staged(self, object_id: str, object_version: int, part_number: int, *, attempt_id: str) -> int:
        """Remove one attempt's unpublished chunk files. Returns how many went.

        Safe to call on the failure path precisely because the names are private to the
        attempt — unlike the canonical `chunk_<i>.bin` names, which duplicate attempts share
        and which no failure path may ever delete. Best-effort: unpublished bytes leaking
        until the part is reclaimed is a wart, failing the caller's already-failing request
        over it is not.
        """
        part_dir = Path(self.part_path(object_id, object_version, part_number))
        marker = f"{_STAGED_INFIX}{attempt_id}"

        def _discard() -> int:
            removed = 0
            try:
                entries = list(part_dir.iterdir())
            except OSError:
                return 0
            for entry in entries:
                if not entry.name.endswith(marker):
                    continue
                with contextlib.suppress(OSError):
                    entry.unlink()
                    removed += 1
            return removed

        removed = await asyncio.to_thread(_discard)
        if removed:
            logger.debug(
                f"FS: discarded {removed} staged chunk(s): object_id={object_id} v={object_version} part={part_number}"
            )
        return removed

    async def get_chunk(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int
    ) -> Optional[bytes]:
        """Read a chunk from filesystem.

        Gated on meta.json existence — readers only see chunks once the part
        is marked ready. Read recency is recorded via the AccessTracker (into
        fs_cache_inventory.last_access_at) rather than atime; the hook lives
        HERE, at the store level, because the streamer passes fetch_fn =
        fs.get_chunk directly and bypasses every higher-level wrapper.

        Args:
            object_id: Object UUID
            object_version: Object version number
            part_number: Part number
            chunk_index: Chunk index within the part

        Returns:
            Chunk bytes if present, None otherwise
        """
        part_dir = Path(self.part_path(object_id, object_version, part_number))
        meta_path = self._meta_file(part_dir)
        chunk_path = self._chunk_file(part_dir, chunk_index)

        try:
            # Read-recency for hot retention is recorded in
            # fs_cache_inventory.last_access_at (cache/access_tracker.py), not
            # via atime: the old per-read os.utime was silently dead on
            # read-only mounts (prod api-local) and an MDS metadata WRITE on
            # every read elsewhere. stat atime now reflects write recency only.

            # The existence probes run INSIDE the offloaded callable, with the
            # read: on the CephFS fallback tier each is a network metadata
            # round trip, and probing inline blocked the event loop on every
            # miss (0.8s of on-loop pathlib.stat in a 12s py-spy window during
            # a cold read). meta.json first — readers only see chunks once the
            # part is marked ready.
            def _read() -> bytes | None:
                if not meta_path.exists():
                    return None
                if not chunk_path.exists():
                    return None
                with chunk_path.open("rb") as f:
                    return f.read()

            data = await asyncio.to_thread(_read)
            if data is None:
                return None
            # Sync, sampled, no-op in processes that never initialize the
            # tracker (workers/janitor).
            tracker = get_access_tracker()
            if tracker is not None:
                tracker.note_read(object_id, int(object_version), int(part_number))
            logger.debug(
                f"FS: read chunk object_id={object_id} v={object_version} part={part_number} chunk={chunk_index} size={len(data)}"
            )
            return data
        except Exception as e:
            logger.warning(
                f"FS read failed: object_id={object_id} v={object_version} part={part_number} chunk={chunk_index}: {e}"
            )
            return None

    async def chunk_exists(self, object_id: str, object_version: int, part_number: int, chunk_index: int) -> bool:
        """Check if a chunk exists on filesystem.

        Args:
            object_id: Object UUID
            object_version: Object version number
            part_number: Part number
            chunk_index: Chunk index within the part

        Returns:
            True if chunk exists and part is complete (meta.json present)
        """
        part_dir = Path(self.part_path(object_id, object_version, part_number))
        meta_path = self._meta_file(part_dir)

        if not meta_path.exists():
            return False

        chunk_path = self._chunk_file(part_dir, chunk_index)
        return chunk_path.exists()

    async def chunks_exist_batch(
        self, object_id: str, object_version: int, checks: list[tuple[int, int]]
    ) -> list[bool]:
        """Batch existence check for many chunks (stat-based, no Redis).

        One directory scan per distinct part, not one stat per chunk, and the parts are
        scanned CONCURRENTLY. A single `os.scandir` returns every `chunk_<i>.bin` in the
        part, so membership is a set lookup — for a part with N chunks this is 1 readdir
        instead of N stats.

        Both halves matter because on the CephFS pool tier every metadata op is a serial
        MDS round trip (measured on prod: 6.3ms median, 15ms p90). This check gates TTFB,
        so a serial per-chunk sweep made it O(total_chunks) × MDS-latency — ~1250 stats
        ≈ 10s before the first byte on a 5 GB object. Scanning per-part alone leaves
        O(parts) serial round trips, which is still ~0.5s for a 78-part multipart object;
        fanning the scans out bounds it at roughly ceil(parts / concurrency) round trips.

        Semantics are unchanged by either half: a chunk counts as present iff the part's
        `meta.json` exists AND its file is on disk (see `chunk_exists`). That keeps the
        download-in-progress case correct — the downloader writes meta eagerly, so
        meta-present does not imply all chunks present.

        Args:
            object_id: Object UUID
            object_version: Object version
            checks: list of (part_number, chunk_index) tuples

        Returns:
            List of booleans (same length and order as `checks`).
        """
        if not checks:
            return []

        def _present_indices(part_dir: Path) -> set[int] | None:
            """Chunk indices on disk for a complete part, or None if the part is not complete.

            None = no `meta.json` (part not published / not yet known) → every chunk is absent.
            A set = meta present; membership reflects the actual `chunk_<i>.bin` files, so a
            chunk mid-download (meta written eagerly, file not yet landed) is correctly absent.
            """
            try:
                with os.scandir(part_dir) as entries:
                    names = {entry.name for entry in entries}
            except (FileNotFoundError, NotADirectoryError):
                # ENOENT/ENOTDIR only — "there is no such part directory", which is genuinely
                # absent. Everything else (EACCES, EIO, ESTALE) is left to raise, matching the
                # `Path.exists()` this replaced: on the 3.11 runtime that ships, its ignore-list
                # is ENOENT/ENOTDIR/EBADF/ELOOP, so EACCES and ESTALE already propagated. Do not
                # widen this to a bare `OSError` — reporting a genuinely unreadable part as absent
                # sends the caller down the pipeline branch to re-fetch and re-write the same
                # broken directory, which retries forever and reads as a cold miss on every
                # dashboard.
                return None
            if "meta.json" not in names:
                return None
            present: set[int] = set()
            for name in names:
                if not (name.startswith("chunk_") and name.endswith(".bin")):
                    continue
                try:
                    index = int(name[len("chunk_") : -len(".bin")])
                except ValueError:
                    continue  # .tmp / .staged files — not a published chunk
                # Round-trip so membership is exactly the old per-chunk `chunk_<i>.bin` stat.
                # `int()` is far more permissive than the writer: it accepts leading zeros, a
                # sign, surrounding whitespace and non-ASCII digits, so `chunk_007.bin` would
                # otherwise register as index 7. The writer only ever emits the canonical form,
                # so this is unreachable today — it is here to keep "present" defined by the
                # exact filename rather than by whatever `int()` happens to tolerate.
                if name == f"chunk_{index}.bin":
                    present.add(index)
            return present

        def _scan(part_number: int) -> set[int] | None:
            return _present_indices(Path(self.part_path(object_id, object_version, part_number)))

        # dict.fromkeys: distinct part numbers, first-seen order (deterministic scan order).
        distinct_parts = list(dict.fromkeys(int(pn) for pn, _ in checks))
        loop = asyncio.get_running_loop()
        pool = _scan_pool()

        # TWO bounds, and they do different jobs — neither alone is enough.
        #
        # The pool's max_workers caps what this PROCESS puts on the MDS at once. The semaphore caps
        # what ONE REQUEST may have queued in it. Without the semaphore, `gather` submits every part
        # immediately and the pool's queue is unbounded FIFO with no fairness: a 2000-part GET puts
        # 2000 jobs in front of the single job belonging to a 4 KB object that arrives a moment
        # later, so the small read waits out ~2000/max_workers readdirs. Measured on a local FS that
        # turned a 0.13 ms neighbour read into 143 ms; at the pool tier's 6.3 ms readdir it is worth
        # ~0.8 s, and ~4 s for a 10,000-part object. That is the very latency this method exists to
        # remove, moved onto whoever reads alongside a large object.
        sem = asyncio.Semaphore(_scan_pool_size())

        async def _scan_bounded(part_number: int) -> set[int] | None:
            async with sem:
                return await loop.run_in_executor(pool, _scan, part_number)

        scanned = dict(
            zip(
                distinct_parts,
                await asyncio.gather(*(_scan_bounded(pn) for pn in distinct_parts)),
                strict=True,
            )
        )

        results: list[bool] = []
        for part_number, chunk_index in checks:
            present = scanned[int(part_number)]
            # int() on BOTH halves: the old form reached the chunk through `_chunk_file`, which
            # coerced the index, so a caller passing a str index kept working. Set membership
            # would not, and would fail as a silent permanent miss rather than an error.
            results.append(present is not None and int(chunk_index) in present)
        return results

    async def touch_chunk(self, object_id: str, object_version: int, part_number: int, chunk_index: int) -> None:
        """Update atime/mtime of a chunk to mark it as recently accessed.

        Safe to call on missing files (silently no-ops).
        """
        part_dir = Path(self.part_path(object_id, object_version, part_number))
        chunk_path = self._chunk_file(part_dir, chunk_index)
        meta_path = self._meta_file(part_dir)

        def _touch() -> None:
            with contextlib.suppress(OSError):
                os.utime(chunk_path, None)
            with contextlib.suppress(OSError):
                os.utime(meta_path, None)

        await asyncio.to_thread(_touch)

    async def touch_part(self, object_id: str, object_version: int, part_number: int) -> None:
        """Update atime/mtime of every chunk + meta in a part.

        Called by the uploader after successful backend upload — previously
        this refreshed the Redis TTL; now it signals the janitor to keep the
        part hot for the default age-based GC window.
        """
        part_dir = Path(self.part_path(object_id, object_version, part_number))
        if not part_dir.exists():
            return

        def _touch_all() -> None:
            for entry in part_dir.iterdir():
                if entry.is_file():
                    with contextlib.suppress(OSError):
                        os.utime(entry, None)

        await asyncio.to_thread(_touch_all)

    async def read_local_chunk(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int
    ) -> Optional[bytes]:
        """This store's OWN copy of a chunk, ignoring any fallback tier.

        Serves the peer-fetch endpoint, where "local" is the whole point: a peer that
        followed its fallback would put a network hop in front of the pool read the peer
        tier exists to avoid, and two nodes resolving each other could bounce a request
        between them rather than either just reading the pool.

        Calls the base implementation explicitly rather than `self.get_chunk`, so a
        subclass that adds fallback or peer tiers (DualFileSystemPartsStore) cannot change
        what this returns.
        """
        return await FileSystemPartsStore.get_chunk(self, object_id, object_version, part_number, chunk_index)

    async def set_meta(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        *,
        chunk_size: int,
        num_chunks: int,
        size_bytes: int,
    ) -> None:
        """Write metadata atomically. This is the 'complete' marker for a part.

        For the download path, callers write meta EAGERLY at the start of part
        processing (using num_chunks/chunk_size from DB) so that partial fills
        become readable per-chunk as they land. For the upload path, meta is
        written AFTER all chunks — same method, different ordering.

        Args:
            object_id: Object UUID
            object_version: Object version number
            part_number: Part number
            chunk_size: Size of each chunk (bytes)
            num_chunks: Total number of chunks
            size_bytes: Total plaintext size (bytes)

        Raises:
            OSError: If write fails (fatal to request)
        """
        part_dir = Path(self.part_path(object_id, object_version, part_number))
        part_dir.mkdir(parents=True, exist_ok=True)

        meta_path = self._meta_file(part_dir)

        payload = {
            "chunk_size": int(chunk_size),
            "num_chunks": int(num_chunks),
            "size_bytes": int(size_bytes),
        }

        try:
            await asyncio.to_thread(_write_meta_file, meta_path, payload)
            # Fsync the containing directory to ensure durability of the rename
            await self._fsync_dir_async(part_dir)

            logger.debug(
                f"FS: wrote meta object_id={object_id} v={object_version} part={part_number} num_chunks={num_chunks}"
            )
        except Exception as e:
            logger.error(f"FS meta write failed: object_id={object_id} v={object_version} part={part_number}: {e}")
            raise

    async def delete_meta(self, object_id: str, object_version: int, part_number: int) -> None:
        """Delete meta.json only, un-publishing a part dir while keeping its chunks.

        Used by the append CAS-loser cleanup: part-number reservation is FOR-UPDATE
        serialized, so no live winner shares the loser's dir — removing meta makes the
        dir invisible to readers and the drain reconciler again (restores meta-last)
        and stops a future append that reuses the number from inheriting stale
        readiness over mixed content. Idempotent; failures are logged at ERROR (stale
        readiness left behind) but not raised — the caller is already propagating the
        CAS failure.
        """
        part_dir = Path(self.part_path(object_id, object_version, part_number))
        meta_path = self._meta_file(part_dir)

        def _unlink() -> None:
            meta_path.unlink(missing_ok=True)

        try:
            await asyncio.to_thread(_unlink)
        except OSError as e:
            logger.error(
                f"FS meta delete failed — part stays published with stale readiness: "
                f"object_id={object_id} v={object_version} part={part_number}: {e}"
            )

    async def get_meta(self, object_id: str, object_version: int, part_number: int) -> Optional[dict]:
        """Read metadata from filesystem.

        Args:
            object_id: Object UUID
            object_version: Object version number
            part_number: Part number

        Returns:
            Metadata dict with chunk_size, num_chunks, size_bytes, or None if not present
        """
        part_dir = Path(self.part_path(object_id, object_version, part_number))
        meta_path = self._meta_file(part_dir)

        if not meta_path.exists():
            return None

        try:

            def _read_meta() -> dict:
                with meta_path.open("r") as f:
                    return dict(json.load(f))

            data = await asyncio.to_thread(_read_meta)
            logger.debug(f"FS: read meta object_id={object_id} v={object_version} part={part_number}")
            return data
        except Exception as e:
            logger.warning(f"FS meta read failed: object_id={object_id} v={object_version} part={part_number}: {e}")
            return None

    async def get_meta_with_wait(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        deadline_seconds: float = 30.0,
    ) -> Optional[dict]:
        # Poll get_meta with exponential backoff. The uploader writes meta last
        # (after all chunks), and the consumer may dequeue before the writer's
        # fsync has propagated across the shared cache mount. Within the
        # deadline, "missing" = "writer hasn't finished" = wait. After the
        # deadline, "missing" = genuine fault (writer crashed / FS-evicted /
        # never written) = let the caller raise an after-deadline error so the
        # classifier can route it as permanent.
        loop = asyncio.get_running_loop()
        deadline = loop.time() + deadline_seconds
        backoff = 0.1
        while True:
            meta = await self.get_meta(object_id, object_version, part_number)
            if meta is not None:
                return meta
            if loop.time() >= deadline:
                return None
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 2.0)

    def stat_part(self, object_id: str, object_version: int, part_number: int) -> os.stat_result | None:
        """Stat a part's readiness marker for the janitor's SQL-eviction candidate check.

        BLOCKING — callers wrap in `asyncio.to_thread` (each stat is a CephFS metadata
        roundtrip that must not run on the loop). Stats `meta.json` when present, else the part
        dir, returning None when neither exists — the same "meta.json is the part-complete
        signal, fall back to the dir" rule the walk gates on (see `_descend_object`). The
        returned atime drives the hot-retention skip; a None tells the caller the inventory row
        is stale (the dir is already gone) so it can self-heal by clearing the row.
        """
        part_dir = Path(self.part_path(object_id, object_version, part_number))
        meta_path = self._meta_file(part_dir)
        try:
            return meta_path.stat()
        except OSError:
            pass
        try:
            return part_dir.stat()
        except OSError:
            return None

    async def delete_part(self, object_id: str, object_version: int, part_number: int) -> None:
        """Delete a part directory and attempt to prune empty parent directories.

        This is idempotent and race-safe with concurrent append. Only removes
        directories if empty; ignores non-empty errors.

        Args:
            object_id: Object UUID
            object_version: Object version number
            part_number: Part number
        """
        part_dir = Path(self.part_path(object_id, object_version, part_number))

        def _delete_and_prune() -> bool:
            # One thread hop for the whole sequence: the exists-check, rmtree,
            # and the empty-parent prunes are each a CephFS metadata roundtrip;
            # doing them on-loop serialized every janitor worker behind MDS
            # latency.
            if not part_dir.exists():
                return False
            shutil.rmtree(part_dir)
            for parent in (part_dir.parent, part_dir.parent.parent):
                with contextlib.suppress(OSError):
                    parent.rmdir()  # only succeeds if empty — race-safe by contract
            return True

        try:
            deleted = await asyncio.to_thread(_delete_and_prune)
        except Exception as e:
            logger.warning(
                f"FS: failed to delete part object_id={object_id} v={object_version} part={part_number}: {e}"
            )
            return
        if deleted:
            # DEBUG, not INFO — this fires ~11k times per janitor cycle on prod.
            logger.debug(f"FS: deleted part object_id={object_id} v={object_version} part={part_number}")
        else:
            logger.debug(
                f"FS: delete_part no-op (not present) object_id={object_id} v={object_version} part={part_number}"
            )

    async def delete_object(self, object_id: str, object_version: Optional[int] = None) -> None:
        """Delete an entire object or specific version.

        Args:
            object_id: Object UUID
            object_version: If specified, delete only this version; otherwise delete entire object
        """
        safe_id = self._safe_object_id(object_id)
        object_dir = self.root / safe_id

        if object_version is not None:
            version_dir = object_dir / f"v{int(object_version)}"
            if version_dir.exists():
                try:
                    await asyncio.to_thread(shutil.rmtree, version_dir)
                    logger.info(f"FS: deleted version object_id={object_id} v={object_version}")
                except Exception as e:
                    logger.warning(f"FS: failed to delete version object_id={object_id} v={object_version}: {e}")
                    return

                # Try to prune object dir if empty
                try:
                    if object_dir.exists():
                        object_dir.rmdir()
                        logger.debug(f"FS: pruned empty object dir {object_dir}")
                except OSError:
                    pass  # Not empty; ignore
        else:
            # Delete entire object
            if object_dir.exists():
                try:
                    await asyncio.to_thread(shutil.rmtree, object_dir)
                    logger.info(f"FS: deleted object object_id={object_id}")
                except Exception as e:
                    logger.warning(f"FS: failed to delete object object_id={object_id}: {e}")

    async def _fsync_dir_async(self, directory: Path) -> None:
        """Fsync a directory (async wrapper) to ensure rename durability."""

        def _sync_dir() -> None:
            fd = os.open(str(directory), os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

        with contextlib.suppress(OSError):
            await asyncio.to_thread(_sync_dir)
