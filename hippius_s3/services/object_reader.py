from __future__ import annotations

# `db` LIFETIME — READ BEFORE ADDING A QUERY HERE.
# get_object_endpoint does `db = await pool.acquire()` and releases it in a `finally` that runs
# when the endpoint RETURNS — i.e. when the StreamingResponse object is handed to the ASGI
# server, before a single body byte is produced. So `db` is only ours until build_stream_context
# finishes. Anything reached from inside the response body (stream_plan's wait/decrypt path)
# must do ZERO DB work: by then the connection is back in the pool and probably owned by another
# request, and asyncpg Connections are not safe for concurrent use. Violating this raises
# `InterfaceError: another operation is in progress` — which surfaces as a 500 before the first
# byte, or a 200 with a full Content-Length and a truncated body after it, and can also break the
# unrelated request that now legitimately holds that connection. If the body genuinely needs
# something from the DB, resolve it in build_stream_context and close over the VALUE, never `db`.
import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass
from typing import Any
from typing import AsyncGenerator

from fastapi import Response
from fastapi.responses import StreamingResponse

from hippius_s3.api.s3.common import build_headers
from hippius_s3.backend_routing import resolve_object_backends
from hippius_s3.cache.dual_fs_store import DualFileSystemPartsStore
from hippius_s3.cache.dual_fs_store import _stream_tiers
from hippius_s3.cache.notifier import ChunkNotReadyError
from hippius_s3.cache.read_recency import get_read_recency_recorder
from hippius_s3.config import get_config
from hippius_s3.queue import DownloadChainRequest
from hippius_s3.queue import PartChunkSpec
from hippius_s3.queue import PartToDownload
from hippius_s3.queue import enqueue_download_request
from hippius_s3.reader.db_meta import read_parts_list
from hippius_s3.reader.planner import build_chunk_plan
from hippius_s3.reader.streamer import stream_plan
from hippius_s3.reader.types import ChunkPlanItem
from hippius_s3.reader.types import RangeRequest
from hippius_s3.services.crypto_service import CryptoService
from hippius_s3.services.envelope_service import unwrap_dek
from hippius_s3.services.kek_service import get_bucket_kek_bytes
from hippius_s3.services.ray_id_service import ray_id_context
from hippius_s3.storage_version import require_supported_storage_version
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)


class DownloadNotReadyError(Exception):
    pass


@dataclass
class StreamContext:
    plan: list[ChunkPlanItem]
    object_version: int
    storage_version: int
    source: str
    key_bytes: bytes | None
    suite_id: str | None
    bucket_id: str
    upload_id: str


# RQ-4: compare-and-delete Lua — delete the coalesce lock only while it still holds our token, so we
# never steal a lock a later streamer/downloader re-acquired. Fixed script (no user input in the body);
# mirrors the downloader's release. The key format must match _enqueue_missing_downloads exactly.
_COALESCE_LOCK_RELEASE_LUA = (
    "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end"
)


async def _release_coalesce_locks(
    redis: Any,
    *,
    object_id: str,
    object_version: int,
    part_numbers: set[int],
    ray_token: str,
) -> None:
    """Best-effort CAD-release of this streamer's per-part coalesce locks (RQ-4).

    Called when the first-chunk wait times out so the next GET re-enqueues immediately instead of
    waiting out the lock TTL. CAD-on-token means locks we don't own (lost the coalesce race) no-op.
    """
    for pn in part_numbers:
        lock_key = f"download_in_progress:{object_id}:v:{int(object_version)}:part:{int(pn)}"
        with contextlib.suppress(Exception):
            await redis.eval(_COALESCE_LOCK_RELEASE_LUA, 1, lock_key, ray_token)


# RQ-3: backends whose downloader fetches by content id (CID) rather than a deterministic
# backend_identifier. Only for these does the per-part CID resolution matter; Arion (the production
# backend) is deterministically addressed and its downloader ignores spec.cid entirely.
_CID_ADDRESSED_BACKENDS: frozenset[str] = frozenset({"ipfs"})


async def _enqueue_download_for_parts(
    db: Any,
    redis: Any,
    info: dict,
    *,
    object_version: int,
    storage_version: int,
    indices_by_part: dict[int, set[int]],
    address: str,
    cfg: Any,
) -> None:
    """Coalesce-lock the given parts and enqueue ONE DownloadChainRequest for the un-coalesced ones.

    Shared core of two call sites, BOTH of which run before streaming starts while this request
    still legitimately owns `db`: the primary cold-read miss and the envelope-race version fallback.
    Nothing may call this from inside the response body — see the module note on `db` lifetime.
    Idempotent: the per-part SET NX lock guarantees only one enqueue per (object_id, version, part)
    until the lock TTL, so calling it again for a part already being fetched is a no-op.
    """
    if not indices_by_part:
        return
    object_id = info["object_id"]

    # Coalesce concurrent misses on the same part: only one streamer
    # actually enqueues a download request per (object_id, version, part).
    # Others will just wait on the pub/sub notification emitted by the
    # downloader. The lock TTL covers crashed-streamer / crashed-downloader
    # cases — on TTL expiry, the next miss re-enqueues.
    lock_ttl = int(getattr(cfg, "download_coalesce_lock_ttl_seconds", 120))
    ray_token = str(info.get("ray_id") or "anonymous")
    # RD-6: acquire every part's coalesce lock in one pipelined round trip instead of one SET NX per
    # part. Fail-open on a Redis hiccup (treat all as acquired) so the download still happens — a
    # duplicate enqueue is deduped by the downloader via chunk_exists.
    parts = list(indices_by_part.keys())
    lock_keys = [f"download_in_progress:{object_id}:v:{object_version}:part:{pn}" for pn in parts]
    try:
        pipe = redis.pipeline(transaction=False)
        for lk in lock_keys:
            pipe.set(lk, ray_token, nx=True, ex=lock_ttl)
        set_results = await pipe.execute()
    except Exception:
        set_results = [True] * len(parts)

    acquired_parts: set[int] = set()
    for pn, acquired in zip(parts, set_results, strict=True):
        if acquired:
            acquired_parts.add(pn)
        else:
            logger.debug(
                "download coalesced: another streamer is fetching object_id=%s v=%s part=%s (lock held)",
                object_id,
                object_version,
                pn,
            )

    # If every missing part is already being fetched by someone else,
    # we don't enqueue anything — we just fall through to stream_plan,
    # which will wait on pub/sub for each chunk.
    # RQ-3: the downloader resolves each chunk's location from chunk_backend itself and never reads
    # spec.cid — CIDs only matter to a content-addressed backend. Resolve the object's backends once
    # and skip the per-part CID query entirely unless a CID-addressed backend actually serves it.
    db_backends = await resolve_object_backends(db, object_id, object_version)
    needs_cid = bool(set(db_backends) & _CID_ADDRESSED_BACKENDS)

    dl_parts: list[PartToDownload] = []
    for pn, idxs in indices_by_part.items():
        if pn not in acquired_parts:
            continue
        include = {int(i) for i in idxs}
        by_index: dict[int, tuple[str | None, int | None]] = {}
        if needs_cid:
            try:
                rows = await db.fetch(
                    get_query("get_part_chunks_by_object_and_number"),
                    object_id,
                    object_version,
                    int(pn),
                )
                for r in rows or []:
                    ci = int(r[0])
                    if ci not in include:
                        continue
                    cid_raw = r[1]
                    cid_val = str(cid_raw).strip() if cid_raw is not None else None
                    if cid_val and cid_val.lower() in {"", "none", "pending"}:
                        cid_val = None
                    clen = int(r[2]) if (len(r) > 2 and r[2] is not None) else None
                    by_index[ci] = (cid_val, clen)
            except Exception:
                # If chunk metadata isn't present (common for CID-less objects), keep cid=None
                by_index = {}

        specs: list[PartChunkSpec] = []
        for ci in sorted(include):
            cid_val, clen = by_index.get(int(ci), (None, None))
            specs.append(PartChunkSpec(index=int(ci), cid=cid_val, cipher_size_bytes=clen))

        dl_parts.append(PartToDownload(part_number=int(pn), chunks=specs))
    if dl_parts:
        req = DownloadChainRequest(
            request_id=f"{object_id}::shared",
            object_id=object_id,
            object_version=object_version,
            object_storage_version=int(storage_version),
            object_key=info.get("object_key", ""),
            bucket_name=info.get("bucket_name", ""),
            address=address,
            subaccount=address,
            substrate_url=cfg.substrate_url,
            size=int(info.get("size_bytes") or 0),
            multipart=bool(info.get("multipart")),
            chunks=dl_parts,
            ray_id=info.get("ray_id"),
            download_backends=db_backends if db_backends else None,
            # A6: a genuine backstop so the downloader's stale-discard isn't dead for read-path
            # DCRs. cache_ttl is the longest any streamer waits, so a DCR older than that has no
            # live waiter and is safe to drop unprocessed.
            expire_at=time.time() + float(cfg.cache_ttl_seconds),
        )
        await enqueue_download_request(req)


async def _enqueue_missing_downloads(
    db: Any,
    redis: Any,
    info: dict,
    *,
    object_version: int,
    storage_version: int,
    plan: list[ChunkPlanItem],
    exist_results: list[bool],
    address: str,
    cfg: Any,
    skip_parts: set[int] | None = None,
) -> None:
    """Enqueue a DownloadChainRequest for the chunks in `plan` that are missing from the FS cache.

    Coalesces concurrent misses per (object_id, version, part) via a Redis NX lock so only one
    streamer enqueues; the rest wait on the downloader's pub/sub notification. Shared by the primary
    read path AND the envelope-race version fallback — the fallback used to return a `pipeline`
    source without enqueuing anything, so a cold read of the fallback version hung on pub/sub until
    the wait timed out.

    `skip_parts` are missing parts a peer holds unreplicated (see `_peer_held_unreplicated_parts`):
    Arion cannot have them yet, so the download would only come back empty ~1s later.
    """
    indices_by_part: dict[int, set[int]] = {}
    for item, cached in zip(plan, exist_results, strict=True):
        if not cached and int(item.part_number) not in (skip_parts or ()):
            indices_by_part.setdefault(int(item.part_number), set()).add(int(item.chunk_index))
    await _enqueue_download_for_parts(
        db,
        redis,
        info,
        object_version=object_version,
        storage_version=storage_version,
        indices_by_part=indices_by_part,
        address=address,
        cfg=cfg,
    )


def _dual_store(obj_cache: Any) -> DualFileSystemPartsStore | None:
    """The tiered store behind the cache facade, or None where there is no peer/pool tiering."""
    fs = getattr(obj_cache, "fs", None)
    return fs if isinstance(fs, DualFileSystemPartsStore) else None


async def _peer_held_unreplicated_parts(
    obj_cache: Any, object_id: str, object_version: int, plan: list[ChunkPlanItem], exist_results: list[bool]
) -> set[int]:
    """The missing parts a reachable peer holds on SSD while the pool (and so Arion) has no copy.

    A fresh part lives on its ingest node alone until the drain replicates it, and Arion is
    uploaded from the pool copy after that — so a DownloadChainRequest for such a part comes back
    with nothing. The peer tier serves it on the way through `wait_for_chunk` instead. Runs in
    build_stream_context, where the resolver's Postgres query is still allowed; the answer is
    memoised per part, so the streamer's own peer fetches pay nothing extra for it.
    """
    dual = _dual_store(obj_cache)
    if dual is None:
        return set()
    missing = {int(item.part_number) for item, cached in zip(plan, exist_results, strict=True) if not cached}
    held: set[int] = set()
    for pn in sorted(missing):
        owner, unreplicated = await dual.peer_locate(object_id, object_version, pn)
        if owner is not None and unreplicated:
            held.add(pn)
    return held


async def build_stream_context(
    db: Any,
    redis: Any,
    obj_cache: Any,
    info: dict,
    *,
    rng: RangeRequest | None,
    address: str,
    parts: list[dict] | None = None,
) -> StreamContext:
    cfg = get_config()
    storage_version = require_supported_storage_version(int(info["storage_version"]))
    # v4-only policy: always decrypt at read time.

    ov = int(info.get("object_version") or info.get("current_object_version") or 1)
    # RD-3: the GET endpoint already built the parts catalog; reuse it instead of re-reading. HEAD and
    # copy callers pass nothing and keep the DB read.
    if parts is None:
        parts = await read_parts_list(db, info["object_id"], ov)
    plan = await build_chunk_plan(db, info["object_id"], parts, rng, object_version=ov)

    # Batch check all chunks in a single Redis pipeline round trip
    source = "cache"
    checks = [(int(item.part_number), int(item.chunk_index)) for item in plan]
    exist_results = await obj_cache.chunks_exist_batch(info["object_id"], ov, checks)

    # Build missing set from batch results
    indices_by_part: dict[int, set[int]] = {}
    for item, cached in zip(plan, exist_results, strict=True):
        if not cached:
            source = "pipeline"
            idx_set = indices_by_part.setdefault(int(item.part_number), set())
            idx_set.add(int(item.chunk_index))

    if source == "pipeline":
        await _enqueue_missing_downloads(
            db,
            redis,
            info,
            object_version=ov,
            storage_version=storage_version,
            plan=plan,
            exist_results=exist_results,
            address=address,
            cfg=cfg,
            skip_parts=await _peer_held_unreplicated_parts(obj_cache, info["object_id"], ov, plan, exist_results),
        )

    object_version = int(info.get("object_version") or info.get("current_object_version") or 1)
    bucket_id = str(info.get("bucket_id") or "")
    upload_id = str(info.get("upload_id") or "")
    suite_id: str | None = None
    key_bytes: bytes | None = None

    suite_id = str(info.get("enc_suite_id") or "hip-enc/aes256gcm")
    kek_id = info.get("kek_id")
    wrapped_dek = info.get("wrapped_dek")
    if not bucket_id or not kek_id or not wrapped_dek:
        # Current version is mid-write (overwrite in progress). Fall back to the highest COMPLETED
        # version below it. Not `object_version - 1`: numbering is sparse, because an aborted MPU
        # retains its reserved row (abort_cleanup_orphan_version.sql) and the migrator mints
        # versions out of band, so the immediately-preceding number can be a placeholder with no
        # envelope — falling onto one turns a recoverable read into a 500.
        prev_version = await db.fetchval(
            get_query("get_prev_serveable_version"),
            info.get("object_id"),
            object_version,
        )
        if prev_version:
            logger.warning(
                "Envelope missing on v%s of %s, falling back to v%s",
                object_version,
                info.get("object_id"),
                prev_version,
            )
            prev_info = await db.fetchrow(
                get_query("get_object_for_download_with_permissions_by_version"),
                info.get("bucket_name"),
                info.get("object_key"),
                prev_version,
            )
            if prev_info and prev_info.get("kek_id") and prev_info.get("wrapped_dek"):
                # Use the previous version's envelope and data (single attempt, no recursion)
                info = dict(prev_info)
                object_version = int(info.get("object_version") or info.get("current_object_version") or prev_version)
                bucket_id = str(info.get("bucket_id") or "")
                suite_id = str(info.get("enc_suite_id") or "hip-enc/aes256gcm")
                kek_id = info["kek_id"]
                wrapped_dek = info["wrapped_dek"]
                parts = await read_parts_list(db, info["object_id"], object_version)
                plan = await build_chunk_plan(db, info["object_id"], parts, rng, object_version=object_version)
                checks = [(int(item.part_number), int(item.chunk_index)) for item in plan]
                exist_results = await obj_cache.chunks_exist_batch(info["object_id"], object_version, checks)
                source = "cache" if all(exist_results) else "pipeline"
                if source == "pipeline":
                    # Cold read of the fallback version: enqueue the missing chunks so the streamer's
                    # pub/sub wait is actually fulfilled instead of hanging until it times out.
                    await _enqueue_missing_downloads(
                        db,
                        redis,
                        info,
                        object_version=object_version,
                        storage_version=storage_version,
                        plan=plan,
                        exist_results=exist_results,
                        address=address,
                        cfg=cfg,
                        skip_parts=await _peer_held_unreplicated_parts(
                            obj_cache, info["object_id"], object_version, plan, exist_results
                        ),
                    )
                kek_bytes = await get_bucket_kek_bytes(bucket_id=bucket_id, kek_id=kek_id)
                aad = f"hippius-dek:{bucket_id}:{info['object_id']}:{object_version}".encode("utf-8")
                key_bytes = unwrap_dek(kek=kek_bytes, wrapped_dek=bytes(wrapped_dek), aad=aad)
                return StreamContext(
                    plan=plan,
                    object_version=object_version,
                    storage_version=storage_version,
                    source=source,
                    key_bytes=key_bytes,
                    suite_id=suite_id,
                    bucket_id=bucket_id,
                    upload_id=str(info.get("upload_id") or ""),
                )
        raise RuntimeError("v5_missing_envelope_metadata")
    kek_bytes = await get_bucket_kek_bytes(bucket_id=bucket_id, kek_id=kek_id)
    aad = f"hippius-dek:{bucket_id}:{info.get('object_id')}:{object_version}".encode("utf-8")
    key_bytes = unwrap_dek(kek=kek_bytes, wrapped_dek=bytes(wrapped_dek), aad=aad)
    if not CryptoService.is_supported_suite_id(suite_id):
        raise RuntimeError(f"unsupported_enc_suite_id:{suite_id}")
    return StreamContext(
        plan=plan,
        object_version=object_version,
        storage_version=storage_version,
        source=source,
        key_bytes=key_bytes,
        suite_id=suite_id,
        bucket_id=bucket_id,
        upload_id=upload_id,
    )


async def _touch_plan_parts(object_id: str, ctx: StreamContext) -> None:
    """Stamp every part of a multi-part read as used NOW, before the first chunk is waited on.

    The recorder uses its own pool, so this is not DB work on the request's `db` — but the module
    rule stands: nothing here runs from inside the response body. The per-chunk stamp in the store
    only reaches a part when its first chunk streams, and on a long stream that can be minutes
    later — after the evictor has already ranked the tail parts of this very object as its
    coldest. Single-part reads are covered by the per-chunk stamp at no extra round trip. The
    recorder swallows its own failures and bounds its wait, so this cannot fail or stall the read.
    """
    recorder = get_read_recency_recorder()
    if recorder is None:
        return
    # `getattr` rather than attribute access: tests drive this with bare `object()` plan items.
    plan_parts = sorted({int(item.part_number) for item in ctx.plan if getattr(item, "part_number", None) is not None})
    if len(plan_parts) > 1:
        await recorder.touch_parts(object_id, int(ctx.object_version), plan_parts)


async def read_response(
    db: Any,
    redis: Any,
    obj_cache: Any,
    info: dict,
    *,
    read_mode: str,
    rng: RangeRequest | None,
    address: str,
    range_was_invalid: bool = False,
    parts: list[dict] | None = None,
) -> Response:
    cfg = get_config()
    # RD-3: the endpoint already built the parts catalog; pass it through so build_stream_context and
    # the planner don't re-read `parts`. None → they read it themselves (unchanged).
    ctx = await build_stream_context(
        db,
        redis,
        obj_cache,
        info,
        rng=rng,
        address=address,
        parts=parts,
    )
    await _touch_plan_parts(str(info["object_id"]), ctx)
    gen = stream_plan(
        obj_cache=obj_cache,
        object_id=info["object_id"],
        object_version=ctx.object_version,
        plan=ctx.plan,
        storage_version=ctx.storage_version,
        key_bytes=ctx.key_bytes,
        suite_id=ctx.suite_id,
        bucket_id=ctx.bucket_id,
        upload_id=ctx.upload_id,
        address=address,
        bucket_name=str(info.get("bucket_name", "")),
        prefetch_chunks=int(getattr(cfg, "http_stream_prefetch_chunks", 0) or 0),
        chunk_timeout=float(cfg.stream_chunk_timeout_seconds),
    )
    # A2: bound the wait for the FIRST chunk. `stream_plan` otherwise waits up to cache_ttl_seconds
    # (~1h) per chunk, so an un-drained object whose part is on no backend yet would hang the whole
    # GET for an hour. Peek the first chunk here, before the StreamingResponse is returned, so a
    # first-chunk timeout surfaces as a retryable 503 (DownloadNotReadyError, caught by the endpoint)
    # *before* the 200/206 headers are committed. Warm reads return the chunk immediately. A3 bounds
    # each LATER chunk to stream_chunk_timeout_seconds (via chunk_timeout above), so a mid-stream
    # permanent failure breaks the stream in minutes instead of hanging the open response ~1h.
    first_timeout = float(cfg.stream_first_chunk_timeout_seconds)
    first_chunk: bytes | None = None
    # Per-stream tier counts. Set BEFORE the first-chunk peek, in the request's own context, so
    # every fetch task the streamer spawns — during the peek here and later from the response
    # body — inherits the same dict by reference and counts into it.
    tiers: dict[str, int] = {}
    _stream_tiers.set(tiers)
    try:
        first_chunk = await asyncio.wait_for(gen.__anext__(), timeout=first_timeout)
    except StopAsyncIteration:
        first_chunk = None  # empty (zero-byte) object — nothing to stream
    except (TimeoutError, asyncio.TimeoutError, ChunkNotReadyError) as exc:
        # ChunkNotReadyError: the downloader gave up fast on a backend miss and notified anyway, so
        # the peek woke to an empty cache. Same retryable outcome as a timeout — a 503, not a 500.
        await gen.aclose()
        # RQ-4: release the coalesce locks this streamer set (CAD on our ray token) so the client's
        # retry re-enqueues immediately rather than waiting out the lock TTL (default 600s).
        await _release_coalesce_locks(
            redis,
            object_id=str(info["object_id"]),
            object_version=int(ctx.object_version),
            # Best-effort on the error path: a malformed plan item must never turn the 503 into a 500.
            part_numbers={
                int(getattr(item, "part_number", 0))
                for item in ctx.plan
                if getattr(item, "part_number", None) is not None
            },
            ray_token=str(info.get("ray_id") or "anonymous"),
        )
        raise DownloadNotReadyError(
            "Parts not ready: first chunk did not arrive within the initial stream timeout"
        ) from exc

    object_id = str(info["object_id"])
    ray_id = str(info.get("ray_id") or ray_id_context.get())
    dual = _dual_store(obj_cache)
    # Best-effort like the error path above: a malformed plan item must not fail the stream log.
    first_part = getattr(ctx.plan[0], "part_number", None) if ctx.plan else None

    async def _body() -> AsyncGenerator[bytes, None]:
        nonlocal first_chunk
        yielded = 0
        # try/finally, not try/except: nothing is swallowed. The finally is the one place that
        # sees a stream end for every reason (complete, client gone, mid-stream fault) and it is
        # where the per-stream tier split is worth logging. `peer_last_owner` is memo-only — no
        # DB work from inside the body, per the module note.
        try:
            if first_chunk is not None:
                yielded += len(first_chunk)
                yield first_chunk
                first_chunk = None  # release the (up to ~4 MiB) first chunk for the rest of the stream
            async for chunk in gen:
                yielded += len(chunk)
                yield chunk
        finally:
            # `set(None)` rather than `reset(token)`: the body runs in whatever task the ASGI
            # server streams from, which may be a child context of the one the token came from,
            # and resetting a token in a different context raises. The dict itself is closed over.
            _stream_tiers.set(None)
            owner = None
            if dual is not None and first_part is not None:
                owner = dual.peer_last_owner(object_id, ctx.object_version, int(first_part))
            logger.info(
                "STREAM tiers ray_id=%s object_id=%s v=%s local=%d peer=%d pool=%d bytes=%d owner=%s",
                ray_id,
                object_id,
                ctx.object_version,
                tiers.get("local", 0),
                tiers.get("peer", 0),
                tiers.get("pool", 0),
                yielded,
                owner,
            )

    headers = build_headers(
        info,
        source=ctx.source,
        metadata=info.get("metadata") or {},
        rng=(rng.start, rng.end) if rng is not None else None,
        range_was_invalid=range_was_invalid,
    )
    status_code = 200 if rng is None or range_was_invalid else 206
    return StreamingResponse(
        _body(),
        status_code=status_code,
        media_type=info.get("content_type", "application/octet-stream"),
        headers=headers,
    )


async def stream_object(
    db: Any,
    redis: Any,
    obj_cache: Any,
    info: dict,
    *,
    rng: RangeRequest | None,
    address: str,
    bound_first_chunk: bool = False,
) -> Any:
    """Return an async iterator of plaintext bytes for the requested object.

    This wraps build_stream_context and stream_plan so callers don't need to know
    about parts catalogs, chunk plans, or downloader details.

    A2/A3: `bound_first_chunk=True` (used by streaming CopyObject, which reads a *source* object)
    eagerly peeks the first chunk under `stream_first_chunk_timeout_seconds` and raises
    `DownloadNotReadyError` if it doesn't arrive — so a copy whose source is still draining fails
    fast with a retryable 503 *before* the caller writes a partial destination, instead of hanging
    up to cache_ttl. Every chunk is bounded by `stream_chunk_timeout_seconds` regardless.
    """
    cfg = get_config()
    ctx = await build_stream_context(
        db,
        redis,
        obj_cache,
        info,
        rng=rng,
        address=address,
    )
    # A streaming CopyObject reads its whole multi-part source; its tail parts need the same
    # up-front stamp as a GET's, or the evictor can take them while the head is still copying.
    await _touch_plan_parts(str(info["object_id"]), ctx)
    gen = stream_plan(
        obj_cache=obj_cache,
        object_id=info["object_id"],
        object_version=ctx.object_version,
        plan=ctx.plan,
        storage_version=ctx.storage_version,
        key_bytes=ctx.key_bytes,
        suite_id=ctx.suite_id,
        bucket_id=ctx.bucket_id,
        upload_id=ctx.upload_id,
        address=address,
        bucket_name=str(info.get("bucket_name", "")),
        prefetch_chunks=int(getattr(cfg, "http_stream_prefetch_chunks", 0) or 0),
        chunk_timeout=float(cfg.stream_chunk_timeout_seconds),
    )
    if not bound_first_chunk:
        return gen

    # Eager bounded first-chunk peek (A2), mirroring read_response, so a not-ready source surfaces
    # as DownloadNotReadyError before any destination bytes are written.
    try:
        first_chunk = await asyncio.wait_for(gen.__anext__(), timeout=float(cfg.stream_first_chunk_timeout_seconds))
    except StopAsyncIteration:
        first_chunk = None  # empty (zero-byte) source
    except (TimeoutError, asyncio.TimeoutError, ChunkNotReadyError) as exc:
        # See read_response: a terminal miss is retryable, so map it to 503 rather than a 500.
        await gen.aclose()
        raise DownloadNotReadyError(
            "Parts not ready: source first chunk did not arrive within the initial stream timeout"
        ) from exc

    async def _bounded() -> AsyncGenerator[bytes, None]:
        if first_chunk is not None:
            yield first_chunk
        async for chunk in gen:
            yield chunk

    return _bounded()
