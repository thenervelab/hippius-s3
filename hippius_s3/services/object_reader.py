from __future__ import annotations

# `db` LIFETIME — READ BEFORE ADDING A QUERY HERE.
# get_object_endpoint holds its pooled connection only for the `async with acquire_with_timeout`
# block that ends right after build_stream_context returns — BEFORE read_response's first-chunk
# wait, and before the StreamingResponse object is handed to the ASGI server. (It used to be held
# until the endpoint returned; a cold read then pinned a pool slot for the whole
# stream_first_chunk_timeout_seconds, and ~60 such reads on one pod starved every PUT on it into
# a pool-acquire 503 — observed 2026-09-08.) So `db` is only ours until build_stream_context
# finishes. Anything reached from inside the response body (stream_plan's fetch/decrypt path)
# must do ZERO DB work: by then the connection is back in the pool and probably owned by another
# request, and asyncpg Connections are not safe for concurrent use. Violating this raises
# `InterfaceError: another operation is in progress` — which surfaces as a 500 before the first
# byte, or a 200 with a full Content-Length and a truncated body after it, and can also break the
# unrelated request that now legitimately holds that connection. If the body genuinely needs
# something from the DB, resolve it in build_stream_context and close over the VALUE, never `db`.
# This is why every missing chunk's backend location is resolved HERE, up front, and carried on
# the StreamContext: the body fetches by those values and never asks the DB where a chunk is.
import asyncio
import logging
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import AsyncGenerator
from typing import Literal

from fastapi import Response
from fastapi.responses import StreamingResponse

from hippius_s3.api.s3.common import build_headers
from hippius_s3.backend_routing import resolve_object_backends
from hippius_s3.config import get_config
from hippius_s3.reader.backend_fetch import BackendLocation
from hippius_s3.reader.backend_fetch import ChunkUnavailableError
from hippius_s3.reader.backend_fetch import get_backend_fetcher
from hippius_s3.reader.db_meta import read_parts_list
from hippius_s3.reader.planner import build_chunk_plan
from hippius_s3.reader.streamer import FetchMissingFn
from hippius_s3.reader.streamer import stream_plan
from hippius_s3.reader.types import ChunkPlanItem
from hippius_s3.reader.types import RangeRequest
from hippius_s3.services.crypto_service import CryptoService
from hippius_s3.services.envelope_service import unwrap_dek
from hippius_s3.services.kek_service import get_bucket_kek_bytes
from hippius_s3.storage_version import require_supported_storage_version
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)


# Why a read could not produce its first chunk. Bounded so it can be a log field and, later, a
# metric label. `chunk_unavailable`: a tier reported failure — no backend location after the
# wait, fetch budget saturated, a bounded backend timeout (the fetcher's own connect/read/pool
# bounds sit inside the first-chunk bound, so a hung Arion fetch lands HERE with "timed out" in
# the message), or every location failed. `first_chunk_timeout`: nothing reported a failure
# before the reader's own bound — a stall the httpx bounds do not reach (a local tier read,
# decrypt, or an env override that broke queue + connect + read < first — or a retried sequence
# of transient Arion errors (5xx, connection reset), each attempt re-paying the slot wait and
# backoff; check the `backend chunk fetch failed … retry=True` lines first). Rare by design; when
# it fires, look outside the Arion client. The two need different responders; for 17 h in
# 2026-09 they logged the same sentence.
NotReadyCause = Literal["first_chunk_timeout", "chunk_unavailable"]


class DownloadNotReadyError(Exception):
    def __init__(self, message: str, *, cause: NotReadyCause) -> None:
        super().__init__(message)
        self.cause: NotReadyCause = cause


# Where each chunk of the plan can be fetched from when no local tier holds it, keyed by
# (part_number, chunk_index), in the object's download-backend order. Empty for a chunk no backend
# holds yet (its part is still in its upload window).
ChunkLocations = dict[tuple[int, int], tuple[BackendLocation, ...]]


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
    locations: ChunkLocations = field(default_factory=dict)


# A warm plan longer than this resolves locations too: the body of a multi-GB read runs for
# minutes, long enough for the evictor to drop a chunk the plan-time check saw, and the body
# never touches the DB — so a long warm read carries its backend fallback from the start.
# 64 chunks × 4 MiB = 256 MiB; below that a mid-stream eviction is unlikely enough that the
# extra query on every warm GET is not worth it.
_RESOLVE_LOCATIONS_MIN_CHUNKS = 64


def _needs_locations(source: str, plan: list[ChunkPlanItem]) -> bool:
    return source == "pipeline" or len(plan) > _RESOLVE_LOCATIONS_MIN_CHUNKS


async def _resolve_chunk_locations(db: Any, object_id: str, object_version: int) -> ChunkLocations:
    """Every backend location of every chunk of the version, in download-backend order.

    One query per backend the object is served from (RD-1's batched lookup), resolved while this
    request still owns `db`. The whole version is resolved rather than just the plan's misses: a
    chunk the local tiers held at plan time can be evicted before the body reaches it, and the
    body may not ask the DB.
    """
    backends = await resolve_object_backends(db, object_id, object_version)
    locations: dict[tuple[int, int], list[BackendLocation]] = {}
    for backend in backends:
        rows = await db.fetch(
            get_query("get_chunk_backend_identifiers_by_part"), backend, object_id, int(object_version)
        )
        for row in rows or []:
            identifier = row["backend_identifier"]
            if not identifier:
                continue
            locations.setdefault((int(row["part_number"]), int(row["chunk_index"])), []).append(
                (backend, str(identifier))
            )
    return {key: tuple(found) for key, found in locations.items()}


def make_fetch_missing(ctx: StreamContext, obj_cache: Any, *, object_id: str, address: str) -> FetchMissingFn:
    """The streamer's lowest tier for one request: fetch a chunk from its backend into memory.

    Closes over the locations resolved in `build_stream_context` — never over `db`. A chunk with
    no location is on no backend yet (its part is inside the upload window, on some node's SSD);
    only a peer can serve it, so the local tiers are re-polled for a bounded time before the
    request gives up with a retryable error.
    """
    cfg = get_config()
    fetcher = get_backend_fetcher()
    object_version = int(ctx.object_version)
    wait_s = float(getattr(cfg, "read_missing_chunk_wait_seconds", 10.0))

    async def _fetch_missing(item: ChunkPlanItem) -> bytes:
        key = (int(item.part_number), int(item.chunk_index))
        locations = ctx.locations.get(key, ())
        if locations:
            return await fetcher.fetch(locations, address)
        deadline = asyncio.get_running_loop().time() + wait_s
        while True:
            await asyncio.sleep(1.0)
            cached = await obj_cache.get_chunk(object_id, object_version, key[0], key[1])
            if cached is not None:
                return cached
            if asyncio.get_running_loop().time() >= deadline:
                raise ChunkUnavailableError(
                    f"chunk on no backend yet and no local tier served it within {wait_s:.0f}s: "
                    f"{object_id} v{object_version} part {key[0]} chunk {key[1]}"
                )

    return _fetch_missing


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
    storage_version = require_supported_storage_version(int(info["storage_version"]))
    # v4-only policy: always decrypt at read time.

    ov = int(info.get("object_version") or info.get("current_object_version") or 1)
    # RD-3: the GET endpoint already built the parts catalog; reuse it instead of re-reading. HEAD and
    # copy callers pass nothing and keep the DB read.
    if parts is None:
        parts = await read_parts_list(db, info["object_id"], ov)
    plan = await build_chunk_plan(db, info["object_id"], parts, rng, object_version=ov)

    # Batch check all chunks in a single pass; a plan with a miss pays for the location lookup,
    # and so does a long warm one (see `_needs_locations`).
    checks = [(int(item.part_number), int(item.chunk_index)) for item in plan]
    exist_results = await obj_cache.chunks_exist_batch(info["object_id"], ov, checks)
    source = "cache" if all(exist_results) else "pipeline"
    locations: ChunkLocations = {}
    if _needs_locations(source, plan):
        locations = await _resolve_chunk_locations(db, info["object_id"], ov)

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
                # Cold read of the fallback version: resolve ITS chunks' locations, not the
                # current version's — the body fetches whatever this context says.
                locations = (
                    await _resolve_chunk_locations(db, info["object_id"], object_version)
                    if _needs_locations(source, plan)
                    else {}
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
                    locations=locations,
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
        locations=locations,
    )


def _stream(ctx: StreamContext, obj_cache: Any, info: dict, *, address: str) -> AsyncGenerator[bytes, None]:
    cfg = get_config()
    return stream_plan(
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
        fetch_missing=make_fetch_missing(ctx, obj_cache, object_id=str(info["object_id"]), address=address),
        has_backend_copy=lambda item: (int(item.part_number), int(item.chunk_index)) in ctx.locations,
    )


async def read_response(
    ctx: StreamContext,
    redis: Any,
    obj_cache: Any,
    info: dict,
    *,
    read_mode: str,
    rng: RangeRequest | None,
    address: str,
    range_was_invalid: bool = False,
) -> Response:
    """Turn an already-built StreamContext into the streaming response.

    Takes the context rather than `db` on purpose: everything below — the first-chunk wait (up
    to stream_first_chunk_timeout_seconds on a cold read) and the body — does zero DB work, so
    the endpoint releases its pooled connection before calling this. See the module note.
    """
    cfg = get_config()
    gen = _stream(ctx, obj_cache, info, address=address)
    # A2: bound the wait for the FIRST chunk, so a cold read whose chunk cannot be served (a part
    # still inside its upload window on another node, a backend outage) surfaces as a retryable
    # 503 (DownloadNotReadyError, caught by the endpoint) *before* the 200/206 headers are
    # committed. Warm reads return the chunk immediately. A3 bounds each LATER chunk to
    # stream_chunk_timeout_seconds (via chunk_timeout in `_stream`), so a mid-stream permanent
    # failure breaks the stream in minutes instead of hanging the open response.
    first_timeout = float(cfg.stream_first_chunk_timeout_seconds)
    first_chunk: bytes | None = None
    try:
        first_chunk = await asyncio.wait_for(gen.__anext__(), timeout=first_timeout)
    except StopAsyncIteration:
        first_chunk = None  # empty (zero-byte) object — nothing to stream
    except (TimeoutError, asyncio.TimeoutError) as exc:
        await gen.aclose()
        raise DownloadNotReadyError(
            "Parts not ready: first chunk did not arrive within the initial stream timeout",
            cause="first_chunk_timeout",
        ) from exc
    except ChunkUnavailableError as exc:
        # No tier could serve the chunk (nothing on a backend yet, or the backend fetch failed after
        # its retries). Same retryable outcome as a timeout — a 503, not a 500 — but a different
        # cause, and the fetcher's reason travels in the message so the log says which tier gave up.
        await gen.aclose()
        raise DownloadNotReadyError(f"Parts not ready: {exc}", cause="chunk_unavailable") from exc

    async def _body() -> AsyncGenerator[bytes, None]:
        nonlocal first_chunk
        # `finally: aclose()` runs the streamer's own cleanup (cancel the prefetch tasks, which
        # releases their backend-budget slots and closes their HTTP streams) the moment the
        # response ends — a client that disconnects mid-stream must not leave up to
        # prefetch+1 chunks' worth of fetches running until the generator is garbage-collected.
        try:
            if first_chunk is not None:
                yield first_chunk
                first_chunk = None  # release the (up to ~4 MiB) first chunk for the rest of the stream
            async for chunk in gen:
                yield chunk
        finally:
            await gen.aclose()

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
    about parts catalogs, chunk plans, or backend locations.

    A2/A3: `bound_first_chunk=True` (used by streaming CopyObject and UploadPartCopy, which read a
    *source* object) eagerly peeks the first chunk under `stream_first_chunk_timeout_seconds` and
    raises `DownloadNotReadyError` if it doesn't arrive — so a copy whose source is not servable
    yet fails fast with a retryable 503 *before* the caller writes a partial destination. Every
    chunk is bounded by `stream_chunk_timeout_seconds` regardless.
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
    gen = _stream(ctx, obj_cache, info, address=address)
    if not bound_first_chunk:
        return gen

    # Eager bounded first-chunk peek (A2), mirroring read_response, so a not-ready source surfaces
    # as DownloadNotReadyError before any destination bytes are written.
    try:
        first_chunk = await asyncio.wait_for(gen.__anext__(), timeout=float(cfg.stream_first_chunk_timeout_seconds))
    except StopAsyncIteration:
        first_chunk = None  # empty (zero-byte) source
    except (TimeoutError, asyncio.TimeoutError) as exc:
        await gen.aclose()
        raise DownloadNotReadyError(
            "Parts not ready: source first chunk did not arrive within the initial stream timeout",
            cause="first_chunk_timeout",
        ) from exc
    except ChunkUnavailableError as exc:
        # See read_response: a terminal miss is retryable, so map it to 503 rather than a 500.
        await gen.aclose()
        raise DownloadNotReadyError(f"Parts not ready: source {exc}", cause="chunk_unavailable") from exc

    async def _bounded() -> AsyncGenerator[bytes, None]:
        if first_chunk is not None:
            yield first_chunk
        async for chunk in gen:
            yield chunk

    return _bounded()
