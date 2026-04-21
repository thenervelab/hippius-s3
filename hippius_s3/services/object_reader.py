from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from fastapi import Response
from fastapi.responses import StreamingResponse

from hippius_s3.api.s3.common import build_headers
from hippius_s3.backend_routing import resolve_object_backends
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
from hippius_s3.storage_version import require_supported_storage_version


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


async def build_stream_context(
    db: Any,
    redis: Any,
    obj_cache: Any,
    info: dict,
    *,
    rng: RangeRequest | None,
    address: str,
) -> StreamContext:
    cfg = get_config()
    storage_version = require_supported_storage_version(int(info["storage_version"]))
    # v4-only policy: always decrypt at read time.

    ov = int(info.get("object_version") or info.get("current_object_version") or 1)
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
        # Coalesce concurrent misses on the same part: only one streamer
        # actually enqueues a download request per (object_id, version, part).
        # Others will just wait on the pub/sub notification emitted by the
        # downloader. The lock TTL covers crashed-streamer / crashed-downloader
        # cases — on TTL expiry, the next miss re-enqueues.
        lock_ttl = int(getattr(cfg, "download_coalesce_lock_ttl_seconds", 120))
        ray_token = str(info.get("ray_id") or "anonymous")
        acquired_parts: set[int] = set()
        for pn in list(indices_by_part.keys()):
            lock_key = f"download_in_progress:{info['object_id']}:v:{ov}:part:{pn}"
            try:
                acquired = await redis.set(lock_key, ray_token, nx=True, ex=lock_ttl)
            except Exception:
                # Redis hiccup — fail open: behave as if we acquired the lock
                # so the download still happens. Worst case is a duplicate
                # enqueue, which the downloader deduplicates via chunk_exists.
                acquired = True
            if acquired:
                acquired_parts.add(pn)
            else:
                logger.debug(
                    "download coalesced: another streamer is fetching object_id=%s v=%s part=%s (lock held)",
                    info["object_id"],
                    ov,
                    pn,
                )

        # If every missing part is already being fetched by someone else,
        # we don't enqueue anything — we just fall through to stream_plan,
        # which will wait on pub/sub for each chunk.
        dl_parts: list[PartToDownload] = []
        # CIDs are optional.
        # - If per-chunk CIDs exist in part_chunks, include them so the IPFS downloader can fetch from IPFS.
        # - Otherwise, keep cid=None so other backends (deterministic addressing) can handle it.
        for pn, idxs in indices_by_part.items():
            if pn not in acquired_parts:
                continue
            include = {int(i) for i in idxs}
            by_index: dict[int, tuple[str | None, int | None]] = {}
            try:
                from hippius_s3.utils import get_query  # local import

                rows = await db.fetch(
                    get_query("get_part_chunks_by_object_and_number"),
                    info["object_id"],
                    int(info.get("object_version") or info.get("current_object_version") or 1),
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
            db_backends = await resolve_object_backends(db, info["object_id"], ov)
            req = DownloadChainRequest(
                request_id=f"{info['object_id']}::shared",
                object_id=info["object_id"],
                object_version=int(info.get("object_version") or info.get("current_object_version") or 1),
                object_storage_version=int(storage_version),
                object_key=info.get("object_key", ""),
                bucket_name=info.get("bucket_name", ""),
                address=address,
                subaccount=address,
                subaccount_seed_phrase="",
                substrate_url=cfg.substrate_url,
                size=int(info.get("size_bytes") or 0),
                multipart=bool(info.get("multipart")),
                chunks=dl_parts,
                ray_id=info.get("ray_id"),
                download_backends=db_backends if db_backends else None,
            )
            await enqueue_download_request(req)

    object_version = int(info.get("object_version") or info.get("current_object_version") or 1)
    bucket_id = str(info.get("bucket_id") or "")
    upload_id = str(info.get("upload_id") or "")
    suite_id: str | None = None
    key_bytes: bytes | None = None

    suite_id = str(info.get("enc_suite_id") or "hip-enc/aes256gcm")
    kek_id = info.get("kek_id")
    wrapped_dek = info.get("wrapped_dek")
    if not bucket_id or not kek_id or not wrapped_dek:
        # Current version is mid-write (overwrite in progress). Fall back to the
        # previous version which is guaranteed to have a complete envelope.
        prev_version = object_version - 1
        if prev_version >= 1:
            logger.warning(
                "Envelope missing on v%s of %s, falling back to v%s",
                object_version,
                info.get("object_id"),
                prev_version,
            )
            from hippius_s3.utils import get_query as _get_query

            prev_info = await db.fetchrow(
                _get_query("get_object_for_download_with_permissions_by_version"),
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
) -> Response:
    cfg = get_config()
    ctx = await build_stream_context(
        db,
        redis,
        obj_cache,
        info,
        rng=rng,
        address=address,
    )
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
        gen,
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
) -> Any:
    """Return an async iterator of plaintext bytes for the requested object.

    This wraps build_stream_context and stream_plan so callers don't need to know
    about parts catalogs, chunk plans, or downloader details.
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
    )
