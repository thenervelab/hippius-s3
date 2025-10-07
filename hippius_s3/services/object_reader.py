from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from typing import AsyncIterator

from fastapi import Response
from fastapi.responses import StreamingResponse

from hippius_s3.api.s3.common import build_headers
from hippius_s3.config import get_config
from hippius_s3.queue import ChunkToDownload
from hippius_s3.queue import DownloadChainRequest
from hippius_s3.queue import enqueue_download_request
from hippius_s3.reader.db_meta import read_parts_manifest
from hippius_s3.reader.planner import build_chunk_plan
from hippius_s3.reader.streamer import stream_plan
from hippius_s3.reader.types import RangeRequest


class DownloadNotReadyError(Exception):
    pass


# Back-compat type stubs for existing imports
@dataclass
class ObjectInfo:
    object_id: str
    bucket_name: str
    object_key: str
    size_bytes: int
    content_type: str
    md5_hash: str
    created_at: Any
    metadata: dict
    multipart: bool
    should_decrypt: bool
    simple_cid: str | None = None
    upload_id: str | None = None


@dataclass
class Range:
    start: int
    end: int


class ObjectReader:  # noqa: D401 (compat shim)
    """Compatibility stub; legacy ObjectReader no longer used."""

    def __init__(self, config: Any | None = None) -> None:
        self.config = config


async def read_response(
    db: Any,
    redis: Any,
    obj_cache: Any,
    info: dict,
    *,
    read_mode: str,
    rng: RangeRequest | None,
    address: str,
    seed_phrase: str,
    range_was_invalid: bool = False,
) -> Response:
    cfg = get_config()

    parts = await read_parts_manifest(db, info["object_id"])
    plan = await build_chunk_plan(db, info["object_id"], parts, rng)

    # Decide source header based on whether all planned chunks are already cached
    source = "cache"
    try:
        for item in plan:
            exists = await obj_cache.chunk_exists(info["object_id"], int(item.part_number), int(item.chunk_index))  # type: ignore[attr-defined]
            if not exists:
                source = "pipeline"
                break
    except Exception:
        source = "pipeline"

    # Enqueue downloader for missing chunks (minimal indices per-part)
    if source == "pipeline":
        # Map part_number -> cid from manifest
        cid_by_part: dict[int, str] = {}
        for p in parts:
            try:
                pn = int(p.get("part_number", 0))
                cid_raw = p.get("cid")
                if cid_raw and str(cid_raw).strip().lower() not in {"", "none", "pending"}:
                    cid_by_part[pn] = str(cid_raw)
            except Exception:
                continue
        # Build index sets
        indices_by_part: dict[int, set[int]] = {}
        for item in plan:
            ok = await obj_cache.chunk_exists(info["object_id"], int(item.part_number), int(item.chunk_index))  # type: ignore[attr-defined]
            if ok:
                continue
            s = indices_by_part.setdefault(int(item.part_number), set())
            s.add(int(item.chunk_index))
        dl_parts: list[ChunkToDownload] = []
        for pn, idxs in indices_by_part.items():
            cid = cid_by_part.get(int(pn))
            if not cid:
                continue
            dl_parts.append(
                ChunkToDownload(
                    cid=cid,
                    part_id=int(pn),
                    redis_key=None,
                    chunk_indices=sorted(idxs),
                )
            )
        if dl_parts:
            req = DownloadChainRequest(
                request_id=f"{info['object_id']}::shared",
                object_id=info["object_id"],
                object_key=info.get("object_key", ""),
                bucket_name=info.get("bucket_name", ""),
                address=address,
                subaccount=address,
                subaccount_seed_phrase=seed_phrase,
                substrate_url=cfg.substrate_url,
                ipfs_node=cfg.ipfs_get_url,
                should_decrypt=bool(info.get("should_decrypt")),
                size=int(info.get("size_bytes") or 0),
                multipart=bool(info.get("multipart")),
                chunks=dl_parts,
            )
            await enqueue_download_request(req, redis)

    # Legacy SDK whole-part decrypt fallback: only for full GET on private objects
    if bool(info.get("should_decrypt")) and rng is None:
        try:
            # Attempt to assemble all chunks in-order; if any missing, skip fallback
            buf: list[bytes] = []
            for item in plan:
                c = await obj_cache.get_chunk(info["object_id"], int(item.part_number), int(item.chunk_index))  # type: ignore[attr-defined]
                if c is None:
                    buf = []
                    break
                buf.append(c)
            if buf:
                ct = b"".join(buf)
                # Try decrypt using legacy SDK key (account+bucket scoped)
                import base64 as _b64

                import nacl.secret as _secret

                from hippius_s3.ipfs_service import get_encryption_key  # local import

                account_bucket = f"{address}:{info.get('bucket_name', '')}"
                key_b64 = await get_encryption_key(account_bucket)
                key = _b64.b64decode(key_b64)
                box = _secret.SecretBox(key)
                pt = bytes(box.decrypt(ct))

                async def _once() -> AsyncIterator[bytes]:
                    yield pt

                headers = build_headers(
                    info, source=source, metadata=info.get("metadata") or {}, rng=None, range_was_invalid=False
                )
                return StreamingResponse(
                    _once(),
                    status_code=200,
                    media_type=info.get("content_type", "application/octet-stream"),
                    headers=headers,
                )
        except Exception:
            pass

    # Whole-object legacy SDK fallback (multipart, private): decrypt once then slice
    from hippius_s3.config import get_config as _get_cfg  # local to avoid circulars

    if (
        bool(info.get("should_decrypt"))
        and isinstance(parts, list)
        and len(parts) > 1
        and _get_cfg().enable_legacy_sdk_compat
    ):
        try:
            # Assemble full ciphertext across parts
            from hippius_s3.reader.fetcher import fetch_chunk_blocking  # local import to avoid cycles

            ct_all: list[bytes] = []
            # Sort parts by part_number
            ordered_parts = sorted(parts, key=lambda p: int(p.get("part_number", 0)))
            for p in ordered_parts:
                pn = int(p.get("part_number", 0))
                # Determine chunk count from cache meta if present
                num_chunks = 0
                try:
                    meta = await obj_cache.get_meta(info["object_id"], pn)  # type: ignore[attr-defined]
                except Exception:
                    meta = None
                if isinstance(meta, dict):
                    try:
                        num_chunks = int(meta.get("num_chunks", 0))
                    except Exception:
                        num_chunks = 0
                idx = 0
                while True:
                    if num_chunks and idx >= num_chunks:
                        break
                    c = await fetch_chunk_blocking(
                        obj_cache, info["object_id"], pn, idx, sleep_seconds=float(cfg.http_download_sleep_loop)
                    )
                    if not c:
                        break
                    ct_all.append(c)
                    idx += 1

            # Decrypt once using SDK key
            from hippius_s3.legacy.sdk_compat import decrypt_whole_ciphertext  # local import

            pt_all = await decrypt_whole_ciphertext(
                ciphertext=b"".join(ct_all), address=address, bucket_name=str(info.get("bucket_name", ""))
            )

            # If range present, slice once here; otherwise stream whole
            async def _gen_whole() -> AsyncIterator[bytes]:
                yield pt_all

            async def _gen_range(start: int, end: int) -> AsyncIterator[bytes]:
                yield pt_all[int(start) : int(end) + 1]

            headers = build_headers(
                info,
                source=source,
                metadata=info.get("metadata") or {},
                rng=(rng.start, rng.end) if rng is not None else None,
                range_was_invalid=range_was_invalid,
            )
            if rng is None:
                return StreamingResponse(
                    _gen_whole(),
                    status_code=200,
                    media_type=info.get("content_type", "application/octet-stream"),
                    headers=headers,
                )
            return StreamingResponse(
                _gen_range(int(rng.start), int(rng.end)),
                status_code=206 if not range_was_invalid else 200,
                media_type=info.get("content_type", "application/octet-stream"),
                headers=headers,
            )
        except Exception:
            # Fall through to normal streaming
            pass

    gen = stream_plan(
        obj_cache=obj_cache,
        object_id=info["object_id"],
        plan=plan,
        should_decrypt=bool(info.get("should_decrypt")),
        seed_phrase=seed_phrase,
        sleep_seconds=float(cfg.http_download_sleep_loop),
        address=address,
        bucket_name=str(info.get("bucket_name", "")),
    )
    headers = build_headers(
        info,
        source=source,
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
