from __future__ import annotations

from dataclasses import dataclass
from typing import Any

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
            exists = await obj_cache.chunk_exists(
                info["object_id"],
                int(info.get("object_version") or info.get("current_object_version") or 1),
                int(item.part_number),
                int(item.chunk_index),
            )  # type: ignore[attr-defined]
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
            ok = await obj_cache.chunk_exists(
                info["object_id"],
                int(info.get("object_version") or info.get("current_object_version") or 1),
                int(item.part_number),
                int(item.chunk_index),
            )  # type: ignore[attr-defined]
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
                object_version=int(info.get("object_version") or info.get("current_object_version") or 1),
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

    storage_version = int(info.get("storage_version") or 2)
    object_version = int(info.get("object_version") or info.get("current_object_version") or 1)
    gen = stream_plan(
        obj_cache=obj_cache,
        object_id=info["object_id"],
        object_version=object_version,
        plan=plan,
        should_decrypt=bool(info.get("should_decrypt")),
        seed_phrase=seed_phrase,
        sleep_seconds=float(cfg.http_download_sleep_loop),
        address=address,
        bucket_name=str(info.get("bucket_name", "")),
        storage_version=storage_version,
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
