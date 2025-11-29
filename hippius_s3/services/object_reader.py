from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fastapi import Response
from fastapi.responses import StreamingResponse

from hippius_s3.api.s3.common import build_headers
from hippius_s3.config import get_config
from hippius_s3.queue import DownloadChainRequest
from hippius_s3.queue import PartToDownload
from hippius_s3.queue import enqueue_download_request
from hippius_s3.reader.db_meta import read_parts_manifest
from hippius_s3.reader.planner import build_chunk_plan
from hippius_s3.reader.streamer import stream_plan
from hippius_s3.reader.types import ChunkPlanItem
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


@dataclass
class StreamContext:
    plan: list[ChunkPlanItem]
    object_version: int
    storage_version: int
    should_decrypt: bool
    source: str


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

    ov = int(info.get("object_version") or info.get("current_object_version") or 1)
    parts = await read_parts_manifest(db, info["object_id"], ov)
    plan = await build_chunk_plan(db, info["object_id"], parts, rng, object_version=ov)

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

    if source == "pipeline":
        cid_by_part: dict[int, str] = {}
        for p in parts:
            try:
                pn = int(p.get("part_number", 0))
                cid_raw = p.get("cid")
                if cid_raw and str(cid_raw).strip().lower() not in {"", "none", "pending"}:
                    cid_by_part[pn] = str(cid_raw)
            except Exception:
                continue
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
        dl_parts: list[PartToDownload] = []
        for pn, idxs in indices_by_part.items():
            try:
                from hippius_s3.utils import get_query  # local import

                rows = await db.fetch(
                    get_query("get_part_chunks_by_object_and_number"),
                    info["object_id"],
                    int(info.get("object_version") or info.get("current_object_version") or 1),
                    int(pn),
                )
                all_entries = [(int(r[0]), str(r[1]), int(r[2]) if r[2] is not None else None) for r in rows or []]
                chunk_specs: list[dict] = []
                include = {int(i) for i in idxs}
                for ci, cid, clen in all_entries:
                    if int(ci) in include:
                        chunk_specs.append(
                            {
                                "index": int(ci),
                                "cid": str(cid),
                                "cipher_size_bytes": int(clen) if clen is not None else None,
                            }
                        )
                if not chunk_specs:
                    continue
                dl_parts.append(
                    PartToDownload(
                        part_number=int(pn),
                        chunks=[
                            # type: ignore[arg-type]
                            {
                                "index": s["index"],
                                "cid": s["cid"],
                                "cipher_size_bytes": s.get("cipher_size_bytes"),
                            }
                            for s in chunk_specs
                        ],
                    )
                )
            except Exception:
                continue
        if dl_parts:
            req = DownloadChainRequest(
                request_id=f"{info['object_id']}::shared",
                object_id=info["object_id"],
                object_version=int(info.get("object_version") or info.get("current_object_version") or 1),
                object_key=info.get("object_key", ""),
                bucket_name=info.get("bucket_name", ""),
                address=address,
                subaccount=address,
                subaccount_seed_phrase="",
                substrate_url=cfg.substrate_url,
                ipfs_node=cfg.ipfs_get_url,
                should_decrypt=bool(info.get("should_decrypt")),
                size=int(info.get("size_bytes") or 0),
                multipart=bool(info.get("multipart")),
                chunks=dl_parts,
                ray_id=info.get("ray_id"),
            )
            await enqueue_download_request(req)

    storage_version = int(info.get("storage_version") or 2)
    object_version = int(info.get("object_version") or info.get("current_object_version") or 1)
    if "should_decrypt" in info:
        should_decrypt = bool(info.get("should_decrypt"))
    else:
        is_public = bool(info.get("is_public", False))
        should_decrypt = (storage_version >= 3) or (not is_public)

    return StreamContext(
        plan=plan,
        object_version=object_version,
        storage_version=storage_version,
        should_decrypt=should_decrypt,
        source=source,
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
        should_decrypt=ctx.should_decrypt,
        sleep_seconds=float(cfg.http_download_sleep_loop),
        address=address,
        bucket_name=str(info.get("bucket_name", "")),
        storage_version=ctx.storage_version,
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
    about manifests, chunk plans, or downloader details.
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
        should_decrypt=ctx.should_decrypt,
        sleep_seconds=float(cfg.http_download_sleep_loop),
        address=address,
        bucket_name=str(info.get("bucket_name", "")),
        storage_version=ctx.storage_version,
    )
