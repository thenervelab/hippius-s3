from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import hashlib
import json
import logging
from dataclasses import dataclass
from typing import Any
from typing import AsyncGenerator

from fastapi import Response
from fastapi.responses import StreamingResponse

from hippius_s3.api.s3.common import build_headers
from hippius_s3.api.s3.range_utils import calculate_chunks_for_range
from hippius_s3.api.s3.range_utils import extract_range_from_chunks
from hippius_s3.cache import ObjectPartsCache
from hippius_s3.cache import RedisDownloadChunksCache
from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.queue import ChunkToDownload
from hippius_s3.queue import DownloadChainRequest
from hippius_s3.queue import enqueue_download_request
from hippius_s3.services.caching.cache_assembler import assemble_from_cache
from hippius_s3.services.manifest_service import ManifestService
from hippius_s3.utils import get_query


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
    simple_cid: str | None
    upload_id: str | None


@dataclass
class Part:
    part_number: int
    cid: str | None
    size_bytes: int


class ReadMode:
    AUTO = "auto"
    CACHE_ONLY = "cache_only"
    PIPELINE_ONLY = "pipeline_only"


@dataclasses.dataclass
class Range:
    start: int
    end: int


class ObjectReader:
    def __init__(self, config: Any) -> None:
        self.config = config
        self._logger = logging.getLogger(__name__)

    async def fetch_object_info(self, db: Any, bucket_name: str, object_key: str, account_id: str) -> ObjectInfo:
        row = await db.fetchrow(
            get_query("get_object_for_download_with_permissions"),
            bucket_name,
            object_key,
            account_id,
        )
        if not row:
            raise ValueError("object_not_found")
        md = row.get("metadata") or {}
        if isinstance(md, str):
            import json as _json

            try:
                md = _json.loads(md)
            except Exception:
                md = {}
        return ObjectInfo(
            object_id=str(row["object_id"]),
            bucket_name=row["bucket_name"],
            object_key=row["object_key"],
            size_bytes=int(row["size_bytes"]),
            content_type=row["content_type"],
            md5_hash=row["md5_hash"],
            created_at=row["created_at"],
            metadata=md,
            multipart=bool(row["multipart"]),
            should_decrypt=bool(row["should_decrypt"]),
            simple_cid=row.get("simple_cid"),
            upload_id=row.get("upload_id"),
        )

    async def build_manifest(self, db: Any, info: ObjectInfo) -> list[Part]:
        chunks = await ManifestService.build_initial_download_chunks(db, dataclasses.asdict(info))
        return [
            Part(part_number=int(c["part_number"]), cid=c.get("cid"), size_bytes=int(c.get("size_bytes", 0)))
            for c in chunks
        ]

    async def plan_parts_for_range(self, db: Any, info: ObjectInfo, rng: Range | None) -> tuple[list[Part], list[dict]]:
        parts = await self.build_manifest(db, info)
        filtered = self.filter_parts_for_range(rng, parts) if rng is not None else parts
        layout = [{"part_number": p.part_number, "size_bytes": p.size_bytes} for p in parts]
        return filtered, layout

    async def read_base_bytes(self, db: Any, redis: Any, info: ObjectInfo, *, strict_pipeline: bool = False) -> bytes:
        """Return base bytes for simple reads (append/copy), preferring cache, with pipeline fallback."""
        # Try unified cache first
        try:
            data = await RedisObjectPartsCache(redis).get(info.object_id, 0)
            if data:
                return data
        except Exception:
            pass

        if strict_pipeline:
            # Require object-level CID
            base_cid = await db.fetchval(
                "SELECT c.cid FROM objects o JOIN cids c ON o.cid_id = c.id WHERE o.object_id = $1",
                info.object_id,
            )
            if not base_cid:
                base_cid = await db.fetchval("SELECT ipfs_cid FROM objects WHERE object_id = $1", info.object_id)
            if not base_cid:
                raise RuntimeError("object_not_ready")
            # The caller (endpoint) should use IPFSService to fetch by CID if needed.
            raise RuntimeError("pipeline_fetch_required")

        # As a last resort, try object-level CID and expect the caller to fetch via IPFSService.
        raise RuntimeError("pipeline_fetch_required")

    async def enqueue_and_wait(
        self,
        redis: Any,
        info: ObjectInfo,
        parts: list[Part],
        request_uuid: str,
        *,
        address: str,
        subaccount: str,
        seed_phrase: str,
        max_checks: int,
    ) -> None:
        dl_parts = [ChunkToDownload(cid=p.cid, part_id=p.part_number) for p in parts if p.cid]
        req = DownloadChainRequest(
            request_id=request_uuid,
            object_id=info.object_id,
            object_key=info.object_key,
            bucket_name=info.bucket_name,
            address=address,
            subaccount=subaccount,
            subaccount_seed_phrase=seed_phrase,
            substrate_url=self.config.substrate_url,
            ipfs_node=self.config.ipfs_get_url,
            should_decrypt=info.should_decrypt,
            size=info.size_bytes,
            multipart=info.multipart,
            chunks=dl_parts,
        )
        flag_key = f"download_in_progress:{request_uuid}"
        set_flag = await redis.set(flag_key, "1", nx=True, ex=300)
        if set_flag:
            await enqueue_download_request(req, redis)

        # Wait until all chunks are in Redis
        dl_cache = RedisDownloadChunksCache(redis)
        for p in dl_parts:
            for _ in range(max_checks):
                if await dl_cache.exists(info.object_id, request_uuid, int(p.part_id)):
                    break
                await asyncio.sleep(self.config.http_download_sleep_loop)
            else:
                raise RuntimeError(f"missing_chunk_{p.part_id}")

    async def verify_etags(self, db: Any, info: ObjectInfo, parts: list[Part], redis: Any, request_uuid: str) -> None:
        if not info.upload_id:
            return
        rows = await db.fetch(get_query("get_parts_etags"), info.upload_id)
        if not rows:
            return
        expected = {int(r["part_number"]): r["etag"].split("-")[0] for r in rows}
        dl_cache = RedisDownloadChunksCache(redis)
        for p in parts:
            if p.part_number not in expected:
                continue
            data = await dl_cache.get(info.object_id, request_uuid, int(p.part_number))
            if data is None:
                raise RuntimeError("integrity_missing_chunk")
            md5 = hashlib.md5(data).hexdigest()
            if md5 != expected[p.part_number]:
                raise RuntimeError("integrity_mismatch")

    def filter_parts_for_range(self, rng: Range | None, parts: list[Part]) -> list[Part]:
        if rng is None:
            return parts
        layout = [{"part_number": p.part_number, "size_bytes": p.size_bytes} for p in parts]
        needed = calculate_chunks_for_range(rng.start, rng.end, layout)
        return [p for p in parts if p.part_number in needed]

    async def _get_chunk_from_redis(self, redis: Any, object_id: str, request_id: str, chunk: ChunkToDownload) -> bytes:
        for _ in range(self.config.http_redis_get_retries):
            data = await RedisDownloadChunksCache(redis).get(object_id, request_id, int(chunk.part_id))
            if data:
                return data
            await asyncio.sleep(self.config.http_download_sleep_loop)
        raise RuntimeError(f"Chunk {chunk.part_id} not found in Redis")

    async def read_response(
        self,
        db: Any,
        redis: Any,
        obj_cache: ObjectPartsCache,
        info: ObjectInfo,
        *,
        read_mode: str,
        rng: Range | None,
        address: str,
        seed_phrase: str,
    ) -> Response:
        # Decide cache vs pipeline
        has_cache = False
        try:
            # Consider either base(0) or bootstrap(1) as cache-present
            has_cache = bool((await obj_cache.exists(info.object_id, 0)) or (await obj_cache.exists(info.object_id, 1)))
        except Exception:
            has_cache = False
        main_cid_missing = info.simple_cid is None
        get_from_cache = main_cid_missing or has_cache

        if get_from_cache:
            # Prefer structured assembly from cache (enriches contiguous parts beyond DB manifest)
            try:
                # Debug probe: which parts are in cache right now
                try:
                    present = [_pn for _pn in range(0, 4) if await obj_cache.exists(info.object_id, _pn)]
                    with contextlib.suppress(Exception):
                        self._logger.info(
                            f"OBJECT_READER cache-probe object_id={info.object_id} present_parts={present}"
                        )
                except Exception:
                    pass
                parts_for_cache = await self.build_manifest(db, info)
                download_chunks = [{"part_number": p.part_number, "size_bytes": p.size_bytes} for p in parts_for_cache]
                with contextlib.suppress(Exception):
                    self._logger.info(
                        f"OBJECT_READER cache-manifest object_id={info.object_id} manifest={[d['part_number'] for d in download_chunks]}"
                    )
                return await assemble_from_cache(
                    obj_cache,
                    {
                        "object_id": info.object_id,
                        "bucket_name": info.bucket_name,
                        "object_key": info.object_key,
                        "size_bytes": info.size_bytes,
                        "content_type": info.content_type,
                        "md5_hash": info.md5_hash,
                        "created_at": info.created_at,
                        "metadata": info.metadata,
                        "multipart": True,
                        "download_chunks": json.dumps(download_chunks),
                    },
                    range_header=None if rng is None else f"bytes={rng.start}-{rng.end}",
                    start_byte=None if rng is None else rng.start,
                    end_byte=None if rng is None else rng.end,
                )
            except Exception:
                # Fallback: naive concatenation of any cached parts
                try:
                    collected: list[tuple[int, bytes]] = []
                    for pn in range(0, 256):
                        data = await obj_cache.get(info.object_id, pn)
                        if data is not None:
                            collected.append((pn, data))
                    if collected:
                        collected.sort(key=lambda x: x[0])
                        blob = b"".join(b for _, b in collected)
                        # Normalize metadata
                        _md = info.metadata
                        if isinstance(_md, str):
                            try:
                                _md = json.loads(_md)
                            except Exception:
                                _md = {}
                        if rng is not None:
                            slice_bytes = blob[rng.start : rng.end + 1]
                            headers = build_headers(
                                dataclasses.asdict(info), source="cache", metadata=_md, rng=(rng.start, rng.end)
                            )
                            return StreamingResponse(
                                iter([slice_bytes]), status_code=206, media_type=info.content_type, headers=headers
                            )
                        headers = build_headers(dataclasses.asdict(info), source="cache", metadata=_md)
                        return StreamingResponse(iter([blob]), media_type=info.content_type, headers=headers)
                except Exception:
                    pass

        parts, layout = await self.plan_parts_for_range(db, info, rng)
        parts = [p for p in parts if p.cid and str(p.cid).strip().lower() not in {"", "none", "pending"}]
        request_id = f"{info.object_id}_{address}"
        if rng is not None:
            request_id += f"_{rng.start}_{rng.end}"

        # Wait for required CIDs
        try:
            rows_required = await db.fetch("SELECT part_number FROM parts WHERE object_id = $1", info.object_id)
            required = {int(r[0]) for r in rows_required}
        except Exception:
            required = set()
        available = {p.part_number for p in parts}
        if required and not required.issubset(available):
            waited = await ManifestService.wait_for_cids(db, info.object_id, required, attempts=10, interval_sec=0.5)
            if waited:
                parts = [
                    Part(part_number=int(c["part_number"]), cid=str(c["cid"]), size_bytes=int(c.get("size_bytes", 0)))
                    for c in waited
                ]

        if not parts:
            base_cid = await db.fetchval(
                "SELECT c.cid FROM objects o JOIN cids c ON o.cid_id = c.id WHERE o.object_id = $1",
                info.object_id,
            )
            if not base_cid:
                base_cid = await db.fetchval("SELECT ipfs_cid FROM objects WHERE object_id = $1", info.object_id)
            if base_cid and str(base_cid).strip().lower() not in {"", "none", "pending"}:
                parts = [Part(part_number=0, cid=str(base_cid), size_bytes=info.size_bytes)]

        parts = sorted(parts, key=lambda p: p.part_number)

        # Probe base bytes in cache to optionally prepend in pipeline streaming when base CID is missing
        base_bytes: bytes | None = None
        try:
            base_bytes = await obj_cache.get(info.object_id, 0)
        except Exception:
            base_bytes = None

        await self.enqueue_and_wait(
            redis,
            info,
            parts,
            request_id,
            address=address,
            subaccount=address,
            seed_phrase=seed_phrase,
            max_checks=self.config.http_redis_get_retries,
        )

        # Range response
        if rng is not None:

            async def gen_range() -> AsyncGenerator[bytes, None]:
                dl = [ChunkToDownload(cid=p.cid, part_id=p.part_number) for p in parts]
                data_chunks = [await self._get_chunk_from_redis(redis, info.object_id, request_id, c) for c in dl]
                data = extract_range_from_chunks(
                    data_chunks, rng.start, rng.end, layout, [p.part_number for p in parts]
                )
                yield data

            _md = info.metadata
            if isinstance(_md, str):
                try:
                    _md = json.loads(_md)
                except Exception:
                    _md = {}
            headers = build_headers(dataclasses.asdict(info), source="pipeline", metadata=_md, rng=(rng.start, rng.end))
            return StreamingResponse(gen_range(), status_code=206, media_type=info.content_type, headers=headers)

        # Full response
        async def gen_full() -> AsyncGenerator[bytes, None]:
            # If base part is not in planned parts but exists in cache, yield it first
            planned_part_numbers = {p.part_number for p in parts}
            if 0 not in planned_part_numbers and base_bytes is not None and rng is None:
                yield base_bytes
            dl = [ChunkToDownload(cid=p.cid, part_id=p.part_number) for p in parts]
            for c in dl:
                yield await self._get_chunk_from_redis(redis, info.object_id, request_id, c)

        # If we are augmenting with cache base, mark source as cache
        source_header = (
            "cache"
            if (base_bytes is not None and (0 not in {p.part_number for p in parts}) and rng is None)
            else ("cache" if has_cache else "pipeline")
        )
        _md = info.metadata
        if isinstance(_md, str):
            try:
                _md = json.loads(_md)
            except Exception:
                _md = {}
        headers = build_headers(dataclasses.asdict(info), source=source_header, metadata=_md)
        return StreamingResponse(gen_full(), media_type=info.content_type, headers=headers)
