from __future__ import annotations

import asyncio
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
from hippius_s3.queue import ChunkToDownload
from hippius_s3.queue import DownloadChainRequest
from hippius_s3.queue import enqueue_download_request
from hippius_s3.services.manifest_service import ManifestService
from hippius_s3.utils import get_query


class DownloadNotReadyError(Exception):
    pass


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

    async def hydrate_object_cache(
        self,
        redis: Any,
        obj_cache: ObjectPartsCache,
        info: ObjectInfo,
        parts: list[Part],
        *,
        address: str,
        seed_phrase: str,
    ) -> tuple[set[int], list[Part]]:
        """Ensure required parts are present in obj: cache by copying existing bytes or queueing pipeline.

        Returns (satisfied_from_cache_part_numbers, missing_for_pipeline_parts).
        """
        satisfied_from_cache: set[int] = set()
        missing_for_pipeline: list[Part] = []

        # Stage 1: check obj: presence; no per-request duplication
        for p in parts:
            try:
                data = await obj_cache.get(info.object_id, int(p.part_number))
                if data is not None:
                    satisfied_from_cache.add(int(p.part_number))
                else:
                    missing_for_pipeline.append(p)
            except Exception:
                # On any error, fall back to pipeline for this part
                missing_for_pipeline.append(p)

        # Stage 2: enqueue only for missing parts (guard with a flag per part)
        if missing_for_pipeline:
            dl_parts = [ChunkToDownload(cid=p.cid, part_id=p.part_number) for p in missing_for_pipeline if p.cid]
            req = DownloadChainRequest(
                request_id=f"{info.object_id}::shared",
                object_id=info.object_id,
                object_key=info.object_key,
                bucket_name=info.bucket_name,
                address=address,
                subaccount=address,
                subaccount_seed_phrase=seed_phrase,
                substrate_url=self.config.substrate_url,
                ipfs_node=self.config.ipfs_get_url,
                should_decrypt=info.should_decrypt,
                size=info.size_bytes,
                multipart=info.multipart,
                chunks=dl_parts,
            )
            # Per-part guard to avoid duplicate enqueue storms
            for chunk in dl_parts:
                flag_key = f"download_in_progress:{info.object_id}:{int(chunk.part_id)}"
                set_flag = await redis.set(flag_key, "1", nx=True, ex=300)
                if set_flag:
                    await enqueue_download_request(req, redis)

        return satisfied_from_cache, missing_for_pipeline

    async def verify_etags(
        self, db: Any, info: ObjectInfo, parts: list[Part], obj_cache: ObjectPartsCache, redis: Any, request_uuid: str
    ) -> None:
        if not info.upload_id:
            return
        rows = await db.fetch(get_query("get_parts_etags"), info.upload_id)
        if not rows:
            return
        expected = {int(r["part_number"]): r["etag"].split("-")[0] for r in rows}
        for p in parts:
            if p.part_number not in expected:
                continue
            data = await obj_cache.get(info.object_id, int(p.part_number))
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

    async def _get_chunk_from_redis(
        self, redis: Any, obj_cache: ObjectPartsCache, object_id: str, request_id: str, chunk: ChunkToDownload
    ) -> bytes:
        for _ in range(self.config.http_redis_get_retries):
            data = await obj_cache.get(object_id, int(chunk.part_id))
            if data:
                return data
            await asyncio.sleep(self.config.http_download_sleep_loop)
        raise RuntimeError(f"Chunk {chunk.part_id} not found in Redis")

    async def _handle_pending_parts_with_cache_fallback(
        self,
        db: Any,
        obj_cache: ObjectPartsCache,
        info: ObjectInfo,
        pending_parts: list[Part],
        redis: Any,
        address: str,
        seed_phrase: str,
    ) -> None:
        """Handle parts with pending CIDs by falling back to cache with bounded polling."""
        if not pending_parts:
            return

        # Check which pending parts are already available in cache (batch async)
        existence = await asyncio.gather(*[obj_cache.exists(info.object_id, int(p.part_number)) for p in pending_parts])
        still_pending = [p for p, exists in zip(pending_parts, existence, strict=True) if not exists]

        if not still_pending:
            # All pending parts are already in cache
            return

        # Do bounded polling for missing pending parts
        poll_deadline = asyncio.get_event_loop().time() + 2.0  # 2 second timeout
        poll_interval = self.config.http_download_sleep_loop

        while asyncio.get_event_loop().time() < poll_deadline:
            # Check if any missing parts have become available in cache
            newly_available = []
            for p in still_pending[:]:  # Copy to avoid modification during iteration
                if await obj_cache.exists(info.object_id, int(p.part_number)):
                    newly_available.append(p)
                    still_pending.remove(p)

            if newly_available:
                self._logger.info(
                    f"Pending parts became available in cache: {[(p.part_number, p.cid) for p in newly_available]}"
                )

            # Also check if any CIDs have become available (and enqueue downloads)
            required_parts = {p.part_number for p in still_pending}
            if required_parts:
                # Check if CIDs have become available for any pending parts
                cid_updates = await ManifestService.wait_for_cids(
                    db, info.object_id, required_parts, attempts=1, interval_sec=0
                )
                if cid_updates:
                    # Convert CID updates to parts and enqueue downloads
                    cid_parts = [
                        Part(
                            part_number=int(c["part_number"]), cid=str(c["cid"]), size_bytes=int(c.get("size_bytes", 0))
                        )
                        for c in cid_updates
                    ]
                    await self.hydrate_object_cache(
                        redis, obj_cache, info, cid_parts, address=address, seed_phrase=seed_phrase
                    )
                    # Remove from still_pending since they're now being hydrated
                    for update in cid_updates:
                        part_num = int(update["part_number"])
                        still_pending = [p for p in still_pending if p.part_number != part_num]

            if not still_pending:
                # All parts are now available or being hydrated
                break

            await asyncio.sleep(poll_interval)

        # If we still have pending parts after polling, they're not available
        if still_pending:
            self._logger.warning(
                f"Parts still unavailable after polling: {[(p.part_number, p.cid) for p in still_pending]}"
            )
            raise DownloadNotReadyError(f"Parts not ready: {[p.part_number for p in still_pending]}")

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
        range_was_invalid: bool = False,
    ) -> Response:
        # Step 1: Build the full manifest of all parts (includes base 0 + appended parts)
        full_parts = await self.build_manifest(db, info)
        full_parts = sorted(full_parts, key=lambda p: p.part_number)
        layout = [{"part_number": p.part_number, "size_bytes": p.size_bytes} for p in full_parts]

        # Step 2: Derive needed parts for the requested range
        if rng is not None:
            needed_part_numbers = calculate_chunks_for_range(rng.start, rng.end, layout)
            parts = [p for p in full_parts if p.part_number in needed_part_numbers]
        else:
            parts = full_parts

        parts = sorted(parts, key=lambda p: p.part_number)

        # Step 3: Classify parts by availability and handle pending CIDs
        cid_backed_parts = []  # Parts with concrete CIDs that can be hydrated
        pending_only_parts = []  # Parts with pending/None CIDs that need cache fallback

        for p in parts:
            if p.cid:
                cid_backed_parts.append(p)
            else:
                pending_only_parts.append(p)

        # Check cache availability for all needed parts
        all_parts_available = True
        for p in parts:
            if not await obj_cache.exists(info.object_id, int(p.part_number)):
                all_parts_available = False
                break

        source_header = "cache" if all_parts_available else "pipeline"

        # Step 4: Hydrate cache for CID-backed parts and handle pending-only parts
        if cid_backed_parts:
            # Only enqueue downloads for parts that have concrete CIDs
            await self.hydrate_object_cache(
                redis,
                obj_cache,
                info,
                cid_backed_parts,  # Only CID-backed parts
                address=address,
                seed_phrase=seed_phrase,
            )

        # Handle pending-only parts with bounded polling
        if pending_only_parts:
            await self._handle_pending_parts_with_cache_fallback(
                db,
                obj_cache,
                info,
                pending_only_parts,
                redis,
                address,
                seed_phrase,
            )

        # Step 5: Pre-flight check to ensure the first part is available before sending headers
        import time as _time

        if parts:
            first_part = parts[0]
            deadline = _time.time() + float(self.config.http_stream_initial_timeout_seconds)
            while True:
                if (await obj_cache.get(info.object_id, int(first_part.part_number))) is not None:
                    break
                if _time.time() > deadline:
                    raise DownloadNotReadyError("initial_stream_timeout")
                await asyncio.sleep(self.config.http_download_sleep_loop)

        # Step 6: Prepare and stream the response
        _md = info.metadata
        if isinstance(_md, str):
            try:
                _md = json.loads(_md)
            except Exception:
                _md = {}

        # Handle Range Request
        if rng is not None:

            async def gen_range() -> AsyncGenerator[bytes, None]:
                data_chunks: list[bytes] = []
                for p in parts:
                    while True:
                        data = await obj_cache.get(info.object_id, int(p.part_number))
                        if data is not None:
                            data_chunks.append(data)
                            break
                        await asyncio.sleep(self.config.http_download_sleep_loop)
                data = extract_range_from_chunks(
                    data_chunks, rng.start, rng.end, layout, [p.part_number for p in parts]
                )
                yield data

            headers = build_headers(
                dataclasses.asdict(info),
                source=source_header,
                metadata=_md,
                rng=(rng.start, rng.end),
                range_was_invalid=range_was_invalid,
            )
            status_code = 200 if range_was_invalid else 206
            return StreamingResponse(
                gen_range(), status_code=status_code, media_type=info.content_type, headers=headers
            )

        # Handle Full Request
        async def gen_full() -> AsyncGenerator[bytes, None]:
            for p in parts:
                while True:
                    data = await obj_cache.get(info.object_id, int(p.part_number))
                    if data is not None:
                        yield data
                        break
                    await asyncio.sleep(self.config.http_download_sleep_loop)

        headers = build_headers(dataclasses.asdict(info), source=source_header, metadata=_md)
        return StreamingResponse(gen_full(), media_type=info.content_type, headers=headers)
