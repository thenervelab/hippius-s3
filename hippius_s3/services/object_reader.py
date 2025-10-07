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
from typing import cast

from fastapi import Response
from fastapi.responses import StreamingResponse

from hippius_s3.api.s3.common import build_headers
from hippius_s3.api.s3.range_utils import calculate_chunks_for_range
from hippius_s3.cache import ObjectPartsCache
from hippius_s3.metadata.meta_reader import read_cache_meta
from hippius_s3.metadata.meta_reader import read_db_meta
from hippius_s3.planning.range_planner import PartInput
from hippius_s3.planning.range_planner import build_part_offsets
from hippius_s3.queue import ChunkToDownload
from hippius_s3.queue import DownloadChainRequest
from hippius_s3.queue import enqueue_download_request
from hippius_s3.services.crypto_service import CryptoService
from hippius_s3.services.manifest_service import ManifestService
from hippius_s3.utils import get_query
from hippius_s3.utils.timing import async_timing_context
from hippius_s3.utils.timing import log_timing


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
    enc_suite_id: str | None = None


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
            enc_suite_id=row.get("enc_suite_id"),
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
        rng: Range | None = None,
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
        # Skip generic enqueue for range requests; a minimal per-range enqueue happens below
        if missing_for_pipeline and rng is None:
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
        rows = await db.fetch(get_query("get_parts_etags"), info.object_id)
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

        # Do bounded polling for missing pending parts (align with HTTP stream initial timeout)
        poll_deadline = asyncio.get_event_loop().time() + float(self.config.http_stream_initial_timeout_seconds)
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
                # Allow brief polling to account for pinner committing CIDs shortly after PUT
                cid_updates = await ManifestService.wait_for_cids(
                    db, info.object_id, required_parts, attempts=10, interval_sec=0.2
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
                        redis, obj_cache, info, cid_parts, address=address, seed_phrase=seed_phrase, rng=None
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
        import time as _timing_time

        read_start = _timing_time.perf_counter()

        # Step 1: Build the full manifest of all parts (includes base 0 + appended parts)
        async with async_timing_context(
            "object_reader.build_manifest", extra={"object_id": info.object_id, "parts_count": "TBD"}
        ):
            full_parts = await self.build_manifest(db, info)
            full_parts = sorted(full_parts, key=lambda p: p.part_number)
        with contextlib.suppress(Exception):
            self._logger.info(
                f"READER Step1: object_id={info.object_id} initial full_parts count={len(full_parts)} parts={[(p.part_number, p.size_bytes, p.cid) for p in full_parts]}"
            )

        # Plaintext size map for range computation (declared here, updated throughout)
        plaintext_sizes: dict[int, int] = {}  # part_number -> plaintext size

        # Step 2: For range requests, compute plaintext sizes and filter to needed parts only
        if rng is not None:
            # Compute plaintext sizes from cache metadata
            for p in full_parts:
                pn = int(p.part_number)
                try:
                    meta = await obj_cache.get_meta(info.object_id, pn)
                    if meta and not info.should_decrypt:
                        # Public object: cache has plaintext, meta size_bytes is accurate
                        plaintext_sizes[pn] = int(meta.get("size_bytes", p.size_bytes))
                    elif meta and info.should_decrypt:
                        # Encrypted object: compute plaintext size from ciphertext
                        ct_size = int(meta.get("size_bytes", 0))
                        num_chunks = int(meta.get("num_chunks", 0))
                        if ct_size > 0 and num_chunks > 0:
                            overhead = CryptoService.OVERHEAD_PER_CHUNK * num_chunks
                            plaintext_sizes[pn] = max(0, ct_size - overhead)
                        else:
                            # No meta yet: use DB size minus estimated overhead
                            if info.should_decrypt and p.size_bytes > CryptoService.OVERHEAD_PER_CHUNK:
                                plaintext_sizes[pn] = int(p.size_bytes) - CryptoService.OVERHEAD_PER_CHUNK
                            else:
                                plaintext_sizes[pn] = int(p.size_bytes)
                    else:
                        # No meta: estimate from DB size
                        if info.should_decrypt and p.size_bytes > CryptoService.OVERHEAD_PER_CHUNK:
                            plaintext_sizes[pn] = int(p.size_bytes) - CryptoService.OVERHEAD_PER_CHUNK
                        else:
                            plaintext_sizes[pn] = int(p.size_bytes)
                except Exception:
                    plaintext_sizes[pn] = int(p.size_bytes)

            # Build layout with plaintext sizes for range calculation
            layout = [
                {"part_number": p.part_number, "size_bytes": plaintext_sizes.get(p.part_number, p.size_bytes)}
                for p in full_parts
            ]
            with contextlib.suppress(Exception):
                self._logger.info(f"READER Step2: plaintext_sizes={plaintext_sizes} layout={layout}")

            # Calculate which parts are actually needed for this range
            needed_part_numbers = calculate_chunks_for_range(rng.start, rng.end, layout)
            parts = [p for p in full_parts if p.part_number in needed_part_numbers]
            with contextlib.suppress(Exception):
                self._logger.info(f"READER Step2: range={rng.start}-{rng.end} needed_parts={needed_part_numbers}")
        else:
            # Full object request: fetch all parts
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
            # If a range is provided, pass minimal chunk indices per part
            if rng is not None:
                # Build offsets from DB plain sizes (no cache dependency)
                part_inputs = cast(
                    list[PartInput],
                    [
                        {
                            "part_number": fp.part_number,
                            "plain_size": plaintext_sizes.get(fp.part_number, fp.size_bytes),
                        }
                        for fp in full_parts
                    ],
                )
                part_offsets = build_part_offsets(part_inputs)
                offset_map = {int(po["part_number"]): (int(po["offset"]), int(po["plain_size"])) for po in part_offsets}

                # Determine chunk_size for each part from DB only; if missing -> 503/SlowDown
                per_part_chunk_size: dict[int, int] = {}
                missing_chunk_size_parts: list[int] = []
                for p in cid_backed_parts:
                    pn = int(p.part_number)
                    db_meta = await read_db_meta(db, info.object_id, pn)
                    if db_meta and db_meta["chunk_size_bytes"]:
                        per_part_chunk_size[pn] = int(db_meta["chunk_size_bytes"])  # type: ignore[index]
                    else:
                        missing_chunk_size_parts.append(pn)
                if missing_chunk_size_parts:
                    self._logger.warning(
                        f"chunk_size_bytes_missing object_id={info.object_id} parts={sorted(missing_chunk_size_parts)}"
                    )
                    raise DownloadNotReadyError("chunk_size_bytes_missing")

                # Plan minimal indices per part (pure math, per-part chunk_size)
                per_part_indices: dict[int, list[int]] = {}
                for p in cid_backed_parts:
                    pn = int(p.part_number)
                    if pn not in per_part_chunk_size or pn not in offset_map:
                        continue
                    chunk_size = int(per_part_chunk_size[pn])
                    part_start, part_plain_size = offset_map[pn]
                    # Compute local range within this part
                    local_start = max(0, int(rng.start) - part_start)
                    local_end = min(max(0, part_plain_size - 1), int(rng.end) - part_start)
                    if local_start > local_end or part_plain_size <= 0 or chunk_size <= 0:
                        continue
                    s_chunk = local_start // chunk_size
                    e_chunk = local_end // chunk_size
                    per_part_indices[pn] = list(range(int(s_chunk), int(e_chunk) + 1))

                # Enqueue with minimal indices
                dl_parts = []
                for p in cid_backed_parts:
                    indices = per_part_indices.get(int(p.part_number))
                    if p.cid:
                        dl = ChunkToDownload(cid=p.cid, part_id=p.part_number, redis_key=None, chunk_indices=indices)
                        dl_parts.append(dl)
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
                await enqueue_download_request(req, redis)
            else:
                await self.hydrate_object_cache(
                    redis,
                    obj_cache,
                    info,
                    cid_backed_parts,  # Only CID-backed parts
                    address=address,
                    seed_phrase=seed_phrase,
                    rng=None,
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

        # Step 5 removed: no pre-header readiness wait; generator will handle bounded waits per chunk

        # Step 6: Prepare and stream the response
        _md = info.metadata
        if isinstance(_md, str):
            try:
                _md = json.loads(_md)
            except Exception:
                _md = {}

        # For range requests: do a bounded polling refresh to ensure we have all appended parts
        # before computing the range. Appends commit to DB atomically but read may race.
        # Update plaintext_sizes as we discover new parts or size changes.
        if rng is not None:
            import time as _tr

            refresh_start = _tr.time()
            refresh_deadline = refresh_start + 2.0  # up to 2s to observe appends in DB
            initial_count = len(full_parts)
            while _tr.time() < refresh_deadline:
                try:
                    latest_parts = await self.build_manifest(db, info)
                    with contextlib.suppress(Exception):
                        self._logger.debug(
                            f"RANGE refresh: initial={initial_count} latest={len(latest_parts)} parts={[(p.part_number, p.size_bytes) for p in latest_parts]}"
                        )
                    # BUGFIX: Always update full_parts if we got new data, not just when count increases
                    # Parts can have size updates (e.g., after pinner writes CID) without count changing
                    if latest_parts and len(latest_parts) > len(full_parts):
                        # New parts appeared: update full_parts AND recalculate needed parts for range
                        full_parts = sorted(latest_parts, key=lambda p: p.part_number)
                        # Recompute plaintext sizes for new parts
                        for p in full_parts:
                            pn = int(p.part_number)
                            if pn not in plaintext_sizes:
                                # Estimate plaintext size for new part
                                if info.should_decrypt and p.size_bytes > CryptoService.OVERHEAD_PER_CHUNK:
                                    plaintext_sizes[pn] = int(p.size_bytes) - CryptoService.OVERHEAD_PER_CHUNK
                                else:
                                    plaintext_sizes[pn] = int(p.size_bytes)
                        # Note: we don't re-filter parts here; the initial filter was conservative
                        # and gen_range will stream all available parts in the range dynamically
                    # If we've extended and all known parts are in cache, we're done
                    if latest_parts and len(latest_parts) >= initial_count:
                        all_in_cache = True
                        for lp in latest_parts:
                            if not await obj_cache.exists(info.object_id, int(lp.part_number)):
                                all_in_cache = False
                                break
                        if all_in_cache:
                            with contextlib.suppress(Exception):
                                self._logger.debug(f"RANGE refresh complete: all {len(full_parts)} parts in cache")
                            break
                except Exception:
                    pass
                await asyncio.sleep(0.05)

            # Build plaintext size map from cache metadata
            # For encrypted objects, cache meta stores ciphertext size; we need to probe actual decrypted size
            # For public objects, cache stores plaintext directly
            for p in full_parts:
                pn = int(p.part_number)
                try:
                    # Try to get plaintext size by checking cache or reading the part
                    meta = await obj_cache.get_meta(info.object_id, pn)
                    if meta and not info.should_decrypt:
                        # Public object: cache has plaintext, meta size_bytes is accurate
                        plaintext_sizes[pn] = int(meta.get("size_bytes", p.size_bytes))
                    elif meta and info.should_decrypt:
                        # Encrypted object: need to compute plaintext size
                        # Ciphertext size = plaintext_size + OVERHEAD_PER_CHUNK * num_chunks
                        # So plaintext_size = ciphertext_size - OVERHEAD_PER_CHUNK * num_chunks
                        ct_size = int(meta.get("size_bytes", 0))
                        num_chunks = int(meta.get("num_chunks", 0))
                        if ct_size > 0 and num_chunks > 0:
                            overhead = CryptoService.OVERHEAD_PER_CHUNK * num_chunks
                            plaintext_sizes[pn] = max(0, ct_size - overhead)
                        else:
                            # Fallback: use DB size (may be stale but better than nothing)
                            plaintext_sizes[pn] = int(p.size_bytes)
                    else:
                        # No meta yet: use DB size as best guess
                        # This may be ciphertext size if pinner ran, or plaintext if it hasn't
                        # For now, assume it's ciphertext and subtract estimated overhead
                        if info.should_decrypt and p.size_bytes > CryptoService.OVERHEAD_PER_CHUNK:
                            # Estimate: assume single chunk per part for sizing
                            plaintext_sizes[pn] = int(p.size_bytes) - CryptoService.OVERHEAD_PER_CHUNK
                        else:
                            plaintext_sizes[pn] = int(p.size_bytes)
                except Exception:
                    # Fallback to DB size
                    plaintext_sizes[pn] = int(p.size_bytes)

            with contextlib.suppress(Exception):
                self._logger.info(f"RANGE plaintext_sizes computed: {plaintext_sizes}")

        async def _read_part_bytes(part_number: int) -> bytes:
            # Assemble ciphertext/plaintext chunks from cache; decrypt once as needed
            import time as _ptime

            part_start = _ptime.perf_counter()
            meta = await obj_cache.get_meta(info.object_id, int(part_number))  # type: ignore[attr-defined]
            with contextlib.suppress(Exception):
                self._logger.debug(f"READER part={int(part_number)} should_decrypt={info.should_decrypt} meta={meta}")
            if not meta:
                # Best-effort short wait for meta to appear (hydration in progress)
                import time as _t

                deadline_meta = _t.time() + float(self.config.http_stream_initial_timeout_seconds)
                while _t.time() < deadline_meta:
                    m2 = await obj_cache.get_meta(info.object_id, int(part_number))  # type: ignore[attr-defined]
                    if m2:
                        meta = m2
                        break
                    await asyncio.sleep(self.config.http_download_sleep_loop)

                if not meta:
                    data = await obj_cache.get(info.object_id, int(part_number))
                else:
                    data = None
                with contextlib.suppress(Exception):
                    self._logger.debug(
                        f"READER part={int(part_number)} fallback whole get size={0 if data is None else len(data)}"
                    )
                if data is not None:
                    return data
            # If meta is present but chunks are not yet written, wait briefly
            num_chunks = int(meta.get("num_chunks", 0)) if meta else 0
            if num_chunks <= 0:
                import time as _tw

                deadline_nc = _tw.time() + float(self.config.http_stream_initial_timeout_seconds)
                while _tw.time() < deadline_nc:
                    m3 = await obj_cache.get_meta(info.object_id, int(part_number))  # type: ignore[attr-defined]
                    try:
                        num_chunks = int(m3.get("num_chunks", 0)) if m3 else 0
                    except Exception:
                        num_chunks = 0
                    if num_chunks > 0:
                        meta = m3 or meta
                        break
                    await asyncio.sleep(self.config.http_download_sleep_loop)
                if num_chunks <= 0:
                    # Last fallback to whole-get if available
                    data_fallback = await obj_cache.get(info.object_id, int(part_number))
                    if data_fallback is not None:
                        return data_fallback
                    return b""
            chunks: list[bytes] = []
            for ci in range(num_chunks):
                while True:
                    chunk_bytes = await obj_cache.get_chunk(info.object_id, int(part_number), ci)  # type: ignore[attr-defined]
                    if chunk_bytes is not None:
                        break
                    await asyncio.sleep(self.config.http_download_sleep_loop)
                chunks.append(chunk_bytes)
            with contextlib.suppress(Exception):
                self._logger.debug(
                    f"READER part={int(part_number)} num_chunks={num_chunks} first_chunk_len={len(chunks[0]) if chunks else -1}"
                )

            if info.should_decrypt:
                # Try legacy whole-part decrypt first; fallback to per-chunk
                ct_all = b"".join(chunks)
                try:
                    return CryptoService.decrypt_part_auto(
                        ct_all,
                        seed_phrase=seed_phrase,
                        object_id=info.object_id,
                        part_number=int(part_number),
                        chunk_count=len(chunks),
                        chunk_loader=lambda i: chunks[i],
                    )
                except Exception:
                    try:
                        pts: list[bytes] = []
                        for ci, c in enumerate(chunks):
                            pts.append(
                                CryptoService.decrypt_chunk(
                                    c,
                                    seed_phrase=seed_phrase,
                                    object_id=info.object_id,
                                    part_number=int(part_number),
                                    chunk_index=ci,
                                )
                            )
                        return b"".join(pts)
                    except Exception:
                        # Fallback: data may be plaintext in cache despite should_decrypt flag
                        with contextlib.suppress(Exception):
                            self._logger.warning(
                                f"READER decrypt failed; returning plaintext for part={int(part_number)}"
                            )
                        return b"".join(chunks)
            # Public/plaintext or private-but-plaintext-cached
            result = b"".join(chunks)
            part_ms = (_ptime.perf_counter() - part_start) * 1000.0
            log_timing(
                "object_reader.read_part",
                part_ms,
                extra={
                    "object_id": info.object_id,
                    "part_number": int(part_number),
                    "size_bytes": len(result),
                    "decrypted": info.should_decrypt,
                },
            )
            return result

        # Handle Range Request
        if rng is not None:

            async def gen_range() -> AsyncGenerator[bytes, None]:
                # Stream parts needed for range: compute offsets using PLAINTEXT sizes
                import time as _t

                cursor = int(rng.start)
                end_inclusive = int(rng.end)

                # Use the already-refreshed full_parts as the source of truth
                ordered_parts = sorted(full_parts, key=lambda x: x.part_number)
                with contextlib.suppress(Exception):
                    self._logger.info(
                        f"RANGE gen_range: start={cursor} end={end_inclusive} full_parts_count={len(full_parts)} ordered_parts={[(p.part_number, p.size_bytes) for p in ordered_parts]}"
                    )
                # BUGFIX: Use plaintext sizes for range computation (user requested range is in plaintext bytes)
                layout_full = [
                    {"part_number": p.part_number, "size_bytes": plaintext_sizes.get(p.part_number, p.size_bytes)}
                    for p in ordered_parts
                ]
                with contextlib.suppress(Exception):
                    self._logger.info(
                        f"RANGE gen_range: layout_full (plaintext)={[(e['part_number'], e['size_bytes']) for e in layout_full]}"
                    )
                # Compute offsets once upfront
                offsets: dict[int, int] = {}
                acc = 0
                for e in layout_full:
                    pn_tmp = int(e["part_number"])
                    offsets[pn_tmp] = acc
                    acc += int(e["size_bytes"])

                total_plain = acc
                with contextlib.suppress(Exception):
                    self._logger.info(f"RANGE gen_range: total_plain={total_plain} offsets={offsets}")

                # Preflight removed: we will poll per-chunk with a bounded timeout while streaming

                while cursor <= end_inclusive:
                    # Identify part covering current cursor
                    target_pn = None
                    part_start = 0
                    size_bytes = 0
                    for e in layout_full:
                        pn_candidate = int(e["part_number"])
                        start_candidate = int(offsets[pn_candidate])
                        end_candidate = start_candidate + int(e["size_bytes"]) - 1
                        if start_candidate <= cursor <= end_candidate:
                            target_pn = pn_candidate
                            part_start = start_candidate
                            size_bytes = int(e["size_bytes"])
                            break
                    if target_pn is None:
                        with contextlib.suppress(Exception):
                            self._logger.warning(
                                f"RANGE gen_range: No part covers cursor={cursor}, breaking. total_plain={total_plain} end_inclusive={end_inclusive}"
                            )
                        break

                    with contextlib.suppress(Exception):
                        self._logger.info(
                            f"RANGE gen_range: target_pn={target_pn} part_start={part_start} size_bytes={size_bytes} cursor={cursor}"
                        )

                    # Per-part readiness wait (bounded) before reading
                    wait_deadline = _t.time() + float(self.config.http_stream_initial_timeout_seconds)
                    while True:
                        meta = await obj_cache.get_meta(info.object_id, target_pn)  # type: ignore[attr-defined]
                        ready = bool(meta and int(meta.get("num_chunks", 0)) > 0)
                        if not ready:
                            whole = await obj_cache.get(info.object_id, target_pn)
                            ready = isinstance(whole, (bytes, bytearray)) and len(whole) > 0
                        if ready or _t.time() > wait_deadline:
                            break
                        await asyncio.sleep(self.config.http_download_sleep_loop)

                    # Yield only the plaintext needed within this part using per-chunk decrypt
                    # Compute local range within part
                    local_start = max(0, cursor - part_start)
                    local_end_inclusive = min(size_bytes - 1, end_inclusive - part_start)

                    # Prefer meta; if missing but part bytes exist, slice whole-part for this request only
                    meta = await obj_cache.get_meta(info.object_id, target_pn)  # type: ignore[attr-defined]
                    if not isinstance(meta, dict):
                        data = await _read_part_bytes(target_pn)
                        if not data:
                            raise DownloadNotReadyError("chunk_size_bytes_missing")
                        yield data[local_start : local_end_inclusive + 1]
                        cursor = part_start + local_end_inclusive + 1
                        continue

                    try:
                        chunk_size = int(meta.get("chunk_size", 0))
                        num_chunks = int(meta.get("num_chunks", 0))
                    except Exception:
                        chunk_size = 0
                        num_chunks = 0

                    if num_chunks <= 0 or chunk_size <= 0:
                        # Fallback to whole-part slice if available in cache for this request
                        data = await _read_part_bytes(target_pn)
                        if not data:
                            raise DownloadNotReadyError("chunk_size_bytes_missing")
                        yield data[local_start : local_end_inclusive + 1]
                        cursor = part_start + local_end_inclusive + 1
                        continue

                    start_chunk = int(local_start // chunk_size)
                    end_chunk = int(local_end_inclusive // chunk_size)
                    for ci in range(start_chunk, min(end_chunk, num_chunks - 1) + 1):
                        # Fetch single ciphertext chunk with bounded polling per chunk
                        cbytes = None
                        chunk_deadline = _t.time() + float(self.config.http_stream_initial_timeout_seconds)
                        while _t.time() < chunk_deadline:
                            cbytes = await obj_cache.get_chunk(info.object_id, int(target_pn), int(ci))  # type: ignore[attr-defined]
                            if cbytes is not None:
                                break
                            await asyncio.sleep(self.config.http_download_sleep_loop)
                        if cbytes is None:
                            raise DownloadNotReadyError("range_chunk_missing")

                        # Decrypt if needed
                        if info.should_decrypt:
                            try:
                                pt = CryptoService.decrypt_chunk(
                                    cbytes,
                                    seed_phrase=seed_phrase,
                                    object_id=info.object_id,
                                    part_number=int(target_pn),
                                    chunk_index=int(ci),
                                )
                            except Exception as err:
                                # Surface as SlowDown rather than stalling/assembling whole part
                                raise DownloadNotReadyError("range_decrypt_failed") from err
                        else:
                            pt = cbytes

                        # Slice within this plaintext chunk
                        chunk_pt_start = ci * chunk_size
                        s_local = max(0, local_start - chunk_pt_start)
                        e_local_excl = min(len(pt), (local_end_inclusive - chunk_pt_start) + 1)
                        if e_local_excl > s_local:
                            yield pt[s_local:e_local_excl]

                    cursor = part_start + local_end_inclusive + 1

            # Build range-aware headers safely
            headers = build_headers(
                dataclasses.asdict(info),
                source=source_header,
                metadata=_md,
                rng=(rng.start, rng.end) if rng is not None else None,
                range_was_invalid=range_was_invalid,
            )
            status_code = 200 if range_was_invalid else 206
            return StreamingResponse(
                gen_range(), status_code=status_code, media_type=info.content_type, headers=headers
            )

        # Handle Full Request (per-chunk streaming)
        async def gen_full() -> AsyncGenerator[bytes, None]:
            import time as _ft

            for p in parts:
                pn = int(p.part_number)

                # Determine chunk plan: prefer cache meta; fallback to DB meta
                chunk_size: int = 0
                num_chunks: int = 0

                try:
                    cmeta = await read_cache_meta(obj_cache, info.object_id, pn)
                    if cmeta:
                        chunk_size = int(cmeta.get("chunk_size", 0))
                        num_chunks = int(cmeta.get("num_chunks", 0))
                except Exception:
                    chunk_size = 0
                    num_chunks = 0

                if num_chunks <= 0 or chunk_size <= 0:
                    try:
                        dbm = await read_db_meta(db, info.object_id, pn)
                    except Exception:
                        dbm = None
                    if dbm:
                        try:
                            chunk_size = int(dbm.get("chunk_size_bytes") or 0)  # type: ignore[arg-type]
                        except Exception:
                            chunk_size = 0
                        try:
                            num_chunks = int(dbm.get("num_chunks_db") or 0)  # type: ignore[arg-type]
                        except Exception:
                            num_chunks = 0

                # If still unknown, do a bounded wait for cache meta to appear
                if num_chunks <= 0 or chunk_size <= 0:
                    deadline = _ft.time() + float(self.config.http_stream_initial_timeout_seconds)
                    while _ft.time() < deadline:
                        try:
                            cmeta2 = await read_cache_meta(obj_cache, info.object_id, pn)
                        except Exception:
                            cmeta2 = None
                        if cmeta2 and int(cmeta2.get("num_chunks", 0)) > 0 and int(cmeta2.get("chunk_size", 0)) > 0:
                            chunk_size = int(cmeta2.get("chunk_size", 0))
                            num_chunks = int(cmeta2.get("num_chunks", 0))
                            break
                        await asyncio.sleep(self.config.http_download_sleep_loop)

                if num_chunks <= 0 or chunk_size <= 0:
                    # Unable to determine chunking; signal client to retry later
                    raise DownloadNotReadyError("chunk_size_bytes_missing")

                # Stream each chunk with bounded polling and per-chunk decrypt if needed
                for ci in range(int(num_chunks)):
                    chunk_deadline = _ft.time() + float(self.config.http_stream_initial_timeout_seconds)
                    cbytes = None
                    while _ft.time() < chunk_deadline:
                        cbytes = await obj_cache.get_chunk(info.object_id, pn, int(ci))  # type: ignore[attr-defined]
                        if cbytes is not None:
                            break
                        await asyncio.sleep(self.config.http_download_sleep_loop)
                    if cbytes is None:
                        # Chunk not ready within deadline
                        raise DownloadNotReadyError("range_chunk_missing")

                    if info.should_decrypt:
                        try:
                            pt = CryptoService.decrypt_chunk(
                                cbytes,
                                seed_phrase=seed_phrase,
                                object_id=info.object_id,
                                part_number=pn,
                                chunk_index=int(ci),
                            )
                        except Exception as err:
                            raise DownloadNotReadyError("range_decrypt_failed") from err
                        yield pt
                    else:
                        yield cbytes

        # Log total read_response time
        total_read_ms = (_timing_time.perf_counter() - read_start) * 1000.0
        log_timing(
            "object_reader.read_response_total",
            total_read_ms,
            extra={"object_id": info.object_id, "range": bool(rng), "source": source_header},
        )

        headers = build_headers(dataclasses.asdict(info), source=source_header, metadata=_md)
        return StreamingResponse(gen_full(), media_type=info.content_type, headers=headers)
