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
    info: dict,
    *,
    rng: RangeRequest | None,
    address: str,
    fs_store: Any,
) -> StreamContext:
    cfg = get_config()
    storage_version = require_supported_storage_version(int(info["storage_version"]))
    # v4-only policy: always decrypt at read time.

    ov = int(info.get("object_version") or info.get("current_object_version") or 1)
    parts = await read_parts_list(db, info["object_id"], ov)
    plan = await build_chunk_plan(db, info["object_id"], parts, rng, object_version=ov)

    source = "cache"
    try:
        for item in plan:
            exists = await fs_store.chunk_exists(
                info["object_id"],
                int(info.get("object_version") or info.get("current_object_version") or 1),
                int(item.part_number),
                int(item.chunk_index),
            )
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
            ok = await fs_store.chunk_exists(
                info["object_id"],
                int(info.get("object_version") or info.get("current_object_version") or 1),
                int(item.part_number),
                int(item.chunk_index),
            )
            if ok:
                continue
            idx_set = indices_by_part.setdefault(int(item.part_number), set())
            idx_set.add(int(item.chunk_index))
        dl_parts: list[PartToDownload] = []
        if storage_version >= 4:
            # v4+: CIDs are optional.
            # - If per-chunk CIDs exist in part_chunks, include them so the IPFS downloader can hydrate from IPFS.
            # - Otherwise, keep cid=None so alternative hydrators (deterministic addressing) can handle it.
            for pn, idxs in indices_by_part.items():
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

                v4_specs: list[PartChunkSpec] = []
                for ci in sorted(include):
                    cid_val, clen = by_index.get(int(ci), (None, None))
                    v4_specs.append(PartChunkSpec(index=int(ci), cid=cid_val, cipher_size_bytes=clen))

                dl_parts.append(PartToDownload(part_number=int(pn), chunks=v4_specs))
        else:
            missing_meta_parts: list[int] = []
            for pn, idxs in indices_by_part.items():
                try:
                    from hippius_s3.utils import get_query  # local import

                    rows = await db.fetch(
                        get_query("get_part_chunks_by_object_and_number"),
                        info["object_id"],
                        int(info.get("object_version") or info.get("current_object_version") or 1),
                        int(pn),
                    )
                    # Preserve raw cid (may be NULL on corrupt rows) so validation is explicit.
                    all_entries = [(int(r[0]), r[1], int(r[2]) if r[2] is not None else None) for r in rows or []]
                    specs: list[PartChunkSpec] = []
                    include = {int(i) for i in idxs}
                    for ci, cid, clen in all_entries:
                        if int(ci) in include:
                            cid_val = str(cid).strip() if cid is not None else None
                            if cid_val and cid_val.lower() in {"", "none", "pending"}:
                                cid_val = None
                            specs.append(
                                PartChunkSpec(
                                    index=int(ci),
                                    cid=cid_val,
                                    cipher_size_bytes=int(clen) if clen is not None else None,
                                )
                            )
                    if not specs:
                        missing_meta_parts.append(int(pn))
                        continue
                    found = {int(spec.index) for spec in specs}
                    missing = include - found
                    if missing:
                        logger.debug(
                            "STREAM legacy missing chunk metadata indices object_id=%s v=%s part=%s missing=%s",
                            info.get("object_id"),
                            int(info.get("object_version") or info.get("current_object_version") or 1),
                            int(pn),
                            sorted(missing),
                        )
                        missing_meta_parts.append(int(pn))
                        continue
                    # Defensive: ensure every required chunk has a concrete CID (legacy path).
                    # If any are missing/placeholder, treat the whole part as not-ready to
                    # avoid hanging mid-stream on chunks that can never be fetched.
                    bad = []
                    for spec in specs:
                        c = str(spec.cid or "").strip().lower()
                        if c in {"", "none", "pending"}:
                            bad.append(spec.index)
                    if bad:
                        logger.debug(
                            "STREAM legacy bad CID(s) object_id=%s v=%s part=%s bad_indices=%s",
                            info.get("object_id"),
                            int(info.get("object_version") or info.get("current_object_version") or 1),
                            int(pn),
                            sorted([int(x) for x in bad if isinstance(x, int)]),
                        )
                        missing_meta_parts.append(int(pn))
                        continue
                    dl_parts.append(
                        PartToDownload(
                            part_number=int(pn),
                            chunks=specs,
                        )
                    )
                except Exception:
                    missing_meta_parts.append(int(pn))
                    continue
            if missing_meta_parts:
                raise DownloadNotReadyError(
                    f"Parts not ready: missing chunk metadata for parts {sorted(set(missing_meta_parts))}"
                )
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

    if storage_version >= 5:
        suite_id = str(info.get("enc_suite_id") or "hip-enc/aes256gcm")
        kek_id = info.get("kek_id")
        wrapped_dek = info.get("wrapped_dek")
        if not bucket_id or not kek_id or not wrapped_dek:
            raise RuntimeError("v5_missing_envelope_metadata")
        from hippius_s3.services.envelope_service import unwrap_dek
        from hippius_s3.services.kek_service import get_bucket_kek_bytes

        kek_bytes = await get_bucket_kek_bytes(bucket_id=bucket_id, kek_id=kek_id)
        aad = f"hippius-dek:{bucket_id}:{info.get('object_id')}:{object_version}".encode("utf-8")
        key_bytes = unwrap_dek(kek=kek_bytes, wrapped_dek=bytes(wrapped_dek), aad=aad)
    else:
        # v2-v4: per-bucket key from keystore DB
        suite_id = str(info.get("enc_suite_id") or "hip-enc/legacy")
        from hippius_s3.services.key_service import get_or_create_encryption_key_bytes

        bucket_name = str(info.get("bucket_name") or "")
        if not address or not bucket_name:
            raise RuntimeError("missing_address_or_bucket")
        key_bytes = await get_or_create_encryption_key_bytes(main_account_id=address, bucket_name=bucket_name)
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
    info: dict,
    *,
    read_mode: str,
    rng: RangeRequest | None,
    address: str,
    range_was_invalid: bool = False,
    fs_store: Any,
) -> Response:
    cfg = get_config()
    ctx = await build_stream_context(
        db,
        info,
        rng=rng,
        address=address,
        fs_store=fs_store,
    )
    gen = stream_plan(
        object_id=info["object_id"],
        object_version=ctx.object_version,
        plan=ctx.plan,
        sleep_seconds=cfg.http_download_sleep_loop,
        storage_version=ctx.storage_version,
        key_bytes=ctx.key_bytes,
        suite_id=ctx.suite_id,
        bucket_id=ctx.bucket_id,
        upload_id=ctx.upload_id,
        address=address,
        bucket_name=str(info.get("bucket_name", "")),
        prefetch_chunks=int(getattr(cfg, "http_stream_prefetch_chunks", 0) or 0),
        fs_store=fs_store,
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
    info: dict,
    *,
    rng: RangeRequest | None,
    address: str,
    fs_store: Any,
) -> Any:
    """Return an async iterator of plaintext bytes for the requested object.

    This wraps build_stream_context and stream_plan so callers don't need to know
    about parts catalogs, chunk plans, or downloader details.
    """
    cfg = get_config()
    ctx = await build_stream_context(
        db,
        info,
        rng=rng,
        address=address,
        fs_store=fs_store,
    )
    return stream_plan(
        object_id=info["object_id"],
        object_version=ctx.object_version,
        plan=ctx.plan,
        sleep_seconds=cfg.http_download_sleep_loop,
        storage_version=ctx.storage_version,
        key_bytes=ctx.key_bytes,
        suite_id=ctx.suite_id,
        bucket_id=ctx.bucket_id,
        upload_id=ctx.upload_id,
        address=address,
        bucket_name=str(info.get("bucket_name", "")),
        prefetch_chunks=int(getattr(cfg, "http_stream_prefetch_chunks", 0) or 0),
        fs_store=fs_store,
    )
