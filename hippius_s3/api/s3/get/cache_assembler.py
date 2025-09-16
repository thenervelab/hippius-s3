from __future__ import annotations

import contextlib
import json
import logging

from fastapi import Response
from fastapi.responses import StreamingResponse

from hippius_s3.api.s3.range_utils import calculate_chunks_for_range
from hippius_s3.api.s3.range_utils import extract_range_from_chunks
from hippius_s3.cache import ObjectPartsCache


logger = logging.getLogger(__name__)


async def assemble_from_cache(
    obj_cache: ObjectPartsCache,
    object_info: dict,
    *,
    range_header: str | None = None,
    start_byte: int | None = None,
    end_byte: int | None = None,
) -> Response:
    is_multipart = object_info["multipart"]

    if is_multipart:
        object_id_str = str(object_info["object_id"])  # type: ignore[index]

        # Build part list directly from cache presence to ensure base+appends are joined
        enriched_chunks: list[dict] = []
        for pn in range(0, 256):
            try:
                if await obj_cache.exists(object_id_str, pn):
                    try:
                        size_val = await obj_cache.strlen(object_id_str, pn)
                    except Exception:
                        size_val = 0
                    enriched_chunks.append({"part_number": pn, "size_bytes": int(size_val)})
            except Exception:
                continue

        enriched_chunks = sorted(enriched_chunks, key=lambda x: x["part_number"])
        with contextlib.suppress(Exception):
            logger.info(
                f"CACHE_ASSEMBLER parts for object_id={object_id_str} -> {[c['part_number'] for c in enriched_chunks]}"
            )
        if not enriched_chunks:
            # Fallback to base-only using manifest if cache probes failed
            enriched_chunks = [{"part_number": 0, "size_bytes": int(object_info.get("size_bytes", 0))}]

        if range_header and start_byte is not None and end_byte is not None:
            needed_parts = calculate_chunks_for_range(start_byte, end_byte, enriched_chunks)
            range_chunks_data: list[bytes] = []
            for chunk_info in enriched_chunks:
                if chunk_info["part_number"] in needed_parts:
                    part_num = int(chunk_info["part_number"])
                    chunk_data = await obj_cache.get(object_id_str, part_num)
                    if chunk_data is None:
                        raise RuntimeError(f"Cache key missing: {obj_cache.build_key(object_id_str, part_num)}")
                    range_chunks_data.append(chunk_data)

            range_data = extract_range_from_chunks(
                range_chunks_data, start_byte, end_byte, enriched_chunks, needed_parts
            )

            headers = {
                "Content-Type": object_info["content_type"],
                "Content-Range": f"bytes {start_byte}-{end_byte}/{object_info['size_bytes']}",
                "Accept-Ranges": "bytes",
                "x-hippius-source": "cache",
            }
            return StreamingResponse(
                iter([range_data]), status_code=206, media_type=object_info["content_type"], headers=headers
            )

        # Full object join
        chunks_data: list[bytes] = []
        for chunk_info in enriched_chunks:
            part_num = int(chunk_info["part_number"])
            chunk_data = await obj_cache.get(object_id_str, part_num)
            if chunk_data is None:
                raise RuntimeError(f"Cache key missing: {obj_cache.build_key(object_id_str, part_num)}")
            chunks_data.append(chunk_data)

        headers = {
            "Content-Type": object_info["content_type"],
            "Content-Disposition": f'inline; filename="{object_info["object_key"].split("/")[-1]}"',
            "Accept-Ranges": "bytes",
            "ETag": f'"{object_info["md5_hash"]}"',
            "Last-Modified": object_info["created_at"].strftime("%a, %d %b %Y %H:%M:%S GMT"),
            "x-hippius-source": "cache",
        }

        metadata = object_info.get("metadata") or {}
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = {}
        for key, value in metadata.items():
            if key != "ipfs" and not isinstance(value, dict):
                headers[f"x-amz-meta-{key}"] = str(value)

        return Response(content=b"".join(chunks_data), media_type=object_info["content_type"], headers=headers)

    # Simple objects: read part 0
    object_id_str = str(object_info["object_id"])  # type: ignore[index]
    data = await obj_cache.get(object_id_str, 0)
    if data is None:
        raise RuntimeError(f"Cache key missing: {obj_cache.build_key(object_id_str, 0)}")

    if range_header and start_byte is not None and end_byte is not None:
        range_data = data[start_byte : end_byte + 1]
        headers = {
            "Content-Type": object_info["content_type"],
            "Content-Range": f"bytes {start_byte}-{end_byte}/{len(data)}",
            "Accept-Ranges": "bytes",
            "x-hippius-source": "cache",
        }
        return StreamingResponse(
            iter([range_data]), status_code=206, media_type=object_info["content_type"], headers=headers
        )

    headers = {
        "Content-Type": object_info["content_type"],
        "Content-Disposition": f'inline; filename="{object_info["object_key"].split("/")[-1]}"',
        "Accept-Ranges": "bytes",
        "ETag": f'"{object_info["md5_hash"]}"',
        "Last-Modified": object_info["created_at"].strftime("%a, %d %b %Y %H:%M:%S GMT"),
        "x-hippius-source": "cache",
    }

    metadata = object_info.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except json.JSONDecodeError:
            metadata = {}
    for key, value in metadata.items():
        if key != "ipfs" and not isinstance(value, dict):
            headers[f"x-amz-meta-{key}"] = str(value)

    return StreamingResponse(iter([data]), status_code=200, media_type=object_info["content_type"], headers=headers)
