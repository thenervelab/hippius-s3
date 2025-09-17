from __future__ import annotations

import contextlib
import json
import logging
from typing import Any

from fastapi import Response
from fastapi.responses import StreamingResponse

from hippius_s3.api.s3.common import build_headers
from hippius_s3.cache import ObjectPartsCache


logger = logging.getLogger(__name__)


async def assemble_from_cache(
    obj_cache: ObjectPartsCache,
    object_info: dict[str, Any],
    range_header: str | None = None,
    start_byte: int | None = None,
    end_byte: int | None = None,
) -> Response:
    """Assemble bytes for an object from the ObjectPartsCache and return a Response.

    Expects object_info to contain at least: object_id, size_bytes, content_type,
    md5_hash, created_at, metadata, and download_chunks (json list of parts).
    """
    object_id = str(object_info.get("object_id"))
    size_bytes = int(object_info.get("size_bytes", 0))
    content_type = object_info.get("content_type") or "application/octet-stream"
    md5_hash = object_info.get("md5_hash") or ""
    created_at = object_info.get("created_at")
    metadata = object_info.get("metadata") or {}

    # Parse manifest list of parts
    raw = object_info.get("download_chunks")
    parts_list: list[dict[str, Any]] = []
    if isinstance(raw, str):
        with contextlib.suppress(Exception):
            raw = json.loads(raw)
    if isinstance(raw, list):
        for c in raw:
            try:
                pn_val = c.get("part_number") if isinstance(c, dict) else c
                if pn_val is None:
                    raise ValueError("missing part_number")
                pn = int(pn_val)
                parts_list.append({"part_number": pn})
            except Exception:
                continue

    # Fetch available parts in order, concatenate
    collected: list[tuple[int, bytes]] = []
    seen = set()
    for entry in sorted(parts_list, key=lambda d: d["part_number"]):
        pn = int(entry["part_number"])
        if pn in seen:
            continue
        seen.add(pn)
        data = await obj_cache.get(object_id, pn)
        if data is not None:
            collected.append((pn, data))

    # Augment by probing a small range of parts to capture base(0) and contiguous appends
    for pn in range(0, 256):
        if pn in seen:
            continue
        data = await obj_cache.get(object_id, pn)
        if data is not None:
            collected.append((pn, data))
    collected.sort(key=lambda x: x[0])

    blob = b"".join(b for _, b in collected)

    info = {
        "content_type": content_type,
        "md5_hash": md5_hash,
        "created_at": created_at,
        "size_bytes": size_bytes if size_bytes else len(blob),
    }

    # Range handling
    if start_byte is not None and end_byte is not None:
        slice_bytes = blob[start_byte : end_byte + 1]
        headers = build_headers(info, source="cache", metadata=metadata, rng=(start_byte, end_byte))
        return StreamingResponse(iter([slice_bytes]), status_code=206, media_type=content_type, headers=headers)

    headers = build_headers(info, source="cache", metadata=metadata)
    return StreamingResponse(iter([blob]), media_type=content_type, headers=headers)
