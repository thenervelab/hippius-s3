from __future__ import annotations

from typing import Tuple

from fastapi import Request


def parse_read_mode(request: Request) -> str:
    """Parse x-hippius-read-mode or legacy x-amz-meta-cache header.

    Returns one of: "pipeline_only", "cache_only", or "auto".
    """
    hdr_mode = (request.headers.get("x-hippius-read-mode") or "").lower().strip()
    if hdr_mode in {"pipeline_only", "cache_only", "auto"}:
        return hdr_mode
    # Back-compat: x-amz-meta-cache=true indicates cache_only
    if (request.headers.get("x-amz-meta-cache") or "").lower().strip() == "true":
        return "cache_only"
    return "auto"


def parse_range(request: Request, total_size: int) -> Tuple[object | None, str | None]:
    """Extract Range header value; returns (placeholder_range, header_value).

    We defer validating/constructing concrete ranges to range_utils.parse_range_header.
    """
    rng = request.headers.get("Range") or request.headers.get("range")
    return None, rng
