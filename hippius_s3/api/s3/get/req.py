from __future__ import annotations

from typing import Optional
from typing import Tuple

from fastapi import Request

from hippius_s3.api.s3.range_utils import parse_range_header
from hippius_s3.services.object_reader import Range as ORRange


def parse_read_mode(request: Request) -> str:
    hdr_mode = (request.headers.get("x-hippius-read-mode") or "").lower()
    if hdr_mode == "pipeline_only":
        return "pipeline_only"
    if hdr_mode == "cache_only":
        return "cache_only"
    # Back-compat via x-amz-meta-cache
    if request.headers.get("x-amz-meta-cache", "").lower() == "true":
        return "cache_only"
    return "auto"


def parse_range(request: Request, total_size: int) -> Tuple[Optional[ORRange], Optional[str]]:
    rng_hdr = request.headers.get("Range") or request.headers.get("range")
    if not rng_hdr:
        return None, None
    try:
        start, end = parse_range_header(rng_hdr, total_size)
        return ORRange(start=start, end=end), rng_hdr
    except ValueError:
        return None, rng_hdr


def make_request_id(object_id: str, account_id: str, rng: Optional[ORRange]) -> str:
    rid = f"{object_id}_{account_id}"
    if rng is not None:
        rid += f"_{rng.start}_{rng.end}"
    return rid
