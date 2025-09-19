from __future__ import annotations

from typing import Tuple

from fastapi import Request


def parse_read_mode(request: Request) -> str:
    """Return read mode.

    For production, this is always 'auto'. Header-based modes are deprecated and ignored.
    """
    return "auto"


def parse_range(request: Request, total_size: int) -> Tuple[object | None, str | None]:
    """Extract Range header value; returns (placeholder_range, header_value).

    We defer validating/constructing concrete ranges to range_utils.parse_range_header.
    """
    rng = request.headers.get("Range") or request.headers.get("range")
    return None, rng
