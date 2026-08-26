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


# AWS clients send the literal "null" as the version id of an object that predates versioning. We
# never mint that id, so it means "whatever the current version is" — i.e. no version specified.
NULL_VERSION_ID = "null"


# object_version is a bigint column; anything above this cannot name a real version and would
# otherwise reach asyncpg's int8 encoder, which raises *after* the handler's validation — a 500
# where AWS returns 400.
_MAX_VERSION_ID = 2**63 - 1


def parse_version_id(raw: str | None) -> int | None:
    """Parse an S3 ``versionId`` into our integer version, or None for "current".

    Raises ValueError for anything that is neither absent, "null", nor a positive integer, so each
    caller can shape its own error response (GET/DELETE return XML, HEAD returns bare headers).
    """
    if raw is None or raw == "" or raw == NULL_VERSION_ID:
        return None
    # Not int(raw): it accepts underscore separators ("1_0" -> 10), a leading sign, surrounding
    # whitespace, and non-ASCII decimal digits (Arabic-Indic "٤" -> 4). A version id is an opaque
    # client-supplied string, so every one of those silently resolves to a DIFFERENT version than
    # the caller named. Only bare ASCII digits may parse.
    if not (raw.isascii() and raw.isdigit()):
        raise ValueError(f"version id must be ASCII decimal digits, got {raw!r}")
    version_id = int(raw)
    if version_id <= 0:
        raise ValueError(f"version id must be positive, got {version_id}")
    if version_id > _MAX_VERSION_ID:
        raise ValueError(f"version id out of range, got {version_id}")
    return version_id
