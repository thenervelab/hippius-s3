from __future__ import annotations

from typing import Tuple

from fastapi import Request
from fastapi import Response

from hippius_s3.api.s3 import errors


def expected_bucket_owner_mismatch(request: Request, actual_owner: str | None) -> Response | None:
    """Enforce ``x-amz-expected-bucket-owner``; return an error Response when it does not match.

    AWS's guard against bucket-name confusion: the caller states which account it believes owns the
    bucket, and S3 fails the request with 403 rather than operating on a bucket that was deleted and
    re-created, or whose name was claimed by someone else, since the last time the caller looked.
    Ignoring the header is worse than not supporting it — a client that sends it believes it is
    protected, so a silent no-op turns a deliberate safety check into a false sense of one.

    Absent header means "no expectation", which is the default for every existing caller.
    """
    expected = request.headers.get("x-amz-expected-bucket-owner")
    if not expected:
        return None
    if actual_owner is not None and expected == actual_owner:
        return None
    return errors.s3_error_response(
        "AccessDenied",
        "The bucket is owned by a different account than the one specified in x-amz-expected-bucket-owner.",
        status_code=403,
    )


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
