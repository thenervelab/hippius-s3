from __future__ import annotations

import json
import re
from typing import Any
from typing import Mapping
from typing import Protocol

from starlette.datastructures import QueryParams


class _HeadersLike(Protocol):
    def __setitem__(self, key: str, value: str, /) -> None: ...


_BLAKE3_HEX = re.compile(r"\A[0-9a-f]{64}\Z")


def body_blake3_headers(info: Mapping[str, Any]) -> dict[str, str]:
    """The plaintext-BLAKE3 headers for an object version, or {} when there is nothing to say.

    Always emits the SCOPE alongside the digest, because the digest does not cover the same bytes
    on every upload path and nothing in the value itself reveals which:

      full         simple PUT — the digest covers the whole body.
      first-chunk  multipart — only chunk 0 of part 1 (HIPPIUS_CHUNK_SIZE_BYTES, 4 MiB default)
                   was hashed; see object_writer.mpu_upload_part_stream.
      prefix       the object was appended to (S4). Append materialises its delta as a new part on
                   the SAME version and never rehashes, so the stored digest still describes only
                   the bytes that were there before — a leading prefix of what Content-Length now
                   reports. This is the dangerous case: the value is stale but non-NULL, so
                   without the qualifier it would look like a current whole-body digest.

    Without this, `X-Hippius-Body-Blake3` reads as "BLAKE3 of the body I just gave you" and a
    client verifying a multipart or appended object gets a guaranteed mismatch. AWS has the same
    problem and solves it the same way (`x-amz-checksum-type: FULL_OBJECT | COMPOSITE`).

    The digest is validated rather than trusted: the column is plain `text` with no CHECK, and a
    value carrying CRLF or non-latin-1 would turn a GET into a 500 at header-encoding time.
    """
    digest = info.get("body_blake3")
    if not isinstance(digest, str) or not _BLAKE3_HEX.match(digest):
        return {}
    if int(info.get("append_version") or 0):
        scope = "prefix"
    elif info.get("multipart"):
        scope = "first-chunk"
    else:
        scope = "full"
    return {"X-Hippius-Body-Blake3": digest, "X-Hippius-Body-Blake3-Scope": scope}


RESPONSE_OVERRIDE_PARAMS: dict[str, str] = {
    "response-content-type": "Content-Type",
    "response-content-disposition": "Content-Disposition",
    "response-content-encoding": "Content-Encoding",
    "response-content-language": "Content-Language",
    "response-cache-control": "Cache-Control",
    "response-expires": "Expires",
}

MAX_OVERRIDE_VALUE_LEN = 4096


def parse_response_overrides(query_params: QueryParams, *, is_anonymous: bool) -> dict[str, str]:
    """Parse the six S3 response-header override query params.

    Returns a mapping of {Response-Header-Name: value}. Empty for anonymous requests
    (per S3 spec, overrides only apply to signed requests).
    Raises ValueError on CRLF or oversize values to prevent header injection.
    """
    if is_anonymous:
        return {}
    out: dict[str, str] = {}
    for qparam, header_name in RESPONSE_OVERRIDE_PARAMS.items():
        if qparam not in query_params:
            continue
        value = query_params[qparam]
        if len(value) > MAX_OVERRIDE_VALUE_LEN:
            raise ValueError(f"{qparam} value exceeds {MAX_OVERRIDE_VALUE_LEN} bytes")
        if "\r" in value or "\n" in value:
            raise ValueError(f"{qparam} contains forbidden CRLF")
        out[header_name] = value
    return out


def apply_response_overrides(headers: _HeadersLike, overrides: dict[str, str]) -> None:
    """Overwrite the given headers mapping with parsed response-header overrides."""
    for name, value in overrides.items():
        headers[name] = value


def if_none_match_matches(header_value: str | None, md5_hash: str) -> bool:
    """Return True iff the `If-None-Match` header indicates the client already has the current version.

    Accepts the S3/strong-ETag convention `"<md5_hash>"` or `"<md5_hash>-<parts>"`.
    Also honors the `*` wildcard. Comma-separated lists are supported per RFC 7232.
    """
    if not header_value or not md5_hash:
        return False
    if header_value.strip() == "*":
        return True
    for token in header_value.split(","):
        candidate = token.strip()
        if candidate.startswith("W/"):
            candidate = candidate[2:]
        candidate = candidate.strip('"')
        if candidate == md5_hash:
            return True
    return False


def build_headers(
    info: dict,
    *,
    source: str,
    metadata: dict | str | None = None,
    rng: tuple[int, int] | None = None,
    range_was_invalid: bool = False,
) -> dict[str, str]:
    headers: dict[str, str] = {
        "Content-Type": info["content_type"],
        "Accept-Ranges": "bytes",
        "ETag": f'"{info["md5_hash"]}"',
        "Last-Modified": info["created_at"].strftime("%a, %d %b %Y %H:%M:%S GMT"),
        "x-hippius-source": source,
    }
    if rng is not None and not range_was_invalid:
        start, end = rng
        headers["Content-Range"] = f"bytes {start}-{end}/{info['size_bytes']}"
        headers["Content-Length"] = str(end - start + 1)
    else:
        headers["Content-Length"] = str(info["size_bytes"])
    if metadata:
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = {}
        if isinstance(metadata, dict):
            for k, v in metadata.items():
                if k != "ipfs" and not isinstance(v, dict):
                    headers[f"x-amz-meta-{k}"] = str(v)
    return headers
