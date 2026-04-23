from __future__ import annotations

import json


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
