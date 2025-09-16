from __future__ import annotations

import json


def build_headers(
    info: dict, *, source: str, metadata: dict | None = None, rng: tuple[int, int] | None = None
) -> dict[str, str]:
    headers: dict[str, str] = {
        "Content-Type": info["content_type"],
        "Accept-Ranges": "bytes",
        "ETag": f'"{info["md5_hash"]}"',
        "Last-Modified": info["created_at"].strftime("%a, %d %b %Y %H:%M:%S GMT"),
        "x-hippius-source": source,
    }
    if rng is not None:
        start, end = rng
        headers["Content-Range"] = f"bytes {start}-{end}/{info['size_bytes']}"
    # Normalize metadata: accept dict or JSON string
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except Exception:
            metadata = None
    if metadata:
        for k, v in metadata.items():
            if k != "ipfs" and not isinstance(v, dict):
                headers[f"x-amz-meta-{k}"] = str(v)
    return headers
