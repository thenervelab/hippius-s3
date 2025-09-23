import os
from typing import Any

import requests


def ipfs_ls(cid: str, *, timeout: float = 5.0) -> dict[str, Any]:
    """Return the result of `ipfs ls` for a CID via the configured gateway.

    Uses HIPPIUS_IPFS_GET_URL (defaults to http://ipfs:8080) and calls /api/v0/ls.
    """
    ipfs_gateway = os.environ.get("HIPPIUS_IPFS_GET_URL", "http://ipfs:8080")
    url = f"{ipfs_gateway}/api/v0/ls?arg={cid}"
    resp = requests.post(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()  # type: ignore[no-any-return]


def assert_ipfs_contains_filename(cid: str, filename: str) -> None:
    """Assert that the IPFS object referenced by CID contains a file with given name.

    Works for wrapped directories by searching Links, and for single-file objects by ensuring non-zero size.
    """
    data = ipfs_ls(cid)
    objects = data.get("Objects") or []
    assert objects, f"ipfs ls returned empty for cid={cid}"

    links = objects[0].get("Links") or []
    if links:
        names = [link.get("Name") for link in links]
        assert filename in names, f"expected filename {filename} in IPFS links {names}"
    else:
        # Single object; ensure non-zero size
        size = int(objects[0].get("Size") or 0)
        assert size > 0, "expected non-zero size for single-file IPFS object"
