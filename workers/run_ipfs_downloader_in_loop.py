#!/usr/bin/env python3
"""Per-backend IPFS downloader worker.

Dequeues from ``ipfs_download_requests``, looks up the CID via
``chunk_backend``, fetches ciphertext from the Hippius/IPFS gateway,
and stores it in the Redis chunk cache.
"""

import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.ipfs_service import _stream_cid
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.workers.downloader import run_downloader_loop


config = get_config()
setup_loki_logging(config, "ipfs-downloader")
logger = logging.getLogger(__name__)

BACKEND_NAME = "ipfs"
QUEUE_NAME = "ipfs_download_requests"


async def ipfs_fetch(identifier: str, account_address: str) -> bytes:
    """Download raw ciphertext from IPFS by CID."""
    chunks: list[bytes] = []
    async for chunk in _stream_cid(identifier):
        chunks.append(chunk)
    return b"".join(chunks)


async def main() -> None:
    await run_downloader_loop(
        backend_name=BACKEND_NAME,
        queue_name=QUEUE_NAME,
        fetch_fn=ipfs_fetch,
    )


if __name__ == "__main__":
    asyncio.run(main())
