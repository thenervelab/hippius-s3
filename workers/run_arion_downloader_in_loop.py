#!/usr/bin/env python3
"""Per-backend Arion downloader worker.

Dequeues from ``arion_download_requests``, looks up the Arion identifier
via ``chunk_backend``, fetches ciphertext from the Arion storage gateway,
and stores it in the Redis chunk cache.
"""

import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.services.arion_service import ArionClient
from hippius_s3.workers.downloader import run_downloader_loop


config = get_config()
setup_loki_logging(config, "arion-downloader")
logger = logging.getLogger(__name__)

BACKEND_NAME = "arion"
QUEUE_NAME = "arion_download_requests"


async def arion_fetch(identifier: str, account_address: str) -> bytes:
    """Download ciphertext from Arion by backend identifier."""
    async with ArionClient() as client:
        chunks: list[bytes] = []
        async for chunk in client.download_file(identifier, account_address):
            chunks.append(chunk)
    return b"".join(chunks)


async def main() -> None:
    await run_downloader_loop(
        backend_name=BACKEND_NAME,
        queue_name=QUEUE_NAME,
        fetch_fn=arion_fetch,
    )


if __name__ == "__main__":
    asyncio.run(main())
