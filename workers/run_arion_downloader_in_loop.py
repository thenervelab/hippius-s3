#!/usr/bin/env python3
"""Per-backend Arion downloader worker.

Dequeues from ``arion_download_requests``, looks up the Arion identifier
via ``chunk_backend``, fetches ciphertext from the Arion storage gateway,
writes it into the shared filesystem cache (``FileSystemPartsStore``),
and publishes a pub/sub notification so waiting streamers can read it.
"""

import logging
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.sentry import init_sentry
from hippius_s3.services.arion_service import ArionClient
from hippius_s3.workers.downloader import run_downloader_loop
from hippius_s3.workers.shutdown import run_worker


config = get_config()
setup_loki_logging(config, "arion-downloader")
logger = logging.getLogger(__name__)
init_sentry("arion-downloader", is_worker=True)

BACKEND_NAME = "arion"
QUEUE_NAME = "arion_download_requests"


async def main() -> None:
    # One client for the whole loop — fetch_fn runs once per 4 MiB chunk (~1280 for a 5 GiB
    # part), so a client per call would re-handshake TCP+TLS for every chunk and throw the
    # keep-alive pool away. Mirrors run_arion_uploader_in_loop / run_arion_unpinner_in_loop.
    async with ArionClient() as client:

        async def arion_fetch(identifier: str, account_address: str) -> bytes:
            """Download ciphertext from Arion by backend identifier."""
            chunks = [chunk async for chunk in client.download_file(identifier, account_address)]
            return b"".join(chunks)

        await run_downloader_loop(
            backend_name=BACKEND_NAME,
            queue_name=QUEUE_NAME,
            fetch_fn=arion_fetch,
        )


if __name__ == "__main__":
    run_worker(main, "arion-downloader")
