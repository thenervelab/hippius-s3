#!/usr/bin/env python3
import asyncio
import logging
import sys
import time
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.services.arion_service import ArionClient
from hippius_s3.workers.unpinner import run_unpinner_loop


config = get_config()
setup_loki_logging(config, "arion-unpinner")
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    while True:
        try:
            asyncio.run(
                run_unpinner_loop(
                    backend_name="arion",
                    backend_client_factory=ArionClient,
                    queue_name="arion_unpin_requests",
                )
            )
        except KeyboardInterrupt:
            logger.info("Arion unpinner service stopped by user")
            break
        except Exception as e:
            logger.error(f"Arion unpinner crashed, restarting in 5 seconds: {e}", exc_info=True)
            time.sleep(5)
