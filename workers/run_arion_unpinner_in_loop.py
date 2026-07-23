#!/usr/bin/env python3
import logging
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.sentry import init_sentry
from hippius_s3.services.arion_service import ArionClient
from hippius_s3.workers.shutdown import run_worker
from hippius_s3.workers.unpinner import run_unpinner_loop


config = get_config()
setup_loki_logging(config, "arion-unpinner")
logger = logging.getLogger(__name__)
init_sentry("arion-unpinner", is_worker=True)


if __name__ == "__main__":
    run_worker(
        lambda: run_unpinner_loop(
            backend_name="arion",
            backend_client_factory=ArionClient,
            queue_name="arion_unpin_requests",
        ),
        "arion-unpinner",
        restart_on_crash=True,
    )
