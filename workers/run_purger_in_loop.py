#!/usr/bin/env python3
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.sentry import init_sentry
from hippius_s3.workers.purger import run_purger_loop
from hippius_s3.workers.shutdown import run_worker


config = get_config()

setup_loki_logging(config, "purger")
init_sentry("purger", is_worker=True)


if __name__ == "__main__":
    run_worker(run_purger_loop, "purger")
