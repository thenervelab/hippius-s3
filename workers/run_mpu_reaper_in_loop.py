#!/usr/bin/env python3
import logging
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.sentry import init_sentry
from hippius_s3.services.mpu_cleanup import run_mpu_reaper_loop
from hippius_s3.workers.shutdown import run_worker


config = get_config()
setup_loki_logging(config, "mpu-reaper")
logger = logging.getLogger(__name__)
init_sentry("mpu-reaper", is_worker=True)


if __name__ == "__main__":
    run_worker(run_mpu_reaper_loop, "mpu-reaper", restart_on_crash=True)
