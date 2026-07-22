#!/usr/bin/env python3
import asyncio
import logging
import sys
import time
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.sentry import init_sentry
from hippius_s3.services.mpu_cleanup import run_mpu_reaper_loop


config = get_config()
setup_loki_logging(config, "mpu-reaper")
logger = logging.getLogger(__name__)
init_sentry("mpu-reaper", is_worker=True)


if __name__ == "__main__":
    while True:
        try:
            asyncio.run(run_mpu_reaper_loop())
        except KeyboardInterrupt:
            logger.info("MPU reaper stopped by user")
            break
        except Exception as e:
            logger.error(f"MPU reaper crashed, restarting in 5 seconds: {e}", exc_info=True)
            time.sleep(5)
