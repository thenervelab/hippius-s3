#!/usr/bin/env python3
import asyncio
import logging
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.sentry import init_sentry
from hippius_s3.workers.upload_promoter import run_upload_promoter_loop


config = get_config()
setup_loki_logging(config, "upload-promoter")
logger = logging.getLogger(__name__)
init_sentry("upload-promoter", is_worker=True)


if __name__ == "__main__":
    asyncio.run(run_upload_promoter_loop())
