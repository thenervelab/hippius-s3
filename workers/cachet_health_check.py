#!/usr/bin/env python3
import asyncio
import logging
import sys
from pathlib import Path

import httpx


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging


config = get_config()

setup_loki_logging(config, "cachet-health-checker")
logger = logging.getLogger(__name__)


async def check_health_and_update_cachet():
    """
    Cachet Component Status Codes:
    - Status 1: Operational (everything working normally)
    - Status 2: Performance Issues (degraded but functional)
    - Status 3: Partial Outage (some functionality broken)
    - Status 4: Major Outage (complete failure)
    - Status 0: Unknown (optional, initial state)
    """
    if not config.cachet_api_url or not config.cachet_api_key or not config.cachet_component_id:
        logger.error("Cachet configuration incomplete, skipping health checks")
        return

    health_url = "http://gateway:8080/health"
    cachet_update_url = f"{config.cachet_api_url}/api/v1/components/{config.cachet_component_id}"
    headers = {"X-Cachet-Token": config.cachet_api_key}

    async with httpx.AsyncClient(timeout=10.0) as client:
        while True:
            try:
                response = await client.get(health_url)
                status = 1 if response.status_code == 200 else 3
                logger.info(f"Health check result: {response.status_code}, setting status to {status}")
            except Exception as e:
                status = 4
                logger.error(f"Health check failed with error: {e}, setting status to {status}")

            try:
                update_response = await client.put(cachet_update_url, headers=headers, json={"status": status})
                if update_response.status_code == 200:
                    logger.info(f"Successfully updated Cachet component to status {status}")
                else:
                    logger.error(f"Failed to update Cachet: {update_response.status_code} - {update_response.text}")
            except Exception as e:
                logger.error(f"Failed to update Cachet with error: {e}")

            await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(check_health_and_update_cachet())
