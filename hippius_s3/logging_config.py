import logging
import os
import sys

from loki_logger_handler.loki_logger_handler import LokiLoggerHandler

from hippius_s3.config import Config


def setup_loki_logging(config: Config, service_name: str) -> logging.Logger:
    """
    Configure logging with optional Loki handler.

    Args:
        config: Application configuration
        service_name: Name of the service (e.g., "api", "uploader", "substrate")

    Returns:
        Configured logger instance
    """
    log_level = getattr(logging, config.log_level.upper(), logging.INFO)

    handlers = [logging.StreamHandler(sys.stdout)]

    if config.loki_enabled and config.loki_url:
        loki_handler = LokiLoggerHandler(
            url=config.loki_url,
            labels={
                "service": service_name,
                "environment": config.environment,
                "host": os.getenv("HOSTNAME", "unknown"),
            },
            timeout=10,
            compressed=True,
        )
        handlers.append(loki_handler)

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )

    return logging.getLogger(service_name)
