import contextvars
import logging
import uuid
from typing import Any
from typing import MutableMapping
from typing import Optional


ray_id_context: contextvars.ContextVar[str] = contextvars.ContextVar("ray_id", default="no-ray-id")


def generate_ray_id() -> str:
    """Generate a 16-character hex ray ID from UUID4.

    Returns:
        A 16-character lowercase hex string (first 64 bits of UUID4).
        Example: "a1b2c3d4e5f67890"
    """
    return uuid.uuid4().hex[:16]


class RayIDLoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that injects ray_id into log records.

    This adapter ensures all log messages include the ray_id in their
    extra context, making it available for log formatting.
    """

    def __init__(self, logger: logging.Logger, ray_id: Optional[str] = None):
        """Initialize the adapter with a logger and ray_id.

        Args:
            logger: The underlying logger instance
            ray_id: The ray ID to inject into all log records.
                   Defaults to "no-ray-id" if not provided.
        """
        super().__init__(logger, {"ray_id": ray_id or "no-ray-id"})

    def process(self, msg: str, kwargs: MutableMapping[str, Any]) -> tuple[str, MutableMapping[str, Any]]:
        """Process the logging message and kwargs.

        Adds ray_id to the extra dict so it appears in log records.
        The RayIDFilter will ensure ray_id is present even if not added here.
        """
        if "extra" not in kwargs:
            kwargs["extra"] = {}
        kwargs["extra"]["ray_id"] = self.extra.get("ray_id", "no-ray-id") if self.extra else "no-ray-id"
        return msg, kwargs


def get_logger_with_ray_id(name: str, ray_id: Optional[str] = None) -> RayIDLoggerAdapter:
    """Get a logger that includes ray_id in all messages.

    Args:
        name: The name of the logger (typically __name__)
        ray_id: The ray ID to inject. Defaults to "no-ray-id" if not provided.

    Returns:
        A logger adapter that automatically includes the ray_id in all log records.
    """
    logger = logging.getLogger(name)
    return RayIDLoggerAdapter(logger, ray_id)
