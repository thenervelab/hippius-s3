import logging
import os


logger = logging.getLogger(__name__)


def init_sentry(
    service_name: str,
    *,
    is_worker: bool = False,
) -> None:
    dsn = os.environ.get("SENTRY_DSN", "")
    if not dsn:
        return

    import sentry_sdk
    from sentry_sdk.integrations.asyncio import AsyncioIntegration

    integrations: list = []
    if is_worker:
        integrations.append(AsyncioIntegration())

    sentry_sdk.init(
        dsn=dsn,
        environment=os.environ.get("ENVIRONMENT", "development"),
        traces_sample_rate=float(os.environ.get("SENTRY_TRACES_SAMPLE_RATE", "0.1")),
        server_name=service_name,
        integrations=integrations,
        send_default_pii=True,
    )

    logger.info(f"Sentry initialized for {service_name}")
