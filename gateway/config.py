import dataclasses
import logging
import os


logger = logging.getLogger(__name__)


@dataclasses.dataclass
class GatewayConfig:
    backend_url: str = dataclasses.field(default_factory=lambda: os.getenv("GATEWAY_BACKEND_URL", "http://api:8000"))
    port: int = dataclasses.field(default_factory=lambda: int(os.getenv("GATEWAY_PORT", "8080")))
    database_url: str = dataclasses.field(default_factory=lambda: os.getenv("DATABASE_URL", ""))
    redis_url: str = dataclasses.field(default_factory=lambda: os.getenv("REDIS_URL", "redis://redis:6379/0"))
    redis_accounts_url: str = dataclasses.field(
        default_factory=lambda: os.getenv("REDIS_ACCOUNTS_URL", "redis://redis-accounts:6379/0")
    )
    redis_chain_url: str = dataclasses.field(
        default_factory=lambda: os.getenv("REDIS_CHAIN_URL", "redis://redis-chain:6379/0")
    )
    redis_rate_limiting_url: str = dataclasses.field(
        default_factory=lambda: os.getenv("REDIS_RATE_LIMITING_URL", "redis://redis-rate-limiting:6379/0")
    )
    hippius_chain_url: str = dataclasses.field(default_factory=lambda: os.getenv("HIPPIUS_CHAIN_URL", ""))
    substrate_url: str = dataclasses.field(default_factory=lambda: os.getenv("HIPPIUS_SUBSTRATE_URL", ""))
    enable_public_read: bool = dataclasses.field(
        default_factory=lambda: os.getenv("ENABLE_PUBLIC_READ", "false").lower() == "true"
    )
    enable_banhammer: bool = dataclasses.field(
        default_factory=lambda: os.getenv("ENABLE_BANHAMMER", "false").lower() == "true"
    )
    bypass_credit_check: bool = dataclasses.field(
        default_factory=lambda: os.getenv("HIPPIUS_BYPASS_CREDIT_CHECK", "false").lower() == "true"
    )
    rate_limit_per_minute: int = dataclasses.field(
        default_factory=lambda: int(os.getenv("HIPPIUS_RATE_LIMIT_PER_MINUTE", "7200"))
    )
    frontend_hmac_secret: str = dataclasses.field(default_factory=lambda: os.getenv("FRONTEND_HMAC_SECRET", ""))
    validator_region: str = dataclasses.field(default_factory=lambda: os.getenv("VALIDATOR_REGION", "decentralized"))
    acl_cache_ttl_seconds: int = dataclasses.field(
        default_factory=lambda: int(os.getenv("ACL_CACHE_TTL_SECONDS", "300"))
    )
    public_bucket_cache_ttl_seconds: int = dataclasses.field(
        default_factory=lambda: int(os.getenv("PUBLIC_BUCKET_CACHE_TTL_SECONDS", "300"))
    )
    log_level: str = dataclasses.field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    loki_url: str = dataclasses.field(
        default_factory=lambda: os.getenv("LOKI_URL", "http://localhost:3100/loki/api/v1/push")
    )
    loki_enabled: bool = dataclasses.field(default_factory=lambda: os.getenv("LOKI_ENABLED", "false").lower() == "true")
    environment: str = dataclasses.field(default_factory=lambda: os.getenv("ENVIRONMENT", "development"))


_config: GatewayConfig | None = None


def get_config() -> GatewayConfig:
    global _config
    if _config is None:
        _config = GatewayConfig()
        logger.info(f"Gateway config loaded: backend_url={_config.backend_url}, port={_config.port}")
    return _config
