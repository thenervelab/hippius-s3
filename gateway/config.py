import dataclasses
import logging
import os


logger = logging.getLogger(__name__)


def _parse_csv(value: str) -> list[str]:
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


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
    redis_acl_url: str = dataclasses.field(
        default_factory=lambda: os.getenv("REDIS_ACL_URL", "redis://redis-acl:6379/0")
    )
    hippius_chain_url: str = dataclasses.field(default_factory=lambda: os.getenv("HIPPIUS_CHAIN_URL", ""))
    substrate_url: str = dataclasses.field(default_factory=lambda: os.getenv("HIPPIUS_SUBSTRATE_URL", ""))
    enable_public_read: bool = dataclasses.field(
        default_factory=lambda: os.getenv("ENABLE_PUBLIC_READ", "false").lower() == "true"
    )
    enable_banhammer: bool = dataclasses.field(
        default_factory=lambda: os.getenv("ENABLE_BANHAMMER", "false").lower() == "true"
    )
    enable_audit_logging: bool = dataclasses.field(
        default_factory=lambda: os.getenv("ENABLE_AUDIT_LOGGING", "true").lower() == "true"
    )
    bypass_credit_check: bool = dataclasses.field(
        default_factory=lambda: os.getenv("HIPPIUS_BYPASS_CREDIT_CHECK", "false").lower() == "true"
    )
    rate_limit_per_minute: int = dataclasses.field(
        default_factory=lambda: int(os.getenv("HIPPIUS_RATE_LIMIT_PER_MINUTE", "7200"))
    )

    # Banhammer tuning (enforced in gateway.middlewares.banhammer)
    # Allowlist of exact IP strings (no CIDR parsing yet)
    banhammer_allowlist_ips: set[str] = dataclasses.field(
        default_factory=lambda: set(_parse_csv(os.getenv("BANHAMMER_ALLOWLIST_IPS", "")))
    )

    # Unauthenticated profile (strict)
    banhammer_unauth_window_seconds: int = dataclasses.field(
        default_factory=lambda: int(os.getenv("BANHAMMER_UNAUTH_WINDOW_SECONDS", "60"))
    )
    banhammer_unauth_max: int = dataclasses.field(default_factory=lambda: int(os.getenv("BANHAMMER_UNAUTH_MAX", "50")))
    banhammer_unauth_ban_seconds: int = dataclasses.field(
        default_factory=lambda: int(os.getenv("BANHAMMER_UNAUTH_BAN_SECONDS", "3600"))
    )

    # Authenticated profile (lenient, short ban)
    banhammer_auth_window_seconds: int = dataclasses.field(
        default_factory=lambda: int(os.getenv("BANHAMMER_AUTH_WINDOW_SECONDS", "60"))
    )
    banhammer_auth_max: int = dataclasses.field(default_factory=lambda: int(os.getenv("BANHAMMER_AUTH_MAX", "200")))
    banhammer_auth_ban_seconds: int = dataclasses.field(
        default_factory=lambda: int(os.getenv("BANHAMMER_AUTH_BAN_SECONDS", "300"))
    )

    # Methods where unauthenticated 404s count (default: GET,HEAD)
    banhammer_unauth_404_methods: set[str] = dataclasses.field(
        default_factory=lambda: {m.upper() for m in _parse_csv(os.getenv("BANHAMMER_UNAUTH_404_METHODS", "GET,HEAD"))}
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
    hippius_api_base_url: str = dataclasses.field(
        default_factory=lambda: os.getenv("HIPPIUS_API_BASE_URL", "https://arion.hippius.com/")
    )
    hippius_service_key: str = dataclasses.field(default_factory=lambda: os.getenv("HIPPIUS_SERVICE_KEY", ""))
    hippius_secret_decryption_material: str = dataclasses.field(
        default_factory=lambda: os.getenv("HIPPIUS_AUTH_ENCRYPTION_KEY", "")
    )
    enable_api_docs: bool = dataclasses.field(
        default_factory=lambda: os.getenv("ENABLE_API_DOCS", "true").lower() == "true"
    )
    docs_cache_ttl_seconds: int = dataclasses.field(
        default_factory=lambda: int(os.getenv("DOCS_CACHE_TTL_SECONDS", "300"))
    )
    db_pool_min_size: int = dataclasses.field(default_factory=lambda: int(os.getenv("GATEWAY_DB_POOL_MIN_SIZE", "10")))
    db_pool_max_size: int = dataclasses.field(default_factory=lambda: int(os.getenv("GATEWAY_DB_POOL_MAX_SIZE", "50")))
    db_pool_max_queries: int = dataclasses.field(
        default_factory=lambda: int(os.getenv("GATEWAY_DB_POOL_MAX_QUERIES", "50000"))
    )
    db_pool_max_inactive_lifetime: int = dataclasses.field(
        default_factory=lambda: int(os.getenv("GATEWAY_DB_POOL_MAX_INACTIVE_LIFETIME", "300"))
    )
    db_pool_command_timeout: int = dataclasses.field(
        default_factory=lambda: int(os.getenv("GATEWAY_DB_POOL_COMMAND_TIMEOUT", "30"))
    )


_config: GatewayConfig | None = None


def get_config() -> GatewayConfig:
    global _config
    if _config is None:
        _config = GatewayConfig()
        logger.info(f"Gateway config loaded: backend_url={_config.backend_url}, port={_config.port}")
    return _config
