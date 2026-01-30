import dataclasses
import uuid

import dotenv
import httpx

from hippius_s3.utils import env


dotenv.load_dotenv()


def _parse_csv_urls(value: str | None) -> list[str]:
    out: list[str] = []
    for part in str(value or "").split(","):
        u = part.strip().strip('"').strip("'")
        if not u:
            continue
        out.append(u.rstrip("/"))
    # Preserve order but de-dup
    deduped: list[str] = []
    seen: set[str] = set()
    for u in out:
        if u in seen:
            continue
        seen.add(u)
        deduped.append(u)
    return deduped


def _parse_account_whitelist() -> list[str]:
    """Parse comma-separated account whitelist from environment variable."""
    import os

    whitelist_str = os.environ.get("HIPPIUS_ORPHAN_WORKER_ACCOUNT_WHITELIST", "")
    if whitelist_str:
        return [a.strip() for a in whitelist_str.split(",") if a.strip()]
    return []


@dataclasses.dataclass
class Config:
    """Application configuration settings."""

    # Database Configuration
    database_url: str = env("DATABASE_URL")
    # Inline default prevents KeyError during class init; runtime fallback to DATABASE_URL is applied in get_config()
    encryption_database_url: str = env("HIPPIUS_KEYSTORE_DATABASE_URL:", convert=str)

    # IPFS Configuration
    # Preferred naming: comma-separated IPFS HTTP API base URLs used for reads/writes (`/api/v0/*`).
    # Example: http://ipfs1:5001,http://ipfs2:5001
    ipfs_api_urls: list[str] = env("HIPPIUS_IPFS_API_URLS", convert=_parse_csv_urls)

    # Security
    frontend_hmac_secret: str = env("FRONTEND_HMAC_SECRET")
    rate_limit_per_minute: int = env("RATE_LIMIT_PER_MINUTE", convert=int)
    max_request_size_mb: int = env("MAX_REQUEST_SIZE_MB", convert=int)

    # Logging
    log_level: str = env("LOG_LEVEL")
    loki_url: str = env("LOKI_URL:", convert=str)
    loki_enabled: bool = env("LOKI_ENABLED:false", convert=lambda x: x.lower() == "true")

    # Server Configuration
    host: str = env("HOST")
    port: int = env("PORT", convert=int)
    environment: str = env("ENVIRONMENT")
    debug: bool = env("DEBUG", convert=lambda x: x.lower() == "true")

    # Feature Flags
    enable_audit_logging: bool = env("ENABLE_AUDIT_LOGGING", convert=lambda x: x.lower() == "true")
    enable_api_docs: bool = env("ENABLE_API_DOCS", convert=lambda x: x.lower() == "true")
    enable_request_profiling: bool = env("ENABLE_REQUEST_PROFILING:false", convert=lambda x: x.lower() == "true")
    enable_banhammer: bool = env("ENABLE_BANHAMMER:true", convert=lambda x: x.lower() == "true")
    enable_public_read: bool = env("HIPPIUS_ENABLE_PUBLIC_READ:true", convert=lambda x: x.lower() == "true")
    public_bucket_cache_ttl_seconds: int = env("PUBLIC_BUCKET_CACHE_TTL_SECONDS:60", convert=int)
    enable_bypass_credit_check: bool = env("HIPPIUS_BYPASS_CREDIT_CHECK:false", convert=lambda x: x.lower() == "true")

    # S3 Validation Limits
    min_bucket_name_length: int = env("MIN_BUCKET_NAME_LENGTH", convert=int)
    max_bucket_name_length: int = env("MAX_BUCKET_NAME_LENGTH", convert=int)
    max_object_key_length: int = env("MAX_OBJECT_KEY_LENGTH", convert=int)
    max_metadata_size: int = env("MAX_METADATA_SIZE", convert=int)

    # Blockchain
    substrate_url: str = env("HIPPIUS_SUBSTRATE_URL")
    hippius_service_key: str = env("HIPPIUS_SERVICE_KEY")
    hippius_secret_decryption_material: str = env("HIPPIUS_AUTH_ENCRYPTION_KEY")

    validator_region: str = env("HIPPIUS_VALIDATOR_REGION")
    hippius_api_base_url: str = env("HIPPIUS_API_BASE_URL:https://api.hippius.com/")
    arion_base_url: str = env("HIPPIUS_ARION_BASE_URL:https://arion.hippius.com/")

    # Redis for caching/rate limiting
    redis_url: str = env("REDIS_URL")

    # Redis for account caching (persistent)
    redis_accounts_url: str = env("REDIS_ACCOUNTS_URL:redis://127.0.0.1:6380/0")

    # Redis for chain operations
    redis_chain_url: str = env("REDIS_CHAIN_URL:redis://127.0.0.1:6381/0")

    # Redis for rate limiting and banhammer
    redis_rate_limiting_url: str = env("REDIS_RATE_LIMITING_URL:redis://127.0.0.1:6383/0")

    # Redis for queues (persistent)
    redis_queues_url: str = env("REDIS_QUEUES_URL:redis://127.0.0.1:6382/0")

    # Database connection pool configuration
    db_pool_min_size: int = env("API_DB_POOL_MIN_SIZE:5", convert=int)
    db_pool_max_size: int = env("API_DB_POOL_MAX_SIZE:20", convert=int)
    db_pool_max_queries: int = env("API_DB_POOL_MAX_QUERIES:50000", convert=int)
    db_pool_max_inactive_lifetime: int = env("API_DB_POOL_MAX_INACTIVE_LIFETIME:300", convert=int)
    db_pool_command_timeout: int = env("API_DB_POOL_COMMAND_TIMEOUT:30", convert=int)

    # API signing key for pre-signed URLs
    # Generated on first run if not provided
    api_signing_key: str = env("API_SIGNING_KEY:" + str(uuid.uuid4()))

    # S3 multipart upload settings
    max_multipart_part_size: int = 512 * 1024 * 1024  # 512 MiB per part
    max_multipart_part_count: int = 10000  # AWS S3 standard
    max_object_size: int = 0  # Computed at runtime in get_config()

    # worker specific settings
    unpinner_sleep_loop: float = 5.0
    downloader_sleep_loop: float = 0.01
    cacher_loop_sleep: float = 60.0  # 1 minute
    pin_checker_loop_sleep: float = 7200.0  # 2 hours
    orphan_checker_loop_sleep: int = env("ORPHAN_CHECKER_LOOP_SLEEP:7200", convert=int)  # 2 hours
    orphan_checker_batch_size: int = env("ORPHAN_CHECKER_BATCH_SIZE:500", convert=int)  # Files per API call
    orphan_checker_account_whitelist: list[str] = dataclasses.field(default_factory=_parse_account_whitelist)

    # Uploader configuration (supersedes legacy pinner config)
    uploader_max_attempts: int = env("HIPPIUS_UPLOADER_MAX_ATTEMPTS:5", convert=int)
    uploader_backoff_base_ms: int = env("HIPPIUS_UPLOADER_BACKOFF_BASE_MS:500", convert=int)
    uploader_backoff_max_ms: int = env("HIPPIUS_UPLOADER_BACKOFF_MAX_MS:60000", convert=int)
    uploader_multipart_max_concurrency: int = env("HIPPIUS_UPLOADER_MULTIPART_MAX_CONCURRENCY:5", convert=int)
    uploader_pin_parallelism: int = env("HIPPIUS_UPLOADER_PIN_PARALLELISM:5", convert=int)
    # Heavy validation gating (legacy PINNER_VALIDATE_COVERAGE supported for compat)
    uploader_validate_coverage: bool = env("UPLOADER_VALIDATE_COVERAGE:false", convert=lambda x: x.lower() == "true")

    # Unpinner configuration
    unpinner_parallelism: int = env("HIPPIUS_UNPINNER_PARALLELISM:5", convert=int)
    unpinner_max_attempts: int = env("HIPPIUS_UNPINNER_MAX_ATTEMPTS:5", convert=int)
    unpinner_backoff_base_ms: int = env("HIPPIUS_UNPINNER_BACKOFF_BASE_MS:1000", convert=int)
    unpinner_backoff_max_ms: int = env("HIPPIUS_UNPINNER_BACKOFF_MAX_MS:60000", convert=int)

    # Upload queue configuration
    upload_queue_names: str = env("HIPPIUS_UPLOAD_QUEUE_NAMES:upload_requests", convert=str)

    # Download queue configuration
    download_queue_names: str = env("HIPPIUS_DOWNLOAD_QUEUE_NAMES:download_requests", convert=str)

    # Unpin queue configuration (broadcast to multiple consumers)
    unpin_queue_names: str = env("HIPPIUS_UNPIN_QUEUE_NAMES:unpin_requests", convert=str)

    # Cache TTL (shared across components)
    cache_ttl_seconds: int = env("HIPPIUS_CACHE_TTL:259200", convert=int)
    # Unified object part chunk size (bytes) for cache and range math
    object_chunk_size_bytes: int = env("HIPPIUS_CHUNK_SIZE_BYTES:4194304", convert=int)
    # Downloader behavior (default: no whole-part backfill)
    downloader_allow_part_backfill: bool = env(
        "DOWNLOADER_ALLOW_PART_BACKFILL:false", convert=lambda x: x.lower() == "true"
    )

    # Downloader retry tuning (used by workers/run_downloader_in_loop.py)
    downloader_chunk_retries: int = env("DOWNLOADER_CHUNK_RETRIES:3", convert=int)
    downloader_retry_base_seconds: float = env("DOWNLOADER_RETRY_BASE_SECONDS:0.1", convert=float)
    downloader_retry_jitter_seconds: float = env("DOWNLOADER_RETRY_JITTER_SECONDS:0.1", convert=float)

    # Crypto configuration
    # hip-enc/legacy: SecretBox per-chunk (legacy objects)
    # hip-enc/aes256gcm: AES-256-GCM per-chunk (no upload_id in AAD)
    kek_db_pool_min_size: int = env("KEK_DB_POOL_MIN_SIZE:1", convert=int)
    kek_db_pool_max_size: int = env("KEK_DB_POOL_MAX_SIZE:10", convert=int)
    kek_cache_ttl_seconds: int = env("KEK_CACHE_TTL_SECONDS:300", convert=int)

    # KMS Mode Configuration
    # - "required": KMS is mandatory, fail at startup if not configured (prod/staging/e2e)
    # - "disabled": KMS is off, use local software wrapping (dev only)
    # No silent fallback - misconfiguration crashes fast.
    kms_mode: str = env("HIPPIUS_KMS_MODE:disabled", convert=str)

    # OVH KMS Configuration (for KEK wrapping)
    # Required when kms_mode=required, ignored when kms_mode=disabled
    ovh_kms_endpoint: str = env("HIPPIUS_OVH_KMS_ENDPOINT:", convert=str)
    ovh_kms_okms_id: str = env("HIPPIUS_OVH_KMS_OKMS_ID:", convert=str)
    ovh_kms_default_key_id: str = env("HIPPIUS_OVH_KMS_DEFAULT_KEY_ID:", convert=str)
    ovh_kms_cert_path: str = env("HIPPIUS_OVH_KMS_CERT_PATH:", convert=str)
    ovh_kms_key_path: str = env("HIPPIUS_OVH_KMS_KEY_PATH:", convert=str)
    # CA path (optional):
    # - If set: validates server cert against this CA (private CA / certificate pinning)
    # - If empty: uses system trust store (e.g., Let's Encrypt public CA)
    ovh_kms_ca_path: str = env("HIPPIUS_OVH_KMS_CA_PATH:", convert=str)
    ovh_kms_timeout_seconds: float = env("HIPPIUS_OVH_KMS_TIMEOUT_SECONDS:30.0", convert=float)
    ovh_kms_max_retries: int = env("HIPPIUS_OVH_KMS_MAX_RETRIES:3", convert=int)
    ovh_kms_retry_base_ms: int = env("HIPPIUS_OVH_KMS_RETRY_BASE_MS:500", convert=int)
    ovh_kms_retry_max_ms: int = env("HIPPIUS_OVH_KMS_RETRY_MAX_MS:5000", convert=int)

    # endpoint chunk download settings, quite aggressive
    redis_read_chunk_timeout: int = 60
    http_download_sleep_loop: float = 0.1
    http_redis_get_retries: int = int(redis_read_chunk_timeout / http_download_sleep_loop)

    # initial stream timeout (seconds) before sending first byte
    http_stream_initial_timeout_seconds: float = env("HTTP_STREAM_INITIAL_TIMEOUT_SECONDS:5", convert=float)

    httpx_ipfs_api_timeout: httpx.Timeout = dataclasses.field(default_factory=lambda: httpx.Timeout(5))

    # Download streaming prefetch window (number of chunks to fetch concurrently).
    # Helps cache-hit throughput by reducing per-chunk Redis roundtrip stalls.
    http_stream_prefetch_chunks: int = env("HTTP_STREAM_PREFETCH_CHUNKS:8", convert=int)

    # DLQ configuration
    dlq_dir: str = env("HIPPIUS_DLQ_DIR:/tmp/hippius_dlq")
    dlq_archive_dir: str = env("HIPPIUS_DLQ_ARCHIVE_DIR:/tmp/hippius_dlq_archive")

    # Object parts filesystem cache configuration
    object_cache_dir: str = env("HIPPIUS_OBJECT_CACHE_DIR:/var/lib/hippius/object_cache")
    fs_cache_gc_max_age_seconds: int = env("HIPPIUS_FS_CACHE_GC_MAX_AGE_SECONDS:604800", convert=int)  # 7 days
    mpu_stale_seconds: int = env("HIPPIUS_MPU_STALE_SECONDS:86400", convert=int)  # 1 day
    total_number_of_storage_backends: int = env("HIPPIUS_TOTAL_NUMBER_OF_STORAGE_BACKENDS:2", convert=int)

    # Filesystem cache disk-pressure backoff (ingress control).
    # Threshold can be expressed as either absolute bytes or ratio; we trigger if ANY threshold is hit.
    fs_cache_min_free_bytes: int = env("HIPPIUS_FS_CACHE_MIN_FREE_BYTES:10737418240", convert=int)  # 10 GiB
    fs_cache_min_free_ratio: float = env("HIPPIUS_FS_CACHE_MIN_FREE_RATIO:0.08", convert=float)  # 8%

    # Retry-After (seconds) for SlowDown responses caused by disk pressure.
    fs_cache_retry_after_seconds: float = env("HIPPIUS_FS_CACHE_RETRY_AFTER_SECONDS:2", convert=float)

    # IPFS upload/pin retry settings
    ipfs_max_retries: int = env("HIPPIUS_IPFS_MAX_RETRIES:3", convert=int)
    ipfs_retry_base_ms: int = env("HIPPIUS_IPFS_RETRY_BASE_MS:500", convert=int)
    ipfs_retry_max_ms: int = env("HIPPIUS_IPFS_RETRY_MAX_MS:5000", convert=int)

    # Storage version to assign for newly created/overwritten objects
    # Defaults to 4 (latest layout)
    target_storage_version: int = env("HIPPIUS_TARGET_STORAGE_VERSION:5", convert=int)

    # Cachet health monitoring
    cachet_api_url: str = env("CACHET_API_URL", convert=str)
    cachet_api_key: str = env("CACHET_API_KEY", convert=str)
    cachet_component_id: int = env("CACHET_COMPONENT_ID", convert=int)


def get_config() -> Config:
    """Get application configuration."""
    try:
        cfg = Config()
    except KeyError as e:
        # env() raises KeyError for required vars without defaults (e.g. ENVIRONMENT).
        raise ValueError(f"{e.args[0]} environment variable is required but not set or empty") from None

    # Validate required ENVIRONMENT variable
    env_value = cfg.environment
    if not env_value or not str(env_value).strip():
        raise ValueError("ENVIRONMENT variable is required but not set or empty")

    # Ensure a usable keystore DSN (falls back to DATABASE_URL)
    if not cfg.encryption_database_url:
        object.__setattr__(cfg, "encryption_database_url", cfg.database_url)

    # Sanity checks (avoid division-by-zero and confusing runtime behavior)
    if cfg.http_download_sleep_loop <= 0:
        raise ValueError("HTTP_DOWNLOAD_SLEEP_LOOP_SECONDS must be > 0")

    # Enforce environment constraints:
    # - Only in 'test' can enable_bypass_credit_check be True
    if env_value.lower() != "test":
        object.__setattr__(cfg, "enable_bypass_credit_check", False)

    # Compute max object size: min of S3 standard (5 TiB) and (part_size × part_count)
    s3_max_object_size = 5 * 1024**4  # 5 TiB - AWS S3 max object size
    computed_max = cfg.max_multipart_part_size * cfg.max_multipart_part_count
    object.__setattr__(cfg, "max_object_size", min(s3_max_object_size, computed_max))

    # Validate KMS mode
    if cfg.kms_mode not in ("required", "disabled"):
        raise ValueError(f"HIPPIUS_KMS_MODE must be 'required' or 'disabled', got: {cfg.kms_mode}")

    if cfg.kms_mode == "required":
        # KMS is mandatory - validate all env vars are present at config time
        kms_vars = [
            ("ovh_kms_endpoint", "HIPPIUS_OVH_KMS_ENDPOINT"),
            ("ovh_kms_okms_id", "HIPPIUS_OVH_KMS_OKMS_ID"),
            ("ovh_kms_default_key_id", "HIPPIUS_OVH_KMS_DEFAULT_KEY_ID"),
            ("ovh_kms_cert_path", "HIPPIUS_OVH_KMS_CERT_PATH"),
            ("ovh_kms_key_path", "HIPPIUS_OVH_KMS_KEY_PATH"),
        ]
        for attr, env_var in kms_vars:
            if not getattr(cfg, attr):
                raise ValueError(f"{env_var} is required when HIPPIUS_KMS_MODE=required")

        # Note: Cert file existence is validated at startup via init_kms_client()  # to handle docker-compose race conditions where cert files may be  # generated by another container.

    # Local wrapping requires a secret to derive the wrapping key
    if cfg.kms_mode == "disabled" and not cfg.hippius_secret_decryption_material:
        raise ValueError(
            "HIPPIUS_AUTH_ENCRYPTION_KEY is required when HIPPIUS_KMS_MODE=disabled (used for local KEK wrapping)"
        )

    return cfg
