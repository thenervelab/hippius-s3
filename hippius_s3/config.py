import dataclasses
import uuid

import dotenv
import httpx

from hippius_s3.utils import env


dotenv.load_dotenv()


@dataclasses.dataclass
class Config:
    """Application configuration settings."""

    # Database Configuration
    database_url: str = env("DATABASE_URL")
    # Inline default prevents KeyError during class init; runtime fallback to DATABASE_URL is applied in get_config()
    encryption_database_url: str = env("HIPPIUS_KEYSTORE_DATABASE_URL:", convert=str)

    # IPFS Configuration
    ipfs_get_url: str = env("HIPPIUS_IPFS_GET_URL")
    ipfs_store_url: str = env("HIPPIUS_IPFS_STORE_URL")

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
    validator_region: str = env("HIPPIUS_VALIDATOR_REGION")

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

    # API signing key for pre-signed URLs
    # Generated on first run if not provided
    api_signing_key: str = env("API_SIGNING_KEY:" + str(uuid.uuid4()))

    # s3 specific settings
    max_multipart_file_size = 15 * 1024 * 1024 * 1024  # 15GB
    max_multipart_chunk_size = 128 * 1024 * 1024  # 128 MB

    # worker specific settings
    unpinner_sleep_loop = 5
    downloader_sleep_loop = 0.01
    cacher_loop_sleep = 60  # 1 minute
    pin_checker_loop_sleep = 60  # 1 minute

    # Resubmission settings
    resubmission_seed_phrase: str = env("RESUBMISSION_SEED_PHRASE")

    # Uploader configuration (supersedes legacy pinner config)
    uploader_max_attempts: int = env("HIPPIUS_UPLOADER_MAX_ATTEMPTS:5", convert=int)
    uploader_backoff_base_ms: int = env("HIPPIUS_UPLOADER_BACKOFF_BASE_MS:500", convert=int)
    uploader_backoff_max_ms: int = env("HIPPIUS_UPLOADER_BACKOFF_MAX_MS:60000", convert=int)
    uploader_multipart_max_concurrency: int = env("HIPPIUS_UPLOADER_MULTIPART_MAX_CONCURRENCY:5", convert=int)
    # Heavy validation gating (legacy PINNER_VALIDATE_COVERAGE supported for compat)
    uploader_validate_coverage: bool = env("UPLOADER_VALIDATE_COVERAGE:false", convert=lambda x: x.lower() == "true")

    # Substrate worker configuration
    substrate_batch_size: int = env("HIPPIUS_SUBSTRATE_BATCH_SIZE:5000", convert=int)
    substrate_batch_max_age_sec: int = env("HIPPIUS_SUBSTRATE_BATCH_MAX_AGE_SEC:60", convert=int)
    substrate_max_retries: int = env("HIPPIUS_SUBSTRATE_MAX_RETRIES:3", convert=int)
    substrate_retry_base_ms: int = env("HIPPIUS_SUBSTRATE_RETRY_BASE_MS:5000", convert=int)
    substrate_retry_max_ms: int = env("HIPPIUS_SUBSTRATE_RETRY_MAX_MS:15000", convert=int)
    substrate_call_timeout_seconds: float = env("HIPPIUS_SUBSTRATE_CALL_TIMEOUT_SECONDS:20.0", convert=float)

    # Upload queue configuration
    upload_queue_names: str = env("HIPPIUS_UPLOAD_QUEUE_NAMES:upload_requests", convert=str)

    # EC configuration
    ec_enabled: bool = env("HIPPIUS_EC_ENABLED:false", convert=lambda x: x.lower() == "true")
    ec_scheme: str = env("HIPPIUS_EC_SCHEME:rs-v1", convert=str)
    ec_policy_version: int = env("HIPPIUS_EC_POLICY_VERSION:1", convert=int)
    ec_k: int = env("HIPPIUS_EC_K:8", convert=int)
    ec_m: int = env("HIPPIUS_EC_M:4", convert=int)
    # Threshold-based policy: replicate below threshold, EC at or above
    ec_replication_factor: int = env("HIPPIUS_REPLICATION_FACTOR:2", convert=int)
    # Shard sizing bounds
    ec_min_chunk_size_bytes: int = env("HIPPIUS_EC_MIN_CHUNK_SIZE_BYTES:131072", convert=int)  # 128 KiB default
    ec_max_chunk_size_bytes: int = env("HIPPIUS_EC_MAX_CHUNK_SIZE_BYTES:4194304", convert=int)  # 4 MiB
    # Worker limits
    ec_worker_concurrency: int = env("HIPPIUS_EC_WORKER_CONCURRENCY:2", convert=int)
    ec_queue_name: str = env("HIPPIUS_EC_QUEUE_NAME:ec_encode_requests", convert=str)

    # Cache TTL (shared across components)
    cache_ttl_seconds: int = env("HIPPIUS_CACHE_TTL:259200", convert=int)
    # Unified object part chunk size (bytes) for cache and range math
    object_chunk_size_bytes: int = env("HIPPIUS_CHUNK_SIZE_BYTES:4194304", convert=int)
    # Downloader behavior (default: no whole-part backfill)
    downloader_allow_part_backfill: bool = env(
        "DOWNLOADER_ALLOW_PART_BACKFILL:false", convert=lambda x: x.lower() == "true"
    )

    # Crypto configuration
    # Default encryption suite for new objects
    # hip-enc/legacy: SecretBox per-chunk (current default)
    # hip-enc/1: Reserved for future adapter variants
    crypto_suite_id: str = env("HIPPIUS_CRYPTO_SUITE_ID:hip-enc/legacy")

    # endpoint chunk download settings, quite aggressive
    redis_read_chunk_timeout = 60
    http_download_sleep_loop = 0.1
    http_redis_get_retries = int(60 / http_download_sleep_loop)

    # initial stream timeout (seconds) before sending first byte
    http_stream_initial_timeout_seconds: float = env("HTTP_STREAM_INITIAL_TIMEOUT_SECONDS:5", convert=float)
    httpx_ipfs_api_timeout = httpx.Timeout(10.0, read=300.0)

    # DLQ configuration
    dlq_dir: str = env("HIPPIUS_DLQ_DIR:/tmp/hippius_dlq")
    dlq_archive_dir: str = env("HIPPIUS_DLQ_ARCHIVE_DIR:/tmp/hippius_dlq_archive")

    # Object parts filesystem cache configuration
    object_cache_dir: str = env("HIPPIUS_OBJECT_CACHE_DIR:/var/lib/hippius/object_cache")
    fs_cache_gc_max_age_seconds: int = env("HIPPIUS_FS_CACHE_GC_MAX_AGE_SECONDS:604800", convert=int)  # 7 days
    mpu_stale_seconds: int = env("HIPPIUS_MPU_STALE_SECONDS:86400", convert=int)  # 1 day

    # IPFS upload/pin retry settings
    ipfs_max_retries: int = env("HIPPIUS_IPFS_MAX_RETRIES:3", convert=int)
    ipfs_retry_base_ms: int = env("HIPPIUS_IPFS_RETRY_BASE_MS:500", convert=int)
    ipfs_retry_max_ms: int = env("HIPPIUS_IPFS_RETRY_MAX_MS:5000", convert=int)

    # Substrate worker configuration
    substrate_queue_name: str = env("HIPPIUS_SUBSTRATE_QUEUE_NAME:substrate_requests", convert=str)

    # Publishing toggle (read directly from env; default true)
    publish_to_chain: bool = env("PUBLISH_TO_CHAIN:true", convert=lambda x: x.lower() == "true")

    # Legacy SDK compatibility (temporary; set LEGACY_SDK_COMPAT=true to enable)
    enable_legacy_sdk_compat: bool = env("LEGACY_SDK_COMPAT:true", convert=lambda x: x.lower() == "true")

    # Storage version to assign for newly created/overwritten objects
    # Defaults to 3 (latest layout)
    target_storage_version: int = env("HIPPIUS_TARGET_STORAGE_VERSION:3", convert=int)

    # Cachet health monitoring
    cachet_api_url: str = env("CACHET_API_URL", convert=str)
    cachet_api_key: str = env("CACHET_API_KEY", convert=str)
    cachet_component_id: int = env("CACHET_COMPONENT_ID", convert=int)


def get_config() -> Config:
    """Get application configuration."""
    cfg = Config()

    # Validate required ENVIRONMENT variable
    env_value = getattr(cfg, "environment", None)
    if not env_value or not env_value.strip():
        raise ValueError("ENVIRONMENT variable is required but not set or empty")

    try:
        if not getattr(cfg, "encryption_database_url", None):
            object.__setattr__(cfg, "encryption_database_url", cfg.database_url)
    except Exception:
        # Last resort: ensure a usable value
        object.__setattr__(cfg, "encryption_database_url", cfg.database_url)


    # Enforce environment constraints:
    # - Only in 'test' can enable_bypass_credit_check be True
    # - Only in 'test' can publish_to_chain be False
    if env_value.lower() != "test":
        object.__setattr__(cfg, "enable_bypass_credit_check", False)
        object.__setattr__(cfg, "publish_to_chain", True)

    # Deterministic relationship: threshold = k * min_chunk_size (no separate env)
    try:
        computed_threshold = int(cfg.ec_k) * int(cfg.ec_min_chunk_size_bytes)
        object.__setattr__(cfg, "ec_threshold_bytes", computed_threshold)
    except Exception:
        pass

    # Normalize queue names (strip quotes/whitespace)
    q = (getattr(cfg, "ec_queue_name", "redundancy_requests") or "redundancy_requests").strip().strip("\"'")
    object.__setattr__(cfg, "ec_queue_name", q or "redundancy_requests")

    return cfg
