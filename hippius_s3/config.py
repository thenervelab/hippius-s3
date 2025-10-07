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

    # Server Configuration
    host: str = env("HOST")
    port: int = env("PORT", convert=int)
    environment: str = env("ENVIRONMENT")
    debug: bool = env("DEBUG", convert=lambda x: x.lower() == "true")

    # Feature Flags
    enable_audit_logging: bool = env("ENABLE_AUDIT_LOGGING", convert=lambda x: x.lower() == "true")
    enable_strict_validation: bool = env("ENABLE_STRICT_VALIDATION", convert=lambda x: x.lower() == "true")
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
    redis_accounts_url: str = env("REDIS_ACCOUNTS_URL")

    # Redis for chain operations
    redis_chain_url: str = env("REDIS_CHAIN_URL:redis://127.0.0.1:6381/0")

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
    substrate_batch_size: int = env("HIPPIUS_SUBSTRATE_BATCH_SIZE:16", convert=int)
    substrate_batch_max_age_sec: int = env("HIPPIUS_SUBSTRATE_BATCH_MAX_AGE_SEC:10", convert=int)
    substrate_max_retries: int = env("HIPPIUS_SUBSTRATE_MAX_RETRIES:3", convert=int)
    substrate_retry_base_ms: int = env("HIPPIUS_SUBSTRATE_RETRY_BASE_MS:500", convert=int)
    substrate_retry_max_ms: int = env("HIPPIUS_SUBSTRATE_RETRY_MAX_MS:5000", convert=int)
    substrate_call_timeout_seconds: float = env("HIPPIUS_SUBSTRATE_CALL_TIMEOUT_SECONDS:20.0", convert=float)

    # Upload queue configuration
    upload_queue_names: str = env("HIPPIUS_UPLOAD_QUEUE_NAMES:upload_requests", convert=str)

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
    # hip-enc/1: XSalsa20-Poly1305 with random nonces (current)
    # hip-enc/legacy: Legacy whole-part encryption (SDK compatibility)
    crypto_suite_id: str = env("HIPPIUS_CRYPTO_SUITE_ID:hip-enc/1")

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

    # IPFS upload/pin retry settings
    ipfs_max_retries: int = env("HIPPIUS_IPFS_MAX_RETRIES:3", convert=int)
    ipfs_retry_base_ms: int = env("HIPPIUS_IPFS_RETRY_BASE_MS:500", convert=int)
    ipfs_retry_max_ms: int = env("HIPPIUS_IPFS_RETRY_MAX_MS:5000", convert=int)

    # Substrate worker configuration
    substrate_queue_name: str = env("HIPPIUS_SUBSTRATE_QUEUE_NAME:substrate_requests", convert=str)

    # Publishing toggle (read directly from env; default true)
    publish_to_chain: bool = env("PUBLISH_TO_CHAIN:true", convert=lambda x: x.lower() == "true")

    # Legacy SDK compatibility (temporary; set LEGACY_SDK_COMPAT=true to enable)
    enable_legacy_sdk_compat: bool = env("LEGACY_SDK_COMPAT:false", convert=lambda x: x.lower() == "true")


def get_config() -> Config:
    """Get application configuration."""
    cfg = Config()
    try:
        if not getattr(cfg, "encryption_database_url", None):
            object.__setattr__(cfg, "encryption_database_url", cfg.database_url)
    except Exception:
        # Last resort: ensure a usable value
        object.__setattr__(cfg, "encryption_database_url", cfg.database_url)
    return cfg
