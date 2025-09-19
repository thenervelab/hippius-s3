import dataclasses
import uuid

import dotenv

from hippius_s3.utils import env


dotenv.load_dotenv()


@dataclasses.dataclass
class Config:
    """Application configuration settings."""

    # Database Configuration
    database_url: str = env("DATABASE_URL")

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

    # API signing key for pre-signed URLs
    # Generated on first run if not provided
    api_signing_key: str = env("API_SIGNING_KEY:" + str(uuid.uuid4()))

    # s3 specific settings
    max_multipart_file_size = 15 * 1024 * 1024 * 1024  # 15GB
    max_multipart_chunk_size = 128 * 1024 * 1024  # 128 MB

    # worker specific settings
    pinner_sleep_loop = 1
    unpinner_sleep_loop = 5
    downloader_sleep_loop = 0.01
    cacher_loop_sleep = 60  # 1 minute
    pin_checker_loop_sleep = 60  # 1 minute

    # Resubmission settings
    resubmission_seed_phrase: str = env("RESUBMISSION_SEED_PHRASE")

    # endpoint chunk download settings, quite aggressive
    redis_read_chunk_timeout = 60
    http_download_sleep_loop = 0.1
    http_redis_get_retries = int(60 / http_download_sleep_loop)

    # initial stream timeout (seconds) before sending first byte
    http_stream_initial_timeout_seconds: float = env("HTTP_STREAM_INITIAL_TIMEOUT_SECONDS:5", convert=float)

    # Manifest builder configuration
    manifest_builder_enabled: bool = env("MANIFEST_BUILDER_ENABLED:false", convert=bool)
    manifest_stabilization_window_sec: int = env("MANIFEST_STABILIZATION_WINDOW_SEC:600", convert=int)
    manifest_scan_interval_sec: int = env("MANIFEST_SCAN_INTERVAL_SEC:120", convert=int)
    manifest_builder_max_concurrency: int = env("MANIFEST_BUILDER_MAX_CONCURRENCY:5", convert=int)


def get_config() -> Config:
    """Get application configuration."""
    return Config()
