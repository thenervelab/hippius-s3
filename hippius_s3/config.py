import dataclasses
import uuid

from hippius_s3.utils import env


@dataclasses.dataclass
class Config:
    """Application configuration settings."""

    database_url: str = env("DATABASE_URL")
    ipfs_get_url: str = env("HIPPIUS_IPFS_GET_URL")
    ipfs_store_url: str = env("HIPPIUS_IPFS_STORE_URL")
    validator_region: str = env("HIPPIUS_VALIDATOR_REGION")
    redis_url: str = env("REDIS_URL")

    debug: bool = env("DEBUG:false", convert=lambda x: x.lower() == "true")

    host: str = env("HOST:0.0.0.0")
    port: int = env("PORT:8000", convert=int)

    # API signing key for pre-signed URLs
    # Generated on first run if not provided
    api_signing_key: str = env("API_SIGNING_KEY:" + str(uuid.uuid4()))
    substrate_url: str = env("HIPPIUS_SUBSTRATE_URL")

    # Frontend HMAC secret for user endpoints authentication
    frontend_hmac_secret: str = env("FRONTEND_HMAC_SECRET")


def get_config() -> Config:
    """Get application configuration."""
    return Config()
