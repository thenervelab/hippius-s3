import dataclasses
import uuid

from hippius_s3.utils import env


@dataclasses.dataclass
class Config:
    """Application configuration settings."""

    database_url: str = env("DATABASE_URL:postgresql://postgres:postgres@localhost:5432/hippius")
    ipfs_service_url: str = env("HIPPIUS_IPFS_SERVICE_URL:http://127.0.0.1:5001")

    s3_bucket_name: str = env("HIPPIUS_S3_BUCKET_NAME:hippius-bucket")
    s3_region: str = env("HIPPIUS_S3_REGION:us-east-1")

    debug: bool = env("DEBUG:false", convert=lambda x: x.lower() == "true")

    host: str = env("HOST:0.0.0.0")
    port: int = env("PORT:8000", convert=int)

    # API signing key for pre-signed URLs
    # Generated on first run if not provided
    api_signing_key: str = env("API_SIGNING_KEY:" + str(uuid.uuid4()))


def get_config() -> Config:
    """Get application configuration."""
    return Config()
