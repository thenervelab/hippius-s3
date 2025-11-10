import base64
import os
import secrets
import subprocess
from pathlib import Path
from typing import Any
from typing import AsyncGenerator
from typing import Callable
from typing import Generator
from typing import Iterator

import asyncpg
import boto3
import dotenv
import pytest
import redis.asyncio as redis
from botocore.config import Config
from httpx import ASGITransport
from httpx import AsyncClient

from tests.e2e.conftest import is_real_aws


@pytest.fixture(scope="session", autouse=True)
def _load_test_env() -> Generator[None, None, None]:
    """Load test environment variables from base + local env files."""
    project_root = Path(__file__).parents[2]
    dotenv.load_dotenv(project_root / ".env.defaults", override=True)
    dotenv.load_dotenv(project_root / ".env.test-local", override=True)
    yield


@pytest.fixture
def boto3_client(
    test_seed_phrase: str,
) -> Any:
    """Create a boto3 S3 client configured for integration testing."""
    if is_real_aws():
        return boto3.client(
            "s3",
            region_name=os.getenv("AWS_REGION", "us-east-1"),
            config=Config(
                signature_version="s3v4",
            ),
        )

    access_key = base64.b64encode(test_seed_phrase.encode()).decode()
    secret_key = test_seed_phrase

    return boto3.client(
        "s3",
        endpoint_url="http://localhost:8000",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            signature_version="s3v4",
        ),
    )


@pytest.fixture
def cleanup_buckets(
    boto3_client: Any,
) -> Iterator[Callable[[str], None]]:
    """Cleanup function to remove test buckets after tests."""
    created_buckets = []

    def track_bucket(bucket_name: str) -> None:
        created_buckets.append(bucket_name)

    yield track_bucket

    for bucket in created_buckets:
        try:
            objects = boto3_client.list_objects_v2(Bucket=bucket)
            if "Contents" in objects:
                for obj in objects["Contents"]:
                    boto3_client.delete_object(Bucket=bucket, Key=obj["Key"])

            boto3_client.delete_bucket(Bucket=bucket)
        except Exception as e:
            print(f"Warning: Failed to cleanup bucket {bucket}: {e}")


@pytest.fixture
def unique_bucket_name(
    test_run_id: str,
) -> Callable[[str], str]:
    """Generate a unique bucket name for each test."""

    def _unique_name(base_name: str = "test-bucket") -> str:
        return f"{base_name}-{test_run_id}-{secrets.token_hex(4)}"

    return _unique_name


@pytest.fixture
def stopped_worker(
    compose_project_name: str,
    docker_services: Any,
) -> Generator:
    """Stop a specific worker container for integration testing.

    Usage in test:
        stopped_worker("unpinner")  # stops the unpinner worker
        stopped_worker("substrate")  # stops the substrate worker
    """
    stopped_workers = []

    def _stop_worker(worker_name: str) -> None:
        if is_real_aws():
            return

        env = {**os.environ, "COMPOSE_PROJECT_NAME": compose_project_name}
        project_root = str(Path(__file__).resolve().parents[2])

        subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                "docker-compose.yml",
                "-f",
                "docker-compose.e2e.yml",
                "stop",
                worker_name,
            ],
            env=env,
            cwd=project_root,
            check=True,
            capture_output=True,
        )
        stopped_workers.append(worker_name)

    yield _stop_worker

    for worker in stopped_workers:
        env = {**os.environ, "COMPOSE_PROJECT_NAME": compose_project_name}
        project_root = str(Path(__file__).resolve().parents[2])
        subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                "docker-compose.yml",
                "-f",
                "docker-compose.e2e.yml",
                "start",
                worker,
            ],
            env=env,
            cwd=project_root,
            check=False,
            capture_output=True,
        )


@pytest.fixture
async def proxy_db_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    """Create a PostgreSQL connection pool for proxy tests."""
    pool = await asyncpg.create_pool(os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius"))
    yield pool
    await pool.close()


@pytest.fixture
async def proxy_redis_clients() -> AsyncGenerator[dict[str, Any], None]:
    """Create Redis clients for proxy tests."""
    redis_client = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=False)
    redis_accounts = redis.from_url(os.getenv("REDIS_ACCOUNTS_URL", "redis://localhost:6380/0"), decode_responses=False)
    redis_chain = redis.from_url(os.getenv("REDIS_CHAIN_URL", "redis://localhost:6381/0"), decode_responses=False)
    redis_rate_limiting = redis.from_url(
        os.getenv("REDIS_RATE_LIMITING_URL", "redis://localhost:6383/0"), decode_responses=False
    )

    yield {
        "redis": redis_client,
        "redis_accounts": redis_accounts,
        "redis_chain": redis_chain,
        "redis_rate_limiting": redis_rate_limiting,
    }

    await redis_client.close()
    await redis_accounts.close()
    await redis_chain.close()
    await redis_rate_limiting.close()


@pytest.fixture
async def proxy_app(proxy_db_pool: asyncpg.Pool, proxy_redis_clients: dict[str, Any]) -> AsyncGenerator[Any, None]:
    """Create a proxy app instance for testing."""
    from proxy_gateway.main import factory  # type: ignore[import-not-found]

    app = factory()

    app.state.postgres_pool = proxy_db_pool
    app.state.redis_client = proxy_redis_clients["redis"]
    app.state.redis_accounts = proxy_redis_clients["redis_accounts"]
    app.state.redis_chain = proxy_redis_clients["redis_chain"]
    app.state.redis_rate_limiting = proxy_redis_clients["redis_rate_limiting"]

    from proxy_gateway.config import get_config  # type: ignore[import-not-found]
    from proxy_gateway.services.forward_service import ForwardService  # type: ignore[import-not-found]

    config = get_config()
    app.state.forward_service = ForwardService(config.backend_url)

    yield app

    if hasattr(app.state, "forward_service"):
        await app.state.forward_service.close()


@pytest.fixture
async def gateway_client(proxy_app: Any) -> AsyncGenerator[AsyncClient, None]:
    """Create an async test client for the proxy."""
    transport = ASGITransport(app=proxy_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
