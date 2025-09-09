"""E2E test configuration and fixtures for Hippius S3."""

import base64
import os
import secrets
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any
from typing import Callable
from typing import Generator
from typing import Iterator
from unittest.mock import AsyncMock
from unittest.mock import patch

import boto3  # type: ignore[import-untyped]
import pytest
from botocore.config import Config  # type: ignore[import-untyped]


# type: ignore[import-untyped]
# Note: event_loop fixture removed as it's not needed for synchronous tests


@pytest.fixture(scope="session")
def test_run_id() -> str:
    """Generate a unique ID for this test run to ensure isolation."""
    return str(uuid.uuid4())[:8]


@pytest.fixture(scope="session")
def test_seed_phrase() -> str:
    """Generate a unique seed phrase for this test run."""
    # For now, use a fixed test seed. In production, generate or load from secure source.
    return "test twelve word seed phrase for e2e testing purposes only"


@pytest.fixture(scope="session")
def compose_project_name(test_run_id: str) -> str:
    """Static docker compose project name for e2e runs."""
    return "hippius-e2e"


@pytest.fixture(scope="session")
def docker_services(compose_project_name: str) -> Iterator[None]:
    """Ensure e2e services are running for tests.

    Behavior:
    - Uses a static compose project name for faster hot reload cycles.
    - If the project is not running, it will be started.
    - Teardown is controlled by HIPPIUS_E2E_TEARDOWN (default 1). Set to 0 to skip teardown.
    """
    # When running against real AWS, do not start local docker compose stack
    if os.getenv("RUN_REAL_AWS") == "1":
        yield
        return

    env = os.environ.copy()
    env["COMPOSE_PROJECT_NAME"] = compose_project_name
    project_root = str(Path(__file__).resolve().parents[2])

    def compose_cmd(args: list[str]) -> subprocess.CompletedProcess[bytes]:
        return subprocess.run(
            ["docker", "compose", "-f", "docker-compose.yml", "-f", "docker-compose.e2e.yml", *args],
            env=env,
            cwd=project_root,
            check=False,
            capture_output=True,
        )

    # Determine if environment is already running (any container for this project)
    ps = compose_cmd(["ps", "-q"])
    already_running = ps.returncode == 0 and bool(ps.stdout.strip())

    if not already_running:
        # Start services with e2e override
        subprocess.run(
            ["docker", "compose", "-f", "docker-compose.yml", "-f", "docker-compose.e2e.yml", "up", "-d", "--wait"],
            env=env,
            check=True,
            cwd=project_root,
        )
        print("Waiting for services to be ready...")
        time.sleep(10)

    # Health check for API service
    import requests  # type: ignore[import-untyped]

    max_retries = 10
    for attempt in range(max_retries):
        try:
            response = requests.get("http://localhost:8000/", timeout=5)
            if response.status_code in [200, 400, 403]:  # API is responding
                print("API service is ready")
                break
        except requests.exceptions.RequestException:
            print(f"API not ready yet, attempt {attempt + 1}/{max_retries}")
            time.sleep(5)
    else:
        raise RuntimeError("API service failed to start within timeout")

    yield

    # Teardown based on env flag (default: teardown)
    teardown = env.get("HIPPIUS_E2E_TEARDOWN", "1") not in {"0", "false", "FALSE", "no", "NO"}
    if teardown:
        subprocess.run(
            ["docker", "compose", "-f", "docker-compose.yml", "-f", "docker-compose.e2e.yml", "down", "-v"],
            env=env,
            cwd=project_root,
        )


@pytest.fixture
def boto3_client(test_seed_phrase: str) -> Any:
    """Create a boto3 S3 client configured for testing.

    RUN_REAL_AWS=1 to run against real AWS. Otherwise uses local endpoint.
    """
    if os.getenv("RUN_REAL_AWS") == "1":
        return boto3.client(
            "s3",
            region_name=os.getenv("AWS_REGION", "us-east-1"),
            config=Config(
                signature_version="s3v4",  # Use default virtual host addressing on AWS
            ),  # Credentials resolved via default AWS chain
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
def cleanup_buckets(boto3_client: Any) -> Iterator[Callable[[str], None]]:
    """Cleanup function to remove test buckets after tests."""
    created_buckets = []

    def track_bucket(bucket_name: str) -> None:
        created_buckets.append(bucket_name)

    yield track_bucket

    # Cleanup after test
    for bucket in created_buckets:
        try:
            # Delete all objects in bucket
            objects = boto3_client.list_objects_v2(Bucket=bucket)
            if "Contents" in objects:
                for obj in objects["Contents"]:
                    boto3_client.delete_object(Bucket=bucket, Key=obj["Key"])

            # Delete bucket
            boto3_client.delete_bucket(Bucket=bucket)
        except Exception as e:
            print(f"Warning: Failed to cleanup bucket {bucket}: {e}")


@pytest.fixture
def unique_bucket_name(test_run_id: str) -> Callable[[str], str]:
    """Generate a unique bucket name for each test."""

    def _unique_name(base_name: str = "test-bucket") -> str:
        return f"{base_name}-{test_run_id}-{secrets.token_hex(4)}"

    return _unique_name


@pytest.fixture(autouse=True)
def mock_download_system() -> Generator[None, None, None]:
    """Mock the download queue system for e2e tests.

    This fixture:
    1. Mocks enqueue_download_request to do nothing (skip queueing)
    2. Mocks Redis get to return test content based on object key
    """

    async def mock_enqueue(*args: Any, **kwargs: Any) -> None:
        """Mock enqueue - do nothing, download system is mocked"""
        pass

    async def mock_redis_get(key: str) -> bytes | None:
        """Mock Redis get to return appropriate test content"""
        # Extract object info from Redis key pattern: downloaded:{object_key}:{part}:{request_uuid}
        if key.startswith("downloaded:"):
            parts = key.split(":")
            if len(parts) >= 2:
                object_key = parts[1]

                # Return appropriate test content based on object key
                if object_key == "file.txt":
                    return b"hello get object"

                # Default test content for other files
                return f"test content for {object_key}".encode()

        # For non-download keys, return None (not found)
        return None

    # Apply patches
    with (
        patch("hippius_s3.queue.enqueue_download_request", new=AsyncMock(side_effect=mock_enqueue)),
        patch("redis.asyncio.Redis.get", new=AsyncMock(side_effect=mock_redis_get)),
    ):
        yield
