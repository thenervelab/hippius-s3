import base64
import os
import secrets
import subprocess
from pathlib import Path
from typing import Any
from typing import Callable
from typing import Generator
from typing import Iterator

import boto3
import pytest
from botocore.config import Config

from tests.e2e.conftest import is_real_aws


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
