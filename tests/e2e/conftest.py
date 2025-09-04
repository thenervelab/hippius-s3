"""E2E test configuration and fixtures for Hippius S3."""

import base64
import os
import secrets
import subprocess
import time
import uuid

import boto3
import pytest
from botocore.config import Config
from pathlib import Path


# Note: event_loop fixture removed as it's not needed for synchronous tests


@pytest.fixture(scope="session")
def test_run_id():
    """Generate a unique ID for this test run to ensure isolation."""
    return str(uuid.uuid4())[:8]


@pytest.fixture(scope="session")
def test_seed_phrase():
    """Generate a unique seed phrase for this test run."""
    # For now, use a fixed test seed. In production, generate or load from secure source.
    return "test twelve word seed phrase for e2e testing purposes only"


@pytest.fixture(scope="session")
def compose_project_name(test_run_id):
    """Unique docker compose project name for isolation."""
    return f"hippius-e2e-{test_run_id}"


@pytest.fixture(scope="session")
def docker_services(compose_project_name):
    """Start docker services for e2e tests."""
    env = os.environ.copy()
    env["COMPOSE_PROJECT_NAME"] = compose_project_name

    # Start services with e2e override
    subprocess.run(
        ["docker", "compose", "-f", "docker-compose.yml", "-f", "docker-compose.e2e.yml", "up", "-d", "--wait"],
        env=env,
        check=True,
        cwd=str(Path(__file__).resolve().parents[2])
    )

    # Wait for services to be ready
    print("Waiting for services to be ready...")
    time.sleep(15)  # Increased wait time

    # Health check for API service
    import requests
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

    # Cleanup
    subprocess.run(
        ["docker", "compose", "-f", "docker-compose.yml", "-f", "docker-compose.e2e.yml", "down", "-v"],
        env=env,
        cwd=str(Path(__file__).resolve().parents[2])
    )




@pytest.fixture
def boto3_client(test_seed_phrase):
    """Create a boto3 S3 client configured for testing."""
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
def cleanup_buckets(boto3_client):
    """Cleanup function to remove test buckets after tests."""
    created_buckets = []

    def track_bucket(bucket_name):
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
def unique_bucket_name(test_run_id):
    """Generate a unique bucket name for each test."""
    def _unique_name(base_name="test-bucket"):
        return f"{base_name}-{test_run_id}-{secrets.token_hex(4)}"
    return _unique_name
