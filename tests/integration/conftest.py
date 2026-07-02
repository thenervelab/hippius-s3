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
import pytest_asyncio
from botocore.config import Config
from httpx import ASGITransport
from httpx import AsyncClient

from gateway.services.acl_service import BucketLookup
from tests.e2e.conftest import is_real_aws


_project_root = Path(__file__).parents[2]
dotenv.load_dotenv(_project_root / ".env.defaults", override=True)
dotenv.load_dotenv(_project_root / ".env.test-local", override=True)
os.environ["HIPPIUS_BYPASS_CREDIT_CHECK"] = "true"
os.environ["ENABLE_BANHAMMER"] = "false"


@pytest.fixture
def test_run_id() -> str:
    """Short unique ID for this integration test run (mirrors e2e semantics)."""
    import uuid

    return str(uuid.uuid4())[:8]


@pytest.fixture
def test_access_key() -> str:
    """Return a hip_ access key for local integration tests."""
    return "hip_integration_test_master"


@pytest.fixture
def test_access_key_secret() -> str:
    """Return the plaintext secret paired with test_access_key."""
    return "integration_test_secret_for_hip_keys"


@pytest.fixture
def _mock_backend_forwarding() -> Generator[None, None, None]:
    """Stub out the real backend so forwarded requests don't hit a real network address.

    boto3_client-driven tests exercise auth + middleware end-to-end via the
    in-process ASGI app; once auth succeeds, GET/HEAD object requests are
    proxied to config.backend_url (no real backend exists in this test job).
    Respond 404 NoSuchKey for anything forwarded, which is within the
    response range these tests already accept.
    """
    import httpx
    import respx

    with respx.mock(assert_all_called=False) as respx_router:
        respx_router.route(host__regex=".*").mock(
            return_value=httpx.Response(
                404,
                content=b'<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchKey</Code></Error>',
            )
        )
        yield


@pytest.fixture
def _mock_access_key_auth(
    monkeypatch: pytest.MonkeyPatch,
    test_access_key: str,
    test_access_key_secret: str,
    _mock_backend_forwarding: None,
) -> None:
    """Make test_access_key verify successfully without calling the real Hippius API.

    Access-key signature verification needs a valid TokenAuthResponse (with a
    decryptable secret) to compute the expected SigV4 signature. Unlike the
    old seed-phrase flow, which verified entirely locally, access-key auth
    always calls out to the Hippius API — so for fully offline/in-process
    integration tests we fake that response here using the same encryption
    key the gateway expects (HIPPIUS_AUTH_ENCRYPTION_KEY).
    """
    from nacl.secret import SecretBox

    from gateway.config import get_config
    from hippius_s3.services.hippius_api_service import TokenAuthResponse

    key_hex = get_config().hippius_secret_decryption_material
    box = SecretBox(bytes.fromhex(key_hex))
    encrypted_secret = base64.b64encode(box.encrypt(test_access_key_secret.encode())).decode()

    async def _fake_cached_auth(access_key: str, redis_client: Any) -> TokenAuthResponse:
        if access_key != test_access_key:
            return TokenAuthResponse(valid=False, status="invalid")
        return TokenAuthResponse(
            valid=True,
            status="active",
            account_address="5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty",
            token_type="master",
            encrypted_secret=encrypted_secret,
            nonce=base64.b64encode(b"\x00" * 24).decode(),
        )

    monkeypatch.setattr("gateway.middlewares.access_key_auth.cached_auth", _fake_cached_auth)


@pytest.fixture
def boto3_client(
    test_access_key: str,
    test_access_key_secret: str,
    _mock_access_key_auth: None,
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

    return boto3.client(
        "s3",
        endpoint_url="http://test",
        aws_access_key_id=test_access_key,
        aws_secret_access_key=test_access_key_secret,
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


@pytest_asyncio.fixture
async def gateway_db_pool() -> AsyncGenerator[Any, None]:
    """Create a mock PostgreSQL pool for gateway tests."""
    from unittest.mock import AsyncMock
    from unittest.mock import MagicMock

    mock_pool = MagicMock()
    mock_pool.fetchrow = AsyncMock(return_value=None)
    mock_pool.fetch = AsyncMock(return_value=[])
    mock_pool.execute = AsyncMock()
    yield mock_pool


@pytest_asyncio.fixture
async def gateway_redis_clients() -> AsyncGenerator[dict[str, Any], None]:
    """Create mock Redis clients for gateway tests."""
    from unittest.mock import AsyncMock
    from unittest.mock import MagicMock

    def create_mock_redis() -> Any:
        mock_redis = MagicMock()
        mock_redis.get = AsyncMock(return_value=None)
        mock_redis.set = AsyncMock()
        mock_redis.delete = AsyncMock()
        mock_redis.incr = AsyncMock(return_value=1)
        mock_redis.expire = AsyncMock()
        return mock_redis

    yield {
        "redis": create_mock_redis(),
        "redis_accounts": create_mock_redis(),
        "redis_chain": create_mock_redis(),
        "redis_rate_limiting": create_mock_redis(),
    }


@pytest_asyncio.fixture
async def gateway_app(
    gateway_db_pool: asyncpg.Pool, gateway_redis_clients: dict[str, Any]
) -> AsyncGenerator[Any, None]:
    """Create a gateway app instance for testing."""
    from unittest.mock import AsyncMock
    from unittest.mock import MagicMock

    from gateway.main import factory

    app = factory()

    app.state.postgres_pool = gateway_db_pool
    app.state.redis_client = gateway_redis_clients["redis"]
    app.state.redis_accounts = gateway_redis_clients["redis_accounts"]
    app.state.redis_chain = gateway_redis_clients["redis_chain"]
    app.state.redis_rate_limiting = gateway_redis_clients["redis_rate_limiting"]

    from gateway.config import get_config
    from gateway.services.forward_service import ForwardService

    config = get_config()
    app.state.forward_service = ForwardService(config.backend_url)

    mock_rate_limit_service = MagicMock()
    mock_rate_limit_service.check_rate_limit = AsyncMock(return_value=True)
    app.state.rate_limit_service = mock_rate_limit_service

    mock_banhammer_service = MagicMock()
    mock_banhammer_service.is_banned = AsyncMock(return_value=False)
    app.state.banhammer_service = mock_banhammer_service

    mock_acl_service = MagicMock()
    mock_acl_service.check_permission = AsyncMock(return_value=True)
    mock_acl_service.get_bucket_owner = AsyncMock(return_value="test-owner-id")
    mock_acl_service.get_bucket_id = AsyncMock(side_effect=lambda b: b)
    mock_acl_service.get_bucket_owner_and_id = AsyncMock(
        return_value=BucketLookup(owner_id="test-owner-id", bucket_id="test-bucket-id", is_cache_warm=False)
    )
    app.state.acl_service = mock_acl_service

    mock_scope_repo = MagicMock()
    mock_scope_repo.get = AsyncMock(return_value=None)
    app.state.sub_token_scope_repo = mock_scope_repo

    yield app

    if hasattr(app.state, "forward_service"):
        await app.state.forward_service.close()


@pytest_asyncio.fixture
async def gateway_client(gateway_app: Any) -> AsyncGenerator[AsyncClient, None]:
    """Create an async test client for the gateway."""
    transport = ASGITransport(app=gateway_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest_asyncio.fixture
async def gateway_app_no_auth(
    gateway_db_pool: asyncpg.Pool, gateway_redis_clients: dict[str, Any]
) -> AsyncGenerator[Any, None]:
    """Create a gateway app without authentication for forwarding tests."""
    from unittest.mock import AsyncMock
    from unittest.mock import MagicMock

    from fastapi import FastAPI
    from fastapi import Request

    app = FastAPI()

    app.state.postgres_pool = gateway_db_pool
    app.state.redis_client = gateway_redis_clients["redis"]
    app.state.redis_accounts = gateway_redis_clients["redis_accounts"]
    app.state.redis_chain = gateway_redis_clients["redis_chain"]
    app.state.redis_rate_limiting = gateway_redis_clients["redis_rate_limiting"]

    from gateway.config import get_config
    from gateway.services.forward_service import ForwardService

    config = get_config()
    app.state.forward_service = ForwardService(config.backend_url)

    mock_rate_limit_service = MagicMock()
    mock_rate_limit_service.check_rate_limit = AsyncMock(return_value=True)
    app.state.rate_limit_service = mock_rate_limit_service

    mock_banhammer_service = MagicMock()
    mock_banhammer_service.is_banned = AsyncMock(return_value=False)
    app.state.banhammer_service = mock_banhammer_service

    mock_acl_service = MagicMock()
    mock_acl_service.check_permission = AsyncMock(return_value=True)
    mock_acl_service.get_bucket_owner = AsyncMock(return_value="test-owner-id")
    mock_acl_service.get_bucket_id = AsyncMock(side_effect=lambda b: b)
    mock_acl_service.get_bucket_owner_and_id = AsyncMock(
        return_value=BucketLookup(owner_id="test-owner-id", bucket_id="test-bucket-id", is_cache_warm=False)
    )
    app.state.acl_service = mock_acl_service

    mock_scope_repo = MagicMock()
    mock_scope_repo.get = AsyncMock(return_value=None)
    app.state.sub_token_scope_repo = mock_scope_repo

    @app.get("/health")
    async def health() -> dict:
        return {"status": "healthy", "service": "gateway"}

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "HEAD", "PATCH"])
    async def forward_all(request: Request, path: str) -> Any:
        forward_service = request.app.state.forward_service
        return await forward_service.forward_request(request)

    yield app

    if hasattr(app.state, "forward_service"):
        await app.state.forward_service.close()


@pytest_asyncio.fixture
async def gateway_client_no_auth(gateway_app_no_auth: Any) -> AsyncGenerator[AsyncClient, None]:
    """Create an async test client without authentication."""
    transport = ASGITransport(app=gateway_app_no_auth)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
