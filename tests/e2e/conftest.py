"""E2E test configuration and fixtures for Hippius S3.

Environment configuration:
- E2E tests run via docker-compose which loads:
  - .env.defaults (common test configuration)
  - .env.test-docker (Docker service DNS names)
- Integration tests and pytest conftest use .env.defaults + .env.test-local (base + localhost URLs)
- See .env.defaults, .env.test-local, and .env.test-docker for configuration
"""

import base64
import os
import secrets
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any
from typing import Callable
from typing import Iterator

import boto3  # type: ignore[import-untyped]
import pytest
from botocore.config import Config  # type: ignore[import-untyped]
from botocore.exceptions import ClientError  # type: ignore[import-untyped]
from botocore.exceptions import ReadTimeoutError  # type: ignore[import-untyped]

from .support.compose import enable_arion_proxy
from .support.compose import enable_ipfs_proxy
from .support.compose import enable_kms_proxy
from .support.compose import wait_for_toxiproxy


# Ensure required app config vars exist during collection (some modules call get_config() at import-time).
os.environ.setdefault("HIPPIUS_IPFS_API_URLS", "http://127.0.0.1:5001")

# type: ignore[import-untyped]
# Note: event_loop fixture removed as it's not needed for synchronous tests


def pytest_collection_modifyitems(config, items):  # type: ignore[no-untyped-def]
    """Skip s4/local/hippius_* tests when running against real AWS."""
    run_real = os.getenv("RUN_REAL_AWS") == "1" or os.getenv("AWS") == "1"
    if not run_real:
        return
    skip_s4 = pytest.mark.skip(reason="S4 extensions not supported on real AWS")
    skip_local = pytest.mark.skip(reason="Local-only tests are skipped on real AWS")
    skip_hippius = pytest.mark.skip(reason="Hippius-specific behavior not available on real AWS")
    for item in items:
        if item.get_closest_marker("s4"):
            item.add_marker(skip_s4)
        if item.get_closest_marker("local"):
            item.add_marker(skip_local)
        if item.get_closest_marker("hippius_cache") or item.get_closest_marker("hippius_headers"):
            item.add_marker(skip_hippius)


def is_real_aws() -> bool:
    """Return True when tests are running against real AWS."""
    return os.getenv("RUN_REAL_AWS") == "1" or os.getenv("AWS") == "1"


def assert_hippius_source(headers: dict[str, str] | dict[str, object], allowed: set[str] | None = None) -> None:
    """Assert Hippius-specific source header when running locally; no-op on real AWS.

    Parameters:
    - headers: mapping of response headers
    - allowed: allowed values for x-hippius-source (defaults to {"cache", "pipeline"})
    """
    if is_real_aws():
        return
    allowed_values = allowed or {"cache", "pipeline"}
    # Some callers pass botocore ResponseMetadata HTTPHeaders (dict[str, str])
    value = headers.get("x-hippius-source")  # type: ignore[arg-type]
    assert value in allowed_values


@pytest.fixture(scope="session")
def test_run_id() -> str:
    """Generate a unique ID for this test run to ensure isolation."""
    return str(uuid.uuid4())[:8]


@pytest.fixture(scope="session")
def test_seed_phrase() -> str:
    """Generate a unique seed phrase for this test run."""
    # For now, use a fixed test seed. In production, generate or load from secure source.
    return "about acid actor absent action able actual abandon abstract above ability achieve"


@pytest.fixture(scope="session")
def compose_project_name(test_run_id: str) -> str:
    """Docker compose project name for e2e runs.

    Allows overriding via HIPPIUS_E2E_PROJECT to reuse an already running stack
    (e.g., the default project name from manual `docker compose up`).
    Defaults to a stable name to keep hot reload cycles fast.
    """
    override = os.environ.get("HIPPIUS_E2E_PROJECT")
    if override and override.strip():
        return override.strip()
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
    # Important: tests run on the host, but docker-compose variable interpolation uses this env dict.
    # We want containers to talk to IPFS via toxiproxy inside the compose network, not 127.0.0.1.
    env["HIPPIUS_IPFS_API_URLS"] = os.environ.get("HIPPIUS_IPFS_API_URLS_DOCKER", "http://toxiproxy:15001")
    project_root = str(Path(__file__).resolve().parents[2])

    # Ensure .e2e directories exist for volume mounts
    e2e_dir = Path(project_root) / ".e2e"
    e2e_dir.mkdir(exist_ok=True)
    dlq_dir = e2e_dir / "dlq"
    dlq_dir.mkdir(exist_ok=True)
    dlq_archive_dir = e2e_dir / "dlq_archive"
    dlq_archive_dir.mkdir(exist_ok=True)

    # Ensure external network exists in CI runners
    def _ensure_network(name: str) -> None:
        try:
            probe = subprocess.run(["docker", "network", "inspect", name], check=False, capture_output=True)
            if probe.returncode != 0:
                subprocess.run(["docker", "network", "create", name], check=True)
        except Exception as e:  # noqa: PERF203
            # Do not hard-fail on network creation; compose may still succeed locally
            print(f"Warning: failed ensuring docker network '{name}': {e}")

    _ensure_network("hippius_net")

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
        up_cmd = [
            "docker",
            "compose",
            "-f",
            "docker-compose.yml",
            "-f",
            "docker-compose.e2e.yml",
            "up",
            "-d",
            "--wait",
                "--build",
        ]
        result = subprocess.run(up_cmd, env=env, cwd=project_root, capture_output=True, text=True)
        if result.returncode != 0:
            print("DOCKER COMPOSE UP FAILED")
            print("=" * 80)
            print("\nSTDOUT:")
            print(result.stdout or "(empty)")
            print("\nSTDERR:")
            print(result.stderr or "(empty)")

            # Capture diagnostics to artifacts for CI visibility
            try:
                artifacts_dir = Path(project_root) / "artifacts"
                artifacts_dir.mkdir(parents=True, exist_ok=True)
                (artifacts_dir / "compose_up.stdout.txt").write_text(result.stdout or "")
                (artifacts_dir / "compose_up.stderr.txt").write_text(result.stderr or "")
                ps_out = compose_cmd(["ps"]).stdout
                (artifacts_dir / "ps.txt").write_bytes(ps_out or b"")
                print(f"\nDiagnostics saved to: {artifacts_dir}")
            except Exception as e:  # noqa: PERF203
                print(f"Warning: failed to write compose diagnostics: {e}")
            raise RuntimeError(
                "docker compose up failed; see output above and artifacts/compose_up.stderr.txt for details"
            )

    # Health check for API service
    import requests  # type: ignore[import-untyped]

    max_retries = 60
    for attempt in range(max_retries):
        try:
            response = requests.get("http://localhost:8080/", timeout=5)
            if response.status_code in [200, 400, 403]:  # Gateway is responding
                print("Gateway service is ready")
                break
        except requests.exceptions.RequestException:
            print(f"API not ready yet, attempt {attempt + 1}/{max_retries}")
            time.sleep(5)
    else:
        print("\nAPI HEALTH CHECK FAILED")
        print("=" * 80)

        ps_result = compose_cmd(["ps"])
        print("\nContainer status:")
        print(ps_result.stdout.decode() if ps_result.stdout else "(empty)")

        logs_result = compose_cmd(["logs", "--tail=100"])
        print("\nRecent logs:")
        print(logs_result.stdout.decode() if logs_result.stdout else "(empty)")

        artifacts_dir = Path(project_root) / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        (artifacts_dir / "health_check_ps.txt").write_bytes(ps_result.stdout or b"")
        (artifacts_dir / "health_check_logs.txt").write_bytes(logs_result.stdout or b"")
        print(f"\nDiagnostics saved to: {artifacts_dir}")

        raise RuntimeError("API service failed to start within timeout")

    yield

    # Always attempt to dump service state/logs before teardown when in CI or explicitly requested
    try:
        dump_logs = os.environ.get("CI", "").lower() in {"true", "1", "yes"} or os.environ.get(
            "HIPPIUS_E2E_DUMP_LOGS", ""
        ).lower() in {"true", "1", "yes"}
        if dump_logs:
            artifacts_dir = Path(project_root) / "artifacts"
            artifacts_dir.mkdir(parents=True, exist_ok=True)

            # docker compose ps
            ps_out = compose_cmd(["ps"]).stdout
            (artifacts_dir / "ps.txt").write_bytes(ps_out or b"")

            # service logs (best-effort)
            for svc in ["api", "gateway", "downloader", "uploader", "unpinner"]:
                log_result = compose_cmd(["logs", svc])
                (artifacts_dir / f"{svc}.log").write_bytes(log_result.stdout or b"")
    except Exception as e:  # noqa: PERF203
        print(f"Warning: failed to dump logs: {e}")

    # Teardown based on env flag (default: teardown)
    teardown = env.get("HIPPIUS_E2E_TEARDOWN", "1") not in {"0", "false", "FALSE", "no", "NO"}
    if teardown:
        subprocess.run(
            ["docker", "compose", "-f", "docker-compose.yml", "-f", "docker-compose.e2e.yml", "down", "-v"],
            env=env,
            cwd=project_root,
        )


# Ensure docker services are started for all e2e tests without having to depend on the fixture explicitly
@pytest.fixture(scope="session", autouse=True)
def _ensure_services(docker_services: None) -> Iterator[None]:
    yield


@pytest.fixture(scope="session", autouse=True)
def _init_ipfs_proxies(docker_services: None) -> Iterator[None]:
    """Ensure Toxiproxy IPFS proxies exist and are enabled before any tests run.

    Depends on docker_services so the compose stack (including toxiproxy) is up.
    No-op when running against real AWS.
    """
    if os.getenv("RUN_REAL_AWS") == "1" or os.getenv("AWS") == "1":
        yield
        return
    assert wait_for_toxiproxy(), "Toxiproxy API not available"
    enable_ipfs_proxy()
    yield


@pytest.fixture(scope="session", autouse=True)
def _init_kms_proxy(docker_services: None) -> Iterator[None]:
    """Ensure Toxiproxy KMS proxy exists and is enabled before any tests run.

    Routes KMS traffic through toxiproxy to allow fault injection testing.
    No-op when running against real AWS.
    """
    if os.getenv("RUN_REAL_AWS") == "1" or os.getenv("AWS") == "1":
        yield
        return
    assert wait_for_toxiproxy(), "Toxiproxy API not available"
    enable_kms_proxy()
    yield


@pytest.fixture(scope="session", autouse=True)
def _init_arion_proxy(docker_services: None) -> Iterator[None]:
    """Ensure Toxiproxy Arion proxy exists and is enabled before any tests run.

    Routes Arion traffic through toxiproxy to allow fault injection testing.
    No-op when running against real AWS.
    """
    if os.getenv("RUN_REAL_AWS") == "1" or os.getenv("AWS") == "1":
        yield
        return
    assert wait_for_toxiproxy(), "Toxiproxy API not available"
    enable_arion_proxy()
    yield


@pytest.fixture
def boto3_client(test_seed_phrase: str) -> Any:
    """Create a boto3 S3 client configured for testing.

    RUN_REAL_AWS=1 to run against real AWS. Otherwise uses local endpoint.
    """
    if os.getenv("RUN_REAL_AWS") == "1" or os.getenv("AWS") == "1":
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
        endpoint_url="http://localhost:8080",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            signature_version="s3v4",
            # E2E reads can legitimately block while the pipeline hydrates from IPFS.
            # Use a short read timeout and retry in the test helper instead of hanging for ~60s.
            connect_timeout=5,
            read_timeout=5,
        ),
    )


@pytest.fixture
def signed_http_get(boto3_client: Any) -> Any:
    """Return a boto3-backed GET helper that mimics requests' response shape.

    Usage: signed_http_get(bucket, key, extra_headers={})
    Supports 'Range' in extra_headers.
    """

    class _Resp:
        def __init__(self, status_code: int, headers: dict[str, str], content: bytes) -> None:
            self.status_code = status_code
            self.headers = headers
            self.content = content

    def _get(bucket: str, key: str, extra_headers: dict[str, str] | None = None) -> _Resp:
        params: dict[str, Any] = {"Bucket": bucket, "Key": key}
        if extra_headers and "Range" in extra_headers:
            params["Range"] = extra_headers["Range"]
        try:
            resp = boto3_client.get_object(**params)
            status = int(resp.get("ResponseMetadata", {}).get("HTTPStatusCode", 200))
            headers = resp.get("ResponseMetadata", {}).get("HTTPHeaders", {})
            body = resp["Body"].read()
            return _Resp(status, headers, body)
        except ReadTimeoutError:
            # The gateway/API may be waiting for downloader hydration; allow caller loops to retry.
            return _Resp(504, {}, b"")
        except ClientError as e:
            meta = (e.response or {}).get("ResponseMetadata", {}) if hasattr(e, "response") else {}
            status = int(meta.get("HTTPStatusCode", 500))
            headers = meta.get("HTTPHeaders", {}) or {}
            return _Resp(status, headers, b"")

    return _get


@pytest.fixture
def wait_until_readable(boto3_client: Any) -> Callable[[str, str, float], None]:
    """No-op fixture - objects are now immediately available from cache."""

    def _wait(bucket: str, key: str, timeout_seconds: float = 60.0) -> None:
        # Objects are now immediately available via cache, no waiting needed
        pass

    return _wait


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
