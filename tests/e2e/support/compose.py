from __future__ import annotations

import os
import subprocess
from contextlib import suppress
from pathlib import Path
from typing import List
from typing import Tuple

import requests


def _repo_root() -> Path:
    here = Path(__file__).resolve()
    for parent in [here.parent, here.parent.parent, here.parent.parent.parent, here.parent.parent.parent.parent]:
        if (parent / "docker-compose.e2e.yml").exists() or (parent / "docker-compose.yml").exists():
            return parent
    return Path.cwd()


def _find_container_by_service(service: str) -> str | None:
    try:
        result = subprocess.run(
            [
                "docker",
                "ps",
                "-q",
                "--filter",
                f"label=com.docker.compose.service={service}",
            ],
            capture_output=True,
            text=True,
        )
        cid = (result.stdout or "").strip().splitlines()
        return cid[0] if cid else None
    except Exception:
        return None


def compose_exec(service: str, args: List[str]) -> Tuple[int, str, str]:
    """Run a command inside a docker compose service or fallback to docker exec by label.

    Returns (returncode, stdout, stderr).
    """
    # Try compose exec relative to repo root
    root = _repo_root()
    cmd = ["docker", "compose", "exec", "-T", service] + args
    result = subprocess.run(cmd, cwd=str(root), capture_output=True, text=True)
    if result.returncode == 0:
        return result.returncode, result.stdout, result.stderr

    # Fallback: find container by service label and docker exec into it
    cid = _find_container_by_service(service)
    if not cid:
        return 1, "", f"container for service '{service}' not found"
    result2 = subprocess.run(["docker", "exec", "-i", cid] + args, capture_output=True, text=True)
    return result2.returncode, result2.stdout, result2.stderr


def exec_python_module(service: str, module: str, args: List[str]) -> Tuple[int, str, str]:
    """Execute a Python module inside a compose service using the service's Python interpreter.

    Example: exec_python_module("api", "hippius_s3.scripts.dlq_requeue", ["peek"])
    """
    return compose_exec(service, ["python", "-m", module] + args)


def ipfs_http_ok(service: str = "api", timeout_seconds: float = 1.0) -> bool:
    """Check if IPFS control API is reachable via Toxiproxy from inside a service container.
    Probes POST /api/v0/version on host 'toxiproxy' port 15001 by default.
    Uses stdlib http.client to avoid external deps."""
    py = (
        "import sys, http.client;\n"
        "host = __import__('os').environ.get('TOXI_HOST','toxiproxy')\n"
        "port = int(__import__('os').environ.get('TOXI_IPFS_CTL_PORT','15001'))\n"
        "try:\n"
        f"  c = http.client.HTTPConnection(host, port, timeout={timeout_seconds});\n"
        "  c.request('POST', '/api/v0/version');\n"
        "  r = c.getresponse();\n"
        "  # consider any HTTP response as 'reachable'\n"
        "  sys.exit(0 if (100 <= r.status < 600) else 1)\n"
        "except Exception:\n"
        "  sys.exit(2)\n"
    )
    code, _, _ = compose_exec(service, ["python", "-c", py])
    return code == 0


def wait_for_ipfs_state(
    desired_up: bool, service: str = "api", timeout_seconds: float = 10.0, poll_seconds: float = 0.5
) -> bool:
    """Wait until IPFS HTTP is reachable (desired_up=True) or unreachable (False)."""
    import time

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        ok = ipfs_http_ok(service=service, timeout_seconds=poll_seconds)
        if desired_up and ok:
            return True
        if not desired_up and not ok:
            return True
        time.sleep(poll_seconds)
    return False


# ---- Toxiproxy helpers ----

_TOXI_BASE = os.environ.get("TOXI_BASE", "http://localhost:8474")
_TOXI_LISTEN_GET = os.environ.get("TOXI_LISTEN_GET", "0.0.0.0:18080")
_TOXI_LISTEN_CTL = os.environ.get("TOXI_LISTEN_CTL", "0.0.0.0:15001")
_TOXI_UPSTREAM_GET = os.environ.get("TOXI_UPSTREAM_GET", "ipfs:8080")
_TOXI_UPSTREAM_CTL = os.environ.get("TOXI_UPSTREAM_CTL", "ipfs:5001")

# KMS proxy config
_TOXI_LISTEN_KMS = os.environ.get("TOXI_LISTEN_KMS", "0.0.0.0:18443")
_TOXI_UPSTREAM_KMS = os.environ.get("TOXI_UPSTREAM_KMS", "mock-kms:8443")


def _toxiproxy_get(name: str) -> requests.Response:
    return requests.get(f"{_TOXI_BASE}/proxies/{name}", timeout=2)


def _toxiproxy_create(name: str, listen: str, upstream: str) -> bool:
    resp = requests.post(
        f"{_TOXI_BASE}/proxies",
        json={"name": name, "listen": listen, "upstream": upstream},
        timeout=3,
    )
    return resp.status_code in (200, 201)


def _toxiproxy_set_enabled(name: str, enabled: bool) -> bool:
    # Toxiproxy API requires PUTting the full proxy config, not just the enabled field
    get_url = f"{_TOXI_BASE}/proxies/{name}"
    put_url = f"{_TOXI_BASE}/proxies/{name}"
    print(f"DEBUG: Setting proxy {name} enabled={enabled}")

    # First get current proxy config
    get_resp = requests.get(get_url, timeout=3)
    if get_resp.status_code != 200:
        print(f"DEBUG: Failed to get proxy config: {get_resp.status_code}, {get_resp.text}")
        return False

    proxy_config = get_resp.json()
    print(f"DEBUG: Current proxy config: {proxy_config}")

    # Update enabled field
    proxy_config["enabled"] = enabled

    # PUT back the full config
    put_resp = requests.put(
        put_url,
        json=proxy_config,
        timeout=3,
    )
    print(f"DEBUG: PUT response status: {put_resp.status_code}, content: {put_resp.text}")
    return put_resp.status_code in (200, 204)


def _toxiproxy_delete(name: str) -> bool:
    url = f"{_TOXI_BASE}/proxies/{name}"
    resp = requests.delete(url, timeout=3)
    return resp.status_code in (200, 204)


def _ensure_ipfs_proxies() -> None:
    # Ensure both GET and control proxies exist
    try:
        print("DEBUG: Checking if IPFS proxies exist")
        r1 = _toxiproxy_get("ipfs_get")
        print(f"DEBUG: ipfs_get proxy status: {r1.status_code}")
        if r1.status_code == 404:
            print(f"DEBUG: Creating ipfs_get proxy: {_TOXI_LISTEN_GET} -> {_TOXI_UPSTREAM_GET}")
            ok = _toxiproxy_create("ipfs_get", _TOXI_LISTEN_GET, _TOXI_UPSTREAM_GET)
            if not ok:
                raise RuntimeError("failed to create toxiproxy ipfs_get")
            print("DEBUG: Created ipfs_get proxy")
        r2 = _toxiproxy_get("ipfs_store")
        print(f"DEBUG: ipfs_store proxy status: {r2.status_code}")
        if r2.status_code == 404:
            print(f"DEBUG: Creating ipfs_store proxy: {_TOXI_LISTEN_CTL} -> {_TOXI_UPSTREAM_CTL}")
            ok = _toxiproxy_create("ipfs_store", _TOXI_LISTEN_CTL, _TOXI_UPSTREAM_CTL)
            if not ok:
                raise RuntimeError("failed to create toxiproxy ipfs_store")
            print("DEBUG: Created ipfs_store proxy")
        print("DEBUG: IPFS proxies ensured")
    except Exception as e:
        # Bubble up to fail fast in tests
        print(f"DEBUG: Error ensuring IPFS proxies: {e}")
        raise


def disable_ipfs_proxy() -> None:
    # Hard-disable by deleting proxies so no listener is bound
    # Best-effort deletes; 404 means already absent
    with suppress(Exception):
        _toxiproxy_delete("ipfs_get")
    with suppress(Exception):
        _toxiproxy_delete("ipfs_store")


def enable_ipfs_proxy() -> None:
    _ensure_ipfs_proxies()


def wait_for_toxiproxy(timeout_seconds: float = 10.0, poll_seconds: float = 0.5) -> bool:
    """Wait for Toxiproxy control API to be available."""
    import time

    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        try:
            # Try to get a proxy - if it fails, toxiproxy isn't ready
            r = _toxiproxy_get("ipfs_get")
            if r.status_code in (200, 404):  # 404 means no proxy exists yet, but toxiproxy is responding
                return True
        except Exception:
            pass
        time.sleep(poll_seconds)
    return False


# ---- KMS Toxiproxy helpers ----


def _ensure_kms_proxy() -> None:
    """Ensure the KMS proxy exists in toxiproxy."""
    try:
        r = _toxiproxy_get("kms")
        if r.status_code == 404:
            print(f"DEBUG: Creating kms proxy: {_TOXI_LISTEN_KMS} -> {_TOXI_UPSTREAM_KMS}")
            ok = _toxiproxy_create("kms", _TOXI_LISTEN_KMS, _TOXI_UPSTREAM_KMS)
            if not ok:
                raise RuntimeError("failed to create toxiproxy kms")
            print("DEBUG: Created kms proxy")
    except Exception as e:
        print(f"DEBUG: Error ensuring KMS proxy: {e}")
        raise


def disable_kms_proxy() -> None:
    """Disable KMS proxy to simulate KMS outage."""
    _ensure_kms_proxy()
    ok = _toxiproxy_set_enabled("kms", enabled=False)
    if not ok:
        raise RuntimeError("failed to disable kms proxy")


def enable_kms_proxy() -> None:
    """Enable KMS proxy to restore KMS connectivity."""
    _ensure_kms_proxy()
    ok = _toxiproxy_set_enabled("kms", enabled=True)
    if not ok:
        raise RuntimeError("failed to enable kms proxy")


def pause_service(service: str) -> None:
    """Pause a service by disabling its toxiproxy.

    Supported services: 'mock-kms', 'ipfs'
    """
    if service == "mock-kms":
        disable_kms_proxy()
    elif service == "ipfs":
        disable_ipfs_proxy()
    else:
        raise ValueError(f"Unknown service: {service}. Supported: mock-kms, ipfs")


def unpause_service(service: str) -> None:
    """Unpause a service by enabling its toxiproxy.

    Supported services: 'mock-kms', 'ipfs'
    """
    if service == "mock-kms":
        enable_kms_proxy()
    elif service == "ipfs":
        enable_ipfs_proxy()
    else:
        raise ValueError(f"Unknown service: {service}. Supported: mock-kms, ipfs")
