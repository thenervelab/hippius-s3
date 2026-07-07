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


# ---- Toxiproxy helpers ----

_TOXI_BASE = os.environ.get("TOXI_BASE", "http://localhost:8474")

# KMS proxy config
_TOXI_LISTEN_KMS = os.environ.get("TOXI_LISTEN_KMS", "0.0.0.0:18443")
_TOXI_UPSTREAM_KMS = os.environ.get("TOXI_UPSTREAM_KMS", "mock-kms:8443")

# Arion proxy config
_TOXI_LISTEN_ARION = os.environ.get("TOXI_LISTEN_ARION", "0.0.0.0:19090")
_TOXI_UPSTREAM_ARION = os.environ.get("TOXI_UPSTREAM_ARION", "mock-arion:8002")


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
    """Toggle a toxiproxy proxy on/off without deleting it."""
    url = f"{_TOXI_BASE}/proxies/{name}"

    get_resp = requests.get(url, timeout=3)
    if get_resp.status_code != 200:
        return False

    proxy_config = get_resp.json()
    if proxy_config.get("enabled") == enabled:
        return True

    proxy_config["enabled"] = enabled
    post_resp = requests.post(url, json=proxy_config, timeout=3)
    return post_resp.status_code in (200, 201)


def wait_for_toxiproxy(timeout_seconds: float = 10.0, poll_seconds: float = 0.5) -> bool:
    """Wait for Toxiproxy control API to be available."""
    import time

    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        try:
            # Try to get a proxy - if it fails, toxiproxy isn't ready
            r = _toxiproxy_get("arion")
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
    with suppress(Exception):
        _toxiproxy_set_enabled("kms", False)


def enable_kms_proxy() -> None:
    """Ensure KMS proxy exists and is enabled."""
    _ensure_kms_proxy()
    _toxiproxy_set_enabled("kms", True)


# ---- Arion Toxiproxy helpers ----


def _ensure_arion_proxy() -> None:
    """Ensure the Arion proxy exists in toxiproxy."""
    try:
        r = _toxiproxy_get("arion")
        if r.status_code == 404:
            print(f"DEBUG: Creating arion proxy: {_TOXI_LISTEN_ARION} -> {_TOXI_UPSTREAM_ARION}")
            ok = _toxiproxy_create("arion", _TOXI_LISTEN_ARION, _TOXI_UPSTREAM_ARION)
            if not ok:
                raise RuntimeError("failed to create toxiproxy arion")
            print("DEBUG: Created arion proxy")
    except Exception as e:
        print(f"DEBUG: Error ensuring Arion proxy: {e}")
        raise


def disable_arion_proxy() -> None:
    """Disable Arion proxy to simulate Arion outage."""
    with suppress(Exception):
        _toxiproxy_set_enabled("arion", False)


def enable_arion_proxy() -> None:
    """Ensure Arion proxy exists and is enabled."""
    _ensure_arion_proxy()
    _toxiproxy_set_enabled("arion", True)


def pause_service(service: str) -> None:
    """Pause a service by disabling its toxiproxy.

    Supported services: 'mock-kms', 'arion'
    """
    if service == "mock-kms":
        disable_kms_proxy()
    elif service == "arion":
        disable_arion_proxy()
    else:
        raise ValueError(f"Unknown service: {service}. Supported: mock-kms, arion")


def unpause_service(service: str) -> None:
    """Unpause a service by enabling its toxiproxy.

    Supported services: 'mock-kms', 'arion'
    """
    if service == "mock-kms":
        enable_kms_proxy()
    elif service == "arion":
        enable_arion_proxy()
    else:
        raise ValueError(f"Unknown service: {service}. Supported: mock-kms, arion")


# ---- WI-19 §4.4: generic toxics + redis-queues/Postgres proxies + mock fault modes ----
#
# Until WI-19, toxiproxy was used on/off only (whole-proxy enable/disable) with proxies for
# arion + kms alone. The chaos matrix (F3 redis blip/hang, F6 PG failover) and the allocator
# Redis-pathology cells need real toxics (latency/timeout/bandwidth/reset_peer/slicer) in front
# of redis-queues and Postgres, plus the ability to brown-out a mock without tearing it down.

# redis-queues proxy: coordinator + queue Redis. The consumer must point REDIS_QUEUES_URL at the
# proxy for a toxic to bite — the alloc-stress overlay (compose/docker-compose.alloc-stress.yml)
# and the faults overlay do that.
_TOXI_LISTEN_REDIS_QUEUES = os.environ.get("TOXI_LISTEN_REDIS_QUEUES", "0.0.0.0:16379")
_TOXI_UPSTREAM_REDIS_QUEUES = os.environ.get("TOXI_UPSTREAM_REDIS_QUEUES", "redis-queues:6379")

# Postgres proxy (the base compose service is `db`).
_TOXI_LISTEN_PG = os.environ.get("TOXI_LISTEN_PG", "0.0.0.0:15432")
_TOXI_UPSTREAM_PG = os.environ.get("TOXI_UPSTREAM_PG", "db:5432")


def _ensure_proxy(name: str, listen: str, upstream: str) -> None:
    """Create a toxiproxy proxy if it does not already exist."""
    r = _toxiproxy_get(name)
    if r.status_code == 404:
        if not _toxiproxy_create(name, listen, upstream):
            raise RuntimeError(f"failed to create toxiproxy {name} ({listen} -> {upstream})")


def ensure_redis_queues_proxy() -> None:
    _ensure_proxy("redis_queues", _TOXI_LISTEN_REDIS_QUEUES, _TOXI_UPSTREAM_REDIS_QUEUES)


def ensure_pg_proxy() -> None:
    _ensure_proxy("postgres", _TOXI_LISTEN_PG, _TOXI_UPSTREAM_PG)


def add_toxic(
    proxy: str,
    toxic_type: str,
    attributes: dict,
    *,
    name: str | None = None,
    stream: str = "downstream",
    toxicity: float = 1.0,
) -> str:
    """Attach a toxic to an existing proxy and return its name.

    toxic_type ∈ {latency, timeout, bandwidth, limit_data, slicer, reset_peer, slow_close, ...}
    stream ∈ {downstream, upstream}. Examples:
      add_toxic("redis_queues", "latency", {"latency": 800, "jitter": 100})   # +800±100ms
      add_toxic("redis_queues", "timeout", {"timeout": 0})                    # hang (F3)
      add_toxic("arion", "bandwidth", {"rate": 10})                           # 10 KB/s (F4)
      add_toxic("arion", "limit_data", {"bytes": 65536})                      # truncate body (F8)
      add_toxic("postgres", "reset_peer", {"timeout": 0})                     # RST (F6)
    """
    toxic_name = name or f"{toxic_type}_{stream}"
    payload = {
        "name": toxic_name,
        "type": toxic_type,
        "stream": stream,
        "toxicity": toxicity,
        "attributes": attributes,
    }
    resp = requests.post(f"{_TOXI_BASE}/proxies/{proxy}/toxics", json=payload, timeout=3)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"add_toxic {proxy}/{toxic_name} failed: {resp.status_code} {resp.text}")
    return toxic_name


def remove_toxic(proxy: str, toxic_name: str) -> None:
    with suppress(Exception):
        requests.delete(f"{_TOXI_BASE}/proxies/{proxy}/toxics/{toxic_name}", timeout=3)


def clear_toxics(proxy: str) -> None:
    """Remove every toxic on a proxy (leaving the proxy itself enabled)."""
    with suppress(Exception):
        resp = requests.get(f"{_TOXI_BASE}/proxies/{proxy}/toxics", timeout=3)
        if resp.status_code == 200:
            for toxic in resp.json():
                remove_toxic(proxy, toxic["name"])


# ---- Mock fault-mode control (mock_faults.install_fault_controller mounts /_fault) ----


def set_mock_fault(base_url: str, rules: dict | list) -> None:
    """POST fault rule(s) to a mock's /_fault control endpoint.

    base_url is the mock's reachable root, e.g. http://localhost:8002 (arion). A rule is
    {"op": "upload"|"download"|..., "mode": "error"|"slow"|"fail_after_n"|"truncate"|"reject",
     "status": 500, "delay_s": 0, "after_n": 0, "truncate_bytes": 0}.
    """
    resp = requests.post(f"{base_url.rstrip('/')}/_fault", json=rules, timeout=3)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"set_mock_fault {base_url} failed: {resp.status_code} {resp.text}")


def clear_mock_fault(base_url: str) -> None:
    with suppress(Exception):
        requests.delete(f"{base_url.rstrip('/')}/_fault", timeout=3)
