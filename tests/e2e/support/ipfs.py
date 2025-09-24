import os
import subprocess
from pathlib import Path
from typing import Any

import requests


def ipfs_ls(cid: str, *, timeout: float = 5.0) -> dict[str, Any]:
    """Return the result of `ipfs ls` for a CID via the configured gateway.

    Uses HIPPIUS_IPFS_GET_URL (defaults to http://ipfs:8080) and calls /api/v0/ls.
    """
    ipfs_gateway = os.environ.get("HIPPIUS_IPFS_GET_URL", "http://toxiproxy:18080")
    url = f"{ipfs_gateway}/api/v0/ls?arg={cid}"
    resp = requests.post(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()  # type: ignore[no-any-return]


def assert_ipfs_contains_filename(cid: str, filename: str) -> None:
    """Assert that the IPFS object referenced by CID contains a file with given name.

    Works for wrapped directories by searching Links, and for single-file objects by ensuring non-zero size.
    """
    data = ipfs_ls(cid)
    objects = data.get("Objects") or []
    assert objects, f"ipfs ls returned empty for cid={cid}"

    links = objects[0].get("Links") or []
    if links:
        names = [link.get("Name") for link in links]
        assert filename in names, f"expected filename {filename} in IPFS links {names}"
    else:
        # Single object; ensure non-zero size
        size = int(objects[0].get("Size") or 0)
        assert size > 0, "expected non-zero size for single-file IPFS object"


# --- Test helpers to break/heal IPFS during e2e runs ---


def _repo_root() -> Path:
    # tests/e2e/support -> tests/e2e -> tests -> repo root
    here = Path(__file__).resolve()
    for parent in [here.parent, here.parent.parent, here.parent.parent.parent, here.parent.parent.parent.parent]:
        if (parent / "docker-compose.e2e.yml").exists() or (parent / "docker-compose.yml").exists():
            return parent
    return Path.cwd()


def _compose_ps(service: str) -> str | None:
    """Return container ID for a compose service, trying compose context then global labels."""
    root = _repo_root()
    # Try compose-relative query
    try:
        result = subprocess.run(
            ["docker", "compose", "ps", "-aq", service], cwd=str(root), capture_output=True, text=True
        )
        cid = (result.stdout or "").strip()
        if cid:
            return cid
    except Exception:
        pass
    # Fallback: global docker ps by service label
    try:
        result = subprocess.run(
            [
                "docker",
                "ps",
                "-a",
                "-q",
                "--filter",
                f"label=com.docker.compose.service={service}",
            ],
            capture_output=True,
            text=True,
        )
        cids = (result.stdout or "").strip().splitlines()
        return cids[0] if cids else None
    except Exception:
        pass
    # Fallback: match by name pattern *-service-1
    try:
        result = subprocess.run(
            ["docker", "ps", "-a", "--format", "{{.ID}}\t{{.Names}}"], capture_output=True, text=True
        )
        for line in (result.stdout or "").splitlines():
            parts = line.strip().split("\t", 1)
            if len(parts) != 2:
                continue
            cid, name = parts
            if name.endswith(f"-{service}-1") or name == service:
                return cid
            # Also accept exact compose project prefixed names containing ":" variants
            if name.split(":")[0].endswith(f"-{service}-1"):
                return cid
        return None
    except Exception:
        return None


def pause_ipfs() -> bool:
    """Pause the ipfs container via docker compose. Returns True if successful."""
    root = _repo_root()
    try:
        result = subprocess.run(["docker", "compose", "pause", "ipfs"], cwd=str(root), capture_output=True, text=True)
        if result.returncode != 0:
            print(f"DEBUG: pause_ipfs failed: {result.stderr.strip()}")
        return result.returncode == 0
    except Exception as e:
        print(f"DEBUG: pause_ipfs exception: {e}")
        return False


def unpause_ipfs() -> bool:
    """Unpause the ipfs container via docker compose. Returns True if successful."""
    root = _repo_root()
    try:
        result = subprocess.run(["docker", "compose", "unpause", "ipfs"], cwd=str(root), capture_output=True, text=True)
        if result.returncode != 0:
            print(f"DEBUG: unpause_ipfs failed: {result.stderr.strip()}")
        return result.returncode == 0
    except Exception as e:
        print(f"DEBUG: unpause_ipfs exception: {e}")
        return False


def _list_container_networks(container_id: str) -> list[str]:
    try:
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{ json .NetworkSettings.Networks }}", container_id],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            return []
        import json as _json

        obj = _json.loads(result.stdout)
        return list(obj.keys()) if isinstance(obj, dict) else []
    except Exception:
        return []


def disconnect_ipfs_network() -> bool:
    """Disconnect ipfs container from all its networks. Returns True if at least one disconnect succeeded."""
    container = _compose_ps("ipfs")
    if not container:
        return False
    ok_any = False
    # Attempt multiple passes to ensure full detachment
    for _ in range(3):
        networks = _list_container_networks(container)
        for net in networks:
            try:
                r = subprocess.run(
                    ["docker", "network", "disconnect", "-f", net, container], capture_output=True, text=True
                )
                print(f"DEBUG: disconnect {net} from {container} rc={r.returncode} err='{(r.stderr or '').strip()}'")
                ok_any = ok_any or (r.returncode == 0)
            except Exception:
                print(f"DEBUG: disconnect_ipfs_network: failed to disconnect from network {net}")
                pass
        if not _list_container_networks(container):
            break
    remaining = _list_container_networks(container)
    print(f"DEBUG: disconnect_ipfs_network: remaining_networks={remaining}")
    return ok_any and not remaining


def connect_ipfs_network() -> bool:
    """Reconnect ipfs to the same networks the api service is on."""
    api_container = _compose_ps("api")
    ipfs_container = _compose_ps("ipfs")
    print(f"DEBUG: connect_ipfs_network: api_container={api_container}, ipfs_container={ipfs_container}")
    if not api_container or not ipfs_container:
        return False
    target_networks = _list_container_networks(api_container)
    print(f"DEBUG: connect_ipfs_network: target_networks={target_networks}")
    ok_any = False
    for net in target_networks:
        try:
            # Remove stale endpoint if any, ignore errors
            _ = subprocess.run(
                ["docker", "network", "disconnect", "-f", net, ipfs_container], capture_output=True, text=True
            )
            r = subprocess.run(["docker", "network", "connect", net, ipfs_container], capture_output=True, text=True)
            stderr = (r.stderr or "").lower()
            print(f"DEBUG: connect to {net} for {ipfs_container} rc={r.returncode} err='{(r.stderr or '').strip()}'")
            if r.returncode == 0 or "already exists" in stderr:
                ok_any = True
        except Exception:
            print(f"DEBUG: connect_ipfs_network: failed to connect to network {net}")
            pass
    # Verify attached
    final = _list_container_networks(ipfs_container)
    print(f"DEBUG: connect_ipfs_network: final_networks={final}")
    return ok_any and bool(final)


def break_ipfs() -> None:
    """Make IPFS unreachable by disconnecting it from all networks (network-only)."""
    if disconnect_ipfs_network():
        print("DEBUG: break_ipfs: disconnected from networks")
        return
    raise RuntimeError("Failed to break IPFS via network disconnect")


def heal_ipfs() -> None:
    """Reconnect IPFS to the networks used by API (network-only)."""
    ok = connect_ipfs_network()
    print(f"DEBUG: heal_ipfs: connect networks ok={ok}")
