"""WI-19 §4.5 fault injector — apply/clean one F-cell from matrix.yaml.

Thin wrapper over kubectl (Chaos Mesh CRDs + the F5 fill Job) and the toxiproxy control API (F8
limit_data body-truncation + the redis/PG toxics). `apply` blocks until the fault has actually landed
(Chaos Mesh AllInjected=True) so run_chaos.sh never holds through — and reports GREEN on — a fault that
never injected; `cleanup` force-clears a stuck finalizer so a wedged chaos object can't hang the run or
leak into the next cell. Beyond that it stays dumb: run_chaos.sh still owns the hold window, the
inv-guard/ledger assertions, and cleanup ordering.

  python inject.py apply   F2 [--repo-root .] [--toxi http://localhost:8474]
  python inject.py cleanup F2
  python inject.py list

Exit non-zero if the cell is unknown or the underlying kubectl/toxiproxy call fails.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import yaml


# How long to wait for a Chaos Mesh CR to actually inject before treating the cell as a setup failure.
INJECT_TIMEOUT_S = 90.0


def _load_matrix(repo_root: Path) -> dict:
    with (repo_root / "stress-test" / "faults" / "matrix.yaml").open() as fh:
        return yaml.safe_load(fh)


def _kubectl(args: list[str], ns: str) -> None:
    cmd = ["kubectl", "-n", ns, *args]
    print(f"+ {' '.join(cmd)}", file=sys.stderr)
    subprocess.run(cmd, check=True)


def _kubectl_out(args: list[str], ns: str, timeout: float = 40.0) -> tuple[int, str]:
    proc = subprocess.run(["kubectl", "-n", ns, *args], capture_output=True, text=True, timeout=timeout)
    return proc.returncode, (proc.stdout or "").strip()


def _chaos_objects(manifest: Path) -> list[tuple[str, str]]:
    """(kind, name) of every Chaos Mesh object (+ Job) declared in a cell manifest, so `apply` can
    wait for the fault to land and `cleanup` can force-clear a stuck finalizer per object."""
    objs: list[tuple[str, str]] = []
    with manifest.open() as fh:
        for doc in yaml.safe_load_all(fh):
            if not doc:
                continue
            kind = doc.get("kind", "")
            name = (doc.get("metadata") or {}).get("name", "")
            if kind and name and (kind.endswith("Chaos") or kind == "Job"):
                objs.append((kind, name))
    return objs


def _wait_injected(kind: str, obj: str, ns: str, timeout_s: float = INJECT_TIMEOUT_S) -> None:
    """Block until a Chaos Mesh object reports AllInjected=True.

    A chaos CR that is created but never selects a target or never injects (webhook unreachable,
    missing RBAC, a selector that matches nothing, a controller not reconciling) is a SETUP failure,
    not a fault the cell can assert against. Without this wait, run_chaos.sh would hold through the
    recovery window against a fault that never landed and report the cell GREEN — the exact
    silent-no-op the harness is built to prevent. Surfacing it (non-zero exit) is the point."""
    deadline = time.time() + timeout_s
    last = "<no status yet>"
    while time.time() < deadline:
        rc, out = _kubectl_out(
            ["get", kind, obj, "-o",
             "jsonpath={range .status.conditions[?(@.type=='AllInjected')]}{.status}{end}"], ns)
        if rc == 0 and out == "True":
            print(f"  {kind}/{obj} AllInjected=True", file=sys.stderr)
            return
        last = out or last
        time.sleep(2)
    raise SystemExit(
        f"{kind}/{obj} did not reach AllInjected=True within {timeout_s:.0f}s (last={last!r}) — the "
        f"fault never landed; check the chaos-mesh controller (leader/reconciler ready?) and selector")


def _force_delete(kind: str, obj: str, ns: str) -> None:
    """Delete a Chaos Mesh object, stripping a stuck finalizer if the normal delete blocks.

    Chaos Mesh objects carry a `chaos-mesh/records` finalizer the controller only clears after it has
    recovered the fault; if the target was killed or the daemon/controller hit a transient error, the
    delete hangs indefinitely. A bounded delete followed by a finalizer-strip makes per-cell cleanup
    deterministic instead of leaving a wedged object that poisons the next cell."""
    rc, _ = _kubectl_out(["delete", kind, obj, "--ignore-not-found", "--timeout=25s"], ns, timeout=35.0)
    if rc == 0:
        return
    print(f"  {kind}/{obj} delete stalled — stripping finalizer", file=sys.stderr)
    _kubectl_out(["patch", kind, obj, "--type=json",
                  "-p", '[{"op":"remove","path":"/metadata/finalizers"}]'], ns)
    _kubectl_out(["delete", kind, obj, "--ignore-not-found", "--timeout=15s"], ns, timeout=25.0)


def _toxi(base: str, method: str, path: str, body: dict | None = None) -> None:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"{base}{path}", data=data, method=method)
    req.add_header("Content-Type", "application/json")
    print(f"+ toxiproxy {method} {path} {body or ''}", file=sys.stderr)
    with urllib.request.urlopen(req, timeout=5) as resp:  # noqa: S310 (trusted local control API)
        resp.read()


def _toxic_name(cell: dict) -> str:
    return f"chaos-{cell['mechanism'].get('toxic', 'toxic')}"


def apply_cell(name: str, cell: dict, ns: str, repo_root: Path, toxi: str) -> None:
    mech = cell["mechanism"]
    kind = mech["kind"]
    if kind in ("chaos-crd", "job"):
        manifest = repo_root / mech["manifest"]
        _kubectl(["apply", "-f", str(manifest)], ns)
        # Confirm the fault actually injected before returning — a created-but-not-injected chaos CR
        # is a setup failure, not a fault to hold through (see _wait_injected). Jobs have no
        # AllInjected condition, so only wait on the Chaos Mesh kinds.
        for k, obj in _chaos_objects(manifest):
            if k.endswith("Chaos"):
                _wait_injected(k, obj, ns)
    elif kind == "toxiproxy":
        proxy = mech["proxy"]
        payload = {
            "name": _toxic_name(cell),
            "type": mech["toxic"],
            "stream": mech.get("stream", "downstream"),
            "toxicity": mech.get("toxicity", 1.0),
            "attributes": mech.get("attributes", {}),
        }
        _toxi(toxi, "POST", f"/proxies/{proxy}/toxics", payload)
    else:
        raise SystemExit(f"unknown mechanism kind '{kind}' for cell {name}")


def cleanup_cell(name: str, cell: dict, ns: str, repo_root: Path, toxi: str) -> None:
    mech = cell["mechanism"]
    kind = mech["kind"]
    try:
        if kind in ("chaos-crd", "job"):
            # Per-object bounded delete + finalizer-strip so a wedged chaos object can't hang cleanup
            # or leak into the next cell (see _force_delete).
            for k, obj in _chaos_objects(repo_root / mech["manifest"]):
                _force_delete(k, obj, ns)
        elif kind == "toxiproxy":
            proxy = mech["proxy"]
            with urllib.request.urlopen(  # noqa: S310
                urllib.request.Request(f"{toxi}/proxies/{proxy}/toxics/{_toxic_name(cell)}", method="DELETE"),
                timeout=5,
            ) as resp:
                resp.read()
    except Exception as exc:  # cleanup is best-effort — never mask the run's real verdict
        print(f"cleanup {name}: {exc}", file=sys.stderr)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["apply", "cleanup", "list"])
    ap.add_argument("cell", nargs="?", help="F1..F8")
    ap.add_argument("--repo-root", default=".")
    ap.add_argument("--toxi", default="http://localhost:8474")
    args = ap.parse_args()

    repo_root = Path(args.repo_root).resolve()
    matrix = _load_matrix(repo_root)
    ns = matrix.get("namespace", "hippius-s3-staging")
    cells = matrix["cells"]

    if args.action == "list":
        for name, cell in cells.items():
            print(f"{name}: {cell['name']} — {cell['headline_gate']}")
        return 0

    if not args.cell or args.cell not in cells:
        print(f"unknown cell {args.cell!r}; choose one of {list(cells)}", file=sys.stderr)
        return 2

    cell = cells[args.cell]
    if args.action == "apply":
        apply_cell(args.cell, cell, ns, repo_root, args.toxi)
    else:
        cleanup_cell(args.cell, cell, ns, repo_root, args.toxi)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
