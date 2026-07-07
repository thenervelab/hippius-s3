"""WI-19 §4.5 fault injector — apply/clean one F-cell from matrix.yaml.

Thin wrapper over kubectl (Chaos Mesh CRDs + the F5 fill Job) and the toxiproxy control API (F8
limit_data body-truncation + the redis/PG toxics). Deliberately dumb: it applies a fault and returns; run_chaos.sh owns
the timing, the inv-guard/ledger assertions, and cleanup ordering.

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
import urllib.request
from pathlib import Path

import yaml


def _load_matrix(repo_root: Path) -> dict:
    with (repo_root / "stress-test" / "faults" / "matrix.yaml").open() as fh:
        return yaml.safe_load(fh)


def _kubectl(args: list[str], ns: str) -> None:
    cmd = ["kubectl", "-n", ns, *args]
    print(f"+ {' '.join(cmd)}", file=sys.stderr)
    subprocess.run(cmd, check=True)


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
        _kubectl(["apply", "-f", str(repo_root / mech["manifest"])], ns)
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
            _kubectl(["delete", "--ignore-not-found", "-f", str(repo_root / mech["manifest"])], ns)
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
