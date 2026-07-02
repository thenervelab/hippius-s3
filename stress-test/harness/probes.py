"""Cluster-side invariant probes: staging Postgres (cephor_replication_status) + Prometheus (drain_*).

Optional — the S3-facing scenarios run without cluster access. When kubectl + the staging cluster
are reachable, these probes elevate the run to a real drain-internals assertion (single-leader,
replication convergence, backlog, terminal-state monotonicity).
"""

from __future__ import annotations

import contextlib
import json
import subprocess
import time

from .config import Config


class ClusterProbe:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self._pf: subprocess.Popen | None = None
        self._prom_port = 0

    # ---------------------------------------------------------------- availability
    def available(self) -> bool:
        """kubectl reachable AND the pg pod answers a trivial query."""
        r = self._pg_raw("select 1")
        return r == "1"

    # ---------------------------------------------------------------- postgres
    def _pg_raw(self, sql: str) -> str | None:
        cmd = [
            "kubectl", "-n", self.cfg.namespace, "exec", self.cfg.pg_pod, "-c", "postgres",
            "--", "psql", "-U", "postgres", "-d", self.cfg.pg_db, "-tAF|", "-c", sql,
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if proc.returncode != 0:
            return None
        return proc.stdout.strip()

    def pg(self, sql: str) -> list[list[str]] | None:
        """Run SQL; return rows as lists of string columns (| separated). None if unreachable."""
        out = self._pg_raw(sql)
        if out is None:
            return None
        if out == "":
            return []
        return [line.split("|") for line in out.splitlines()]

    def pg_scalar(self, sql: str) -> str | None:
        rows = self.pg(sql)
        if not rows:
            return None
        return rows[0][0]

    # ---------------------------------------------------------------- prometheus (port-forward)
    def start_prometheus(self) -> bool:
        """Start a port-forward to prometheus-server; returns True if it answers."""
        for port in (9095, 9096, 9097):
            self._pf = subprocess.Popen(  # noqa: S603
                ["kubectl", "-n", self.cfg.monitoring_namespace, "port-forward",
                 f"svc/{self.cfg.prometheus_svc}", f"{port}:80"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            self._prom_port = port
            time.sleep(3)
            if self.prom("vector(1)") is not None:
                return True
            self.stop_prometheus()
        return False

    def stop_prometheus(self) -> None:
        if self._pf is not None:
            self._pf.terminate()
            self._pf = None

    def prom(self, promql: str) -> list[tuple[dict, float]] | None:
        """Instant query; return [(labels, value)]. None on failure."""
        if not self._prom_port:
            return None
        import urllib.parse

        url = f"http://localhost:{self._prom_port}/api/v1/query?query={urllib.parse.quote(promql)}"
        try:
            with urllib.request.urlopen(url, timeout=15) as resp:  # noqa: S310
                data = json.load(resp)
        except Exception:
            return None
        if data.get("status") != "success":
            return None
        out = []
        for r in data["data"]["result"]:
            out.append((r["metric"], float(r["value"][1])))
        return out

    def prom_scalar(self, promql: str) -> float | None:
        r = self.prom(promql)
        if not r:
            return None
        return r[0][1]

    @contextlib.contextmanager
    def prometheus(self):
        ok = self.start_prometheus()
        try:
            yield ok
        finally:
            self.stop_prometheus()
