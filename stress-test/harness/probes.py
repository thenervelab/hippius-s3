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
import urllib.parse
import urllib.request

from .config import Config

# psql field separator: ASCII Unit Separator (0x1f), not '|'. A '|' in a cell value (a detail
# string, a base64-ish identifier, a status message) would mis-split the row on a printable
# delimiter; US never appears in our text columns, so splitting on it is unambiguous.
_PSQL_FIELD_SEP = "\x1f"


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
            "--", "psql", "-U", "postgres", "-d", self.cfg.pg_db, "-tAF" + _PSQL_FIELD_SEP, "-c", sql,
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if proc.returncode != 0:
            return None
        return proc.stdout.strip()

    def pg(self, sql: str) -> list[list[str]] | None:
        """Run SQL; return rows as lists of string columns (US-separated). None if unreachable."""
        out = self._pg_raw(sql)
        if out is None:
            return None
        if out == "":
            return []
        return [line.split(_PSQL_FIELD_SEP) for line in out.splitlines()]

    def pg_scalar(self, sql: str) -> str | None:
        rows = self.pg(sql)
        if not rows:
            return None
        return rows[0][0]

    # ---------------------------------------------------------------- ssd-cache eviction (durability gate)
    def api_pods(self) -> list[str]:
        """Names of the api pods carrying the writable SSD ingest cache (one per ingest node)."""
        cmd = [
            "kubectl", "-n", self.cfg.namespace, "get", "pods",
            "-l", f"app={self.cfg.api_selector}", "--field-selector=status.phase=Running",
            "-o", "jsonpath={.items[*].metadata.name}",
        ]
        # A kubectl hang or a missing binary is an environment fault, not a data-loss signal: return
        # no pods so the caller downgrades the gate to non-authoritative rather than failing it red.
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return []
        if proc.returncode != 0:
            return []
        return proc.stdout.split()

    def evict_ssd_parts(self, object_ids: list[str]) -> tuple[int, int, int]:
        """Delete each object's part tree from the WRITABLE SSD primary cache on every api pod.

        The reader's chunks_exist_batch then misses the SSD ingest copy and serves the read-only
        CephFS fallback — the drain's durable output. The CephFS mount is readOnly inside the api
        pod, so this can never delete the drained copy itself; only the ephemeral ingest copy goes.
        An object is ingested on one node only, so its SSD copy lives on a single pod — but the GET
        may be served by either pod, so we evict on ALL of them. Returns
        (objects_targeted, pods_ok, pods_total). pods_ok counts pods whose every rm exec returned 0;
        the caller must require pods_ok == pods_total before trusting the re-verify (rm -rf on an
        absent path also returns 0, so a per-pod success is only meaningful when EVERY pod succeeded).
        """
        if not object_ids:
            return (0, 0, 0)
        pods = self.api_pods()
        dirs = [f"{self.cfg.ssd_cache_dir.rstrip('/')}/{oid}" for oid in object_ids]
        ok_pods = 0
        # `kubectl exec -- rm -rf <paths>` sends every path as a URL query param on the SPDY/exec
        # upgrade request; the API ingress (nginx) rejects the request with `414 Request-URI Too Large`
        # once the encoded URL crosses ~8 KiB. Each path is ~150 bytes URL-encoded, so a batch of 100
        # (~15 KiB) always 414s — the durability gate silently downgraded to OBSERVED with 0/N evicted.
        # 20 paths (~3 KiB) stays comfortably under the limit; the corpus is small so the extra round
        # trips are cheap.
        BATCH = 20
        for pod in pods:
            ok = True
            for i in range(0, len(dirs), BATCH):
                if not self._exec_rm_with_retry(pod, dirs[i:i + BATCH]):
                    ok = False
                    break
            if ok:
                ok_pods += 1
        return (len(object_ids), ok_pods, len(pods))

    def _exec_rm_with_retry(self, pod: str, dirs: list[str], attempts: int = 3) -> bool:
        """rm a batch of dirs on one pod, retrying the transient `kubectl exec` flakes that show up when
        the api pods are busy (right after the concurrency ramp). The rm itself is deterministic —
        root, a 0777 cache dir, `-f` ignores absent paths — so a non-zero result is a kubectl-transport
        blip, not a real failure; a short retry is what makes the durability gate authoritative instead
        of flakily downgrading to OBSERVED. Returns True only if some attempt's rm returned 0.
        The last failure's rc+stderr is printed so a persistent 0/N eviction is debuggable (the gate
        used to swallow it and just report a non-authoritative count with no reason)."""
        last = ""
        for attempt in range(attempts):
            try:
                proc = subprocess.run(  # noqa: S603 — fixed argv, oids are DB uuids
                    ["kubectl", "-n", self.cfg.namespace, "exec", pod, "-c", "api",
                     "--", "rm", "-rf", *dirs],
                    capture_output=True, text=True, timeout=90,
                )
            except (subprocess.TimeoutExpired, FileNotFoundError) as e:
                last = f"exec raised {type(e).__name__}"
                continue  # a hang/missing-binary is transport-level — retry, then give up (non-authoritative)
            if proc.returncode == 0:
                return True
            last = f"rc={proc.returncode} stderr={(proc.stderr or '').strip()[:300]!r}"
            time.sleep(1.0 * (attempt + 1))
        print(f"    [evict] {pod}: {len(dirs)} dirs FAILED after {attempts} attempts — {last}")
        return False

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
