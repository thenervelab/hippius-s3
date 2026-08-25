#!/usr/bin/env python3
"""Production-readiness harness runner.

Runs the scenario suite against an S3 endpoint (default s3-staging.hippius.com) and, when the
staging cluster is reachable via kubectl, the drain internals (Postgres + Prometheus). Emits a
PASS/FAIL report and exits non-zero on any hard-gate failure.

Usage:
    source .aws.cli.env && python stress-test/run.py
    python stress-test/run.py --no-cluster            # S3-only (no kubectl)
    python stress-test/run.py --keep                  # leave test buckets for inspection
"""

from __future__ import annotations

import argparse
import pathlib
import sys
import time


# allow `python stress-test/run.py` from the repo root
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from harness import config  # noqa: E402
from harness import s3util  # noqa: E402
from harness import scenarios  # noqa: E402
from harness.ledger import Ledger  # noqa: E402
from harness.probes import ClusterProbe  # noqa: E402
from harness.report import Report  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description="hippius-s3 drain-stack production-readiness harness")
    ap.add_argument("--endpoint", default=None, help="override S3 endpoint")
    ap.add_argument("--no-cluster", action="store_true", help="skip Postgres/Prometheus invariant probes")
    ap.add_argument("--keep", action="store_true", help="keep test buckets after the run")
    ap.add_argument("--timeout", type=int, default=None, help="drain-convergence timeout (s)")
    # OPT-IN and OPERATOR-SIZED. The scenario never computes this: the ingest SSD on staging is
    # shared with other tenants, so how much may be written is a judgement about the cluster on the
    # day, not a property the suite can derive. Read the disk first (./ssd_disk_report.sh).
    ap.add_argument("--fill-gb", type=float, default=0.0,
                    help="GB to write at the ingest SSD to exercise eviction (0 = skip). "
                         "Requires cluster access. Check ./ssd_disk_report.sh first.")
    ap.add_argument("--fill-object-mb", type=int, default=64,
                    help="object size for --fill-gb (default 64 MB)")
    ap.add_argument("--fill-workers", type=int, default=8, help="concurrent uploads for --fill-gb")
    args = ap.parse_args()

    cfg = config.load()
    if args.endpoint:
        cfg.endpoint_url = args.endpoint
    if args.keep:
        cfg.keep_objects = True
    if args.timeout:
        cfg.drain_convergence_timeout_s = args.timeout

    print(f"== production-readiness harness ==\n   target: {cfg.endpoint_url}\n")
    client = s3util.make_client(cfg)
    ledger = Ledger()
    report = Report(cfg.endpoint_url)
    buckets: dict[str, str] = {}

    probe = ClusterProbe(cfg)
    cluster = (not args.no_cluster) and probe.available()
    print(f"   cluster probes: {'ENABLED (kubectl → staging)' if cluster else 'disabled (S3-only run)'}\n")

    def guarded(label: str, fn) -> None:
        """Run a scenario; a crash becomes a recorded FAIL so the suite continues (harness robustness)."""
        print(f"-- {label} --")
        try:
            fn()
        except Exception as e:  # noqa: BLE001 — a test runner must isolate scenario failures
            from harness.report import Result
            report.add(Result(label, "scenario crashed", passed=False,
                              detail=f"{type(e).__name__}: {e}",
                              criteria="scenario runs to completion without an unhandled error"))

    guarded("T1 functional/adversarial", lambda: scenarios.functional_adversarial(client, cfg, ledger, report, buckets))
    guarded("durability corpus (upload)", lambda: scenarios.durability_corpus(client, cfg, ledger, report, buckets))
    guarded("T5-lite concurrency ramp", lambda: scenarios.concurrency_ramp(client, cfg, ledger, report, buckets))

    prom_ok = False
    if cluster:
        with probe.prometheus() as prom_ok:
            guarded("invariants (cluster)", lambda: scenarios.invariant_assert(probe, cfg, report, prom_ok))
            guarded(f"drain convergence (≤{cfg.drain_convergence_timeout_s}s)",
                    lambda: scenarios.replication_convergence(probe, cfg, report, prom_ok, ledger))
    else:
        print("-- invariants + convergence: skipped (no cluster) --")
        # give the drain a moment before the durability re-verify even without cluster probes
        time.sleep(10)

    if args.fill_gb > 0:
        if not cluster:
            print("-- ssd fill: SKIPPED, needs cluster access to read the disk and the evictor's metrics --")
        else:
            with probe.prometheus():
                guarded(f"ssd fill pressure ({args.fill_gb} GB, operator-specified)",
                        lambda: scenarios.ssd_fill_pressure(
                            client, cfg, ledger, report, buckets, probe,
                            fill_bytes=int(args.fill_gb * 1e9),
                            obj_bytes=args.fill_object_mb * 1024 * 1024,
                            workers=args.fill_workers))

    # Pass the probe only when the cluster is reachable: the re-verify uses it to evict the SSD ingest
    # cache so the re-GET reads the drained copy, not the intact ingest copy (see durability_reverify).
    guarded("durability re-verify (the no-data-loss gate)",
            lambda: scenarios.durability_reverify(client, cfg, ledger, report, probe if cluster else None))

    # cleanup
    if not cfg.keep_objects:
        print("-- cleanup --")
        for b in buckets.values():
            try:
                s3util.delete_bucket_recursive(client, b)
            except Exception as e:  # cleanup is best-effort; never fail the verdict on it
                print(f"    (cleanup skipped for {b}: {e})")
    else:
        print(f"-- keeping buckets: {list(buckets.values())} --")

    out_dir = pathlib.Path(__file__).resolve().parent / "results"
    out_dir.mkdir(exist_ok=True)
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    report.dump(out_dir / f"run-{stamp}.md", out_dir / f"run-{stamp}.json")
    ledger.dump(out_dir / f"ledger-{stamp}.json")

    print("\n" + report.to_markdown())
    print(f"results: stress-test/results/run-{stamp}.md")
    return 0 if report.go else 1


if __name__ == "__main__":
    raise SystemExit(main())
