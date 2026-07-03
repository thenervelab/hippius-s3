"""The production-readiness scenario suite.

Each scenario asserts one readiness criterion and appends a Result. S3-facing scenarios run
against the endpoint; invariant/convergence scenarios additionally consult the staging cluster
(Postgres cephor_replication_status + Prometheus drain_* metrics) when reachable.

Mapping to the WI-19 gate (see ../s3-2.1-todo.md):
  durability_corpus / durability_reverify -> S8 / G3 (no-data-loss)
  functional_adversarial                  -> T1 (correctness, non-happy-path)
  concurrency_ramp                        -> T5-lite (throughput / 503 backpressure / S5-S6)
  invariant_assert                        -> G1 single-leader, G6 terminal monotonicity, S4 backlog
  replication_convergence                 -> the drain actually replicates (lag via SQL)
"""

from __future__ import annotations

import concurrent.futures
import contextlib
import re
import time

import botocore.exceptions

from . import s3util
from .config import Config
from .ledger import Ledger
from .probes import ClusterProbe
from .report import Report
from .report import Result


def _bucket(cfg: Config, suffix: str) -> str:
    return f"{cfg.bucket_prefix}-{suffix}-{int(time.time())}"


def _check(report: Report, name: str, invariant: str, criteria: str, fn) -> None:
    """Run one assertion; a crash becomes a recorded FAIL so sibling checks still run."""
    try:
        passed, detail = fn()
    except Exception as e:  # noqa: BLE001 — per-assertion isolation is the point
        passed, detail = False, f"{type(e).__name__}: {e}"
    report.add(Result(name, invariant, passed=passed, detail=detail, criteria=criteria))


def _upload_and_record(client, cfg: Config, ledger: Ledger, bucket: str, key: str, body: bytes,
                       multipart: bool) -> None:
    if multipart:
        s3util.upload_forced_multipart(client, bucket, key, body)
    else:
        s3util.put(client, bucket, key, body)
    ledger.record(bucket, key, s3util.md5_hex(body), len(body), multipart)


# ---------------------------------------------------------------- 1. DURABILITY CORPUS (S8/G3)
def durability_corpus(client, cfg: Config, ledger: Ledger, report: Report, buckets: dict) -> None:
    b = _bucket(cfg, "durab")
    s3util.ensure_bucket(client, b)
    buckets["durability"] = b
    corpus = (
        [("small", 8 * 1024, False)] * 8
        + [("medium", 1024 * 1024, False)] * 4
        + [("large-mpu", 20 * 1024 * 1024, True)] * 2
        + [("zerobyte", 0, False)]
    )
    t0 = time.time()
    for i, (kind, size, mpu) in enumerate(corpus):
        body = s3util.random_body(size, seed=f"{b}-{i}".encode())
        _upload_and_record(client, cfg, ledger, b, f"{kind}/obj-{i}", body, mpu)
    n = len(corpus)
    mpu_n = sum(1 for _, _, m in corpus if m)
    report.add(Result(
        name="durability-corpus-upload",
        invariant="S8/G3 setup",
        passed=True,
        observed=True,  # setup only — the real no-data-loss gate is durability-reverify
        detail=f"uploaded {n} objects ({mpu_n} multipart) in {time.time()-t0:.1f}s to {b}",
        criteria="corpus PUTs succeed (verified for real in durability-reverify)",
    ))


# ---------------------------------------------------------------- 2. FUNCTIONAL / ADVERSARIAL (T1)
def functional_adversarial(client, cfg: Config, ledger: Ledger, report: Report, buckets: dict) -> None:
    b = _bucket(cfg, "func")
    pub = _bucket(cfg, "pub")
    s3util.ensure_bucket(client, b)
    s3util.ensure_bucket(client, pub, public=True)
    buckets["functional"] = b
    buckets["public"] = pub

    def range_get():
        body = s3util.random_body(4 * 1024 * 1024, seed=b"range")
        s3util.put(client, b, "range-target", body)
        got = s3util.get_bytes(client, b, "range-target", range_header="bytes=0-1048575")
        return got == body[:1024 * 1024], f"got {len(got)} B, match={s3util.md5_hex(got)==s3util.md5_hex(body[:1024*1024])}"
    _check(report, "func-range-get", "T1 range", "Range bytes=0-1048575 returns exactly the first 1 MiB", range_get)

    def overwrite():
        s3util.put(client, b, "ow", s3util.random_body(1000, b"v1"))
        v2 = s3util.random_body(1000, b"v2")
        s3util.put(client, b, "ow", v2)
        latest = s3util.get_bytes(client, b, "ow")
        return latest == v2, f"GET-after-overwrite md5={s3util.md5_hex(latest)[:8]} want={s3util.md5_hex(v2)[:8]}"
    _check(report, "func-overwrite-latest", "T1 overwrite", "GET after overwrite returns the latest bytes", overwrite)

    def mpu_bad_etag():
        up = client.create_multipart_upload(Bucket=b, Key="mpu-badetag")["UploadId"]
        client.upload_part(Bucket=b, Key="mpu-badetag", PartNumber=1, UploadId=up,
                           Body=s3util.random_body(6 * 1024 * 1024, b"part1"))
        rejected = False
        try:
            client.complete_multipart_upload(
                Bucket=b, Key="mpu-badetag", UploadId=up,
                MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": '"deadbeefdeadbeefdeadbeefdeadbeef"'}]})
        except botocore.exceptions.ClientError:
            rejected = True
        finally:
            # On the CORRECT (rejected) path the upload is still open — abort it so the harness
            # doesn't strand an in-progress MPU every run (the A21 orphan cephor-pending-row leak).
            # On the ACCEPTED (bug) path complete already consumed the upload; NoSuchUpload is expected.
            if rejected:
                with contextlib.suppress(botocore.exceptions.ClientError):
                    client.abort_multipart_upload(Bucket=b, Key="mpu-badetag", UploadId=up)
        return rejected, f"wrong-ETag complete was {'rejected' if rejected else 'ACCEPTED (bug)'}"
    _check(report, "func-mpu-wrong-etag", "T1 MPU integrity",
           "CompleteMultipartUpload with a mismatched part ETag is rejected", mpu_bad_etag)

    def mpu_abort():
        up = client.create_multipart_upload(Bucket=b, Key="mpu-abort")["UploadId"]
        client.upload_part(Bucket=b, Key="mpu-abort", PartNumber=1, UploadId=up,
                           Body=s3util.random_body(6 * 1024 * 1024, b"a"))
        client.abort_multipart_upload(Bucket=b, Key="mpu-abort", UploadId=up)
        absent = False
        try:
            client.head_object(Bucket=b, Key="mpu-abort")
        except botocore.exceptions.ClientError:
            absent = True
        return absent, f"aborted MPU object is {'absent' if absent else 'PRESENT (bug)'}"
    _check(report, "func-mpu-abort", "T1 MPU abort", "An aborted MPU leaves no readable object", mpu_abort)

    def acl_anon():
        s3util.put(client, pub, "pubobj", s3util.random_body(1024, b"pub"))
        client.put_object_acl(Bucket=pub, Key="pubobj", ACL="public-read")
        pub_s = s3util.anon_get_status(cfg, pub, "pubobj")
        priv_s = s3util.anon_get_status(cfg, b, "range-target")
        return (pub_s == 200 and priv_s == 403), f"anon public={pub_s} (want 200), private={priv_s} (want 403)"
    _check(report, "func-acl-anon", "T1 ACL", "Anon GET: public-read → 200, private → 403", acl_anon)

    def listv2():
        for k in ("a/1", "a/2", "c/1"):
            s3util.put(client, b, k, b"x")
        resp = client.list_objects_v2(Bucket=b, Delimiter="/")
        prefixes = sorted(p["Prefix"] for p in resp.get("CommonPrefixes", []))
        return ("a/" in prefixes and "c/" in prefixes), f"CommonPrefixes={prefixes}"
    _check(report, "func-listv2-delimiter", "T1 ListObjectsV2", "delimiter '/' groups a/ and c/", listv2)

    def zero_byte():
        s3util.put(client, b, "empty", b"")
        empty = s3util.get_bytes(client, b, "empty")
        return empty == b"", f"zero-byte GET len={len(empty)}"
    _check(report, "func-zero-byte", "T1 zero-byte", "A zero-byte object round-trips as empty", zero_byte)


# ---------------------------------------------------------------- 3. CONCURRENCY RAMP (T5-lite / S5-S6)
def _put_with_503_retry(client, bucket: str, key: str, body: bytes) -> tuple[str, float]:
    """Returns (outcome, elapsed). outcome ∈ {ok, slowdown-then-ok, slowdown-fail, error:<code>}."""
    t0 = time.time()
    for attempt in range(2):
        try:
            client.put_object(Bucket=bucket, Key=key, Body=body)
            return ("ok" if attempt == 0 else "slowdown-then-ok", time.time() - t0)
        except botocore.exceptions.ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
            if status == 503 or code in ("SlowDown", "ServiceUnavailable"):
                time.sleep(1.5)
                continue
            return (f"error:{status or code}", time.time() - t0)
    return ("slowdown-fail", time.time() - t0)


def concurrency_ramp(client, cfg: Config, ledger: Ledger, report: Report, buckets: dict) -> None:
    b = _bucket(cfg, "load")
    s3util.ensure_bucket(client, b)
    buckets["load"] = b
    rungs = [5, 10, 20]
    per_rung = 30
    obj_size = 1024 * 1024  # 1 MiB
    total_ok = total_slow = total_err = 0
    total_bytes = 0
    worst_5xx_rate = 0.0
    for workers in rungs:
        outcomes: list[str] = []
        elapsed: list[float] = []
        t0 = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
            futs = []
            for i in range(per_rung):
                body = s3util.random_body(obj_size, seed=f"{b}-{workers}-{i}".encode())
                key = f"load/w{workers}/obj-{i}"

                def task(k=key, bd=body):
                    outcome, el = _put_with_503_retry(client, b, k, bd)
                    if outcome in ("ok", "slowdown-then-ok"):
                        ledger.record(b, k, s3util.md5_hex(bd), len(bd), False)
                    return outcome, el
                futs.append(ex.submit(task))
            for f in concurrent.futures.as_completed(futs):
                o, el = f.result()
                outcomes.append(o)
                elapsed.append(el)
        wall = time.time() - t0
        ok = sum(1 for o in outcomes if o in ("ok", "slowdown-then-ok"))
        slow = sum(1 for o in outcomes if o.startswith("slowdown"))
        err = sum(1 for o in outcomes if o.startswith("error"))
        total_ok += ok
        total_slow += slow
        total_err += err
        total_bytes += ok * obj_size
        rate_5xx = err / per_rung
        worst_5xx_rate = max(worst_5xx_rate, rate_5xx)
        thr = ok / wall if wall else 0
        print(f"    rung workers={workers}: ok={ok} slowdown={slow} err={err} "
              f"{thr:.1f} obj/s ({ok*obj_size/wall/1e6:.1f} MB/s)")

    # 503 SlowDown is CORRECT backpressure, not a failure. Only non-503 5xx and hangs fail.
    report.add(Result(
        name="concurrency-ramp",
        invariant="S5/S6 (backpressure) + throughput",
        passed=(worst_5xx_rate < 0.01 and total_err == 0),
        detail=f"{total_ok} ok, {total_slow} 503-backpressure(retried), {total_err} non-503-5xx; "
               f"worst non-503 5xx rate={worst_5xx_rate:.1%}; {total_bytes/1e6:.0f} MB",
        criteria="non-503 5xx rate < 1% and no hangs; 503 SlowDown is correct backpressure (informational)",
        metrics={"ok": total_ok, "slowdown": total_slow, "non_503_5xx": total_err},
    ))


# ---------------------------------------------------------------- 4. INVARIANTS (cluster) G1/G6/S4
# cephor_replication_status PK is (object_id, version, part_number); replicated/failed are terminal sinks.
_TERMINAL = frozenset({"replicated", "failed"})
_NON_TERMINAL = frozenset({"pending", "draining"})
_PREFIX_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")


def _safe_prefix(prefix: str) -> str:
    """Constrain the bucket prefix before it goes into a SQL LIKE — it's operator-set, but we build
    raw SQL from it, so reject anything outside the S3 bucket-name charset rather than interpolate it."""
    if not _PREFIX_RE.match(prefix):
        raise ValueError(f"unsafe bucket prefix for SQL LIKE: {prefix!r}")
    return prefix


def _our_repl_counts(probe: ClusterProbe, prefix: str) -> dict[str, int] | None:
    rows = probe.pg(
        "select crs.status, count(*) from cephor_replication_status crs "
        "join objects o on o.object_id::text = crs.object_id::text "
        "join buckets b on b.bucket_id = o.bucket_id "
        f"where b.bucket_name like '{_safe_prefix(prefix)}-%' group by crs.status")
    if rows is None:
        return None
    return {r[0]: int(r[1]) for r in rows}


def _our_repl_rows(probe: ClusterProbe, prefix: str) -> dict[tuple[str, str, str], str] | None:
    """Per-row status of our objects' replication rows, keyed by the PK (object_id, version, part_number).
    The per-row view is what makes a real G6 monotonicity assertion possible — a count can't tell a
    terminal row regressing apart from a new non-terminal row arriving."""
    rows = probe.pg(
        "select crs.object_id, crs.version, crs.part_number, crs.status from cephor_replication_status crs "
        "join objects o on o.object_id::text = crs.object_id::text "
        "join buckets b on b.bucket_id = o.bucket_id "
        f"where b.bucket_name like '{_safe_prefix(prefix)}-%'")
    if rows is None:
        return None
    return {(r[0], r[1], r[2]): r[3] for r in rows}


def invariant_assert(probe: ClusterProbe, cfg: Config, report: Report, prom_ok: bool) -> None:
    # G1 — single leader (Prometheus)
    leaders = probe.prom_scalar('sum(drain_leader{service_namespace="hippius-s3-staging"})') if prom_ok else None
    if leaders is None:
        report.add(Result("inv-G1-single-leader", "G1", passed=True, skipped=True,
                          detail="Prometheus not reachable — skipped", criteria="sum(drain_leader) ≤ 1"))
    else:
        report.add(Result("inv-G1-single-leader", "G1 single-leader", passed=(leaders <= 1.0),
                          detail=f"sum(drain_leader)={leaders:g}",
                          criteria="sum(drain_leader) ≤ 1 (no split-brain)"))

    # G6 — terminal-state monotonicity (a REAL gate, per-row): snapshot our objects' rows keyed by
    # (object_id, version, part_number), sleep, re-snapshot; FAIL if any row that was terminal
    # (replicated/failed) is now non-terminal (pending/draining). A key vanishing between snapshots is
    # GC deleting a terminal row — that is allowed, not a regression. This is still a sampled probe (the
    # plan's end-state is a WAL-tailed event-driven guard, plan §4.2); a flap fully inside the sleep is
    # invisible — but unlike the old count-only version it can now actually catch a regression.
    snap1 = _our_repl_rows(probe, cfg.bucket_prefix)
    if snap1 is None:
        report.add(Result("inv-G6-terminal-monotonic", "G6", passed=True, skipped=True,
                          detail="Postgres not reachable — skipped",
                          criteria="no replicated/failed row regresses to a non-terminal state"))
    else:
        terminal_before = {k: s for k, s in snap1.items() if s in _TERMINAL}
        time.sleep(4)
        snap2 = _our_repl_rows(probe, cfg.bucket_prefix) or {}
        regressed = {k: (terminal_before[k], snap2[k]) for k in terminal_before
                     if snap2.get(k) in _NON_TERMINAL}
        report.add(Result("inv-G6-terminal-monotonic", "G6 terminal monotonicity",
                          passed=(len(regressed) == 0),
                          detail=f"our terminal rows tracked={len(terminal_before)}, regressed={len(regressed)}"
                                 + (f" — {list(regressed.items())[:3]}" if regressed else ""),
                          criteria="no replicated/failed row of ours regresses to pending/draining"))

    # S4 — backlog gauge: reported, not gated. There is no ratified per-node ceiling yet
    # (S4's absolute number is a RATIFY item, todo §SLO table), so this is an OBSERVED measurement.
    backlog = probe.prom_scalar(
        'max(drain_ssd_backlog_bytes{service_namespace="hippius-s3-staging"})/1e9') if prom_ok else None
    if backlog is None:
        report.add(Result("inv-S4-backlog", "S4", passed=True, skipped=True,
                          detail="drain_ssd_backlog_bytes not reachable — skipped",
                          criteria="ingest SSD backlog is a bounded gauge"))
    else:
        report.add(Result("inv-S4-backlog", "S4 backlog gauge",
                          passed=True,
                          observed=True,
                          detail=f"max drain_ssd_backlog_bytes = {backlog:.1f} GB (no ratified ceiling yet)",
                          criteria="backlog exported + bounded (absolute ceiling is a RATIFY item)"))


# ---------------------------------------------------------------- 5. REPLICATION CONVERGENCE (cluster)
def replication_convergence(probe: ClusterProbe, cfg: Config, report: Report, prom_ok: bool) -> None:
    before = probe.prom_scalar(
        'sum(drain_parts_replicated_total{service_namespace="hippius-s3-staging"})') if prom_ok else None
    deadline = time.time() + cfg.drain_convergence_timeout_s
    converged = False
    last: dict[str, int] | None = None
    while time.time() < deadline:
        our = _our_repl_counts(probe, cfg.bucket_prefix)
        if our is None:
            report.add(Result("drain-convergence", "drain replicates", passed=True, skipped=True,
                              detail="Postgres not reachable — skipped",
                              criteria="all our parts reach a terminal state within the drain window"))
            return
        last = our
        non_terminal = our.get("pending", 0) + our.get("draining", 0)
        if non_terminal == 0:
            converged = True
            break
        time.sleep(5)
    elapsed = cfg.drain_convergence_timeout_s - max(0, deadline - time.time())
    after = probe.prom_scalar(
        'sum(drain_parts_replicated_total{service_namespace="hippius-s3-staging"})') if prom_ok else None
    delta = (after - before) if (before is not None and after is not None) else None
    report.add(Result(
        name="drain-convergence",
        invariant="drain actually replicates (lag proxy)",
        passed=converged,
        detail=f"our repl rows={last}; drain_parts_replicated Δ={delta}; "
               f"{'converged' if converged else 'STILL non-terminal'} in ≤{elapsed:.0f}s",
        criteria=f"0 pending/draining rows for our objects within {cfg.drain_convergence_timeout_s}s",
    ))


# ---------------------------------------------------------------- 6. DURABILITY RE-VERIFY (the gate)
def durability_reverify(client, cfg: Config, ledger: Ledger, report: Report) -> None:
    items = ledger.all()
    mismatches: list[str] = []
    missing: list[str] = []

    def verify_one(a):
        try:
            data = s3util.get_bytes(client, a.bucket, a.key, retries=3)
        except Exception as e:  # noqa: BLE001 — a persistent read failure is a data-availability finding
            return ("missing", a.key, type(e).__name__)
        if len(data) != a.size or s3util.md5_hex(data) != a.md5:
            return ("corrupt", a.key, "")
        return ("ok", a.key, "")

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as ex:
        for kind, key, why in ex.map(verify_one, items):
            if kind == "corrupt":
                mismatches.append(key)
            elif kind == "missing":
                missing.append(f"{key}:{why}")
    ok = len(items) - len(mismatches) - len(missing)
    report.add(Result(
        name="durability-reverify",
        invariant="S8/G3 no-data-loss (NON-OVERRIDABLE)",
        passed=(len(mismatches) == 0 and len(missing) == 0),
        detail=f"{ok}/{len(items)} byte-identical; {len(mismatches)} corrupt, {len(missing)} missing"
               + (f" — corrupt:{mismatches[:3]} missing:{missing[:3]}" if (mismatches or missing) else ""),
        criteria="every acked object re-GETs byte-identical (plaintext md5 + size)",
        metrics={"total": len(items), "ok": ok, "corrupt": len(mismatches), "missing": len(missing)},
    ))
