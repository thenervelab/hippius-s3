# stress-test — findings for review (from the first live harness runs vs s3-staging.hippius.com)

> Issues the production-readiness harness surfaced running against `s3-staging.hippius.com` (2026-07-02,
> branch `feat/stress-test-harness`). Each has: what, the evidence, why it matters, and a recommended fix.
> The **durability gate PASSED** (every acked object re-GET byte-identical) — none of these is a data-loss;
> they are correctness / throughput / availability findings. Ordered by severity.

## Positive baseline (what held)
- **No data loss** — durability re-verify: **byte-identical** across the whole corpus (mixed sizes incl. multipart).
- **No split-brain** — `sum(drain_leader) = 1` throughout.
- **Terminal-state monotonicity** held; no `replicated/failed` regression on our tracked objects.
- **No 503 backpressure or non-503 5xx** during the concurrency ramp (0 errors, 90 objects).

---

## F1 — [HIGH · data integrity] CompleteMultipartUpload accepts a WRONG part ETag
- **Evidence:** the harness completed an MPU sending part 1 with ETag `"deadbeef…deadbeef"` (garbage). It was
  **ACCEPTED**, not rejected. (`func-mpu-wrong-etag` → FAIL.)
- **Confirms the readiness plan's blind-spot A7** (`hippius_s3/api/s3/multipart.py:1046-1053`,
  `object_writer.py:860-875`): CompleteMPU checks only part-*number* existence, ignores the client's ETags and
  part list, and composes size/MD5 from all uploaded parts for the version.
- **Why it matters:** a client that sends a wrong part list / ETags gets a **silently different object** than it
  intended, with no error — a data-integrity divergence. S3 requires each supplied part ETag to match.
- **Fix:** on complete, validate each `{PartNumber, ETag}` against the stored part ETag and the client's exact
  list; reject on mismatch (`InvalidPart`/`InvalidPartOrder`). Add an e2e test (the harness case is ready).

## F2 — [HIGH · throughput/lag] Drain replication does NOT keep up with even modest ingest
- **Evidence:** during the load run, `drain_parts_replicated_total` advanced only **+36 to +63 parts** over a
  200–240 s window while the harness added ~110–227 parts; afterwards **1 to 76 parts remained `pending`**.
  Aggregate ingest was only **~2 MB/s** and the drain still fell behind. (`drain-convergence` → FAIL.)
- **Context:** ingest SSD backlog is **731.7 GB** (the WI-20/R1 abandoned-upload leak) — it competes with fresh
  uploads for the drain's bandwidth. **Durability still held** (pending objects are served from SSD/pool via
  `DualFileSystemPartsStore`), so this is **backend-replication lag, not data loss** — but it means fresh objects
  sit un-replicated-to-backend for a long time, and the backlog grows if ingest > drain.
- **Why it matters:** this is the concrete "drain lag under load" evidence WI-19 exists to find. At ~2 MB/s per
  node and a 731 GB backlog, the durable-replication window is minutes-to-hours; a sustained ingest burst would
  grow the backlog until the SSD fills (→ 503 storm, per R1).
- **Fix / next:** (a) prioritize **WI-20 orphan-GC** to reclaim the 731 GB and free drain bandwidth; (b) measure
  the real per-node drain MB/s (the S1 SLO is unmeasured/aspirational) and ratify S2/S3 lag; (c) consider drain
  concurrency (`CEPHOR_DRAIN_CONCURRENCY`) / batch `mark_replicated` (PR-8) to lift throughput.

## F3 — [MED · availability] CreateBucket intermittently fails on a billing-balance fetch
- **Evidence:** `CreateBucket` returned `UploadNotPermitted: Failed to fetch billing balance` intermittently
  (crashed a whole run before the harness added a retry).
- **Why it matters:** bucket creation has a hard dependency on a billing-balance fetch with no retry/fallback; a
  transient billing-service blip makes CreateBucket fail outright. Availability finding.
- **Fix:** retry / circuit-break the billing-balance fetch on the create path, or fail-open with a bounded grace.

## F4 — [MED · read path] Reading a just-uploaded (backend-pending) object is slow / can break the stream
- **Evidence:** a range GET immediately after PUT returned `IncompleteRead(0 bytes read, 1 MiB expected)` (the
  connection broke mid-stream); a retry resolved it. Re-reading many still-pending objects was slow enough that a
  serial re-verify blew a 540 s budget (fixed in the harness by parallelizing + retrying).
- **Why it matters:** this is the cross-node cold-read / SSD→pool window (plan A1/A2). A bare range read during the
  replication window can break the connection rather than streaming from the SSD/pool fallback.
- **Fix:** confirm `DualFileSystemPartsStore` pool-fallback timing on the read path; a range read during the
  replication window must stream from SSD/pool without breaking the connection (and cold-fallback should enqueue a
  download rather than hang).

## F5 — [INFO] Confirmed known state
- Ingest SSD backlog **731.7 GB** (WI-20/R1) — still not reclaimed; directly implicated in F2.
- `drain_ssd_pressure` still absent (only 5 drain metrics) — the fill is invisible except via this harness's `df`/
  backlog probe.

---

## Harness status + next increments
- **Built & runnable** (`python stress-test/run.py`, branch `feat/stress-test-harness`): durability oracle (md5
  ledger), T1 functional/adversarial, concurrency ramp, cluster invariant probes (Postgres + Prometheus),
  drain-convergence, PASS/FAIL report. Cluster probes auto-enable when kubectl reaches staging.
- **Hardened this session:** CreateBucket billing-retry, per-assertion T1 isolation, GET retry (rides the cold-read
  window), parallel re-verify.
- **Next per `stress-test/plan.md`:** allocator fence-race rigs (Rig A compose overlay + Rig B `alloc_stress.rs`
  with the R3 failpoint), the toxiproxy/redis-PG fault substrate + full chaos matrix (F1–F8), GB-scale + 6 h soak,
  and turning `inv-guard` from sampled into a continuous **event-driven** asserter (WAL/log-tailed).
