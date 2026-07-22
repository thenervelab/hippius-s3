# PR #199 Review (unified) — Drain-direct upload enqueue: delete the upload-promoter

- **PR:** https://github.com/thenervelab/hippius-s3/pull/199
- **Author:** Radu Mutilică (`radu-mutilica`)
- **Branch:** `pr/drain-direct-enqueue` → `staging` · +702 / −832, 33 files
- **Reviewed:** 2026-06-24
- **Reviewers:** George + Claude (findings merged; every claim re-verified against the PR-branch source)
- **Context:** s3-2.1 drain-gated upload (reworks PR-7 "Mode B" → leaner "drain-direct" PR-11)

## Verdict

A clean, well-tested refactor that does what it says — the Rust drain becomes the sole
producer of backend upload requests, deleting the upload-promoter worker, the
`pg_notify`/sweep machinery, and the PR-7a defer-gate.

**One blocker (P1) and one rollout-safety issue (P2) should be resolved before this merges**,
because it is a hard cutover with no feature flag and no Mode-A fallback. The P1 mechanism
turns "object not finalized yet" into a recurring hard Ceph-write failure on the very path
that gates Ceph health — which can wedge a whole node's drain during the exact large-MPU
scenario this PR exists to fix.

---

## Verified correct

These were traced end-to-end and pass:

- **Wire contract matches field-for-field.** Rust `UploadChainRequest` →
  Python `hippius_s3.queue.UploadChainRequest`: all required fields emitted; `ray_id` /
  `last_error` (omitted by Rust) carry defaults; `extra="ignore"` covers the rest;
  `Chunk{id}` matches; `bypass_billing` default agrees.
- **Queue mechanics match.** Rust LPUSHes `{backend}_upload_requests`; Python
  `dequeue_upload_request` BRPOPs the same key → correct FIFO producer→consumer.
- **Backend routing is equivalent.** Python's `compute_effective_backends` for uploads is
  `requested ∩ allowed`; the drain uses the same `HIPPIUS_UPLOAD_BACKENDS`, so the
  intersection is identity — nothing lost by bypassing it.
- **At-least-once seam is real and tested** for the crash-between-enqueue-and-commit case
  (`an_enqueue_failure_never_commits_and_preserves_the_ssd_copy`). `mark_replicated` keeps
  `claim_seq` fencing after dropping the `pg_notify` CTE.
- **No dangling references / orphaned imports** in the resulting tree.
- **SQL bind types are correct.** `object_versions.object_version` is `bigint`
  (migration `20251017000000`), so binding `i64` in `load_upload_context` is right — *not* an
  int4/i64 mismatch (checked precisely because the query is untested — see P2).
- **Concurrent / duplicate enqueue is safe.** Distinct parts drain concurrently (each
  enqueues its own part); `claim_seq` fencing prevents a part being claimed twice; the only
  dup source is crash-redrain, absorbed by the idempotent uploader. Per-part enqueue
  semantics (`chunks=[Chunk{id: part_number}]`) are unchanged from the deleted promoter.
- **Completed-MPU happy path self-cleans:** after `complete` writes the address, parts
  enqueue + `mark_replicated`, SSD frees, status → `replicated`. The leak below is specific
  to **abort/abandon**, not normal completion.

---

## Findings

### 🔴 P1 (blocker) — Enqueue failures are counted as Ceph-write failures; an in-progress large MPU can wedge the whole node's drain

*Source: George. Status: **confirmed**.*

Traced end-to-end:

1. `drain_part` calls `enqueuer.enqueue(part)` **before** `mark_replicated` and maps any
   failure to `PartDrainError::Enqueue` (`partdrain.rs`).
2. `drain_next` feeds the result into the breaker indiscriminately:
   `record_outcome(success = result.is_ok())` → `breaker.record_failure(now)` +
   `snapshot.record_failed(1)` (`worker.rs:144-153`). The comment literally reads *"Any Ok
   is a Ceph-write success for the breaker; an Err is a failure."*
3. For an in-progress MPU, `load_upload_context` returns `None` because
   `object_versions.address` is NULL until `complete_multipart_upload` writes it
   (`multipart.py:1039`) → `EnqueueError::NotReady` (`enqueue.rs:398-400`) → a "drain failure."
4. The reconciler lands every part with a `meta.json` marker on SSD with **no completeness
   gate** (`reconcile.rs:149` + `LocalSsd::scan_parts`), so MPU parts get claimed and
   drained while the MPU is still open (`upload_part` caches under the real
   `object_id`/`object_version`).
5. The breaker trips after `BREAKER_FAILURES = 5` consecutive failures
   (`runtime.rs:31`); `BREAKER_COOLDOWN = 10s`. A `HalfOpen`/`Probing` probe failure
   **reopens immediately** (`enforce.rs:record_failure`); while `Open`, `try_drain` returns
   `Denied(BreakerOpen)` and admits nothing (`enforce.rs:try_drain`). `record_success`
   resets the run only on an actual successful drain.

**Consequence:** a single large MPU (the PR-10 1GB / 128–256-part scenario) draining on an
otherwise-quiet node produces ≥5 consecutive `NotReady` failures → breaker opens → it cycles
Open→HalfOpen-probe(fails)→Open for the MPU's entire duration. Because the drain is now the
**sole** upload producer, the wedged drain stalls uploads for **all** objects on that node.

**Secondary costs of the same root cause** (Claude's original Concern 1):
- `failed` / `error_bps` p99 saturation signal polluted with non-Ceph failures.
- Every `NotReady` part is re-copied to Ceph each re-drive (idempotent but wasted IO).
- SSD copies are retained for the whole MPU (SSD pressure — the failure class PR-7 targeted).

**Cross-dependency** (was Claude's Concern 3): a transient redis-queues outage now also
returns `EnqueueError::Redis` → trips the **Ceph** breaker and halts draining node-wide.
Given the 2026-06-19 redis-queues crash-loop incident, this is a real blast-radius increase.

**Why the old design didn't have this:** the deleted promoter let `mark_replicated` succeed
immediately (Ceph success, SSD freed), and handled address-not-ready outside the drain's
failure accounting (promoter skip + sweep).

**Escalation — aborted/abandoned MPU is a _permanent_ wedge (2nd-pass finding).** P1's
"wedged for the MPU's duration" is the *best* case. Verified:

- `upload_part` writes each part to the FS via `fs_store.set_chunk` + `write_meta`
  (`object_writer.py:623` `mpu_upload_part_stream`), and the api's `HIPPIUS_OBJECT_CACHE_DIR`
  == the drain's `CEPHOR_SSD_ROOT` (`/var/lib/hippius/local_object_cache`, same node). So the
  reconciler picks up MPU parts **while the MPU is still open** — confirming step 4.
- `abort_multipart_upload` (`multipart.py`) deletes only the **Redis** cached chunks and the
  `multipart_uploads` row (cascade `parts`). It does **not** delete the on-disk FS parts or
  the `cephor_replication_status` rows. A repo-wide search found **no** `DELETE FROM
  cephor_replication_status` anywhere except the one-time migration `0006`.

Consequence: an aborted **or abandoned** MPU (client crashes / never calls complete — routine
in real S3 traffic) leaves orphan parts on local SSD and `pending`/`draining` rows in
`cephor_replication_status`. The drain re-claims and re-copies them every claim-lease
interval **forever**, and the enqueue is **permanently** `NotReady` (the address will never
be written; after the cascade `load_upload_context` may return `None` indefinitely). Net:
**a permanent, node-wide Ceph-breaker wedge + SSD leak + endless re-copy from a single
abandoned MPU**, with no cleanup path in the codebase. `mark_failed` is never reached
(`NotReady` isn't a `ChunkMismatch`), so the part never exits the retry cycle. This makes the
P1 fix more urgent and means the fix must also give abandoned-MPU parts a terminal/cleanup
path (e.g. abort removes FS parts + replication-status rows, or a TTL/orphan reaper).

**Recommended fix (minimum):** do not feed enqueue-domain errors (`NotReady`, `Redis`) into
the Ceph circuit breaker or the `failed` metric — scope `record_outcome` / `record_failed`
to the copy/persist/verify outcome only. **Ideally** treat `NotReady` as a benign deferral,
not a drain error (e.g. write `object_versions.address` at `initiate`, or add an
"api-finished" marker, so MPU parts can drain + enqueue incrementally).

### 🟠 P2 (high) — Hard cutover has no backstop for replicated-but-unenqueued parts; deploy ordering matters

*Source: Claude. Status: **confirmed**.*

If the **api rolls out before the drain-agent**, any part the *old* drain marks `replicated`
during the window is never enqueued:

- the new drain's `AlreadyReplicated` fast path returns **before** `enqueue` (`partdrain.rs`), and
- the reconciler leaves `Replicated` parts alone (`replicated_orphan += 1`, no re-drive —
  `reconcile.rs`).

The promoter sweep that used to be the backstop is deleted, so such an object is stuck on
Ceph forever (never on Arion/chain) with **no automated recovery** — needs a manual `status`
reset. The PR's at-least-once guarantee only covers crash-between-enqueue-and-commit (status
stays `draining`); once a row reaches `replicated`, enqueue is assumed done forever.

**Mitigation:** deploy **drain-agent first** (it enqueues; the api still enqueues too →
harmless idempotent dups; then the api stops → single producer, zero gap). Document this in
the PR. Optional belt-and-suspenders: a one-time re-drive for `replicated` parts whose
chunks have no `chunk_backend` row — the deleted `promoter_sweep_unpromoted.sql` finds
exactly these.

### 🟠 P2 (high) — `Store::load_upload_context` is untested

*Source: George. Status: **confirmed** (no test references it; the `mark_replicated` notify
test was removed and nothing replaced it).*

`store.rs:673` adds a 3-way join + correlated `upload_id` subquery, binds version as `i64`,
and returns `None` on NULL address — all on the hot path now, with zero coverage. Add a
`#[sqlx::test]` for: row + address → `Some`; address NULL → `None`; missing version row →
`None`; MPU `upload_id` latest-by-`initiated_at` selection.

### 🟠 P2 (medium) — `upload_id` subquery isn't version-scoped

*Source: George (elevates Claude's "minor"). Status: **confirmed** — `store.rs:1218`
subquery keys on `object_id` only.*

The subquery picks the latest `multipart_uploads.upload_id` for the `object_id` regardless of
`object_version`. A key once uploaded via MPU and later overwritten by a simple PUT (same
`object_id`, new version) would stamp the stale `upload_id` onto the simple-PUT part,
flipping the uploader's request name from `simple::` to `multipart::` (`queue.py:81-85`).
Same behavior as the deleted promoter, so not a regression — **but it is now the only path.**
Verify completed-MPU rows are cleaned up, or scope the subquery by version.

### 🟡 P3 (rollout) — Production manifests not in this PR (staging-only)

*Source: George. Status: **confirmed** — only `k8s/staging/*` changed; no prod drain-agent
manifest exists in the tree yet.*

When this reaches prod, in lockstep with the code: the prod drain-agent DaemonSet must gain
`REDIS_QUEUES_URL` + `HIPPIUS_UPLOAD_BACKENDS`, and the prod kustomization must drop
upload-promoter. `REDIS_QUEUES_URL` is `required()`, so a missing var crash-loops the agent
(fail-fast, not silent) — but uploads stop entirely until corrected. Also: the staging
daemonset **hardcodes** `HIPPIUS_UPLOAD_BACKENDS=arion` where the promoter read it from a
secret — fine for staging, confirm for prod.

### 🟡 P3 (test) — No automated Rust→Python wire-contract test

*Source: Claude.*

The cross-language coupling is "verified" only by inspection + a `KEEP IN SYNC` comment —
the thing most likely to break silently later. Add a round-trip test: a golden JSON fixture
the Rust `UploadChainRequest` serializes to, asserted in Rust *and* fed through
`UploadChainRequest.model_validate` in a Python test.

### ⚪ P4 (low) — `bypass_billing` always false from the drain

*Source: George. Status: plausible / low-risk.*

`enqueue.rs:414` hardcodes `bypass_billing: false`. Confirm no upload flow needed it `true`
via the old PUT/MPU enqueue (the old paths didn't appear to set it; Python default is
`False`, so likely fine). The per-`NotReady`-poll drain-error log spam is resolved by the P1 fix.

---

## Summary

| # | Sev | Finding | Source | Verified |
|---|-----|---------|--------|----------|
| 1 | 🔴 P1 | Enqueue (`NotReady`/`Redis`) failures trip the **Ceph breaker** → node-wide drain wedge during in-progress MPU; + metric pollution, re-copy, SSD retention, redis cross-dep | George (+ Claude secondary) | ✅ |
| 1b | 🔴 P1 | **Aborted/abandoned MPU = _permanent_ breaker wedge + SSD leak**: abort leaves FS parts + `cephor_replication_status` rows; no cleanup path; enqueue `NotReady` forever | Claude (2nd pass) | ✅ |
| 2 | 🟠 P2 | Hard cutover: no backstop for replicated-but-unenqueued parts; deploy drain-agent before api | Claude | ✅ |
| 3 | 🟠 P2 | `load_upload_context` untested | George | ✅ |
| 4 | 🟠 P2 | `upload_id` subquery not version-scoped (now the only path) | George | ✅ |
| 5 | 🟡 P3 | Prod manifests + hardcoded backend (lockstep cutover) | George | ✅ |
| 6 | 🟡 P3 | No Rust→Python wire-contract test | Claude | — |
| 7 | ⚪ P4 | `bypass_billing` hardcoded false | George | plausible |

**Bottom line:** P1 needs a real fix on two fronts — (a) decouple enqueue-domain errors
(`NotReady`/`Redis`) from the Ceph breaker + `failed` metric, ideally treating `NotReady` as
a benign deferral; and (b) give abandoned-MPU parts a terminal/cleanup path (abort must
remove FS parts + `cephor_replication_status` rows, or add an orphan reaper) so a single
abandoned MPU can't permanently wedge a node's drain. P2 (cutover backstop + ordering) and
the P3 prod-manifest lockstep need a documented rollout plan since there's no flag. The rest
are test/coverage hardening.
