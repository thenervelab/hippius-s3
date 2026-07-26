# Drain Head-of-Line Starvation Fix — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.
> Before the first Rust edit, load the `rust-style` skill (mandatory per global standards).
> Call `mcp__hippius-mem__recall` with "drain head-of-line starvation" before starting — the incident
> anatomy is in note `mem_01KYEMV313BBPJ7GDKQ3WAEVP9`.

**Goal:** Make the drain agent immune to the head-of-line starvation class that collapsed node1/node5
drain throughput on 2026-07-26 (incomplete-MPU part walls + oversized-part denial churn), so no single
workload can starve replication of everything behind it.

**Architecture:** Four independent mechanism fixes in the drain crates plus one wake hook in the Python
API: (1) exponential per-row defer backoff (new `defer_attempts` column) so not-ready parts leave the
claim hot path geometrically; (2) debt-carrying token-bucket overdraft so parts larger than the burst
are guaranteed to drain at the budgeted rate instead of waiting for a bucket that is never full;
(3) burst semantics split into part-specific vs node-global outcomes so one unprocessable part no longer
stops the whole claim burst; (4) terminal `failed` state for parts whose SSD source is permanently gone.
The API clears backoffs on CompleteMultipartUpload so finished MPUs drain immediately.

**Tech Stack:** Rust (sqlx/tokio) in `crates/hippius-drain-core` + `crates/hippius-drain-agent`;
one sqlx migration; one small Python change in `hippius_s3`; Prometheus gauge + follow-up alert rule
in `thenervelab/hippius-otel`.

---

## Incident summary (why each fix exists)

Verified on prod 2026-07-26 (full anatomy in hippius-mem `mem_01KYEMV313BBPJ7GDKQ3WAEVP9`):

- Five in-progress ~100 GB MPUs (`beam-dev/destination/backup/100gbdestination1-5`, initiated 03:18,
  `is_completed=false`) registered hundreds of ~41 MiB parts as `pending`. `claim_part` orders strictly
  `ORDER BY landed_at`; each part hit the "upload enqueue not ready" benign deferral with a fixed
  **5-second** backoff (`DEFAULT_DEFER_BACKOFF`, [store.rs:183](../../crates/hippius-drain-core/src/store.rs)),
  so the whole wall re-entered the claimable head every 5s and consumed the 16 claim slots.
- Two 2 GiB single-part uploads hit the oversized-part path in `try_drain`
  ([enforce.rs:368-392](../../crates/hippius-drain-core/src/enforce.rs)): the F1 overdraft admits only
  when the bucket is exactly full, which never happens under load → `Denied(RateLimited)` → released
  with **no backoff** AND the denial returns `Ok(None)` from `drain_next`, which **stops the burst**
  ([worker.rs](../../crates/hippius-drain-agent/src/worker.rs) `drain_until_empty`, `refill = false`).
- ~32 rows from 2026-07-22 whose SSD source dirs no longer exist churn claim→defer forever on every node.
- Net effect: node1 replicated 64 parts/hour vs 13,396/hour landing; parts landed after 03:36 were never
  claimed; backup worker never saw them on the pool; hydrator logged "Missing backup chunk"; uploader
  DLQ'd `missing_cipher_chunk`.

**Rejected alternative:** not registering in-progress-MPU parts in `cephor_replication_status` until
CompleteMultipartUpload. Rejected because the reconciler registers from SSD directory scans and has no
cheap MPU-state source, and because exponential backoff fixes the whole *class* of not-ready parts
(abandoned MPUs, missing addresses, future cases), not just this instance.

## Conventions and guardrails

- Rust: load `rust-style` skill first; `cargo clippy --all-targets --all-features -- -D warnings` and
  `cargo fmt` must stay clean. Tests are in-file `#[test]`/`#[sqlx::test]` modules following the
  existing patterns in each file (store.rs has ~47, enforce.rs ~24).
- Test command (matches CI, [test-and-lint.yml:262](../../.github/workflows/test-and-lint.yml)):
  ```bash
  DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  CEPHOR_TEST_REDIS_URL=redis://localhost:6379/0 \
  cargo test --workspace --all-features --locked -- --include-ignored
  ```
  Local Postgres/Redis: `docker compose up -d postgres redis` from the repo root. For fast iteration,
  scope to one crate: `cargo test -p hippius-drain-core`.
- Python: ruff + mypy strict, line length 120, no defensive try/except except narrowly justified.
- Commits: one logical change each, imperative mood, never push without being asked.
- Rollout: `staging` branch → staging soak → `k8s-production` (repo rule; the migration is additive so
  old agents keep working mid-rollout).

---

### Task 0: Worktree + baseline

**Step 1:** Create an isolated worktree (use superpowers:using-git-worktrees), branch
`fix/drain-head-of-line-starvation` off `main`.

**Step 2:** Run the baseline crate tests and record the result (must be green before any edit):

```bash
docker compose up -d postgres redis
DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
CEPHOR_TEST_REDIS_URL=redis://localhost:6379/0 \
cargo test -p hippius-drain-core -p hippius-drain-agent --all-features -- --include-ignored
```

Expected: all pass (some Redis-dependent tests may be `ignored` if Redis is absent — that's fine, note it).

---

### Task 1: Migration — `defer_attempts` column

**Files:**
- Create: `crates/hippius-drain-core/migrations/0014_replication_defer_attempts.sql`

**Step 1: Write the migration**

```sql
-- Exponential defer backoff needs a per-row attempt counter: a part that keeps
-- deferring (in-progress MPU, missing address, vanished source) must back off
-- geometrically instead of re-entering the claim head every fixed interval
-- (the 2026-07-26 head-of-line starvation incident).
ALTER TABLE cephor_replication_status
    ADD COLUMN defer_attempts INTEGER NOT NULL DEFAULT 0;
```

**Step 2: Verify sqlx picks it up**

Run: `cargo test -p hippius-drain-core` (the `#[sqlx::test]` harness applies migrations).
Expected: PASS (no behavior change yet).

**Step 3: Commit** — `feat(drain): add defer_attempts column for exponential backoff`

---

### Task 2: Exponential defer backoff in `defer_part`

**Files:**
- Modify: `crates/hippius-drain-core/src/store.rs` (`defer_part`, `DEFAULT_DEFER_BACKOFF` area,
  `Store` builder)
- Modify: `crates/hippius-drain-agent/src/config.rs` (new `CEPHOR_DEFER_BACKOFF_CAP_SECS`)
- Modify: `crates/hippius-drain-agent/src/main.rs` (wire the cap via a new `with_defer_backoff_cap`)

**Step 1: Write the failing tests** (in `store.rs` tests module, following the existing
`#[sqlx::test]` style there):

```rust
#[sqlx::test(migrator = "MIGRATOR")]
async fn defer_part_backs_off_exponentially(pool: PgPool) {
    // base 5s: attempt 0 defers ~5s, attempt 1 ~10s, attempt 2 ~20s.
    let store = test_store(pool, "node-a").with_defer_backoff(Duration::from_secs(5));
    let part = landed_part(&store).await; // helper: record_landed_part + claim_part
    for expected_min in [5i64, 10, 20] {
        store.defer_part(part.part()).await.unwrap();
        let (deferred_until, attempts) = defer_state(&store, part.part()).await;
        let delta = (deferred_until - now(&store).await).num_seconds();
        assert!(delta >= expected_min - 1 && delta <= expected_min + 1, "attempt backoff {delta} != ~{expected_min}");
        reclaim_after_clearing_backoff(&store, part.part()).await; // set deferred_until = now(), claim again
    }
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn defer_backoff_is_capped(pool: PgPool) {
    // With attempts pre-set to 30, the deferral must not exceed the cap.
    let store = test_store(pool, "node-a")
        .with_defer_backoff(Duration::from_secs(5))
        .with_defer_backoff_cap(Duration::from_secs(600));
    // ... seed defer_attempts = 30 directly, defer, assert delta <= 600 + 1
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn release_part_preserves_defer_attempts(pool: PgPool) {
    // A transient Ceph-failure release must not reset the not-ready escalation.
}
```

(Write the small helpers — `defer_state`, `reclaim_after_clearing_backoff` — inside the test module;
mirror the existing test helpers in that file rather than inventing new patterns.)

**Step 2: Run tests to verify they fail**

Run: `cargo test -p hippius-drain-core defer_part_backs_off` — expected FAIL
(`with_defer_backoff_cap` not defined / backoff constant).

**Step 3: Implement**

In `store.rs`:
- Add `defer_backoff_cap: Duration` to `Store` (default `DEFAULT_DEFER_BACKOFF_CAP: Duration =
  Duration::from_secs(600)`) + `with_defer_backoff_cap` builder next to `with_defer_backoff`
  (store.rs:241-245).
- Change the `defer_part` UPDATE to compute the backoff in SQL and bump the counter:

```sql
UPDATE cephor_replication_status
SET status = 'pending', updated_at = now(), claimed_at = NULL,
    defer_attempts = defer_attempts + 1,
    deferred_until = now() + LEAST($4 * power(2, LEAST(defer_attempts, 16)), $5) * interval '1 second'
WHERE object_id = $1 AND version = $2 AND part_number = $3 AND status = 'draining'
```

with `$4 = defer_backoff.as_secs_f64()`, `$5 = defer_backoff_cap.as_secs_f64()`. The `LEAST(…, 16)`
exponent clamp prevents `power` overflow; the outer `LEAST` enforces the cap.

In `config.rs` / `main.rs`: read `CEPHOR_DEFER_BACKOFF_CAP_SECS` (default 600) with the existing
`duration_secs` helper (config.rs:302 pattern) and wire `with_defer_backoff_cap`.

Doc-comment the *why* on `defer_part`: fixed backoff let an MPU part wall re-enter the claim head
every interval and starve younger parts (2026-07-26).

**Step 4: Run tests** — `cargo test -p hippius-drain-core` — expected PASS, zero clippy warnings.

**Step 5: Commit** — `fix(drain): exponential defer backoff — not-ready parts leave the claim head`

---

### Task 3: Reset backoff on CompleteMultipartUpload (Python wake)

Without this, a legitimately completed 100 GB MPU could wait up to the backoff cap (10 min) before its
parts start draining — unacceptable for the cross-node read path.

**Files:**
- Modify: `hippius_s3/api/s3/multipart.py` (the CompleteMultipartUpload handler — **verify the exact
  function first**: `rg -n "complete" hippius_s3/api/s3/multipart.py`)
- Create: `hippius_s3/sql/queries/` entry if the repo pattern demands file-based SQL (check
  neighboring queries; otherwise inline via the repository layer used by the handler)
- Test: `tests/unit/test_mpu_complete_drain_wake.py`

**Step 1: Write the failing test** — behavior: after the complete handler succeeds, an UPDATE ran:

```python
async def test_complete_mpu_clears_drain_backoff(mock_db):
    # Arrange a completed MPU flow with mocked db; assert the executed SQL set
    # includes the cephor_replication_status wake for (object_id, version).
```

Mock only the DB boundary (repo testing rule). Assert on the query + params, not internals.

**Step 2: Run** — `pytest tests/unit/test_mpu_complete_drain_wake.py -xvs` — expected FAIL.

**Step 3: Implement** — after the MPU-complete transaction commits, run:

```sql
UPDATE cephor_replication_status
SET deferred_until = NULL, defer_attempts = 0, updated_at = now()
WHERE object_id = $1 AND version = $2 AND status = 'pending'
```

Best-effort: a failure here must not fail an already-committed complete (narrow, justified exception
to the no-try/except rule — log at WARNING). Comment the why: parts of an in-progress MPU sit in
exponential defer backoff; completion is the wake signal.

**Step 4: Run** — test passes; `ruff check . && mypy hippius_s3` clean.

**Step 5: Commit** — `fix(mpu): wake drain backoff on CompleteMultipartUpload`

---

### Task 4: Debt-carrying overdraft in `TokenBucket`

**Files:**
- Modify: `crates/hippius-drain-core/src/enforce.rs` (`TokenBucket` at :137, `try_drain` at :368,
  `DenyReason` at :275)

**Step 1: Write the failing tests** (in enforce.rs tests module, same style as
`bucket_credits_fractional_tokens_across_small_refills`):

```rust
#[test]
fn oversized_part_admits_via_debt_and_blocks_next_overdraft() {
    // burst 100, rate 100/s. A 250-byte part admits immediately when debt == 0:
    // tokens drop to 0 and debt = 150. A second oversized part is denied while
    // debt > 0. After 1.5s of refill the debt is paid and tokens grow again.
}

#[test]
fn refund_pays_debt_before_crediting_tokens() {
    // After an overdraft admission charged 250, refund(250) must zero the debt
    // and restore tokens — the state as if the admission never happened.
}

#[test]
fn normal_take_is_denied_while_debt_outstanding() {}
```

**Step 2: Run** — `cargo test -p hippius-drain-core enforce` — expected FAIL.

**Step 3: Implement**

- `TokenBucket`: add `debt: u64`. In `refill()` (enforce.rs:183-190): newly minted tokens pay `debt`
  first, remainder credits `tokens` (still capped at burst). New method:

```rust
/// Admits a part larger than the burst by taking everything available now and
/// carrying the remainder as debt the refill pays off before any new tokens mint.
/// One overdraft at a time: denied while debt is outstanding. This guarantees an
/// oversized part drains at the budgeted rate; the prior full-bucket-only overdraft
/// never fired under continuous load (2026-07-26: two 2 GiB parts churned the claim
/// head for 4h), while long-run throughput stays <= rate because the debt suppresses
/// exactly `bytes` of future admissions.
pub fn try_take_overdraft(&mut self, bytes: u64, now: Instant) -> bool
```

- `refund(bytes)`: pay down `debt` first, then credit tokens (mirror of the charge).
- `try_drain`: replace the full-bucket overdraft branch (:382) with `try_take_overdraft`; when it
  fails because debt is outstanding return `Denied(DenyReason::OverdraftOutstanding)` — **new
  variant** on `DenyReason` (doc: part-specific wait, not node-global exhaustion; the worker defers
  the part instead of stopping the burst).
- `charged` bookkeeping for the concurrency-deny refund path stays `bytes` for the overdraft case.

**Step 4: Run** — enforce tests + full crate: PASS, clippy clean. Check the existing overdraft test
(`a part larger than the burst…` around the F1 fix) — update its expectations to the debt semantics
and rename it to describe the new behavior.

**Step 5: Commit** — `fix(drain): debt-carrying overdraft guarantees oversized parts drain at budget rate`

---

### Task 5: Burst semantics — part-specific outcomes don't stop the burst

**Files:**
- Modify: `crates/hippius-drain-agent/src/worker.rs` (`drain_next`, `drain_until_empty`)

**Step 1: Write the failing tests** (worker.rs test module + `crates/hippius-drain-core/tests/it/`
if an end-to-end drain-cycle harness exists there — check `tests/it/main.rs` for the pattern):

```rust
// 1. A part denied OverdraftOutstanding is deferred (deferred_until set, backoff)
//    and the burst continues draining the parts behind it.
// 2. A part whose SSD source stat fails with NotFound is deferred and the burst
//    continues (was: released with no backoff + burst stop).
// 3. BreakerOpen / RateLimited / AtConcurrencyLimit still stop the burst (idle),
//    and the claimed part is released promptly (unchanged global semantics).
```

**Step 2: Run** — expected FAIL (no way to express "skip and continue" today).

**Step 3: Implement**

- Change `drain_next` to return `Result<ClaimOutcome, DrainCycleError>` with

```rust
/// One claim-slot outcome, distinguishing part-specific skips (keep claiming)
/// from node-global stops (budget spent / breaker open / backlog empty).
pub enum ClaimOutcome {
    Drained(DrainOutcome),
    /// This part cannot proceed right now but others can: it was deferred
    /// (backoff) and the burst must keep refilling.
    Skipped,
    /// Nothing claimable, or a node-global denial: stop refilling this burst.
    Idle,
}
```

- Mapping inside `drain_next`:
  - claim returns `None` → `Idle`.
  - `part_size` stat failure: if the underlying io error is `NotFound` → `store.defer_part` +
    `Skipped` (Task 6 adds the terminal escalation); any other io error → release + `Idle`
    (genuine transient node I/O trouble — backing the whole node off briefly is right).
  - `Denied(OverdraftOutstanding)` → `store.defer_part` + `Skipped` + `snapshot.record_throttled(1)`.
  - `Denied(BreakerOpen | RateLimited | AtConcurrencyLimit)` → release + `Idle` (unchanged).
  - benign-deferral drain errors keep their current Err path (already continues).
- `drain_until_empty`: `Drained` → count + refill; `Skipped` → refill (do NOT count as drained);
  `Idle` → `refill = false`. Preserve the existing in-flight wind-down and cancellation semantics
  exactly (the doc comment block on that function is the contract — update it).

**Step 4: Run** — worker + core tests PASS; clippy clean.

**Step 5: Commit** — `fix(drain): part-specific denials defer and skip instead of stopping the burst`

---

### Task 6: Terminal state for permanently missing SSD sources

**Files:**
- Modify: `crates/hippius-drain-core/src/store.rs` (expose `defer_attempts` on `ClaimedPart`;
  new `mark_failed_missing_source`)
- Modify: `crates/hippius-drain-agent/src/worker.rs` (escalation in the stat-`NotFound` path)

**Step 1: Write the failing tests**

```rust
// store.rs: mark_failed_missing_source moves a draining row to 'failed' and is a
// no-op if the row already advanced (same guard pattern as release_part).
// worker.rs: with defer_attempts >= MISSING_SOURCE_FAIL_ATTEMPTS (const 20) and a
// NotFound stat, the part transitions to failed (WARN logged) and the burst continues;
// below the threshold it defers as in Task 5.
```

**Step 2: Run** — expected FAIL.

**Step 3: Implement**

- `claim_part` RETURNING gains `defer_attempts`; thread it through `ClaimedPart`.
- `mark_failed_missing_source(part)`: `UPDATE … SET status='failed', updated_at=now() WHERE … AND
  status='draining'`. Doc: the reconciler only re-registers parts that exist on SSD, so a row whose
  source is gone can never drain — after N observed-missing claims it must leave the claim set
  instead of deferring forever (the 2026-07-22 legacy rows). Guard on `NotFound` only — any other
  stat error must not burn toward the threshold.
- Worker: `const MISSING_SOURCE_FAIL_ATTEMPTS: u32 = 20;` — at ~20 exponential defers the row is
  hours old; a genuinely slow mount does not hit this.
- WARN log includes object_id/version/part so operators can audit what was written off.

**Step 4: Run** — PASS. Note: the ~32 prod legacy rows will self-heal through this path after deploy
(no manual cleanup script — verify in staging that a seeded missing-source row converges to `failed`).

**Step 5: Commit** — `fix(drain): fail parts whose SSD source is permanently gone instead of deferring forever`

---

### Task 7: Starvation observability — oldest-pending-age gauge

The incident's earliest unambiguous signal was "oldest pending row age exploding on one node" — make
it a first-class metric so the alert (follow-up) can page before reads fail.

**Files:**
- Modify: `crates/hippius-drain-core/src/store.rs` (query), `crates/hippius-drain-core/src/snapshot.rs`
  (`record_…`), `crates/hippius-drain-agent/src/runtime.rs` (the existing undrained-count tick at
  :220-236 — same cadence), `crates/hippius-drain-agent/src/metrics.rs` (export)

**Step 1: Failing test** — store query returns the age in seconds of the oldest `pending` row for the
node (0 when none), exercised via `#[sqlx::test]`; snapshot gauge follows the `record_undrained_count`
pattern.

**Step 2: Run** — FAIL. **Step 3:** Implement (`drain_pending_oldest_age_seconds` gauge, labelled by
node like the existing backlog gauges). **Step 4:** Run — PASS, clippy clean.

**Step 5: Commit** — `feat(drain): export oldest-pending-age gauge (starvation signal)`

**Follow-up (cross-repo, separate PR, do NOT do here):** alert rule in `thenervelab/hippius-otel`
(single source of truth for alerting — plan constraint from
[2026-07-24-outage-prevention-implementation-plan.md](2026-07-24-outage-prevention-implementation-plan.md)):
`max(drain_pending_oldest_age_seconds) > 1800`, `for: 15m`. This rule would have fired ~03:40 on
2026-07-26.

---

### Task 8: Full verification + rollout prep

**Step 1:** Full workspace gate:

```bash
cargo fmt --check && cargo clippy --all-targets --all-features -- -D warnings
DATABASE_URL=… CEPHOR_TEST_REDIS_URL=… cargo test --workspace --all-features --locked -- --include-ignored
ruff check . && mypy hippius_s3 && pytest tests/unit -q
```

All green, zero warnings.

**Step 2:** Scenario test (integration, `crates/hippius-drain-core/tests/it/` or the agent runtime
test harness — reuse whichever already spins a fake backlog): seed one node with (a) 50 not-ready
MPU parts landed first, (b) one oversized part, (c) 20 ready parts landed last. Assert all 20 ready
parts replicate within a bounded number of poll cycles. **This is the regression test for the whole
incident** — it must fail on `main` and pass on the branch (verify both).

**Step 3:** Use superpowers:requesting-code-review, then merge path per repo rules: PR → `staging` →
soak (watch `drain_pending_oldest_age_seconds`, per-node replicated/hour via the diagnosis query in
the hippius-mem note) → `k8s-production`. Do not push or open the PR without the user's go-ahead.

**Rollout notes:**

- **Deploy the allocator before the agent DaemonSet.** The allocator is the sole `migrate()` caller
  (`crates/hippius-drain-allocator/src/main.rs:54`), so migrations 0014/0015 apply only when it rolls.
  An agent from this branch against the pre-0014 schema errors on every `defer_part` /
  `defer_part_missing_source` (missing `defer_attempts` / `missing_source_attempts` columns); it
  degrades safely via claim-lease recovery rather than losing data, but is noisy and cannot back
  off a not-ready part until the columns exist.
- **The `drain_pending_oldest_age_seconds` alert must pair its threshold with an absence/staleness
  check** (e.g. `absent(...)` or `... unless timestamp(...) < time() - <staleness>`): an agent
  restarting during a DB blip briefly serves 0 for the gauge, so a threshold-only rule reads a
  live starvation as recovered exactly when the node is least healthy.

**Step 4:** After prod soak, `mcp__hippius-mem__remember` the deploy outcome and link it to
`mem_01KYEMV313BBPJ7GDKQ3WAEVP9`; mark the fix-directions paragraph there as implemented via
`mcp__hippius-mem__edit`.
