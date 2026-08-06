# Differential audit — inventory-hotness stack (PRs #352/#353/#354)

**Date:** 2026-07-27
**Scope:** `git diff 1698cef..HEAD` on `feat/inventory-hotness` (merged to staging) — 17 files, ~1,190 insertions.
**Features:** queue-depth gauges (#352), shared `fs_cache:pressure` signal (#353), `fs_cache_inventory.last_access_at` read recency replacing atime refresh (#354).
**Method:** differential review (risk-first), independent adversarial failure modeling, mutation-checked test-coverage mapping, unit + e2e execution against the live compose stack.

---

## Headline verdict

**No new permanent-data-loss path.** The replication gate (`is_replicated_on_all_backends`) is re-run on the worker connection immediately before every `delete_part` in the SQL-eviction path ([run_janitor_in_loop.py:1972](../../workers/run_janitor_in_loop.py)), is byte-identical to the walk path's, fails closed on exceptions, and returns False for zero-coverage parts (`total_chunks == 0`) — so the one unrecoverable population (CopyObject destinations with zero `chunk_backend` rows) is excluded twice (gate + prefilter anti-join in [janitor_evictable_candidates.sql](../../hippius_s3/sql/queries/janitor_evictable_candidates.sql)). No modeled sequence (crash timing, DB/Redis outage, clock skew, unapplied migration, rollback, mixed-version fleet) deletes a not-fully-replicated part or drops an in-flight PUT after 200 OK.

What the stack does introduce: **availability regressions around eviction timing** and **silent-degradation modes in the new safeguards**. Findings below.

---

## Findings (ranked)

### F1 (High, availability) — Mid-stream eviction truncates in-flight GETs
`build_stream_context` checks chunk existence **once** ([object_reader.py:236-249](../../hippius_s3/services/object_reader.py)); all-present → `source="cache"`, no coalesce lock, no download enqueued. If the janitor evicts a later part mid-stream, `wait_for_chunk` subscribes to a notification no producer will ever send, polls to `stream_chunk_timeout_seconds` (300s), then raises mid-body — after headers and Content-Length are on the wire. Truncated response; connection pinned 5 min; self-heals on the next GET.
This diff **widens the window**: the old per-read `os.utime` protected a part from all three delete paths synchronously; now only the SQL path sees reads, lagging ≥30s (tracker flush) — and up to a full sample window (hot-window/4) after a dropped flush.
**Fix direction:** design decision — the streamer must re-enqueue a download on a mid-stream miss instead of waiting on pub/sub.

### F2 (High, operational) — Pressure signal has zero monitoring; janitor gained a startup dependency
No alert rule, panel, or staleness gauge references `fs_cache:pressure`, `fs_cache_pressure_mode`, or `queue_depth` in `monitoring/` or `k8s/`. If the janitor dies or every publish fails (caught + warn-logged by design), the key expires after 120s and every api pod **silently** reverts to the node-NVMe-only statvfs check — the exact 2026-07-24 blind spot this stack exists to close. Separately, `run_janitor_loop` now re-raises on cache-Redis client construction failure, so a bad `REDIS_URL` crashloops the only process that frees cache space.
**Fix:** one staleness gauge + one alert rule on signal absence; consider tolerating cache-Redis failure at janitor startup (degrade to unpublished rather than crashloop).

### F3 (Medium, behavior) — Read recency protects only 1 of the janitor's 3 delete paths
- `evict_from_inventory`: atime + `last_access_at` — read-aware.
- `_age_gc_decision` (24h age GC): atime-only, which is now **write** recency — read-blind.
- Stale reap (`mpu_stale_seconds`, 2 days): mtime-only, no hot check — read-blind, and it lost the *implicit* protection `os.utime` used to give (reads no longer push mtime forward).
Actively-read content older than 24h now gets evicted daily and re-hydrated from Arion (read amplification, feeds F1). Parts with no `fs_cache_inventory` row (e.g. s3-backup-hydrator-written parts) have **zero** read protection. The code comments state the asymmetry; nothing pins it, and the CLAUDE.md docs contradict it (F9).
**Fix:** deliberate call — teach the age-GC/stale-reap paths about `last_access_at`, or document read-blindness as intended.

### F4 (Medium, operational) — Unapplied migration silently stalls SQL eviction
The `SELECT last_access_at` ([run_janitor_in_loop.py:1961](../../workers/run_janitor_in_loop.py)) is unwrapped; `UndefinedColumnError` is swallowed per-item by `_run_worker_pool`, so with migration `20260725120000_fs_cache_inventory_last_access.sql` unapplied the phase frees **zero** with up to 50k warnings/cycle — the same silent-stall shape as the cursor bug this stack just fixed. Recovers only at CRITICAL pressure (hot_window==0 skips the query). Migrations run on API startup, not the janitor, so a janitor image rolling ahead of the API lands here.
**Fix (cheap):** select the column in `janitor_evictable_candidates.sql` so a missing column fails loudly once per cycle.

### F5 (Medium-Low) — AccessTracker freshness holes
`_pending` is unbounded (`MAX_TRACKED_KEYS` bounds `_last_noted` only) — a key-diverse read storm produces an uncapped flush against the 15.6M-row table the janitor concurrently scans. A zero-row UPDATE (no inventory row) is silent. A dropped flush leaves the key sampling-suppressed for the full sample window (900s at defaults) despite continued reads; at elevated pressure two lost windows can push a continuously-read part out of its own protection. Replication-gated → thrash, not loss.

### F6 (Low) — Concurrent flush deadlock
`flush_once` builds unnest arrays in unsorted set order; two api pods with overlapping keys can deadlock in Postgres — one aborts, recency silently dropped, precisely for hot shared objects. **Fix:** sort the batch before the UPDATE.

### F7 (Low) — New cross-clock comparison
`last_access.timestamp() > (now - hot_window)` compares Postgres `now()` to janitor `time.time()`. TIMESTAMPTZ + aware datetimes make `.timestamp()` correct; NTP skew is milliseconds; stale `now` errs in the safe (hotter) direction. No action.

### F8 (Info) — Deploy/rollback asymmetry
Mixed-version rollout is safe (old pods bump atime, new pods write `last_access_at`, new janitor checks both — union over-protects). Rolling back the **janitor** while new api code runs makes every part read-cold (nothing bumps atime, nothing reads `last_access_at`). Replication-gated → thrash, not loss.

### F9 (Low, docs) — Committed docs now contradict the code
[hippius_s3/cache/CLAUDE.md](../../hippius_s3/cache/CLAUDE.md) ("Every successful read calls `os.utime`…") and root [CLAUDE.md](../../CLAUDE.md) §5.1 ("Hot retention via os.utime") are now false — and they're the first files an agent reads to reason about eviction safety. Also stale: root CLAUDE.md's `mypy hippius_s3` command (mypy is not configured in this repo).

---

## Test coverage (mutation-checked)

Well-covered (no action): the **never-evict-unreplicated** invariant is pinned at three layers (unit prefilter/gate divergence, real-SQL candidate filter with six negative cases, e2e with real gate + PG); SQL-evict cursor mechanics (14 tests); consumer memo semantics (read-error holds last-good, absence does not).

Structural holes, in priority order — each survives the listed mutation with a green suite:

1. **No read→recency→retention round trip at any level.** Zero references to `last_access_at`/`access_tracker`/`note_read` in `tests/e2e/` or `tests/integration/`. The two unit halves never meet; the one e2e janitor test forces pressure=2 which disables the recency check outright. **Highest-value fix:** an integration test that inserts an inventory row, runs `AccessTracker.flush_once()` against real Postgres, then asserts `evict_from_inventory` keeps the part — it simultaneously pins the migration's presence (F4), the column-type match, and the join key.
2. **`fs_cache_pressure_middleware` has zero tests.** Replacing `published_mode=...` with `None` — silently reverting the incident fix — leaves the suite green.
3. **Publisher's pool arm never exercised.** Every publisher test passes `mgr_metrics_url=""`; replacing `max(local, pool)` with `local` passes. The pool arm *is* the incident fix.
4. **Stale `last_access_at` must still allow eviction.** Only fresh/NULL tested; mutating to `if last_access is not None: return False` (permanent unevictability → disk-fill outage) passes.
5. **Consumer last-good expiry unpinned.** Deleting the TTL guard (dead Redis pins a stale mode → permanently wedged PUT gate) passes.
6. Smaller: `FLUSH_CHUNK_SIZE` chunking never iterates; `compute_mode` exact boundary values (all four `>=`↔`>` flips pass) and the mode-2→ratio-0.5→mode-1 step; all three background `run()` loops (cancellation vs swallow) untested; `_shutdown` re-raises if a background task died with a non-CancelledError before closing clients; queue age gauge not cleared on drain; OTel gauge callbacks never invoked.

---

## E2E against the live stack

Environment note: first run executed against **stale images** (pre-provisioned stack without `--build`; the conftest reuses a running `hippius-e2e` project as-is) and produced 4 false failures — rebuilt with current code and re-run.

**Result: 163 passed, 11 skipped, 1 xfailed, 1 failed — no functional regressions.**

- Stack-relevant tests all pass against freshly built images: `test_janitor_sql_eviction.py` (SQL eviction + GET re-fetch round trip), `test_ColdReadPubSub.py` (multichunk cold read byte-exact, concurrent coalesce), `test_Backend_Resilience.py` (read recovers after cache eviction).
- The single failure, `test_GetObject.py::test_get_object_eventual_consistency`, is **environmental**: it asserts `meta.json` existence at `/var/lib/hippius/...` from the pytest host process. Those are bind-mount paths that resolve inside Docker Desktop's VM on macOS (verified: 205 objects present in-container, path absent on host). Passes on Linux CI. The janitor deleted zero parts during the entire run (verified from logs), ruling out eviction as a cause.
- Two local-run traps encountered and recorded in team memory (mem_01KYHHDFHK87MPQ0YR37E9D09Y): DB assertions need `HIPPIUS_E2E_DB_DSN` pointed at the remapped port when `docker-compose.e2e-local.yml` is in play (otherwise they silently query the host Postgres and cold-read tests fail falsely), and pre-provisioning the stack without `--build` makes the conftest reuse stale images.

---

## Known systemic issues (pre-existing, out of this diff's scope, still open)

- `redis-queues` is `maxmemory 1gb + allkeys-lru` in k8s yet holds the only record of pending uploads (queue + retry ZSET + DLQ) — Redis can silently evict the work queue with no DB-backed reconciler (mem_01KXQXP0TH9J4EA24TMWE4FN2A).
- CopyObject destinations never enqueue an Arion upload → never replicated, never evictable (mem_01KXQV6KH945F009SX78VBM5FW). Protected from eviction by the gate, but a cache-volume loss loses them permanently.
- Broken-v5 rows (storage_version≥5, NULL kek_id/wrapped_dek) still 500 on GET (mem_01KXZ696PZ3SG5V4YX7MH5V592).
- s3-backup hydrator writes parts without inventory rows → invisible to SQL eviction and (now) to all read protection (mem_01KYC16BN5THH12KH3EHW92JZY).

## Recommended action order

1. **F2** — staleness gauge + alert on `fs_cache:pressure` (restores observability on the safeguard everything leans on).
2. **F4** — make the missing column fail loudly (one-line SQL change).
3. **Coverage #1** — the round-trip integration test (closes three gaps at once).
4. **Coverage #2/#3** — middleware + publisher pool-arm tests.
5. **F1** — design: streamer re-enqueue on mid-stream miss.
6. **F3 + F9** — decide read-blind paths' fate; update CLAUDE.md files either way.

Memory notes: mem_01KYHFVN9BGEY04TX9NEZ60JPG, mem_01KYHFWEC5HHCZVEC4JDW7HZTZ, mem_01KYHFVNHY8Q0FEZA691EWQ4M3.
