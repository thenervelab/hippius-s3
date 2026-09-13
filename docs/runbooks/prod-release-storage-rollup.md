# Prod release: storage-usage rollup + billing-plan quota path

Step-by-step. **Read the whole thing before starting** — step order is load-bearing and one step is
irreversible in the sense that it changes what customers are charged against.

Day-to-day operation of the rollup (alerts, drift diagnosis, the kill switch) is a different
document: [storage-usage-rollup.md](storage-usage-rollup.md).

## What ships, and what it changes

A per-bucket byte counter maintained by Postgres triggers, replacing an O(objects) aggregate the
plans-cacher ran every cycle. Plus the billing-plan quota gate's usage path, plus two unrelated S3
features from #522/#523 (Content-MD5 verification, `If-None-Match: *`).

**Billing enforcement stays OFF for the whole release.** `HIPPIUS_ENABLE_BILLING_PLANS_PROD` is
`false`, so every account keeps taking the pay-as-you-go path and no quota can refuse anyone. The
counter machinery goes live; the gate does not.

## Why two merges rather than one

The trigger takes `FOR NO KEY UPDATE` on a version row while holding the `objects` row, which
imposes **lock `objects` before `object_versions`**. The PUT tail and the S4 append reserve reach
the `objects` row through an `object_id` FK on their `parts` / `multipart_uploads` INSERTs — so
without step A's `lock_object_row_by_id` they take the two rows in the forbidden order.

Measured: **4/48 (8%) of concurrent same-key PUTs deadlock at concurrency 8, 46/192 (24%) at 32.**
Each one is a 500 returned *after* the whole request body was received and staged.

Every pod runs `dbmate up` before uvicorn (`start-api.sh:23`), and `db-migrations` is created by the
same `kubectl apply -k` as the DaemonSet while the roll takes up to 20 minutes. So a "migrations
pre-step" is already what happens and is exactly what opens the window — and the Job cannot be moved
after the roll, because timing is decided by which migration files are in the **image**. The only
lever is the release contents:

| | contains | schema after |
|---|---|---|
| **Step A** | all code + rollup tables + **unlocked** triggers | `storage_usage_version_bytes_locked` absent |
| **Step B** | the three version-lock migrations | both version reads locked |

Between A and B the counter over-counts under concurrency. **This is harmless**:
`storage_usage_rollup_state.backfilled_at` is NULL, so `usage_service` refuses to serve a number at
all, and the backfill — which runs *after* B — SETS every counter to truth.

---

## Step 0 — Pre-flight (5 minutes, all read-only)

Run from a pod: `kubectl -n hippius-s3-prod exec <api-local-pod> -- python - <<'PY' ... PY`

Every `kubectl` command below was executed against production while writing this, so the selectors
and paths are real. Note the pod label is `app=api-local`, not `app=api` — the obvious guess returns
nothing silently.

**0.1 No long transaction in flight.** This decides whether the trigger DDL succeeds.

```sql
SELECT pid, state, extract(epoch from (now()-xact_start))::int AS xact_age_s, left(query,80)
FROM pg_stat_activity
WHERE backend_type='client backend' AND xact_start IS NOT NULL
ORDER BY xact_age_s DESC LIMIT 5;
```

Expect everything ≤ ~2s (normal is ListObjects at ~1s). **Anything older, or any
`idle in transaction`, wait it out.** Migrations carry `SET LOCAL lock_timeout = '3s'`, so a long
holder makes the migration *fail* rather than stall the fleet — safe, but you'd rather not retry.

**0.2 No bulk job running.** `nuke_user.py`, `purge_buckets.py`, the janitor's hard-delete sweep, or
the v4→v5 migrator. Once the triggers exist every row they touch writes a ledger row. Not dangerous,
but don't do both at once.

**0.3 Cluster healthy.** `kubectl -n hippius-s3-prod get clusters.postgresql.cnpg.io` → `postgres-nvme`
should be `Cluster in healthy state`. (`postgres` is the hibernated Ceph cluster; ignore it.)
Check replica lag is milliseconds, not seconds.

**0.4 The flag is off.**

```bash
kubectl -n hippius-s3-prod get secret hippius-s3-secrets \
  -o jsonpath='{.data.HIPPIUS_ENABLE_BILLING_PLANS_PROD}' | base64 -d; echo
```
Must print `false`. If it prints `true`, **stop** — the release would enforce quotas off an
unbackfilled counter.

**0.5 Nothing from this release is already applied.** All three should be absent:

```sql
SELECT to_regclass('storage_delta_ledger'), to_regclass('bucket_storage_usage'),
       to_regclass('storage_usage_rollup_state');
SELECT count(*) FROM pg_trigger t JOIN pg_class k ON k.oid=t.tgrelid
 WHERE NOT t.tgisinternal AND t.tgname LIKE '%storage_delta%';   -- expect 0
```

**0.6 No trigger-name collision.** The only pre-existing trigger on `objects` should be
`objects_reject_duplicate_live_name`.

---

## Step 1 — Merge A, deploy, and wait for the FULL roll

Merge `release/a-code-and-tables` → `main`.

**What the migrations do to locks, measured on PG 18.1 (both environments run it):**

```
CREATE TRIGGER                   -> ShareRowExclusiveLock   (blocks writes, NOT reads)
DROP TRIGGER IF EXISTS (absent)  -> no lock at all
DROP TRIGGER IF EXISTS (present) -> AccessExclusiveLock
```

On a first apply — which this is — the `DROP ... IF EXISTS` guards take **nothing**, so only
`CREATE TRIGGER`'s SHARE ROW EXCLUSIVE applies. **GET and ListObjects are never blocked.** Writes to
`objects` / `object_versions` pause for the milliseconds a catalog update takes. The
`multipart_uploads` column add is metadata-only (non-volatile DEFAULT, no 68 GB rewrite) and bounded
by its own `lock_timeout`.

**⛔ Wait for this to finish before step 2:**

```bash
kubectl -n hippius-s3-prod rollout status daemonset/api-local --timeout=25m
```

It is a DaemonSet at `maxUnavailable: 1` across 5 ingest nodes. **Every pod must be on the new
image**, because step B's lock is only safe once they all carry `lock_object_row_by_id`:

```bash
kubectl -n hippius-s3-prod get pods -l app=api-local \
  -o jsonpath='{range .items[*]}{.spec.containers[0].image}{"\n"}{end}' | sort -u
```
One line only. Two lines means the roll is not done — **do not proceed**.

**Verify step A's schema is the intended intermediate:**

```sql
SELECT count(*) FROM pg_proc WHERE proname='storage_usage_version_bytes_locked';  -- expect 0
SELECT regexp_count(prosrc,'_bytes_locked') FROM pg_proc
 WHERE proname='storage_usage_objects_update_trigger';                            -- expect 0
SELECT count(*) FROM pg_trigger t JOIN pg_class k ON k.oid=t.tgrelid
 WHERE NOT t.tgisinternal AND t.tgname LIKE '%storage_delta%';                    -- expect 5
SELECT backfilled_at FROM storage_usage_rollup_state;                             -- expect NULL
```

`0 / 0 / 5 / NULL`. A helper count of 0 with a non-zero `locked_reads` means a half-applied schema —
the trigger would fail on the next write. Stop and investigate.

**Confirm nothing is being served yet:** the `plans-cacher` log should show
`StorageRollupNotBackfilled` and it should keep publishing its previous roll. That is correct at this
stage — the alert `PlansCacheStale` will fire once the cache passes 15 minutes, and stays until
step 3.

---

## Step 2 — Merge B, deploy

Only once step 1's roll is confirmed complete. Merge `release/b-version-lock` → `main`.

**Verify:**

```sql
SELECT count(*) FROM pg_proc WHERE proname='storage_usage_version_bytes_locked';  -- expect 1
SELECT regexp_count(prosrc,'_bytes_locked') FROM pg_proc
 WHERE proname='storage_usage_objects_update_trigger';                            -- expect 2
SELECT regexp_count(prosrc,'_bytes_locked') FROM pg_proc
 WHERE proname='storage_usage_objects_insert_trigger';                            -- expect 1
```

**`2` matters.** Both the outgoing *and* the incoming version read must be locked; locking only the
outgoing one leaves the mirrored defect, which **under**-counts.

**Then check for deadlocks**, which is the specific thing this split exists to avoid:

```bash
for p in $(kubectl -n hippius-s3-prod get pods -l app=api-local -o name); do
  kubectl -n hippius-s3-prod logs $p -c api --since=20m | grep -ciE "deadlock|40P01"
done
```
All zeros. A non-zero count means a pod is still on step A's image — check the image list again.

---

## Step 3 — The backfill (manual, ~15–20 minutes)

Nothing in CI/CD runs this: `k8s/backfill-bucket-storage-usage-job.yaml` is in no kustomization and
neither deploy workflow applies it. Verified.

**3.1 Pin the image and namespace.** Two edits, both required:

```bash
kubectl -n hippius-s3-prod get ds api-local -o jsonpath='{.spec.template.spec.containers[0].image}'
```
Put that exact tag in the manifest's `image:`, and set `namespace: hippius-s3-prod`. The committed
file says `hippius-s3-staging` and `:latest` — and `latest` is pushed to the **same** GHCR repo by
both deploy workflows, so an unpinned prod backfill can run a staging build.

**3.2 Dry run, and read the output.**

```bash
kubectl create -f k8s/backfill-bucket-storage-usage-job.yaml
kubectl -n hippius-s3-prod logs -f job/backfill-bucket-storage-usage
```

It reports `seeded N bucket(s), M changed, T bytes total` and writes nothing.

**What to check, not just that it finished:** every changed bucket should move `0 -> truth`. A bucket
moving from a **non-zero** value to a different non-zero value means the maintained path had it
wrong — stop and investigate before applying. On staging, 1,060 of 2,823 changed and **every one**
went `0 -> truth`.

**3.3 Apply.** Change `--dry-run` to `--apply`, delete the old Job, re-create. The apply's totals
must match the dry run's exactly — on staging they were byte-identical
(`7,169,666,663,189` both times).

Expect **~15–20 minutes** for the ~49k buckets it walks (it covers soft-deleted ones too). Safe under live traffic and safe to re-run: one bucket
per transaction, recompute SETS rather than adds, and `backfilled_at` is written **only after a
complete pass** — so an interrupted backfill degrades to the pre-rollup behaviour, never to a wrong
bill. It will pause on the largest bucket (millions of objects, tens of seconds); that is expected.

**3.4 Verify:**

```sql
SELECT backfilled_at FROM storage_usage_rollup_state;              -- now set
SELECT count(*) FROM storage_delta_ledger;                         -- expect ~0
SELECT count(*) FROM bucket_storage_usage WHERE bytes_used < 0;    -- expect 0
```

The plans-cacher should publish within ~2 minutes (its poll interval) and `PlansCacheStale` should
clear. On staging it recovered **13 seconds** after `backfilled_at` was set.

**3.5 Let the reconciler confirm it.** Watch `usage-rollup` for a few cycles:

```
usage-rollup reconciled 50 bucket(s), 0 changed
```

**`0 changed` is the pass condition.** Anything else means a write path is unaccounted for — see the
drift section of [storage-usage-rollup.md](storage-usage-rollup.md). Note a full sweep is ~5.6 hours
at 50/300s over the 3,353 live buckets (of 49,047 total), so a handful of clean cycles is a sample, not a proof.

---

## Step 3.6 — Activate the alerts (MANUAL — nothing in CI does this)

⚠️ **The 8 alerting rules live in `k8s/otel/values/prometheus.yaml`, and no workflow applies that
file.** `k8s/otel/install.sh` is run by hand. Verified: `grep -rn "k8s/otel" .github/workflows/`
returns nothing, and the live `prometheus-server` ConfigMap key `alerting_rules.yml` still contains
`{}`.

So merging the release does **not** give you the alerts. Until this step runs,
`/api/v1/rules` returns zero groups and none of the safety net described in
[storage-usage-rollup.md](storage-usage-rollup.md) exists.

```bash
# from the repo root, against the monitoring namespace
helm upgrade --install prometheus prometheus-community/prometheus \
  -n monitoring -f k8s/otel/values/prometheus.yaml
```

Then confirm they loaded — this is the check, not the helm exit code:

```bash
kubectl -n monitoring port-forward svc/prometheus-server 9090:80 &
curl -s localhost:9090/api/v1/rules | python3 -c "import json,sys; \
  print(len(json.load(sys.stdin)['data']['groups']), 'groups')"
```

Expect **2 groups / 8 rules**. `promtool check rules` was run against this file during development
(SUCCESS, 8 rules), so a load failure means the helm values did not reach the ConfigMap, not that
the PromQL is wrong.

`alertmanager` is disabled in this release, so these surface as firing alerts in the Prometheus UI
and Grafana's alert list — they do not page anyone. Routing is a separate piece of work.

## Step 4 — Confirm pay-as-you-go is untouched

Before touching the flag, prove the release changed nothing for the accounts that are actually live.
Upload and delete with a PAYG account and confirm it still works, and that `plan_gate_total` shows
**only** `shadow_allow` (observing) with no `deny` and no `unavailable`.

---

## Step 5 — Turn the flag on (the only customer-visible step)

**Order matters and the second half is not optional.**

1. Set the GitHub Actions secret `HIPPIUS_ENABLE_BILLING_PLANS_PROD` to `true`.
2. **Push to `main`.** The flag is seeded into the k8s Secret *by the deploy workflow* via
   `--from-literal`, so a `kubectl rollout restart` alone does **nothing** — the Secret still holds
   `false`. `production-deploy.yaml` triggers only on push to `main`; there is no `workflow_dispatch`,
   so push an empty commit or re-run the last successful run from the Actions UI.
3. Verify on a pod: `python -c "from hippius_s3.config import get_config; print(get_config().enable_billing_plans)"` → `True`.
4. Watch `plan_gate_total`: it should flip from `shadow_allow` to `allow`. A `deny` means a real
   customer has been refused — cross-check against `StorageRollupDrift` and `PlansCacheStale` before
   telling them they are full.

**Do not do this until the backfill has completed AND one roll has published.** Before that,
`resolve_plan` returns nothing and every plan customer falls through to the credit gate — which a
plan customer cannot satisfy, so they get `402`.

---

## Rollback

| Problem | Action |
|---|---|
| Deadlocks after step B | Roll back step B's migrations (`dbmate down` ×3 — the down halves restore the unlocked bodies). Do **not** roll back step A. |
| Counter obviously wrong | Scale `usage-rollup` to 0. Stops all folding and recomputing instantly, no DDL, no lock. Triggers keep appending to an insert-only ledger at ~1–3 rows/s, which is harmless and fully recoverable. **This is the real brake — not `DISABLE TRIGGER`, which takes ACCESS EXCLUSIVE and blocks reads estate-wide.** |
| Quota refusing people wrongly | Set the flag back to `false` and redeploy. The counter keeps running; nobody is gated. |
| Migration stalls | It won't — `lock_timeout = '3s'` fails it instead, atomically, and the Job retries 3×. |

**Note what rollback cannot undo:** the triggers. Once installed they record deltas. That is fine —
the ledger is insert-only and reads refuse until `backfilled_at` is set — but removing them
cleanly means the down-migration of `20260910120000`, which drops all three tables.

## Known issues shipping with this release

Documented so nobody rediscovers them at 3 a.m. All flag-gated ones are inert until step 5.

**Live from step 1:** a hard delete of a *live* object racing a finalize over-counts (operator
scripts only, not the janitor); the compactor and a multi-bucket bucket-delete can deadlock during
an operator purge (loser retries).

**Live from step 5:** concurrent overshoot — N concurrent uploads each see the same cached usage, so
the bound is throughput × the 120s refresh, not the quota; `Content-Length` is trusted and never
reconciled against bytes written; a `redis-accounts` outage refuses plan customers (`402`) because
the fallback is a credit check they cannot pass.

**Observability:** alertmanager is disabled, so the 8 rules surface in the Prometheus UI and
Grafana's alert list rather than paging anyone.
