# redis-queues HA cutover runbook

**Goal:** move `redis-queues` from a single StatefulSet to the operator-managed
`RedisReplication` + `RedisSentinel` (`k8s/base/redis-queues-ha.yaml`) with **no s3 downtime**
and **no regression of the drain's `cephor:epoch` fence**. Staging first; prod only after the
gating test passes.

## Why this needs a runbook (not a plain deploy)
`redis-queues` holds two things whose loss is *not* just a cache miss:
1. the **upload work queue** (`*_upload_requests`, retries) — a dropped entry is a lost upload;
2. the drain's **`cephor:*` leader lease + epoch fence** — a regressed epoch can let a stale
   drain-allocator win (split-brain; the `s3-prod-health` "epoch went BACKWARDS" = sev-3).

Redis replication is stream-based (sub-second under normal load), but a Sentinel failover
promotes a replica that may be missing the last in-flight writes. For the **epoch** (written
only on leadership changes, rarely) the odds are low but nonzero — so a failover's effect on
the fence is the **gating acceptance test** below, not an assumption.

`REDIS_QUEUES_URL` currently = `redis://redis-queues:6379/0` (secret `hippius-s3-secrets`).
The operator exposes a master-following service `redis-queues-ha-master`; the app only needs
its URL repointed — **no app-code change**.

---

## Phase 0 — Stand up the HA set on STAGING (non-disruptive; the live redis-queues is untouched)
```bash
NS=hippius-s3-staging
kubectl apply -n $NS -f k8s/base/redis-queues-ha.yaml
# Wait for: 1 master + 2 replicas Ready, 3 sentinels Ready, and the master service present.
kubectl -n $NS get redisreplication redis-queues-ha redissentinel redis-queues-sentinel
kubectl -n $NS get pods -l app=redis-queues-ha -o wide
kubectl -n $NS get svc redis-queues-ha-master        # this is the repoint target
# Confirm replication is healthy + the master service points at the master:
kubectl -n $NS exec redis-queues-ha-0 -- redis-cli info replication   # role:master, connected_slaves:2
```
Nothing is repointed yet; the old `redis-queues` still serves all traffic.

**Two prerequisites are baked into the manifest — learned on staging 2026-07-23 (don't strip them):**
1. **`podSecurityContext {runAsUser/runAsGroup/fsGroup: 1000}`** — without `fsGroup` the opstree
   image (uid 1000) can't write `appendonlydir` on the root:root CephFS mount → the master
   CrashLoops on `Can't open the append-only dir: Permission denied`. Symptom if missing:
   `redis-queues-ha-0` in CrashLoopBackOff.
2. **The `allow-redis-operator` NetworkPolicy** — the namespace's `allow-internal` policy does not
   admit `redis-operator-system`, so every operator→pod dial times out. Symptom if missing:
   `connected_slaves:0` (replicas never told `slaveof`) **and no sentinel StatefulSet is ever
   created** (operator errors `Failed to Get the role Info … i/o timeout` and bails before making
   it). Existing `redis-cluster` is unaffected because RedisCluster self-heals via the in-namespace
   :16379 gossip bus; RedisReplication+Sentinel needs ongoing operator→pod connectivity.

3. **The image is pinned to redis 7.4.x** (`quay.io/opstree/redis:v7.4.8`) to match the live
   `redis-queues` (7.4.x). If someone downgrades it below the live version, the Phase-2 seed fails
   (`Failed trying to load the MASTER synchronization DB from disk: Invalid argument`). Sanity check
   before cutover: `kubectl -n $NS exec redis-queues-0 -- redis-cli info server | grep redis_version`
   must be the same minor as the manifest image.

If Phase 0 shows `connected_slaves:0` or a missing sentinel STS, check these two before anything else.

## Phase 1 — GATING TEST on staging: does a failover regress the fence? (do this BEFORE prod)
1. Repoint **staging's** `REDIS_QUEUES_URL` → `redis://redis-queues-ha-master:6379/0` (Phase 2
   method) and roll the staging consumers. Generate active ingest so the drain is live.
2. Record the fence + leader BEFORE:
   ```bash
   kubectl -n $NS exec redis-queues-ha-0 -- redis-cli get cephor:epoch    # note the value
   # (leader_count via the drain probe / cephor:* keys)
   ```
3. **Kill the master** and watch Sentinel promote a replica:
   ```bash
   kubectl -n $NS delete pod redis-queues-ha-0            # the current master
   # within ~5s (downAfterMs) + failoverTimeout, a replica is promoted; master svc follows it
   kubectl -n $NS get svc redis-queues-ha-master -o jsonpath='{.spec.selector}{"\n"}'  # now the new master
   ```
4. **PASS criteria (all must hold):**
   - `cephor:epoch` **did not decrease** (monotonic ↑ or unchanged);
   - drain `leader_count` stays exactly **1** (no split-brain), no allocator crash;
   - a cache-miss GET issued *during* the failover still completes (pub/sub subscribers
     re-subscribed to `notify:*` on the new master — the operator/client reconnect handles this);
   - no upload lost (the drain re-enqueues on transient error; verify the queue drains).
5. **If the epoch regresses or leader_count ≠ 1:** STOP. Do **not** proceed to prod. File a
   drain-fence-hardening issue (make the allocator tolerant of a redis failover — e.g. re-assert
   epoch on leadership, or persist the fence with a WAIT/`min-replicas` guard for the epoch key
   only). The redis manifests can stay; the cutover is blocked on the fence being failover-safe.

## Phase 2 — Data migration (copy the queue + cephor:* keys into the HA master)
The HA set comes up empty. To cut over without losing pending uploads or the fence, seed it from
the live `redis-queues` **before** repointing.

**Do NOT use `replicaof` + promote (the obvious approach FAILS here).** Validated on staging
2026-07-24: making the HA master `replicaof redis-queues` then `replicaof no one` seeds the data,
but the opstree operator **reconciles the topology and FLUSHES the seed on promote** — the promoted
node is reverted to a replica and re-synced from an empty peer (dbsize → 0). The operator fights any
manual topology change.

**Use a logical key-copy into the operator's designated master instead** — these are ordinary client
writes the reconciler does not touch, and they replicate to the replicas normally:
```bash
# 1. Find the operator's current master (the pod the master service points at):
MASTER_IP=$(kubectl -n $NS get endpoints redis-queues-ha-master -o jsonpath='{.subsets[0].addresses[0].ip}')
# 2. MIGRATE COPY every key from the live redis-queues into it (COPY = non-destructive to the source;
#    REPLACE = idempotent; run via `sh -c` so the empty-key arg survives kubectl's arg parsing):
KEYS=$(kubectl -n $NS exec redis-queues-0 -- redis-cli --scan | tr -d '\r' | tr '\n' ' ')
kubectl -n $NS exec redis-queues-0 -- sh -c "redis-cli MIGRATE $MASTER_IP 6379 '' 0 5000 COPY REPLACE KEYS $KEYS"
# 3. Verify on the master (survives an operator reconcile — wait ~45s — and replicates to ha replicas):
kubectl -n $NS exec redis-queues-ha-0 -- redis-cli -h redis-queues-ha-master get cephor:epoch   # == live value
kubectl -n $NS exec redis-queues-ha-0 -- redis-cli -h redis-queues-ha-master dbsize             # == live dbsize
```
For a large keyspace, batch the `KEYS` list (a few hundred per MIGRATE). Both redis versions must be
the **same minor** (7.4.x ↔ 7.4.x) or MIGRATE's serialization is rejected — the same version rule the
manifest's image pin enforces. **Quiesce note:** MIGRATE is a point-in-time copy; any queue entry
written to the live redis *after* the copy and *before* the Phase-3 repoint won't be on the HA master.
The drain re-enqueues on transient error so this is tolerable, but do Phase 2 → Phase 3 back-to-back
(minimize the gap), or briefly pause the producers, to avoid stranding in-flight uploads on the old
redis.

## Phase 3 — Repoint + roll consumers (the actual cutover; seconds)

**CRITICAL — `REDIS_QUEUES_URL` is NOT a GitHub Actions secret; it is a plaintext literal in the
deploy workflow.** The deploy's "Update secrets" step (`.github/workflows/production-deploy.yaml`,
`kubectl create secret generic hippius-s3-secrets --from-literal=REDIS_QUEUES_URL=...`) **recreates
`hippius-s3-secrets` on every deploy from that literal**. So a live `kubectl patch` of the secret is
**silently reverted on the very next deploy**. The durable change is editing the workflow literal.
Do BOTH:

1. **Durable repoint (source of truth):** change the literal in `production-deploy.yaml`
   (`redis://redis-queues:6379/0` → `redis://redis-queues-ha-master:6379/0`) and merge to
   `main`. Prepared as a held PR (see the "cutover repoint" PR). **Merge it only AFTER
   Phase 0** (the `redis-queues-ha-master` service must exist, or that deploy breaks the queue path).
   `staging-deploy.yaml` has the analogous literal — edit it on `staging` for a staging cutover.
2. **Immediate repoint + roll** (the live switch; the merge in step 1 recreates the secret but does
   NOT restart pods without an image change, so you roll them explicitly):
```bash
NEW=$(printf 'redis://redis-queues-ha-master:6379/0' | base64)
kubectl -n $NS patch secret hippius-s3-secrets --type merge -p "{\"data\":{\"REDIS_QUEUES_URL\":\"$NEW\"}}"
kubectl -n $NS rollout restart ds/drain-agent ds/api-local deploy/drain-allocator deploy/mpu-reaper \
  deploy/arion-uploader deploy/arion-downloader deploy/arion-unpinner   # any consumer of the queues client
```
Consumers reconnect to the HA master. The drain is retry-safe, so a per-pod reconnect blip is absorbed.

## Phase 4 — PROD cutover (only after staging Phases 1–3 pass)
Repeat Phases 0/2/3 in `hippius-s3-prod` during a low-traffic window. Keep the old `redis-queues`
StatefulSet running (do not delete) until 24–48 h of clean HA operation.

## Rollback (fast, at any point)
The old standalone `redis-queues` is left running throughout, so rollback = repoint back:
```bash
OLD=$(printf 'redis://redis-queues:6379/0' | base64)
kubectl -n <ns> patch secret hippius-s3-secrets --type merge -p "{\"data\":{\"REDIS_QUEUES_URL\":\"$OLD\"}}"
kubectl -n <ns> rollout restart ds/drain-agent ds/api-local deploy/drain-allocator deploy/mpu-reaper ...
```
**If the durable repoint (Phase 3 step 1) was already merged, also `git revert` that workflow commit**
— otherwise the next deploy recreates the secret pointing back at the HA master and undoes this
rollback. If the HA master accumulated new queue entries after cutover, drain them back or accept the
drain's re-enqueue (idempotent). Instant, no data loss (both redises are CephFS-durable).

## Decommission (after soak)
Once HA is proven in prod: remove the old `redis-queues` StatefulSet + Service from
`k8s/base/redis-statefulsets.yaml`, and wire `k8s/base/redis-queues-ha.yaml` into the
kustomizations so it's the managed default. Separate PR.

## Follow-up: redis-accounts
`redis-accounts` (persistent credit cache) is the other stateful single-replica Redis — same
pattern, lower urgency (its miss falls through to the source). Do it after redis-queues soaks.
