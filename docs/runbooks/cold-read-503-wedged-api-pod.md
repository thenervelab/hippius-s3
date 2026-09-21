# Runbook: cold-read 503s from a wedged api pod

What to do when GETs answer `503 SlowDown` because one `api-local` pod's in-process Arion fetch path
has stalled (incident 2026-09-19 14:30 UTC to 2026-09-20 07:37 UTC, 17 h, no alert fired).

The mechanism lives in [`hippius_s3/reader/backend_fetch.py`](../../hippius_s3/reader/backend_fetch.py)
(the bounded fetcher and the client rebuild), the `read_backend_fetch_*` block of
[`hippius_s3/config.py`](../../hippius_s3/config.py), the first-chunk peek and `NotReadyCause` in
[`hippius_s3/services/object_reader.py`](../../hippius_s3/services/object_reader.py), the 503 log line
in [`get_object_endpoint.py`](../../hippius_s3/api/s3/objects/get_object_endpoint.py), and the
`backend_fetch_*` metrics in [`hippius_s3/monitoring.py`](../../hippius_s3/monitoring.py). The **peer**
httpx client (`app.state.peer_http`, [`hippius_s3/cache/peers.py`](../../hippius_s3/cache/peers.py))
has the same rebuild: `httpx.PoolTimeout` **and** `wait_for` cancelling the stream (the xrbng
CLOSE_WAIT shape) both count toward replacing the client. A dark peer shortcut is
`peer_fetch_shed_total{reason="pool_timeout"}` and ERROR `replaced the peer HTTP client`.
Just-written MPU GETs `IncompleteRead` after part 1 if the shortcut is still dark. A pod can
also look dark from `client_cap` alone if 16 fetches are stuck inside `wait_for` — rebuild
does not reset that semaphore. This file is only about what to do when it goes wrong; the model of how it works is under
[What the code does on its own now](#what-the-code-does-on-its-own-now).

## Symptom

- `download_not_ready` 503s (`SlowDown`, `Retry-After: 1`) on GETs, concentrated on ONE pod.
- Smoke `test_04` / `test_05` flapping, always on the 24 MiB multipart object: a cold multipart read
  needs ~10 backend fetches, so it has ~10x the exposure of the 1 MiB simple object.
- Clients report three shapes of the same event, depending on where their own 60 s read timeout
  lands relative to the server's bounds: `SlowDown` before headers; botocore `ReadTimeoutError`
  when the stall is mid-body; `IncompleteRead(5242880 read, ...)` when the server ends the stream
  after part 1.
- In-cluster clients (JuiceFS/hippius-fs, pg-inventory, tora-m365) reach their node-local pod via
  `trafficDistribution: PreferSameNode`, so one pod can own ~99% of all backend chunk reads. During
  the incident fleet backend reads fell 120k/h to 10k/h and 503s rose from 50-400/h to 3.4-5.3k/h
  while hcfs-server latency stayed flat and its in-flight gauge sat near 0: the requests never
  left the pod.

## Alerts and what to do

The two rules below are **pending**: they are added in `hippius-otel` (process in
[`docs/grafana-alerting.md`](../grafana-alerting.md)). Until they land, this runbook is reached
from smoke failures or from the queries in the next section.

| Alert | Means | First action |
|---|---|---|
| `S3ColdReadNotReady` (pending) | Cold reads failing before the first byte, fleet-wide | Run [Confirm in 60 seconds](#confirm-in-60-seconds). One pod dominant means this runbook. |
| `S3BackendFetchPoolSaturated` (pending) | One worker's self-heal is firing | Check for the `replaced the Arion client` ERROR. If `pool_timeout` keeps rising after it, grab evidence and delete the pod. |

Thresholds as planned: `S3ColdReadNotReady` is more than 100 `download_not_ready` 503s in 10 min
(the incident ran 560-880; normal is under 70); `S3BackendFetchPoolSaturated` is more than 3
`pool_timeout` outcomes in 10 min on one `exported_instance`.

The three existing read-path rules `S3ReadDownloadFailures`, `S3ReadDownloadSlow` and
`S3DownloadQueueStalled` watch the retired `arion-downloader-*` pipeline (`exported_instance`
and `queue_name` selectors that no longer exist). They cannot fire for today's read path; their
silence says nothing.

## Confirm in 60 seconds

Port-forward Prometheus: `kubectl -n monitoring port-forward svc/prometheus-server 9090:80`. Every
query below is a raw counter: run it once, wait 60 s, run it again, subtract. `rate()` and
`increase()` misbehave on these OTel series when a pod restarts.

```promql
# Who is failing? One exported_instance far above the rest = this runbook.
sum by (exported_instance) (s3_errors_total{error_type="download_not_ready"})

# Is that worker's pool wedged? inflight pinned at 32 while ok stops moving, or pool_timeout rising.
backend_fetch_inflight
backend_fetch_waiting
sum by (exported_instance, outcome) (backend_fetch_outcomes_total)
```

`exported_instance` is `pod:pid`; the gauges are per worker process. `backend_fetch_outcomes_total`
counts ATTEMPTS (`ok|pool_timeout|connect_timeout|read_timeout|error`).

Then the log line, on the suspect pod (recent only, see Traps):

```bash
kubectl -n hippius-s3-prod logs <api-local-xxx> --since=15m | grep -F "not ready cause="
```

The line is `GET <bucket>/<key>: not ready cause=<cause>: <message>`. Read `cause=` carefully; it is
the part people get backwards:

- `chunk_unavailable`: a tier REPORTED failure. Covers no backend location after the wait, fetch
  budget saturated (`backend fetch budget saturated for 8s`), the pool
  (`backend connection pool saturated`), every location failed, or the fetcher running out of time
  for another attempt. The first chunk carries the reader's 25 s bound as a deadline into the
  fetcher, and a retry is started only if a whole attempt (queue + pool + connect + read = 24 s)
  plus its backoff still fits, so the first chunk gets exactly one attempt and **a hung Arion
  fetch lands here at 10-14 s with "out of time" in the message** (a read hang fails at the 10 s
  read bound, a connect hang near 14 s; `backend fetch out of time for arion: no budget for another
  attempt`), preceded by the attempt's own `kind=read_timeout retry=True` line and a
  `kind=deadline retry=False` line.
- `first_chunk_timeout`: nothing reported a failure before the reader's own 25 s bound. That is a
  stall the httpx bounds do not reach: a local tier read, decrypt, or an env override that broke
  `queue + pool + connect + read < first`. Rare by design. When it fires, check for
  `backend chunk fetch failed … retry=True` attempt lines first; if none, look outside the Arion
  client.

If no single `exported_instance` dominates, or the dominant pod's `backend_fetch_outcomes_total` is
mostly `ok`, this is not a wedged pod: follow [README.dev.md](../../README.dev.md) section 6.6
step 5 (upstream Arion / KMS / chain API) and check hcfs-server latency.

## Manual mitigation

First run step 1 of [Evidence to grab BEFORE restarting](#evidence-to-grab-before-restarting)
(10 s); take the full five minutes only if 503s are under ~500 per 10 minutes. The restart destroys
the evidence and the root cause of the socket leak is not pinned.

```bash
kubectl -n hippius-s3-prod delete pod <api-local-xxx>
```

Why this is safe (verified 2026-09-20; re-verify against `k8s/` if the DaemonSet or Service
manifests changed): the object cache is a hostPath at `/s3-data`, not `emptyDir`, so nothing is
lost; the `api`/`gateway` ClusterIP Services use `trafficDistribution: PreferSameNode` with
cluster-wide fallback (not `internalTrafficPolicy: Local`), so in-cluster callers fail over to
another node; `terminationGracePeriodSeconds: 45` with a `preStop` `sleep 10` lets endpoints
propagate before SIGTERM. The DaemonSet recreates the pod on the same node (Ready in ~25 s). The
only cost is ~30 s of connection-refused on that node's NodePort 30081
(`externalTrafficPolicy: Local`) for the node-local external client.

Confirm on the NEW pod: CLOSE_WAIT sockets to the edge are 0, and the per-pod `download_not_ready`
counter stops moving within ~5 min (in the incident: 561 in the 10 min before, 0 in the 5 min after).

```bash
kubectl -n hippius-s3-prod exec <api-local-new> -- \
  awk 'NR>1 {split($3,r,":"); if (r[1]=="192B13A2") s[$4]++} END {printf "established=%d close_wait=%d\n", s["01"], s["08"]}' /proc/net/tcp
```

If `download_not_ready` keeps rising on the NEW pod within 5 minutes, the fault is outside the
process: node network/conntrack, or the edge at `162.19.43.25:443`. Next lever: cordon and drain
the node; page whoever owns the Arion edge.

## Evidence to grab BEFORE restarting

The root cause of the socket leak is not pinned; a restart destroys the evidence. Under active
customer impact (503s above ~500 per 10 minutes) run only step 1 (10 seconds) and delete the pod.
Take the full five minutes only while the counter is still low.

```bash
# 1. CLOSE_WAIT sockets to the edge in front of hcfs-server (162.19.43.25:443 = hex 192B13A2;
#    state 08 = CLOSE_WAIT, 01 = ESTABLISHED). The image has no ss/netstat/rg.
kubectl -n hippius-s3-prod exec <api-local-xxx> -- \
  awk 'NR>1 {split($3,r,":"); if (r[1]=="192B13A2") s[$4]++} END {printf "established=%d close_wait=%d\n", s["01"], s["08"]}' /proc/net/tcp

# 2. Which worker holds them: inode column ($10) of /proc/net/tcp, matched to socket fds per pid.
kubectl -n hippius-s3-prod exec <api-local-xxx> -- sh -c \
  'for p in /proc/[0-9]*; do n=$(ls -l $p/fd 2>/dev/null | grep -c "socket:"); echo "pid=${p#/proc/} sockets=$n"; done'
```

Also screenshot or export the per-worker `backend_fetch_inflight` / `backend_fetch_waiting` /
`backend_fetch_outcomes_total` series from the Confirm section; they vanish with the pod.

Tempo (`kubectl -n monitoring port-forward svc/tempo 3200:3200`), TraceQL:

```
{ duration > 24s && resource.service.namespace="hippius-s3-prod" }
```

A hung fetch shows the peer span missing at ~0.5 s, then the `GET https://arion.hippius.com/download/...`
span held to its bound. The httpx span wraps the transport, so pool acquisition time is inside it.
To date when the sockets went bad, plot cAdvisor `container_sockets{pod="<api-local-xxx>"}`; in the
incident the count was frozen for hours (one edge-side close event, never reaped), not a per-hang
leak.

## What the code does on its own now

The model: a GET serves each 4 MiB chunk from the first tier that has it: this node's SSD, a peer's
SSD, the pool, then the backend. The backend tier is in-process: one `ArionClient` per uvicorn
worker, built eagerly in the lifespan with `httpx.Timeout(connect=4, read=10, write=10, pool=2)` and
a connection pool sized to `read_backend_fetch_concurrency` (32), which is also the per-process
semaphore. Pool == semaphore means a healthy worker never queues at the pool, so an
`httpx.PoolTimeout` can only mean connections are held by something that is not a live fetch. The
reader bounds the FIRST chunk at `stream_first_chunk_timeout_seconds` (25 s) and turns a miss into
the 503. The fetcher's own bounds sit inside that: queue (8) + pool (2) + connect (4) + read (10) =
24 < 25, validated at boot, so a hung fetch fails as an ordinary logged, counted exception instead
of being cancelled silently at 25 s (which is how the incident went unseen for 17 h).

A connect/read timeout is a transient failure like a 5xx: it is retried while the chunk's deadline
allows. The streamer hands each chunk's bound to the fetcher as a deadline (25 s for the first
chunk, `stream_chunk_timeout_seconds` = 300 s for later ones) and the fetcher starts a retry only if
a whole attempt (queue + pool + connect + read = 24 s) plus its backoff still fits. Under the 25 s
first-chunk deadline no retry ever fits, so the first chunk gets exactly one attempt by design and
a hung Arion fetch on it fails at 10-14 s with `out of time`, while a single read stall on chunk N
of a large object (300 s deadline) is retried instead of truncating a committed 200 body. A
PoolTimeout on the current client is terminal (retrying the same pool is
useless). If that timeout is the one that replaces the client, the same attempt
is retried once on the new client so a mid-stream GET that already sent 200
can continue. A PoolTimeout that does *not* trip the rebuild still fails the
chunk.

Trap: rebuild is per uvicorn worker PID (`UVICORN_WORKERS=4`). Resetting worker
A does not unwedge B/C/D — each needs its own streak of 3 PoolTimeouts.
`trafficDistribution: PreferSameNode` pins in-cluster callers to the *pod*, not
to one worker; HTTP keepalive can then keep a client on the sick worker.
Graph `backend_fetch_outcomes_total` by `exported_instance` (pod:pid), not by
pod. Deleting the pod is still the blast-radius-clear when more than one PID
on the node is wedged.

Every failed attempt logs one WARNING, `backend chunk fetch failed backend=arion id=... attempt=N/3
kind=<kind> retry=<bool>: ...`. A wedged pool shows `kind=pool_saturated retry=True` when the rebuild fires (the
same attempt continues on the new client) and `retry=False` when it does not; a hung
connection shows `kind=read_timeout retry=True` (or `connect_timeout`) followed by
`kind=deadline retry=False: 15.0s left, attempt needs 25.0s` when the deadline stops the retry.
After `HIPPIUS_READ_BACKEND_FETCH_CLIENT_RESET_AFTER_POOL_TIMEOUTS` (3) consecutive PoolTimeouts
the worker logs ERROR `replacing the Arion client; old pool connections=32 idle=0 expired=0 closed=0
in_use=32` (the old pool's connections by state, taken before the swap) and ERROR `backend fetch
pool saturated 3 times in a row; replaced the Arion client (inflight=N waiting=M)`. Requests that
start after that run on a fresh client; a generation gate makes it one rebuild per streak. The old
client is closed 17 s later (a pool wait plus one wire attempt, so its healthy in-flight fetches
finish), under a 5 s bound. A failed rebuild logs `Arion client rebuild failed; the next PoolTimeout streak
retries it`.

Expect the rebuild to clear a poisoned pool. If `pool_timeout` keeps rising after a rebuild, or the
503s are `out of time` after `read_timeout` attempts with no PoolTimeouts at all, the fault is not
the client object: something on the path (node networking, conntrack, the edge at
`162.19.43.25:443`) is eating the connections. Grab the evidence above, then delete the pod.

## Traps

- Loki is `CrashLoopBackOff` (150+ days). `api-local` pod logs hold ~15 minutes. Prometheus and
  Tempo are the only history; do not go looking for yesterday's log lines.
- `rate()` / `increase()` on these OTel series lie across pod restarts. Diff raw counter values.
- `cause=first_chunk_timeout` does NOT mean "Arion hung". A hung Arion fetch is
  `cause=chunk_unavailable` with "out of time" in the message, at 10-14 s. When it fires, check for
  `backend chunk fetch failed … retry=True` attempt lines first; if none, look outside the Arion
  client. See above.
- `chunk_reads_by_tier_total{tier="backend"}` is not a health signal: it falls both when fetches
  fail and when demand falls, and it cannot tell the two apart. Use `backend_fetch_outcomes_total`.
- Fresh `curl`s from inside the sick pod succeed (0.5 s). That does not clear the pod; it proves
  the stall is in the process's client state, not the network.

## Knobs

All read at boot; `fetch_client_settings` refuses to start the app if the invariant is broken,
naming the variable.

| Variable | Default | Role |
|---|---|---|
| `HIPPIUS_STREAM_FIRST_CHUNK_TIMEOUT_SECONDS` | 25 | Reader's bound on the first chunk; a miss is the 503 |
| `HIPPIUS_READ_BACKEND_FETCH_CONCURRENCY` | 32 | Per-worker semaphore AND httpx pool size |
| `HIPPIUS_READ_BACKEND_FETCH_QUEUE_TIMEOUT_SECONDS` | 8 | Wait for a semaphore slot before failing the read |
| `HIPPIUS_READ_BACKEND_FETCH_CONNECT_TIMEOUT_SECONDS` | 4.0 | httpx connect bound |
| `HIPPIUS_READ_BACKEND_FETCH_READ_TIMEOUT_SECONDS` | 10.0 | httpx read AND write bound (per operation, not per body) |
| `HIPPIUS_READ_BACKEND_FETCH_POOL_TIMEOUT_SECONDS` | 2.0 | httpx pool-acquire bound; a trip is a `pool_timeout` |
| `HIPPIUS_READ_BACKEND_FETCH_CLIENT_RESET_AFTER_POOL_TIMEOUTS` | 3 | Consecutive PoolTimeouts before the client is rebuilt |
| `HIPPIUS_READ_BACKEND_FETCH_ATTEMPTS` | 3 | Attempts per location (3 = 2 retries) for transient errors (429/5xx, resets, connect/read timeouts), deadline permitting |
| `HIPPIUS_READ_BACKEND_FETCH_RETRY_BASE_SECONDS` / `_JITTER_SECONDS` | 1.0 / 0.25 | Backoff between those retries |

Invariant: `QUEUE + POOL + CONNECT + READ < FIRST_CHUNK` (8 + 2 + 4 + 10 = 24 < 25 by default).
Raising any fetch bound without raising the first-chunk bound puts the silent 25 s cancel back.
