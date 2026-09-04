# Locality routing for the SSD ingest tier

How a request is placed on an ingest node, why placement is computed from the object path at the
edge load balancer rather than looked up in a directory, and what the api does for the requests
that placement does not cover. Companion to [hippius_s3/cache/CLAUDE.md](../hippius_s3/cache/CLAUDE.md)
(the read tiers) and [drain-direct-rollout.md](drain-direct-rollout.md) (why an object's bytes sit
on one node's SSD in the first place).

Rollout status (2026-09-04): the hashed edge config ships on staging first; the production
ingresses follow after the soak. Until then prod placement is still round-robin and every
number below that is not from the 2026-09-03 benchmark is a design expectation, not a
measurement. The two-level key of section 2 is a second edge change on top of the phase-1 path
hash; its api-side pieces (the promotion cap, the batched owner lookup, the duplicate-`partNumber`
400) ship with the read-path change and are described here as one model.

## 1. The problem

Under drain-direct, a PUT returns once the bytes are on the SSD of the node that received it. The
drain replicates that copy to the CephFS pool afterwards and keeps the SSD copy as the hot read
tier — so an object's fastest copy lives on exactly one node, and a GET is only fast if it lands
there.

The 2026-09-03 5 GB single-part benchmark against prod measured what that costs:

| Path | Throughput |
|---|---|
| GET lands on the node holding the part (local SSD) | ~650-830 MB/s |
| GET lands elsewhere and peer-fetches chunk by chunk from the owner | ~130-300 MB/s |
| Upload (average) | ~258 MB/s |

With round-robin placement a GET hit the owning node roughly one time in five. Multipart was worse:
every `UploadPart` was balanced independently, so before the change only 2 % of multi-part objects
on prod had all their parts on one node — there was no single node a whole-object GET could
usefully be sent to.

The opposite extreme is just as real: an ingest node has roughly 1 TB of free SSD, and a single
2 TB multipart object that hashed entirely onto one node would fill it and 503 every later PUT
that hashed there. Placement therefore has two goals — locality for everything a client reads
whole, and a bound on how much of one object any node must hold.

## 2. The placement rule at the edge

The edge load balancer (haproxy) classifies each request by its path and, for one request class
only, by one query parameter. The hash key is two-level, with N = 200:

| Request | Hash key | Lands on |
|---|---|---|
| Bucket- and service-level: `/`, `/bucket`, `/bucket/` (ListObjects, `?delete`, `?uploads` listing, acl/policy/location/versioning on a bucket, CreateBucket) | none — round-robin on a sibling backend with identical health checks, timeouts and rate-limit tracking | any node |
| Object-level (`^/[^/]+/.+`), every method and query string except the row below | `path` (query stripped) | the key node |
| `PUT` UploadPart / UploadPartCopy with `partNumber` > N | `path#partNumber` | a per-part node, spread over the ring |

Phase 1 shipped the second row as `balance uri path-only` under `hash-type consistent sdbm
avalanche`. The two-level key replaces it with `balance hash` over an expression that starts with
`path` (same parser: host stripped, query dropped, raw bytes) and appends `#<partNumber>` only
when the request is a `PUT` whose `partNumber` is above N. For every other request the sample is
the bare path, byte-identical to what `balance uri path-only` hashed, so the migration moves no
key that was already placed; only the tail parts of giants in flight during the switch land
differently. UploadPartCopy hashes on the destination path in both rows.

What that means per operation:

- **Single-part objects and all reads land on the key node.** PutObject, Create/Complete/Abort
  MultipartUpload, ListParts, S4 append, DeleteObject, and every read — GET/HEAD, Range,
  `?versionId`, presigned URLs and `?partNumber` reads (the api serves those as whole-object
  reads, section 7) — hash on the path alone. `uploadId`, `versionId` and the
  `X-Amz-*` presign parameters live in the query string and never move a request off the key
  node.
- **A multipart object keeps parts 1..N on the key node.** Reads of an object with at most N
  parts are fully local; with the AWS CLI's default 8 MiB parts that covers objects up to about
  1.6 GB. The prefix any one node must hold for one object is bounded at
  N × 512 MiB = 100 GB (`max_multipart_part_size` is the api's per-part ceiling).
- **Parts above N spread across ingest nodes**, so a 2 TB object no longer fills one node's SSD.
  A read of a spread object still lands on the key node: it serves the prefix locally and
  fetches the tail parts through the peer tier (section 3) — the behaviour every multipart
  object had before the hash. Two api-side bounds keep a giant read cheap for the key node:
  promote-on-read stops at `HIPPIUS_PROMOTE_MAX_PART_NUMBER` (= N), so a read never refills the
  key node with the tail the spread was meant to keep off it, and the tail parts' owners are
  resolved in one batched residency query rather than one lookup per part.

`partNumber` is read at the edge the way the api reads it: the name is exact-case (the api only
recognises `partNumber`), leading zeros are stripped before the compare (`partNumber=0201` is
`201`), an empty or absent value means "no part" and the request hashes on the path alone, and a
repeated parameter takes its first occurrence at the edge while the api rejects the duplicate
with 400 — so a request whose two values disagree is never served from the wrong node.

This is why the base hash is `path-only` rather than the full URI: hashing the whole query string
would put every UploadPart on its own node — including parts 1..N, losing the local prefix — and
would also move `?versionId` and presigned reads of a key off its node. Spreading is wanted for
exactly one parameter and only above N, so that one is folded in deliberately.

**Ring agreement.** The ring is built from each server's `id` and `weight` (vnodes per server are
`weight × 16`). Several load balancers front the same nodes, and a key must map to the same node
from every one of them, so every hashing backend on every box must list the same servers with
identical `id` and `weight`, under the same `hash-type`. A backend with the same servers but
different ids (or a different hash function) computes a different ring and silently sends a
large share of keys to the wrong node — locality degrades to the round-robin baseline with
nothing failing. Weight is set to 100 on all servers (not the default 1) so the ring has enough
vnodes to spread keys evenly. Adding a node means adding it with a new fixed `id` on every box
in the same change; never let haproxy auto-assign ids. The same holds for the second level of
the key: every box must apply the same N and build the `path#partNumber` suffix the same way, or
tail parts of one upload land differently depending on which load balancer took them.

**Bounded-load spill is off initially.** haproxy's `hash-balance-factor` lets a request spill to
the next server on the ring when the owner is "too busy". Its notion of busy is relative to the
backend-wide average of outstanding requests, not to the key's own load: a server is eligible
while

```
owner_slots = ceil((scur + 1) * factor / 100 / nservers)
```

where `scur` is the backend's total current sessions. On a quiet backend that number is tiny, so
a single client's parallel download (the AWS CLI runs 10 concurrent requests by default,
`max_concurrent_requests`, and clients commonly raise it) exceeds it and most of the ranges spill
off the owner onto nodes that must peer-fetch them — the slow path the hash was meant to remove.
Enable the factor only once measured `scur` at typical load gives `owner_slots >= 16` (headroom
over the default client parallelism); until then a hot node is throttled by the api's own
inflight caps (section 4), not by the load balancer.

## 3. Misplaced objects

An object is misplaced when its SSD copy is on a node other than its hash owner: uploaded before
the cutover, uploaded while its owner was out of the ring during a node rollout, or spilled (once
the factor is on). The tail of a spread multipart object (parts above N, section 2) is not
misplaced — it is where the hash put it — but the key node reads it through the same path.
Nothing special is done for either; the existing per-part directory serves them:

1. The api on the hash owner misses locally and resolves the part's holder from
   `cephor_ssd_residency` (joined to `replicated` status), else from
   `cephor_replication_status.node_id`, else from the 60 s `hippius:fresh-part:*` Redis hint the
   write path leaves for parts the drain has not claimed yet.
2. It peer-fetches each chunk from that node (`PeerChunkFetcher`,
   [hippius_s3/cache/peers.py](../hippius_s3/cache/peers.py)) and, with promote-on-read, claims the
   part in its own residency row and copies the chunk onto its own SSD
   ([hippius_s3/cache/dual_fs_store.py](../hippius_s3/cache/dual_fs_store.py)).
3. After one full read the hash owner holds the object locally and every later GET is a local
   read. The original copy stays on its ingest node until that node's evictor reclaims it.
   Promotion stops at `HIPPIUS_PROMOTE_MAX_PART_NUMBER` (= N): parts above it are peer-served on
   every read, by design, so the tail of a giant never accumulates on its key node.

No new state is introduced: the directory, the hint and the promotion path all predate the hash,
and the hash is only a way of making the local hit the common case.

## 4. Hot objects

Consider 100 clients fetching one freshly uploaded private object. All of them hash to the owner,
which serves off local SSD and is bound by its NIC: the readers share that node's egress, so each
sees a fraction of it. That is the ceiling for a single hot key with the factor off; it is a
bandwidth limit, not a stall, and it has not been measured yet.

The stall class the benchmark did expose is on the peer path. When a neighbour (a spill target, or
the hash owner of a misplaced fresh object) peer-fetches a part that the drain has **not yet
replicated**, the owner's serve cap (`HIPPIUS_PEER_SERVE_MAX_INFLIGHT`) or the fetcher's per-peer
cap could shed the fetch "to the pool" — but an unreplicated part has no pool copy, so the reader
sat in `wait_for_chunk` until it timed out: 25 s on the first chunk (a 503) or up to 300 s
mid-stream, because neither the drain landing the chunk nor a neighbour promoting it published a
`notify:` event. Four api-side changes ship with the rollout to close this:

- **Singleflight** per `(object, version, part, chunk)` per pod: one leader task fetches, every
  concurrent reader of that chunk waits on it. A neighbour with 30 readers fetches each chunk from
  the owner once, then serves the rest locally.
- **Unreplicated wait**: for a part known to be unreplicated, the fetcher waits for a peer slot and
  retries a `server_busy` 503 with backoff for up to `HIPPIUS_PEER_FETCH_UNREPLICATED_WAIT_SECONDS`
  (default 10; 0 restores the immediate shed) instead of shedding to a copy that does not exist.
  Replicated parts keep the immediate shed, since the pool has them.
- **Promote-notify**: a promoted chunk publishes `notify:` on landing (the store's `on_promoted`
  hook, wired to `obj_cache.notify_chunk`), so any reader waiting on pub/sub for that chunk wakes
  within about a second rather than at the 300 s timeout.
- **No backend download for peer-held fresh parts**: `build_stream_context` skips the Arion
  `DownloadChainRequest` for a missing part whose owner is known and unreplicated
  (`peer_locate`), since Arion is uploaded from the pool copy and cannot have it yet; the peer
  tier serves it on the way through `wait_for_chunk` instead.

`HIPPIUS_PEER_SERVE_MAX_INFLIGHT` is raised from 16 to 64 in the prod and staging api-local
manifests alongside (the code default stays 16), sized as
`(nodes - 1) × HIPPIUS_PEER_FETCH_MAX_INFLIGHT` so every neighbour can hold a full prefetch window
against one owner at once. The memory bound per pod is unchanged (at most 16 × 4 MiB per peer).

## 5. Eviction vs in-flight reads

Replication never deletes anything. The drain evictor reclaims only parts whose status is
`replicated`, so the pool holds a copy of every part it can remove, and a read that loses its SSD
copy mid-stream falls through to the pool for the remaining chunks — slower, never failed. An
unreplicated part's SSD copy is the only copy and is never evicted.

Two details keep the owner's copy of a hot part from looking cold to the evictor:

- **Serve-path recency.** Reads served to a peer went through the base store's
  `read_local_chunk` (which calls the base `get_chunk`, bypassing the tiered store's
  `_on_local_read`) and so skipped the `last_read_at` stamp: a part that was only ever
  peer-served was evicted in arrival order. `DualFileSystemPartsStore.read_local_chunk` now
  stamps recency on a local hit, the same as a client-facing read, while still reading the
  primary only — never the pool, never a peer.
- **Stream-start touch.** A multi-part stream stamps all of its parts once when it starts, so a
  long read of a large object does not have its tail parts evicted before the stream reaches them.

A per-read eviction lease in the drain agent is deferred until measurements show mid-read
evictions persisting after these two.

## 6. Known and accepted behaviours

- Two encodings of the same key (`a b` vs `a%20b`) hash differently. Consistent clients encode
  consistently, and a mismatch costs a peer fetch, not a wrong answer.
- `/bucket/dir/` and `/bucket/dir` hash differently. They are different keys in S3 too.
- Only authenticated and presigned requests are hashed. Anonymous reads of public objects keep
  going through the edge cache, whose parent selection is independent of the hash, so they hit
  the owner at the round-robin rate and peer-fetch otherwise; repeats are absorbed by the cache.
- Virtual-hosted-style bucket hosts are not on the hashed path; they keep going through the edge
  cache as before.
- AbortMultipartUpload lands on the key node, which holds only the prefix (parts 1..N). The
  handler's local chunk delete removes that prefix and is a no-op for the spread parts;
  `ResidencyRecorder.drop_version` is node-scoped by design (it drops only this node's
  `cephor_ssd_residency` rows) and `PeerRegistry.forget_parts` is node-agnostic (it clears the
  fresh-part hint of every part number the upload listed, wherever the part landed). The spread
  parts' bytes and residency rows are reclaimed by the drain's failed-part path after
  `CEPHOR_RECLAIM_GRACE_SECS`, the same path that reclaims pre-cutover parts on other nodes.
- The ingest-pressure 503 from `fs_cache_pressure` is sticky per part, not per key: a full node
  blocks only the parts hashed there — a whole single-part object or multipart prefix, or the
  individual tail parts that landed on it — and retrying that part retries the same full disk
  while the other parts of the same upload keep landing elsewhere.
- A DaemonSet rollout can truncate a transfer longer than the drain window on the node being
  rolled. Pre-existing; the hash does not change it, but it does make the affected keys
  predictable.
- The CopyObject fast path is DB-only (it re-points chunk rows without moving bytes), so the first
  GET of a fresh copy is a backend download on whichever node owns the destination key.

## 7. Known limitations

- S4 appends carry no part number in the URL, so an append-only object grows on its key node
  without bound (follow-up: api guard).
- Many giants hashing to one node pin up to N × 512 MiB each.
- Reads of a giant funnel through its key node (the bounded-load factor later spreads readers to
  the part owners).
- A request that repeats `partNumber` is rejected by the api with 400 rather than reconciled: the
  edge places it by the first value and the api would otherwise read the last.
- Abort/delete of a giant leaves the spread parts on their nodes until the drain grace (1 h
  failed / 24 h orphan).
- Part numbers need not be contiguous. A client that numbers sparsely (1000, 2000, ...) has every
  part above N spread and reads the whole object through the peer tier; the per-node bound holds.
- Part-wise GET (`?partNumber` reads) is not implemented and no mainstream client uses it.

## 8. Operations

**Draining a node.** Set the server's weight to 0 on both the hashed backend and its round-robin
twin, on every load balancer, through the runtime API (`set weight <backend>/<server> 0` on the
`stats socket`, which the staging config adds in `global` with this change — the production
configs get it when they adopt the hash). Its keys remap to ring-next; in-flight connections
finish. Restore the hashed backend to exactly 100 (the ring weight every box agrees on) and the
`-rr` twin to its configured weight (the default, 1). Weight 0 removes the server's vnodes without
moving anyone else's, and they come back at the same positions on restore, so it takes exactly
its old keys back. Runtime weights do not survive a reload: a deploy while a node is drained
un-drains it.

**Unplanned node loss.** Health checks eject a server in about 4-6 s (`inter 2s fall 2`,
`timeout check 2s`; up to ~8 s when the port drops instead of refusing). Once it is down its whole
key range moves to ring-next until `rise 2` brings it back, and nothing else on the ring moves.
During the ejection window requests for its keys are still hashed to it and pay the connect
timeout per attempt; only with `option redispatch` on the backend is the last retry re-hashed
onto ring-next, otherwise those requests fail once `retries` is exhausted. Check the backend has
the directive before counting on the "slow but successful" behaviour.

**Rollback.** Revert the load-balancer config PR and redeploy; the deploy script restores the
previous config if the reload fails. No api or data change is needed — the api never assumed a
placement, and the per-part directory serves everything either way.

**Soak watchlist** (after the staging deploy and again after prod):

| Signal | Expect |
|---|---|
| Per-server current sessions | Skew between nodes bounded; a persistent outlier is a hot key or a ring disagreement |
| Backend redispatch counter | Flat outside health-check ejections |
| Server check status | All UP; a flapping server moves its keys on every flap |
| 429 rate | Unchanged — the round-robin twin carries the same rate-limit tracking |
| `chunk_reads_by_tier_total{tier="local"}` share | Rising as promotions land on hash owners |
| `peer_fetch_shed_total{reason}` | `client_cap` / `server_busy` falling; `unknown_size` near zero |
| `promotion_skipped_total{reason}` | `disk_pressure` should not climb on the nodes taking the most keys |
| Per-node PUT 503s | Correlated with key distribution, not with one node's disk alone |
| p50 TTFB | Down, as the first chunk is a local read more often |

**Verification header.** Every response carries `X-Hippius-Node`, the node name of the api pod
that handled it (present whenever `NODE_NAME` is set on the pod). For a sequential PUT-then-GET of
one key the GET's node must equal the PUT's. For a multipart upload, parts 1..N and every
control-plane call (Create, ListParts, Complete, Abort) and every GET must show the key node,
while `UploadPart`s with `partNumber` > N must spread across more than one node. Repeated bucket
LISTs must spread across more than one node. The AWS CLI prints response headers under `--debug`:

```bash
aws --endpoint-url "$ENDPOINT" s3api put-object --bucket b --key k --body f --debug 2>&1 | grep -i x-hippius-node
aws --endpoint-url "$ENDPOINT" s3api get-object --bucket b --key k /dev/null --debug 2>&1 | grep -i x-hippius-node
```

**Probe script.** [scripts/locality_probe.py](../scripts/locality_probe.py) runs the checks above
end to end with boto3 and prints one PASS/FAIL line per check (exit 1 on any FAIL). It creates a
throwaway `locality-probe-<epoch>` bucket, checks that `HIPPIUS_PROBE_KEYS` single-part keys read
back (GET × `HIPPIUS_PROBE_GETS`, HEAD) from their PUT node and reports how the PUT nodes spread,
that a 4 x 5 MiB multipart upload (Create, UploadPart, ListParts, Complete, GETs) and an aborted
one sit on one node, that repeated `ListObjectsV2`/`HeadBucket` spread over more than one node,
and that Range, `?versionId` and a presigned GET of one key all land on its PUT node. Every
response's `X-Hippius-Node` is captured through a botocore `after-call` hook; a missing header
fails the check rather than crashing.

```bash
source .venv/bin/activate
HIPPIUS_ROUTING_ENDPOINT="$ENDPOINT" AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
  python scripts/locality_probe.py
```

Optional env: `AWS_DEFAULT_REGION` (default `decentralized`), `HIPPIUS_PROBE_KEYS` (20),
`HIPPIUS_PROBE_GETS` (5), `HIPPIUS_PROBE_LISTS` (50), `HIPPIUS_PROBE_KEEP_BUCKET` (leave the
bucket behind for inspection). Without an endpoint and credentials it prints one line and exits 0.
A FAIL still cleans up; an exception mid-run (the script has no error handling by design) leaves
the bucket behind, so delete `locality-probe-*` by hand after a crash.

`HIPPIUS_PROBE_SPREAD=1` adds the spread check: it uploads N + 5 parts of 5 MiB (about 1 GB at
the default N; `HIPPIUS_PROBE_SPREAD_THRESHOLD` sets N, default 200, and must match the edge's),
completes, and reads the object back once with a streamed md5 check. Create, a sample of the
prefix (parts 1, 2, N-1, N), Complete and the GET must sit on one node; parts N+1..N+5 must spread
over more than one — the bar a two-node fleet can show, so a partial spread passes and the
distribution is printed for the eye.
`HIPPIUS_PROBE_DRILL=1` adds the misplaced-object drill: it waits for you to drain one node at the
edge (section "Draining a node" above), PUTs a key while the node is out of the ring, waits for you
to restore it, then fires 30 concurrent GETs and requires every one to succeed with a TTFB under
5 s on the same node — the restored hash owner, which is expected to differ from the PUT node.
That exercises the peer-fetch + promotion path of section 3 under the singleflight of section 4.

A mismatch on a freshly written key means either the rings disagree (check `id`/`weight` on
every hashing box) or the two requests used different encodings of the key. The server-side
check is the per-stream tier log the api emits when a body finishes:
`STREAM tiers ray_id=... object_id=... v=... local=N peer=N pool=N bytes=N owner=<node> owners=N`
— a GET that landed on its owner shows every chunk under `local=`. For a spread object read on its
key node, `local=` is roughly the prefix's chunk count and `peer=` roughly the tail's; `owner=` is
the memoised holder of the first part and `owners=` the number of distinct nodes the tail resolved
to. A large `peer=` on a single-part object, or `pool=` on anything fresh, is the misplacement
signal; on a giant it is the design.
