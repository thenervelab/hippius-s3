# Locality routing for the SSD ingest tier

How a request is placed on an ingest node, why placement is computed from the object path at the
edge load balancer rather than looked up in a directory, and what the api does for the requests
that placement does not cover. Companion to [hippius_s3/cache/CLAUDE.md](../hippius_s3/cache/CLAUDE.md)
(the read tiers) and [drain-direct-rollout.md](drain-direct-rollout.md) (why an object's bytes sit
on one node's SSD in the first place).

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

## 2. The placement rule at the edge

The edge load balancer (haproxy) classifies each request by its path alone:

- **Object-level** — the path has a non-empty key after the bucket segment, regex `^/[^/]+/.+`.
  These are consistent-hashed on the raw path, with the query string stripped
  (`balance uri path-only`, `hash-type consistent`).
- **Bucket- and service-level** — `/`, `/bucket`, `/bucket/` (ListObjects, `?delete`, `?uploads`
  listing, acl/policy/location/versioning on a bucket, CreateBucket). These stay round-robin on a
  sibling backend with identical health checks, timeouts and rate-limit tracking.

Because the hash covers the path only, every operation on one key lands on one node: PutObject,
every UploadPart, Create/Complete/Abort MultipartUpload, ListParts, UploadPartCopy (hashed on the
destination path), S4 append, GET/HEAD, Range, `?versionId`, presigned URLs and DeleteObject for
that key. `partNumber`, `uploadId`, `versionId` and the `X-Amz-*` presign parameters all live in the
query string, so they never move a request off the key's owner — that is the reason for
`path-only`: hashing the full URI would scatter a multipart upload across nodes again, one node per
`partNumber`.

**Ring agreement.** The ring is built from each server's `id` and `weight` (vnodes per server are
`weight × 16`). Several load balancers front the same nodes, and a key must map to the same node
from every one of them, so every hashing backend on every box must list the same servers with
identical `id` and `weight`. A backend with the same servers but different ids computes a
different ring and silently sends half the keys to the wrong node. Weight is set to 100 on all
servers (not the default 1) so the ring has enough vnodes to spread keys evenly.

**Bounded-load spill is off initially.** haproxy's `hash-balance-factor` lets a request spill to
the next server on the ring when the owner is "too busy". Its notion of busy is relative to the
backend-wide average of outstanding requests, not to the key's own load: a server is eligible
while

```
owner_slots = ceil((scur + 1) * factor / 100 / nservers)
```

where `scur` is the backend's total current sessions. On a quiet backend that number is tiny, so
a single client's parallel download (the AWS CLI opens up to 16 concurrent range requests)
exceeds it and most of the ranges spill off the owner onto nodes that must peer-fetch them — the
slow path the hash was meant to remove. Enable the factor only once measured `scur` at typical
load gives `owner_slots >= 16` (the client parallelism); until then a hot node is throttled by the
api's own inflight caps (section 4), not by the load balancer.

## 3. Misplaced objects

An object is misplaced when its SSD copy is on a node other than its hash owner: uploaded before
the cutover, uploaded while its owner was out of the ring during a node rollout, or spilled (once
the factor is on). Nothing special is done for these; the existing per-part directory serves them:

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

No new state is introduced: the directory, the hint and the promotion path all predate the hash,
and the hash is only a way of making the local hit the common case.

## 4. Hot objects

Consider 100 clients fetching one freshly uploaded private object. All of them hash to the owner,
which serves off local SSD and is bound by its NIC — roughly 12-30 MB/s per reader for a 5 GB
object. That is the ceiling for a single hot key with the factor off; it is a bandwidth limit, not
a stall.

The stall class the benchmark did expose is on the peer path. When a neighbour (a spill target, or
the hash owner of a misplaced fresh object) peer-fetches a part that the drain has **not yet
replicated**, the owner's serve cap (`HIPPIUS_PEER_SERVE_MAX_INFLIGHT`) or the fetcher's per-peer
cap could shed the fetch "to the pool" — but an unreplicated part has no pool copy, so the reader
sat in `wait_for_chunk` until it timed out: 25 s on the first chunk (a 503) or up to 300 s
mid-stream, because neither the drain landing the chunk nor a neighbour promoting it published a
`notify:` event. Three api-side changes ship with the rollout to close this:

- **Singleflight** per `(object, version, part, chunk)` per pod: one leader task fetches, every
  concurrent reader of that chunk waits on it. A neighbour with 30 readers fetches each chunk from
  the owner once, then serves the rest locally.
- **Unreplicated wait**: for a part known to be unreplicated, the fetcher waits for a peer slot and
  retries a `server_busy` 503 with backoff for up to `HIPPIUS_PEER_FETCH_UNREPLICATED_WAIT_SECONDS`
  (default 10; 0 restores the immediate shed) instead of shedding to a copy that does not exist.
  Replicated parts keep the immediate shed, since the pool has them.
- **Promote-notify**: a promoted chunk publishes `notify:` on landing, so any reader waiting on
  pub/sub for that chunk wakes within about a second rather than at the 300 s timeout.

`HIPPIUS_PEER_SERVE_MAX_INFLIGHT` is raised from 16 to 64 alongside, sized as
`(nodes - 1) × HIPPIUS_PEER_FETCH_MAX_INFLIGHT` so every neighbour can hold a full prefetch window
against one owner at once. The memory bound per pod is unchanged (at most 16 × 4 MiB per peer).

## 5. Eviction vs in-flight reads

Replication never deletes anything. The drain evictor reclaims only parts whose status is
`replicated`, so the pool holds a copy of every part it can remove, and a read that loses its SSD
copy mid-stream falls through to the pool for the remaining chunks — slower, never failed. An
unreplicated part's SSD copy is the only copy and is never evicted.

Two details keep the owner's copy of a hot part from looking cold to the evictor:

- **Serve-path recency.** Reads served to a peer went through the base store's `get_chunk` and
  skipped the `last_read_at` stamp, so a part that was only ever peer-served was evicted in
  arrival order. The internal parts endpoint now stamps recency on a local hit, the same as a
  client-facing read.
- **Stream-start touch.** A multi-part stream stamps all of its parts once when it starts, so a
  long read of a large object does not have its tail parts evicted before the stream reaches them.

A per-read eviction lease in the drain agent is deferred until measurements show mid-read
evictions persisting after these two.

## 6. Known and accepted behaviours

- Two encodings of the same key (`a b` vs `a%20b`) hash differently. Consistent clients encode
  consistently, and a mismatch costs a peer fetch, not a wrong answer.
- `/bucket/dir/` and `/bucket/dir` hash differently. They are different keys in S3 too.
- Virtual-hosted-style bucket hosts are not on the hashed path; they keep going through the edge
  cache as before.
- A large multipart upload now lands entirely on one node, so the ingest-pressure 503 from
  `fs_cache_pressure` becomes key-sticky: retrying the same key retries the same full disk. This is
  the correct behaviour (the parts must be on one node) but changes what a client's retry sees.
- A DaemonSet rollout can truncate a transfer longer than the drain window on the node being
  rolled. Pre-existing; the hash does not change it, but it does make the affected keys
  predictable.
- The CopyObject fast path is DB-only (it re-points chunk rows without moving bytes), so the first
  GET of a fresh copy is a backend download on whichever node owns the destination key.

## 7. Operations

**Draining a node.** Set the server's weight to 0 on every hashing backend of every load balancer
(the admin socket `set weight` command). Its keys remap to ring-next; in-flight connections
finish. Restore to 100 to bring it back. Weight 0 does not change the ring — the server keeps its
vnodes and takes its keys back on restore.

**Unplanned node loss.** Health checks eject a server in about 4-6 s (`inter 2s fall 2`). During
that window requests for its keys pay the connect timeout and are redispatched to ring-next
(`option redispatch`); they succeed, slowly. Nothing else on the ring moves.

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
that handled it. For a sequential PUT-then-GET of one key the GET's node must equal the PUT's; a
4-part multipart upload must show one node across every UploadPart and the Complete; 50 bucket
LISTs must spread across more than one node. The probe script under `scripts/` runs exactly those
checks against a routing endpoint.
