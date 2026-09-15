# 16 — Object & Chunk Layout, and the Range-Read Strategy

**Status:** first draft — **partly superseded by [`23-chunking-and-range-prior-art.md`](23-chunking-and-range-prior-art.md) (2026-09-15).** The 4 MiB blob size (C3) stands, but C4 flipped: we **adopt sub-chunk AEAD framing** (256 KiB CTX frames within each 4 MiB blob). So the "reader selects whole blobs, never sends Range to HCFS" rule here is **superseded** — the reader now maps a byte range → covering frames → a *single* HCFS ranged GET, making cold intra-object ranges cheap. See 23 for the design and [`decisions-register.md`](decisions-register.md) C3/C4/C5.

**What this is.** The concrete **object → part → chunk → HCFS-blob** layout for the greenfield
Rust `hippius-s3`, and the **Range-read** strategy against HCFS, given that durable bytes live in
HCFS as content-hash-addressed ciphertext blobs, **reads go straight to HCFS (no local read
cache)**, and **each blob is one encrypted chunk**.

**Reads first (all traced):** [`02-storage-engine-schema.md`](./02-storage-engine-schema.md)
(current parts→chunks→backends), [`03-data-plane-cache-streaming.md`](./03-data-plane-cache-streaming.md)
(the cold-range gap, §3.7), [`06-s3-protocol-conformance.md`](./06-s3-protocol-conformance.md) §4
(Range) / §2–§3 (ETag/MD5), [`10-write-path-decision.md`](./10-write-path-decision.md) (SSD-staging,
DECIDED), [`12-schema-design.md`](./12-schema-design.md) (`blobs`/`chunk_blobs`, refcount),
[`13-hcfs-as-is-integration.md`](./13-hcfs-as-is-integration.md) (HCFS blob contract),
[`01-crypto-envelope.md`](./01-crypto-envelope.md) (AEAD), and
[`crypto-dedup-research.md`](./crypto-dedup-research.md) (dedup model).

This doc does **not** redefine the schema — [`12`](./12-schema-design.md) owns `blobs` /
`chunk_blobs` / `chunks` / `parts`. It specifies the *layout policy and read algorithm* that ride on
top, with a worked example.

---

## 0. The box we design inside (verified constraints)

| Constraint | Value | Source |
|---|---|---|
| **HCFS per-blob cap** | file part **16 MiB**; whole multipart request also ≤16 MiB, so usable is a few KiB under 16 MiB | `13` §Caps; `S3_FILE_FIELD_CAP` `hcfs upload.rs:49`, `router.rs:110` |
| **HCFS blob = one named object**, content-addressed | key `blake3(ciphertext)`; retrieved by `(ss58, file_id=hex(blake3(name)))` | `13` §1; `hcfs backend.rs:988` |
| **HCFS native Range GET** | `Range: bytes=start-end` / `start-` → `206` + `Content-Range` | `13` §2; `hcfs backend.rs:1342,1534` |
| **HCFS: no suffix range** | `bytes=-N` → `None` → full 200 | `13` §2/§Caps; `hcfs backend.rs:140-154` |
| **HCFS: no storage refcount** | delete is unconditional/best-effort → **S3 service owns refcount** | `13` §6; `hcfs backend.rs:774` |
| **No local read cache** | reads go straight to HCFS with Range | `10` "Not a differentiator"; `03` |
| **Blob = 16 × 256 KiB CTX frames** | per frame `nonce(12) ‖ ct ‖ tag(16) ‖ CT(32)`; **60 B**/frame overhead (~0.023%); committing AEAD (CMT-4) | `25` §1.2; `21`, `23` |
| **Default plaintext chunk (blob) size** | **4 MiB** (`4194304`), framed into 256 KiB (tunable to 64 KiB); readers use per-part `chunks.plain_size`, never config | `25` §1; `01` C8; `12` §2.7–§2.8 |
| **Per-frame AAD** | `blob_id(32) ‖ LE32(frame_index) ‖ suite_id` — binds the **opaque** blob identity + slot, NOT object identity | `25` §1.5, T3 |
| **1 chunk → 1 primary blob** | `chunk_blobs.role='primary'`; `role` reserves replica/parity for future EC | `12` §2.8–§2.10 |
| **`blobs.content_hash` ≠ `blobs.hcfs_file_id`** | address (dedup/validator) vs handle (GET/DELETE) — never conflate | `12` §2.9; `13` §1 |

**S3 rules to satisfy** ([`06`](./06-s3-protocol-conformance.md) §4; AWS docs, Sept 2026):
part size **5 MiB–5 GiB** (no min on last part), **≤10,000 parts**, max object **5 TiB**; ETag single
= `hex(md5(plaintext))`, multipart = `hex(md5( md5(part₁)‖…‖md5(partₙ) ))-N`; Range supports suffix
(`bytes=-N`) and open (`bytes=A-`), `start>=size`→**416**, inverted range→**full 200**, **no
`multipart/byteranges`** (single range only).

---

## 1. Chunk size

### The decision

⚑ **D1 — keep the plaintext chunk size at 4 MiB, stored per-part in `parts.chunk_size`, tunable
per-bucket, hard-capped so `chunk_size + 28 ≤ 16 MiB − headroom`.**

Rationale: 4 MiB is the established value (`01` C8), it is what live hippius-s3 already posts to HCFS
one-chunk-per-`POST /upload` (`10` §"Not a differentiator"), it sits far under the 16 MiB HCFS blob
cap, and its AES-GCM overhead is negligible. Storing it **per part** (not reading config) is
mandatory — a reader that assumes config desyncs chunk boundaries on any object written under a
different size (`01` §3.1). Making it a **per-bucket** knob lets a range-heavy bucket trade round-trips
for lower amplification (below) without touching the engine.

### The tradeoff, quantified

Three forces pull on the chunk size. AEAD overhead is **not** the binding one (it is tiny at every
candidate); the real tension is **cold range read-amplification** (smaller is better) vs **per-object
blob/row count and HCFS round-trips** (larger is better), under the **16 MiB HCFS ceiling**.

Each 4 MiB chunk is one HCFS blob, framed into 256 KiB CTX frames (doc 25). Overhead is **60 B/frame**;
cold point-read amplification is **one covering frame**, not the whole chunk (the framing flip, doc 23/25):

| Plaintext chunk | AEAD overhead (60 B/frame) | Cold point-read amplification\* | Blobs for a 100 MiB object | Fits 16 MiB cap? |
|---|---|---|---|---|
| 1 MiB | 0.023% (256 KiB frames) | **≤256 KiB (1 frame)** | 100 | yes |
| **4 MiB (D1)** | **0.023%** (16×60 B = 960 B) | **≤256 KiB (1 frame)** | **25** | yes (huge headroom) |
| 8 MiB | 0.023% | ≤256 KiB (or ≤64 KiB tuned) | 13 | yes |
| ~16 MiB | 0.023% | ≤256 KiB | 7 | **no** — framed blob still exceeds the cap; stay ≤ `16 MiB − frames×60 − request overhead` |

\* **The decryption floor is one 256 KiB frame** (tunable to 64 KiB) — this is the crux, and the whole
reason C4 flipped to framing (doc 23/25). Each *frame* is an independent CTX seal, so a cold 1-byte
Range maps to its one covering frame → **one HCFS ranged GET** over that frame's ciphertext span, then
decrypt just that frame. A 1-byte GET transfers+decrypts ~256 KiB, not the 4 MiB chunk — closing the
[`03`](./03-data-plane-cache-streaming.md) §3.7 gap. (CMT-4 holds per frame, so partial reads stay
fully committed.)

**Consequence for the 16 MiB cap:** a full 16 MiB *plaintext* chunk is impossible — a 4 MiB chunk
frames to ~4,195,264 B (16 × 60 B over 4 MiB), well under the `16,777,216` cap, and the request-level
`DefaultBodyLimit(16 MiB)` also has to cover `account_ss58` + multipart boundaries (`13` §Caps). Any
chunk-size policy must keep `chunk_size + frames×60 + request_overhead ≤ 16 MiB`. 4 MiB has ~12 MiB
of slack.

---

## 2. S3 part ↔ chunk ↔ HCFS-blob mapping

```
S3 Object (a key + version)
 └─ Part 1..N                      # S3 unit: simple PUT = 1 part; MPU = N parts (5 MiB–5 GiB, ≤10,000)
     └─ Chunk 0..M-1               # our unit: 4 MiB plaintext, per-part 0-based (01 C9), = one HCFS blob
         = one HCFS blob           # 16 × 256 KiB CTX frames (doc 25); chunk_blobs(role='primary') →
                                   #   blobs.content_hash = blake3(ciphertext), stored as one HCFS object
                                   #   stored as one HCFS named object (name = content_hash), ≤16 MiB
```

Metadata rows (`12`): `parts(object_id,version_id,part_number,size_bytes,etag,chunk_size)` →
`chunks(part_id,chunk_index,cipher_size,plain_size)` → `chunk_blobs(chunk_id,content_hash,role)` →
`blobs(content_hash,hcfs_file_id,cipher_size,refcount,replication_state)`.

### PutObject (simple)

One part, `part_number = 1`. Plaintext split into `ceil(size/4MiB)` chunks (final chunk short),
each sealed into one blob. `part_count = 1`; `object_versions.etag = hex(md5(plaintext))`;
`body_blake3 = blake3(plaintext)` (`12` §2.6). A single PUT is capped by S3 at 5 GiB.

### Small / empty objects

- **< 4 MiB:** one chunk → one blob. No special path.
- **0 bytes:** version row with `size_bytes = 0`, `part_count = 0`, **no** parts/chunks/blobs;
  `etag = md5("")` (`d41d8cd9…`). Satisfies `12`'s `committed_has_etag` CHECK; nothing to fetch on GET.
- ⚑ **D2 — no separate "inline tiny object in Postgres" path.** Even a 1-byte object is one blob.
  (Revisit only if small-object PUT/GET latency to HCFS proves a hot-path problem; flagged, not built.)

### Multipart part ↔ chunk

⚑ **D3 — part-local chunking (chunks never cross a part boundary).** Each `UploadPart` is chunked
independently, `chunk_index` restarting at 0 per part (matches `01` C9 / `03` §0 and the AAD, which
binds `part_number`+`chunk_index`). A part's last chunk is short; part boundaries and chunk
boundaries are otherwise unrelated.

Why part-local, not global re-chunk at Complete:
- Parts arrive **concurrently, out of order, on different nodes** (`10` SSD-staging DaemonSet). A part
  is chunked, sealed, landed, and forwarded to HCFS the moment it arrives; Complete is **metadata-only,
  no byte movement** (`12` §2.13 promote-on-complete). Global 4 MiB re-chunking at Complete would force
  re-reading every staged part in object order — defeating streaming and the staging model.
- The AAD already binds `part_number`, so re-chunking across parts would change AADs and force
  re-encryption. Part-local keeps ciphertext (and thus blobs) reusable across Complete.
- **Cost accepted:** a part's short tail chunk means two uploads of identical plaintext at different
  part sizes chunk differently and won't share blobs. This is moot under D6 (dedup is
  reference/copy-oriented, not convergent — §4), so it costs nothing today.

### Max object size & the S3 limits vs. our chunking

- **The 10,000-part / 5 GiB-part / 5 TiB-object limits bind the client's multipart layer, not our
  chunking.** We accept any legal part and chunk it internally with no S3-style count limit.
- A 5 GiB part → 1,280 chunks; a 5 TiB object → ~1.28 M chunks. Those are `chunks`/`chunk_blobs`/`blobs`
  rows and HCFS objects, not S3 parts — fine (and `12` §5.3 flags hash-by-`object_id` partitioning for
  these unbounded tables). The **per-blob DEK** seals only that blob's ≤64 frames, so the STREAM-nonce
  frame counter (per DEK) never approaches any limit (doc 25 §2.3/§3) — nonce safety is structural, not
  a global-count argument.
- We never originate multipart *toward* HCFS: each chunk is one ≤~4 MiB blob PUT, always under the
  16 MiB cap.

---

## 3. Range GET strategy

Reads go straight to HCFS; there is no cache to consult. The object's plaintext size and each part's
`size_bytes` + `chunk_size` are in Postgres, so a range is planned with **zero storage round-trips**
(mirrors `03` §3.1/§3.2 — the body does no DB work).

### Step 1 — normalize the range at the S3 layer (this is where suffix support lives)

S3 accepts `bytes=A-B`, `bytes=A-` (open), and `bytes=-N` (suffix); **HCFS supports none of the
open/suffix forms** (`13` §2). Resolve everything to an absolute `[A,B]` against the known total size
`T` (`06` §4.1):

- `A-B` → `[A, min(B, T-1)]`; `A-` → `[A, T-1]`; `-N` → `[max(0, T-N), T-1]`.
- `A ≥ T` → **416** with `Content-Range: bytes */T`, `Content-Length: 0` (`06` §4.2).
- **inverted (`B < A`) → full 200**, not 416 (AWS quirk, `06` §4.1). Track `range_was_invalid` so
  headers reflect the full body.

Because we resolve to absolute offsets here, HCFS's suffix-range gap **never surfaces downstream** —
and any Range header we ever send to HCFS is always an absolute `bytes=start-end` (§3.3).

### Step 2 — absolute range → parts → chunks (plan)

Give parts cumulative plaintext offsets; find the parts/chunks intersecting `[A,B]` using each part's
`chunk_size` (`06` §4.3, `03` §3.1). The first and last covered chunk get `slice_start` /
`slice_end_excl` so plaintext is trimmed **after** decryption. Resolve each chunk's blob
(`chunk_blobs → blobs.hcfs_file_id`) in the up-front plan (long/cold plans batch this, `03` §3.2), and
unwrap the DEK once, before the first byte.

### Step 3 — fetch: **map the range to covering frames; one HCFS Range GET per blob**

**D4 (updated — sub-chunk framing ADOPTED, doc 25).** Each 4 MiB blob is 16 × 256 KiB CTX frames
(tunable to 64 KiB), each an independent committing seal `nonce(12)‖ct‖tag(16)‖CT(32)`. Because a
frame's ciphertext size is constant (`frame_plain + 60`), `ct_offset(i) = i × (frame_plain + 60)` is
**deterministic** — so the reader maps a plaintext range `[A,B]` to its covering frames `[f_lo..f_hi]`
within each touched blob and issues **one HCFS Range GET** over exactly those frames'
ciphertext span (`bytes=ct_offset(f_lo)-ct_offset(f_hi+1)-1`), then decrypts only those frames and
trims first/last with `slice_start`/`slice_end_excl`. CMT-4 holds per frame, so partial reads stay
fully committed.

This **closes the [`03`](./03-data-plane-cache-streaming.md) §3.7 cold-range gap**: a 100-byte read
touches one 256 KiB frame (~2,600× amplification) instead of a whole 4 MiB chunk (~42,000×). HCFS's
native Range GET is now load-bearing (this is the design in which it pays off).

- **Frame size is a per-bucket knob** (256 KiB default, 64 KiB for range-heavy workloads) — smaller
  frames = cheaper point reads at slightly more per-frame overhead (60 B/frame).
- **~~OQ-1 (deferred)~~ RESOLVED:** framing is adopted (doc 23 flipped C4; doc 25 froze it), folded
  into the one mandated re-encrypt so there's no second corpus pass. The crypto sign-off covers it.

### Step 4 — concurrency, decrypt, stream

- Fetch the covering blobs **concurrently** with a bounded read-ahead window (⚑ **D5**, default 16,
  reusing `03` §3.5's `HTTP_STREAM_PREFETCH_CHUNKS` — on a cold read this *is* the per-request HCFS
  parallelism), and **emit plaintext strictly in object order** so the `206` body is correct.
- Decrypt each covering **frame** (256 KiB) on a blocking/crypto pool (`03` §3.6, doc 25). Trim the
  first/last frame with `slice_start`/`slice_end_excl`. **First-byte latency = one frame fetch+decrypt.**
- **No cache-fill:** HCFS-served bytes are decrypted and streamed, written nowhere (`03` §3.4).
- **416/inverted/version-id** headers per `06` §4.2–§4.3 (`x-amz-version-id` on both 200 and 206).

### Amplification recap

Cold intra-chunk point read = **one covering 256 KiB frame** (one HCFS ranged GET), not the whole
chunk — the framed model (doc 23/25) that closes the `03` §3.7 gap. A large range fetches exactly the
covering frames across the touched blobs, concurrently.

---

## 4. ETag vs. content-hash vs. body_blake3 (three hashes, on purpose)

| | Algorithm · input | Column | Purpose | Client-visible? |
|---|---|---|---|---|
| **ETag** | `md5(plaintext)` | `object_versions.etag`, `parts.etag` | S3 ETag contract (`If-Match`, tooling) | **yes** |
| **body_blake3** | `blake3(plaintext)` | `object_versions.body_blake3` | the "Arion hash" / integrity surface (public) | yes (header) |
| **content_hash** | `blake3(ciphertext)` | `blobs.content_hash` | HCFS content address + refcount key | no (internal) |
| **blob_id** | opaque, pre-assigned (random uuid) | `blobs.blob_id` | the frame AAD identity (DEK-independent, doc 25 §T3) | no (internal) |
| **dedup key** | `blake3(plaintext)`, owner-scoped | `blob_dedup.plaintext_hash` | per-owner dedup lookup (private) | no (internal) |

⚑ **D6 — confirmed: ETag is MD5 over PLAINTEXT; blob id is BLAKE3 over CIPHERTEXT.** They are
different algorithms over different bytes and must never be conflated (`12` §2.6 vs §2.9; the same
`content_hash ≠ hcfs_file_id ≠ arion_hash` discipline `12` W6 / `02` §2.7).

- **Single PutObject:** `etag = hex(md5(plaintext))`, 32 hex chars, no dash.
- **Multipart:** `etag = hex(md5( md5(plaintext part₁) ‖ … ‖ md5(plaintext partₙ) )) + "-" + N`
  (`12` §2.6, `06` §1.3). Computed over **plaintext parts**, so Complete needs only the stored per-part
  `md5` (16 B each) — no byte re-read, consistent with metadata-only promote (`12` §2.13).
- All three digests are produced in the **same single streaming pass** on the SSD stage (§5): as
  plaintext flows, feed `md5` (ETag) + `blake3` (body_blake3); as each chunk is sealed, `blake3` the
  ciphertext (blob id). No extra pass.

**Why they must differ, and why that is correct.** Per [`crypto-dedup-research.md`](./crypto-dedup-research.md)
(resolved in `12` Q2): a **random per-blob DEK + random nonce**, so **ciphertext is not a deterministic
function of plaintext**. Therefore:
- Two independent PUTs of identical plaintext → **same ETag** (md5 plaintext) and **same body_blake3**,
  but **different `content_hash`** (different ciphertext) → **no convergent/cross-tenant dedup** (this
  is deliberately rejected as an MLE leak). ETag is a plaintext contract; blob id is a ciphertext
  address; they answer different questions.
- Dedup is **reference/copy-oriented**: CopyObject (and future explicit references) share the existing
  blobs and bump `refcount` — O(1), no re-encryption, because the frame AAD binds the opaque `blob_id`,
  not object identity (`25` §5.2). Key-commitment is **intrinsic** to the frame wire (CTX `CT`,
  CMT-4) — there is no separate `PRF(DEK, blob_id)` value; a blob opens under exactly the one DEK that
  sealed it.
- **OQ-2 — DEK granularity: RESOLVED.** DEK is **per-blob** (`12` §2.9), wrapped once under the owner's
  KEK; `12` §2.6's per-object-version `wrapped_dek` was reconciled away. This is what makes
  content-addressed per-owner dedup + O(1) copy work.

---

## 5. Interaction with the SSD-staging write path (doc 10) and dedup/refcount (doc 12)

**Write path (SSD-staging, DECIDED — `10`).** A PUT/UploadPart body streams to the ingest node's NVMe
in one pass that: splits into 4 MiB chunks, AES-256-GCM-seals each (`01`), and computes `md5(plaintext)`
+ `blake3(plaintext)` + per-chunk `blake3(ciphertext)`. Chunks land on SSD with a `meta.json` (the
`03` §2 part-complete contract); the PUT fast-acks on the durable local write (default buckets) or
waits for HCFS (`sync-before-ack` for WORM/object-lock buckets, `10`). A per-node forwarder then POSTs
each chunk to HCFS (`POST /upload`, first field `account_ss58`, `file` = ciphertext, name =
`content_hash`; `13` §Store); on HCFS `200` it flips `blobs.replication_state pending→durable`, records
`hcfs_file_id`, and deletes the `staged_blobs` row (`12` §7). Re-POST is idempotent (HCFS
content-dedup + S3-side refcount, `10`). Every chunk's ciphertext is ≤ ~4 MiB+28 B — always under the
16 MiB HCFS body cap, so no chunk PUT ever risks the cap.

**Dedup/refcount (`12` §2.9–§2.10, §6).** The blob is the dedup + refcount point:
- **Commit / promote:** inserting `chunk_blobs` (finalized) or `upload_part_chunks` (staging) edges
  bumps `blobs.refcount` via trigger; an in-flight MPU's blobs are pinned by the staging edges and
  never GC'd (`12` §2.13). If a chunk's `content_hash` already exists `durable`, the forwarder **skips
  the HCFS PUT** — dedup hit.
- **Delete / overwrite / version-expiry:** drops the `chunk_blobs`/`upload_part_chunks` edges →
  `refcount` decrements. **Only at `refcount = 0`** does an async GC worker (`12` §6, `idx_blobs_gc`)
  tell HCFS to `DELETE` the blob. This is the S3-side refcount that HCFS structurally lacks (`13` §6):
  HCFS deletes a content-hash blob unconditionally, so without our refcount a shared blob would be
  destroyed out from under another reference. Because we give each unique blob a unique HCFS name
  (= its `content_hash`), one HCFS object ⇄ one blob and delete-at-zero is clean (`13` §6, §Verdict).
- Treat HCFS `DELETE` 404 as success (idempotent), per `13` §3.

---

## 6. Worked example — a 100 MiB object (104,857,600 bytes)

### Multipart, 10 parts × 10 MiB (part-local 4 MiB chunks)

- **Part → chunk:** each 10 MiB part → `ceil(10MiB/4MiB) = 3` chunks: `[4 MiB, 4 MiB, 2 MiB]`.
  → **30 chunks → 30 blobs → 30 HCFS objects.**
- **Chunk → blob ciphertext:** a 4 MiB chunk seals to `4,194,304 + 28 = 4,194,332` B (≪ 16 MiB cap ✓);
  a 2 MiB tail chunk → `2,097,180` B ✓. Overhead: `30 × 28 = 840 B` over 100 MiB = **0.0008%**.
- **Blob ids:** `blake3(ciphertext)` of each of the 30 sealed chunks (= `blobs.content_hash`, = HCFS
  object name).
- **ETag:** `hex(md5( md5(pt part₁) ‖ … ‖ md5(pt part₁₀) )) + "-10"`.
- **Complete** is metadata-only: allocate the version (`12` §5.2), promote staging rows into
  `parts`/`chunks`/`chunk_blobs` (bumps 30 refcounts), CAS `current_version`, drop the MPU. No bytes move.

### Simple PUT alternative

1 part, `ceil(100MiB/4MiB) = 25` chunks (all exactly 4 MiB) → **25 blobs**; `etag = hex(md5(100 MiB
plaintext))` (no dash); `body_blake3 = blake3(plaintext)`.

### Range read: `Range: bytes=50000000-50000099` (100 bytes, cold, no cache) — simple-PUT case

1. **Normalize:** `T = 104,857,600`; `[50,000,000, 50,000,099]` — in bounds → `206`.
2. **Plan:** chunk `= floor(50,000,000 / 4,194,304) = 11`; within chunk 11 the offset is `3,862,656`,
   so the covering **frame** = `floor(3,862,656 / 262,144) = 14` (chunk 11's 15th 256 KiB frame),
   which spans plaintext `[46,137,344 + 14·262,144, …)`. Single frame. `slice_start = 3,862,656 −
   14·262,144 = 194,560`, length 100.
3. **Fetch (D4, framed):** compute the frame's deterministic ciphertext offset within chunk 11's blob
   (`ct_offset(i) = i·(262,144 + 60)`) and issue **one HCFS ranged GET** over just that frame's
   ciphertext span (`bytes=ct_offset(14)-ct_offset(15)-1`) — ~262 KB, not the 4 MiB blob.
4. **Decrypt** that one frame (CTX open: commitment-check-first, then GCM; doc 25), slice
   `[194,560 .. +100]`, serve **100 bytes** with `Content-Range: bytes 50000000-50000099/104857600`.

**Amplification:** ~262 KB fetched+decrypted for a 100-byte read (one 256 KiB frame) — the framed model
closes the `03` §3.7 gap (~2,600× vs the ~42,000× a whole-4 MiB-chunk fetch would cost).

### Range read: `bytes=0-52428799` (first 50 MiB)

Touches chunks 0–12. Chunks 0–11 are **fully** covered → one whole-blob HCFS GET each (a full chunk =
all its frames, so a whole-blob fetch is optimal); chunk 12 is **partially** covered → one HCFS ranged
GET over just its covering frames. Up to 16 concurrent (D5), decrypt in order, trim chunk 12's tail,
stream the `206` body in object order. First byte ready after chunk 0.

---

## 7. ⚑ Decisions

- **D1** Plaintext chunk = **4 MiB**, stored per-part, per-bucket tunable, hard-capped so
  `chunk_size + 28 + request overhead ≤ 16 MiB` (16 MiB *plaintext* is impossible).
- **D2** No inline-tiny-object path; even a 1-byte object is one blob (0-byte = no blob).
- **D3** **Part-local chunking** (chunk_index per part); no global re-chunk at Complete.
- **D4 (updated)** Each blob is 256 KiB CTX frames; a range maps to covering frames → **one HCFS
  Range GET** per touched blob over just those frames' ciphertext (deterministic offsets). HCFS
  native Range GET is now load-bearing (doc 25 §5.4).
- **D5** Range reads fetch covering blobs with a bounded concurrent window (default 16), emit in order.
- **D6** **ETag = md5(plaintext)** (single + composite `-N`); **HCFS address = `content_hash` =
  blake3(ciphertext)**; **AAD identity = opaque `blob_id`** (pre-assigned, DEK-independent);
  **`body_blake3` = blake3(plaintext)** (public) + the *private* owner-scoped `blake3(plaintext)`
  dedup key — four distinct values, never conflated (doc 25 T3); dedup is per-owner
  reference/copy-oriented, refcounted, delete-at-zero **with a grace window** (`12` §2.9).

## 8. Open questions

- **OQ-1 — sub-chunk AEAD framing: RESOLVED (adopted).** Blobs are 256 KiB CTX frames; the reader
  issues one HCFS Range GET over the covering frames (doc 23 flipped C4, doc 25 froze it). Folded into
  the one mandated re-encrypt, covered by the crypto sign-off. No longer open.
- **OQ-2 — DEK granularity: RESOLVED (per-blob).** `12` §2.9 carries the per-blob DEK + opaque
  `blob_id`; the per-version `wrapped_dek` was reconciled away. No longer open.
- **OQ-3 — per-bucket chunk-size policy.** Expose chunk size as a bucket setting (range-heavy buckets
  pick smaller) or keep a single global 4 MiB? Per-part storage already supports mixed sizes.
- **OQ-4 — serve-pending reads (default buckets).** During the SSD durability window (`10`), a GET may
  need node-sticky routing to read a not-yet-durable blob from local SSD. The range planner is
  identical, but blob resolution must fall back from `blobs.hcfs_file_id` to `staged_blobs.ssd_path`
  for `pending` blobs. Confirm the read-resolution fallback and routing (out of scope here; `12` §7).
- **OQ-5 — multi-range requests.** `06` §4.2 already declines `multipart/byteranges` (single range
  only). Keep that (return the first/whole range), or add it later?
- **OQ-6 — `role`-based multi-blob chunks (EC).** `chunk_blobs.role` reserves replica/parity blobs per
  chunk. When erasure coding lands, the reader's Step-3 fetch must pick a `role` set and the layout
  gains N blobs per chunk — noted so D4 isn't assumed to be forever 1:1.

## Sources

- HCFS blob contract, caps, Range/suffix/refcount: [`13-hcfs-as-is-integration.md`](./13-hcfs-as-is-integration.md)
  (traced to `hcfs-server` `upload.rs`, `backend.rs`, `router.rs`).
- Chunk/AEAD/AAD/chunk-size: [`01-crypto-envelope.md`](./01-crypto-envelope.md) C1–C9, §1–§3.
- Range/ETag/multipart S3 semantics: [`06-s3-protocol-conformance.md`](./06-s3-protocol-conformance.md) §1.3, §3, §4.
- Cold-range gap & streaming/prefetch: [`03-data-plane-cache-streaming.md`](./03-data-plane-cache-streaming.md) §3.
- `blobs`/`chunk_blobs`/refcount/promote-on-complete: [`12-schema-design.md`](./12-schema-design.md) §2.8–§2.13, §6.
- SSD-staging write path: [`10-write-path-decision.md`](./10-write-path-decision.md).
- Dedup model: [`crypto-dedup-research.md`](./crypto-dedup-research.md) (via `12` Q2).
- S3 multipart limits: [Amazon S3 multipart upload limits](https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html).
