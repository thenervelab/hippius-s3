# 23 — Chunking & encrypted range-read strategy: prior art and the sub-chunk-framing call

**Status:** research memo with cited recommendations. It answers the three open
decisions carried by [`16-object-chunk-layout.md`](./16-object-chunk-layout.md)
— **C3** (chunk size), **C4** (sub-chunk AEAD framing, = doc 16 OQ-1 / doc 03 OQ-1),
**C5** (small objects + S3 limits mapping) — by grounding them in how comparable
systems size their storage blocks and their AEAD segments, and in the exact AWS S3
byte-range contract we are emulating.

**What we are designing inside (from the prior docs, not re-litigated here):**

- Objects are AES-GCM-sealed chunks stored as **content-addressed ciphertext blobs
  in HCFS**; reads go **straight to HCFS** (no local read cache);
  **1 chunk = 1 blob = one AEAD seal**, so today the decrypt floor is a whole chunk
  (`16` §0, D4; `03` §3.7).
- **HCFS per-blob usable cap ≈ 16 MiB**, HCFS supports a native **absolute** Range
  GET (`bytes=start-end` / `start-`) but **no suffix range**, and has **no
  storage-layer refcount** (`13` §2/§Caps/§6; `16` §0).
- Default plaintext chunk = **4 MiB**, AEAD overhead 28 B/chunk
  (`nonce12‖ct‖tag16`), stored per-part, never read from config (`16` §1, D1; `01`).
- The **new** chunk-crypto suite is being frozen as **CTX (CMT-4) committing AEAD
  over AES-256-GCM**, with AAD **rebound to blob identity** (`blob_id ‖ chunk_index`)
  and a **re-encrypt migration** — commitment/AAD change means there is **no
  in-place upgrade** (`21` §5–§6).

**Reads first:** [`16`](./16-object-chunk-layout.md) (the layout + D1–D6/OQ-1 this
memo resolves), [`03`](./03-data-plane-cache-streaming.md) §3.7 (the cold-range
gap), [`13`](./13-hcfs-as-is-integration.md) (HCFS blob contract + Range/suffix),
[`21`](./21-committing-aead-construction.md) (the CTX freeze this memo must compose
with), [`01-crypto-envelope.md`](./01-crypto-envelope.md) (byte-exact envelope),
[`06-s3-protocol-conformance.md`](./06-s3-protocol-conformance.md) §4 (Range rules).

---

## 1. AWS S3 — the exact limits and Range contract we emulate

### 1.1 Object / part / count limits (verified, Sept 2026)

| Limit | Value | Source |
|---|---|---|
| Multipart **part size** | **5 MiB min** (except the **last part**, no minimum) … **5 GiB max** | AWS S3 "Multipart upload limits" [^qfacts] |
| **Parts per upload** | **10,000** | [^qfacts] |
| **Max object** | **5 TiB** (a single PUT is capped at 5 GiB; larger needs MPU) | [^qfacts] |
| Multipart-upload part-number range | 1–10,000 | [^qfacts] |

These bind the **client's MPU layer**, not our internal chunk/frame layer — we
accept any legal part and sub-divide it ourselves (`16` §2). The Storj and
cloudsqale write-ups restate the same 5 MiB/5 GiB/10,000 envelope and the "size
your part so `part_size × 10,000 ≥ object_size`" corollary that clients hit for
very large objects.[^storj][^cloudsqale]

### 1.2 Range semantics AWS actually serves

- **Single range only.** *"Amazon S3 doesn't support retrieving multiple ranges of
  data per `GET` request."*[^s3get] A `Range: bytes=0-99,200-299` is **not** honored
  as `multipart/byteranges` — S3 serves a single contiguous range (or the whole
  object). This validates `06` §4.2's decision to decline `multipart/byteranges`;
  it is **AWS-faithful**, and it removes a whole class of work from our range planner.
- **Forms:** `bytes=A-B`, open `bytes=A-`, and **suffix `bytes=-N`**. RFC 9110 §14
  defines all three plus the `int-range`/`suffix-range` grammar and the
  `multipart/byteranges` media type for the multi-range case S3 declines.[^rfc9110]
- **416** when the range is unsatisfiable (start ≥ object size), with
  `Content-Range: bytes */T`.[^s3get][^drdroid]
- **Inverted range** (`B < A`) → AWS returns the **full 200**, not 416 (the quirk
  `06` §4.1 already encodes).
- **How AWS serves an arbitrary range at ~constant cost:** the client cannot observe
  S3's internal layout — RGW-style striping or S3's own internal chunking is opaque;
  a range GET returns exactly the requested bytes and the *only* client-visible
  signals are latency, throughput, and correctness. **This is the bar for "tight
  emulation": correct bytes always; cheap seeks as a performance property** (see §5).

**Consequence for us:** HCFS has **no suffix range** (`13` §2), so **suffix support
must live in our S3 layer** — resolve `-N`/`A-` to an absolute `[A,B]` against the
Postgres-known total size, then only ever send HCFS an absolute `bytes=start-end`
(`16` §3 step 1). That resolution is independent of C3/C4 and already decided.

---

## 2. Prior art — two different granularities, don't conflate them

Every comparable system has **two** size knobs, and the literature keeps them
distinct. Our design has the same two:

- **Storage block / stripe** — the durable unit written to the backend. Ours is the
  **chunk = blob (4 MiB)**. Analogues below cluster **1–8 MiB**.
- **AEAD segment / frame** — the *cryptographic* unit that bounds how little you can
  decrypt for a random read. Ours is **today the whole chunk**; the systems that
  serve encrypted ranges well use a **much smaller** frame (**4–256 KiB**).

### 2.1 Storage-block sizing (the C3 peer group)

| System | Storage block | Notes | Source |
|---|---|---|---|
| **Ceph RGW** (S3 gateway over RADOS) | **4 MiB** stripe (`rgw_obj_stripe_size` / `rgw_max_chunk_size`) — head + tail RADOS objects | Closest structural analog: an S3 gateway that splits objects into fixed backend objects. A 4 MiB range = 1×4 MiB IO (replicated) or 4×1 MiB (4+2 EC) | [^cephstripe][^cephchunk] |
| **Garage** | **1 MiB** default (`block_size`), tunable to 10 MiB | Chosen for latency: 1 MiB ≈ 8 ms on 1 Gbps; docs say raise to 10 MiB for large-file/fast-net workloads | [^garagecfg][^garageperf] |
| **SeaweedFS** | **~8 MiB** chunk (flexible 1–10 MiB); manifest chunk indexes ≤1000 chunks | "keep the whole chunk in memory; don't make too many tiny chunks" | [^swfs] |
| **restic** | CDC blobs **512 KiB–8 MiB, ~1 MiB avg** (Rabin, 20-bit target); packed into larger pack files | Content-defined, dedup-oriented — not fixed-size like ours | [^resticchunker][^resticcdc] |
| **hippius-s3 (D1)** | **4 MiB** chunk = blob | Under HCFS 16 MiB cap with ~12 MiB slack | `16` §1 |

**Takeaway:** 4 MiB is squarely in the peer range and *exactly* matches the most
structurally-similar system (Ceph RGW's default stripe). Garage sits lower
(latency-tuned for slow WAN), SeaweedFS higher. Nothing argues for moving off 4 MiB.

### 2.2 AEAD-segment sizing (the C4 peer group)

| System | AEAD segment/frame | Per-segment construction | Random-access? | Source |
|---|---|---|---|---|
| **MinIO SSE (DARE)** | **64 KiB** package (`65536` B) | Independent AEAD seal per package; per-object key | **Yes** — decrypt only the packages a range touches | [^miniosec][^miniodocs] |
| **Google Tink Streaming AEAD** (AES-GCM-HKDF) | **4 KiB** or **1 MiB** presets (`AES256_GCMHKDF_1MB`, `…_4KB`); configurable | `Header ‖ C_0‖…‖C_{n-1}`; IV = `NoncePrefix(7)‖ctr(4, BE)‖final-byte`; 16 B tag/seg | **Yes** — "decrypt `M_i` from `C_i` without decrypting other segments" | [^tinkaesgcm][^tinkstream] |
| **AWS Encryption SDK** | **4 KiB** default frame | AES-GCM per frame, **deterministic IV** = `frameID`; 2³² frame ceiling ⇒ ≤2⁴⁴ B/key | Framed = independently decryptable parts | [^esdkblog][^esdkdefaults] |
| **age** | **64 KiB** chunk | ChaCha20-Poly1305/STREAM; nonce = `ctr(11, BE)‖final-byte`; 16 B tag/chunk | STREAM ⇒ per-chunk | [^age] |
| **Tahoe-LAFS** | **128 KiB** segment | "only the segments that overlap the requested range are downloaded" | **Yes** (since 1.8.0) | [^tahoe] |
| **hippius-s3 today** | **whole chunk (4 MiB)** | one AES-GCM seal per chunk | **No** — 1-byte read = 4 MiB decrypt | `16` D4; `03` §3.7 |

**Takeaway:** **every** system that serves *encrypted* byte ranges efficiently
frames the ciphertext into small, independently-sealed segments of **4 KiB–128 KiB**
(64 KiB is the modal choice: MinIO, age). The **STREAM** construction — a
`NoncePrefix ‖ counter ‖ final-byte` per-segment nonce over a per-object key — is
the near-universal pattern (Tink, age, AWS ESDK all variants of it), precisely
because it gives cheap seeks without nonce-reuse risk. **We are the outlier**: our
AEAD unit (4 MiB) equals our storage unit, which is why our range reads amplify.

---

## 3. C3 — Chunk (storage-block) size: **confirm 4 MiB**

**Decision: keep the plaintext chunk = 4 MiB** (`16` D1), per-part-stored,
per-bucket tunable, hard-capped so `chunk_size + overhead + request_overhead ≤ 16 MiB`.

Grounding:

1. **Prior art brackets it and the closest analog matches it.** Ceph RGW — an S3
   gateway striping objects into fixed backend objects, exactly our shape — defaults
   to **4 MiB** (`rgw_obj_stripe_size`).[^cephstripe] Garage 1 MiB and SeaweedFS
   ~8 MiB bracket us; restic averages ~1 MiB but is CDC/dedup-driven, a different
   goal.[^garagecfg][^swfs][^resticchunker]
2. **The binding tradeoff is not AEAD overhead** (0.00067 % at 4 MiB) — it is **cold
   intra-chunk read-amplification** (smaller better) vs **blob/row count + HCFS
   round-trips** (larger better), under the **16 MiB HCFS ceiling** (`16` §1). 4 MiB
   → 25 blobs for a 100 MiB object with ~12 MiB of cap headroom; ~16 MiB plaintext
   is impossible (seals 28 B over the cap).
3. **Chunk size does not fix the range-amplification problem** — that is **C4's** job
   (§4). Shrinking the chunk to, say, 1 MiB (Garage-style) would cut point-read
   amplification 4× but quadruple blobs/rows/round-trips and *still* leave every
   sub-chunk range fetching a whole (now 1 MiB) blob, because the AEAD unit still
   equals the blob. The right lever is a **smaller AEAD frame inside the 4 MiB blob**,
   not a smaller blob.

**Recommendation:** confirm **4 MiB** as the storage-block/blob size. Leave it a
per-bucket knob (`16` OQ-3) for pathological workloads, but do **not** reach for a
smaller chunk as a range-performance fix — adopt framing (§4) instead.

---

## 4. C4 — Sub-chunk AEAD framing: **ADOPT NOW, folded into the doc-21 CTX freeze**

This is the key call. **Recommendation: adopt a segmented-AEAD scheme now** — fixed
`256 KiB` frames inside each 4 MiB blob, each an independent committing seal — rather
than shipping whole-chunk fetch (D4) and revisiting. The decisive reason is not
range performance in the abstract; it is **timing**: doc 21 already forces a
**one-time re-encrypt of every blob** to move to the CTX committing suite, and that
migration is **the** free opportunity to also introduce framing. Adopting
CTX-over-whole-chunk now and framing later means **re-encrypting the entire corpus
twice**.

### 4.1 Does "very tight AWS emulation" *require* efficient sub-object ranges?

Split the question, because the honest answer is two-part:

- **Correctness / behavioral fidelity: NO.** Whole-chunk fetch (D4) is already
  byte-correct — the planner trims plaintext after decryption, so a client gets
  exactly the requested range with the right `206`/`Content-Range` (`16` §3). A
  conformance suite that checks *bytes and headers* passes without framing.
- **Performance fidelity: EFFECTIVELY YES.** AWS serves an arbitrary range at a cost
  the client cannot distinguish from a small read (§1.2), and **real S3 workloads
  depend on that**: Parquet/ORC readers seek to the footer then to individual column
  chunks; HLS/DASH players issue thousands of small forward ranges; `zip`/OCI tooling
  reads a central directory then scattered entries; `Range`-based resumable
  downloaders probe. Every one of these is a **cold intra-chunk point read**, and
  under D4 each pays **one whole 4 MiB fetch+decrypt**. A gateway that is "very tight"
  in behavior but amplifies every seek ~42,000× (§4.2) is not tight where these
  clients feel it. Prior art agrees unanimously: MinIO, Tink, age, AWS ESDK and Tahoe
  all frame, specifically so encrypted ranges cost ~one frame, not one object.[^miniosec][^tahoe]

So: framing is not required to *pass* S3 semantics, but a genuinely S3-grade
encrypted gateway needs it to *behave like S3* under the range-heavy access patterns
that dominate analytics and media.

### 4.2 The quantified cost of deferring

For a **100-byte cold Range** (the `16` §6 worked example), fetched+decrypted bytes:

| Design | Frame | Bytes moved+decrypted for 100 B | Amplification | HCFS Range GET used? |
|---|---|---|---|---|
| **Defer (D4, whole chunk)** | 4 MiB | **4,194,332 B** | **~42,000×** | No (would return partial ciphertext of one seal — useless) |
| Frame 256 KiB (recommended) | 256 KiB | ~262,204 B | ~2,600× | **Yes** — one aligned `bytes=start-end` |
| Frame 64 KiB (MinIO/age parity) | 64 KiB | ~65,596 B | ~640× | **Yes** |
| Frame 4 KiB (Tink/ESDK min) | 4 KiB | ~4,124 B | ~41× | Yes (many tiny seals) |

Deferring keeps every intra-chunk range at **one whole 4 MiB chunk** (`16` §7 D4,
the `03` §3.7 status quo). For a range-heavy client this is multiplicative: a Parquet
scan touching 40 column chunks scattered across a 1 GiB object fetches **40 × 4 MiB =
160 MiB** to read perhaps a few MiB of columns; with 256 KiB frames it fetches ~40 ×
(column-span rounded up to frame) — often **>10× less transfer and CPU**, straight
off the HCFS bill and TTFB. Framing is also the **only** design in which HCFS's
native Range GET (`13` §2) is worth anything: it pays off exactly when the AEAD unit
is smaller than the fetch unit (`16` §3 step 3).

### 4.3 Why *now*, not later — the migration-coupling argument (decisive)

Doc 21 is explicit that moving to the committing suite is a **decrypt-and-re-encrypt
migration**, not a rewrap: "commitment binds the ciphertext to the DEK, and the AAD
is *also* changing … there is no in-place upgrade and no rewrap-only shortcut"
(`21` §6). That migration already: reads OLD chunks → re-chunks into content-addressed
blobs → **re-seals** under a fresh DEK + the new blob-identity AAD.

**Introducing frames costs nothing extra at that step** — the re-seal simply emits N
frames instead of one seal. Deferring framing means the corpus is re-encrypted once
for CTX (whole-chunk seals) and **again** later to add frames. Because framing and
committing-AAD are both *ciphertext-format* changes gated by the same `enc_suite_id`
mechanism (`21` §5.1, fact 5), the clean move is to **freeze the new suite as
`hip-enc/aes256gcm-ctx-frames-v1` = CTX-over-frames** from the start. **The C4
decision is therefore coupled to the doc-21 crypto-freeze and should be made with
it, before the suite id is minted.**

### 4.4 Framing sketch (composes with doc 21 CTX and content-hash blob identity)

Frame the 4 MiB blob into fixed-size, independently-sealed frames, mirroring
Tink/age STREAM but reusing doc 21's CTX per frame.

**Frame size.** **256 KiB plaintext default** (16 frames per 4 MiB blob) — a balance
between MinIO/age's 64 KiB (finest S3-grade granularity) and Tink's 1 MiB (fewest
seals). Store it per-part alongside `chunk_size` so readers never assume it (same
discipline as `16` D1). Expose as a per-bucket knob: range-heavy buckets pick 64 KiB.

**Per-frame layout** (frame `i` in blob with content id `blob_id`):

```
frame_i = N_i ‖ ct_i ‖ T_i ‖ CT_i          (byte-pinned in doc 25 — this is the frozen layout)
  N_i  = 12-byte STREAM nonce = prefix(7, random per-blob) ‖ frame_index(4 BE) ‖ final_flag(1)
         (exact Tink geometry — safe here: per-blob DEK + encrypt-once ⇒ each (DEK, i) unique)
  ct_i = AES-256-GCM ciphertext of frame i's plaintext
  T_i  = 16-byte GCM tag
  CT_i = 32-byte CTX commitment = BLAKE3( LP(DOMAIN_SEP) ‖ LP(DEK) ‖ LP(N_i) ‖ LP(A_i) ‖ LP(T_i) )  (doc 25 §1.3)
  A_i  = blob-identity AAD = blob_id ‖ LE32(frame_index i) ‖ suite_id
         (blob_id = OPAQUE, pre-assigned, DEK-independent — NOT blake3(ciphertext); doc 25 §T3)
Blob = frame_0 ‖ frame_1 ‖ … ‖ frame_{F-1}     (last frame short)
```

**Overhead:** 12 + 16 + 32 = **60 B/frame** (same per-unit cost doc 21 already
accepts per chunk). At 256 KiB frames that is 16 × 60 = **960 B per 4 MiB blob ≈
0.023 %** — negligible against the 16 MiB HCFS cap (a 4 MiB chunk framed at 256 KiB
seals to ~4,195,264 B, still ~12 MiB under cap). At 64 KiB frames, 64 × 60 =
3,840 B ≈ 0.09 %.

**How it interacts with the three fixed invariants:**

1. **CTX / committing AEAD (doc 21).** CTX commits *per AEAD invocation*; framing just
   makes the invocation per-**frame** instead of per-chunk (`21` §5.3 applies
   verbatim). Each frame is a full **CMT-4** unit — the partitioning-oracle closure
   (`21` §1) holds at frame granularity, and the blob-identity AAD now also binds the
   **frame index**, so a frame provably belongs to its `(blob_id, i)` slot. **No new
   crypto primitive** — same `aes-gcm` + `sha2` glue doc 21 already specifies. The
   deterministic STREAM nonce is **frozen** (doc 25 §2) — safe under per-blob DEK +
   encrypt-once, and it is what doc 25's PoC + golden vectors verify.
2. **Blob identity (three distinct values — doc 25 §T3).** The AAD binds an **opaque,
   pre-assigned `blob_id`** (DEK-independent, known at seal time). Separately, the blob
   is stored as one HCFS object addressed by `content_hash = blake3(ciphertext of the
   whole framed blob)` (`16` §0; `13` §1), and dedup keys off a private owner-scoped
   `blake3(plaintext)`. `blake3(ciphertext)` is the storage address only — **not** the
   AAD `blob_id` (that would be circular). The reader needs frame
   boundaries to be **deterministic** (they are: fixed frame ciphertext size ⇒
   `ciphertext_offset(i) = i × (frame_plain + 60)`), so it can map a plaintext range
   → covering frames → an **absolute HCFS `bytes=start-end`** over just those frames'
   ciphertext span, then decrypt only them.
3. **Dedup granularity.** The blob stays the dedup/refcount unit (`16` D6, §5). Doc 16
   OQ-1's worry that framing "lowers dedup granularity" is **moot under the chosen
   dedup model**: dedup is reference/copy-oriented (random DEK+nonce ⇒ no convergent
   dedup, `16` D6), so nothing dedups at sub-blob level anyway. Framing changes the
   blob's internal bytes, not what the blob *is*.

**Read path change (small).** `16` §3 step 3 (D4) becomes: a range touching frames
`[f_lo..f_hi]` of a blob issues **one HCFS Range GET** for the ciphertext span
`[offset(f_lo), offset(f_hi+1))`, then decrypts those frames and trims the first/last
with `slice_start`/`slice_end_excl`. Whole-blob GET remains the path for full-object
reads. First-byte latency drops from one chunk to **one frame**.

**Frame-count safety.** 4 MiB / 256 KiB = 16 frames/blob ⇒ 16 AEAD invocations per
per-blob DEK — astronomically under the AES-GCM 2³² nonce-per-key bound and Tink's
2³² segment-counter overflow limit[^tinkstream] and AWS ESDK's 2³² frame ceiling
(≤2⁴⁴ B/key).[^esdkblog] Even a 5 TiB object at 64 KiB frames is ~21 M frames total
but only ≤16–64 **per DEK** (per-blob DEK), so no per-key ceiling is ever approached.

**If instead deferring** (not recommended): keep D4 verbatim, mint the CTX suite as
whole-chunk, and accept the §4.2 amplification plus a **second** future re-encrypt to
add frames. Only justified if the crypto reviewer wants the smallest possible v1
crypto surface and range-heavy workloads are provably absent — unlikely for an
S3-compatible product.

---

## 5. C5 — Small objects and the S3-limits mapping

### 5.1 Small / empty objects (framing does not add a special case)

- **0 bytes:** version row, `part_count=0`, **no blob** (`16` D2). `etag = md5("")`.
- **`< 1 frame` (e.g. 100 B, ≤256 KiB):** one blob, **one short frame**. Framing adds
  nothing for tiny objects — a sub-frame object is just a single frame whose plaintext
  is < `frame_size` (identical to how the last frame is always short). First-byte =
  that one short frame's fetch+decrypt, **strictly better** than whole-chunk.
- **`< 4 MiB`:** one blob, `ceil(size / frame_size)` frames.
- **No inline-in-Postgres path** (`16` D2 preserved): even a 1-byte object is one
  blob; revisit only if small-object PUT/GET latency to HCFS proves a hot-path
  problem. MinIO/Tink/age likewise have no tiny-object special path — a small file is
  just a one-segment stream.[^miniosec][^age]

### 5.2 Mapping S3's limits onto our chunk/frame layer

The S3 numbers (§1.1) bind the **client MPU layer**; our chunk and frame units live
below it and carry **no S3-style count limit** (`16` §2):

| S3 unit / limit | Maps to | Our layer's constraint |
|---|---|---|
| Part **5 MiB–5 GiB**, **≤10,000 parts** | **part → chunks** (part-local, 4 MiB, `16` D3) | none — a 5 GiB part → 1,280 chunks; chunks are `chunks`/`blobs` rows, not S3 parts |
| Last part **no minimum** | last chunk of a part is short; its last **frame** is short | clean — short tail is normal at both levels |
| **Max object 5 TiB** | ~1.28 M chunks → ~21 M frames (at 256 KiB) | `12` §5.3 hash-partitions the unbounded `chunks`/`blobs` tables; per-**DEK** frame count stays ≤16–64 (§4.4) |
| Single PUT ≤ 5 GiB | one part, `ceil(size/4 MiB)` chunks, each `ceil(chunk/frame)` frames | under HCFS 16 MiB blob cap always |
| **Single range only** (no `multipart/byteranges`) | one contiguous `[A,B]` → one frame span per covered blob | matches `06` §4.2; AWS-faithful (§1.2) |
| Suffix `-N` / open `A-` | resolved to absolute `[A,B]` in the S3 layer | HCFS never sees a suffix range (`16` §3 step 1; `13` §2) |
| **416** on `A ≥ T` | planner returns 416 before any HCFS call | `06` §4.2 |

We **never originate multipart toward HCFS**: each chunk is one ≤~4 MiB (framed) blob
PUT, always under the 16 MiB cap (`16` §2). The 10,000-part / 5 GiB-part / 5 TiB
ceilings are the client's problem to respect; our internal fan-out is bounded only by
the tables and the read-ahead window (`16` D5).

---

## 6. Recommendations (summary)

- **C3 — Chunk size: confirm 4 MiB.** Matches Ceph RGW's default stripe (the closest
  analog), brackets Garage (1 MiB) / SeaweedFS (8 MiB), sits ~12 MiB under the HCFS
  cap. Keep it a per-bucket knob but do **not** use a smaller chunk as a range fix.
- **C4 — Sub-chunk AEAD framing: ADOPT NOW, as CTX-over-256-KiB-frames.** Prior art is
  unanimous that encrypted range-serving stores frame the ciphertext (MinIO 64 KiB,
  Tink, age, AWS ESDK, Tahoe). Deferring keeps every intra-chunk seek at ~42,000×
  amplification and forgoes HCFS's native Range GET. **Decisive:** doc 21 already
  mandates a full re-encrypt for CTX; folding frames into that migration is free,
  whereas deferring forces a **second** corpus re-encrypt. Freeze the new suite as
  CTX-over-frames before the `enc_suite_id` is minted. Default frame **256 KiB**
  (per-part-stored, per-bucket-tunable to 64 KiB); per-frame `N_i‖ct_i‖T_i‖CT_i`
  (60 B), deterministic STREAM nonce, CTX commitment per frame, blob-identity AAD
  binding the frame index; blob content-hash identity and blob-level dedup unchanged.
- **C5 — Small objects & limits.** No tiny-object special path; a sub-frame object is
  one short frame. S3's 5 MiB/5 GiB/10,000/5 TiB limits bind the client MPU layer,
  not our chunk/frame layer; per-DEK frame counts stay trivially safe.

### Reviewer items — all RESOLVED by doc 25 (kept for provenance)

- **STREAM nonce: FROZEN** — deterministic `prefix(7)‖frame_index(4 BE)‖final_flag(1)` (doc 25 §2), safe under per-blob DEK + encrypt-once; verified by the PoC.
- **Frame size: FROZEN** — **256 KiB default, tunable to 64 KiB** per bucket (doc 25 §1); per-part-stored so mixed sizes read fine.
- **Suite id: FROZEN** — `hip-enc/aes256gcm-ctx-frames-v1`; read-path dispatch on `enc_suite_id` (OLD whole-chunk-GCM vs NEW framed-CTX) stands.
- **Migration emits frames: CONFIRMED** (doc 25 §5.3) — the corpus is re-encrypted exactly once into frames (§4.3).

The only remaining crypto gate is the Phase-0 code review of the wrapper against `ctx-poc/`.

---

## Sources

Retrieved September 2026.

**AWS S3 limits & Range**
- [^qfacts]: Amazon S3 — Multipart upload limits (5 MiB–5 GiB part, 10,000 parts, 5 TiB object). https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
- [^s3get]: Amazon S3 — `GetObject` API ("Amazon S3 doesn't support retrieving multiple ranges of data per GET request"; 416 on unsatisfiable range). https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
- [^storj]: Storj Docs — Optimizing Multipart Upload Part Size. https://storj.dev/dcs/api/s3/multipart-upload/multipart-part-size
- [^cloudsqale]: cloudsqale — S3 Multipart Upload 5 MB Part-Size Limit. https://cloudsqale.com/2020/05/27/s3-multipart-upload-5-mb-part-size-limit/
- [^drdroid]: S3 `InvalidRange` / 416 diagnosis. https://drdroid.io/stack-diagnosis/s3-invalidrange-error-encountered-when-trying-to-access-an-object-in-s3

**HTTP Range**
- [^rfc9110]: RFC 9110 §14 — Range Requests (int-range/suffix-range grammar, `multipart/byteranges`, 206/416, Content-Range). https://www.rfc-editor.org/rfc/rfc9110#section-14 · overview https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/Range_requests

**Storage-block sizing (C3 peers)**
- [^cephstripe]: Ceph — RGW striping / `rgw_obj_stripe_size` 4 MiB default. https://oneuptime.com/blog/post/2026-03-31-rook-rgw-stripe-chunk-sizes/view · https://docs.ceph.com/en/latest/radosgw/config-ref/
- [^cephchunk]: Ceph.io — RGW Deep Dive (head/tail RADOS objects, 4 MiB chunk, EC read fan-out). https://ceph.io/en/news/blog/2025/rgw-deep-dive-2/
- [^garagecfg]: Garage — configuration (`block_size` default 1 MiB). https://garagehq.deuxfleurs.fr/documentation/reference-manual/configuration/
- [^garageperf]: Garage blog — Confronting theoretical design with observed performance (block-size latency rationale). https://garagehq.deuxfleurs.fr/blog/2022-perf/
- [^swfs]: SeaweedFS — Large File Handling / Data Structure for Large Files (~8 MiB chunk, manifest). https://github.com/seaweedfs/seaweedfs/wiki/Large-File-Handling
- [^resticchunker]: restic `chunker` — CDC 512 KiB–8 MiB, ~1 MiB avg (20-bit). https://pkg.go.dev/github.com/restic/chunker
- [^resticcdc]: restic — Introducing Content-Defined Chunking. https://restic.net/blog/2015-09-12/restic-foundation1-cdc/

**AEAD-segment sizing (C4 peers)**
- [^miniosec]: MinIO — Server-Side Encryption / DARE ("Secure Channel splits object content into chunks of a fixed size of 65536 bytes"). https://github.com/minio/minio/blob/master/docs/security/README.md
- [^miniodocs]: MinIO — Server-Side Encryption of Objects (SSE overview, per-object OEK). https://docs.min.io/community/minio-object-store/administration/server-side-encryption.html
- [^tinkaesgcm]: Google Tink — AES-GCM-HKDF Streaming (segment structure `Header‖C_0‖…`; per-segment IV `NoncePrefix(7)‖ctr(4)‖final-byte`; decrypt `M_i` from `C_i` alone). https://developers.google.com/tink/streaming-aead/aes_gcm_hkdf_streaming
- [^tinkstream]: Google Tink — Streaming AEAD (`AES256_GCMHKDF_1MB` = 1 MiB, `…_4KB` = 4 KiB segments; 2³² segment-counter overflow caveat). https://developers.google.com/tink/streaming-aead
- [^esdkblog]: AWS — "How AWS KMS and the AWS Encryption SDK overcome symmetric encryption bounds" (4 KiB default frame; deterministic per-frame IV; 2³² frames ⇒ ≤2⁴⁴ B/key). https://aws.amazon.com/blogs/security/how-aws-kms-and-aws-encryption-sdk-overcome-symmetric-encryption-bounds/
- [^esdkdefaults]: AWS Encryption SDK — default frame length 4096 B. https://aws-encryption-sdk-python.readthedocs.io/en/latest/generated/aws_encryption_sdk.internal.defaults.html · message format https://docs.aws.amazon.com/encryption-sdk/latest/developer-guide/message-format.html
- [^age]: age spec (C2SP) — payload split into 64 KiB chunks, ChaCha20-Poly1305/STREAM, nonce = `ctr(11, BE)‖final-byte`. https://c2sp.org/age@v1.1.0
- [^tahoe]: Tahoe-LAFS — File Encoding / Performance (128 KiB segment; since 1.8.0 only segments overlapping the requested range are downloaded). https://tahoe-lafs.readthedocs.io/en/latest/specifications/file-encoding.html · https://tahoe-lafs.org/trac/tahoe-lafs/wiki/Performance

**Repository context (consumed, not re-litigated):**
[`16-object-chunk-layout.md`](./16-object-chunk-layout.md),
[`03-data-plane-cache-streaming.md`](./03-data-plane-cache-streaming.md),
[`13-hcfs-as-is-integration.md`](./13-hcfs-as-is-integration.md),
[`21-committing-aead-construction.md`](./21-committing-aead-construction.md),
[`01-crypto-envelope.md`](./01-crypto-envelope.md),
[`06-s3-protocol-conformance.md`](./06-s3-protocol-conformance.md).
