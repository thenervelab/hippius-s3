# 21 — Key-committing AEAD construction for chunk encryption

**Status:** Implementation-grade recommendation for the greenfield Rust S3 product.
This is the document a crypto reviewer ratifies before the *new* (blob-dedup)
chunk-encryption format is frozen. Normative where it says "MUST"; the concrete
pick is §5.

> **⚠️ SUPERSEDED FOR BYTE LAYOUT by [`25-crypto-verification.md`](./25-crypto-verification.md) (2026-09-15).**
> Doc 25 is the verified, frozen spec (suite `hip-enc/aes256gcm-ctx-frames-v1`) and is authoritative
> wherever this doc's §5 body disagrees. In particular: (1) the construction is **CTX per 256 KiB
> frame**, not per 4 MiB chunk; the nonce is the **STREAM layout** `prefix(7)‖frame_index(4 BE)‖final_flag(1)`,
> **not** a random 96-bit per-blob nonce; AAD = `blob_id‖LE32(frame_index)‖suite_id`. (2) **`blob_id`
> is an OPAQUE, pre-assigned, DEK-independent id (doc 25 §T3) — it is NOT "plaintext-derived"** as §6
> below states; the plaintext-derived value is a *separate*, private, owner-scoped dedup key. This
> doc's §5.1–§5.4 reasoning (CTX = CMT-4, D-pragmatic wire `N‖ct‖T‖CT`, commitment-check-first OPEN
> order, injective length-prefixed preimage) remains correct and is what doc 25 verified.
> (3) **Suite id** is `hip-enc/aes256gcm-ctx-frames-v1` — the body's `hip-enc/aes256gcm-ctx-v1` is
> stale. (4) **Commitment hash is BLAKE3** (doc 25 §T1); the body's SHA-256 default is superseded
> (SHA-256 is sound but not the frozen choice). (5) **§7's "open questions" Q1–Q9 are all RESOLVED**
> by doc 25 — do not treat the ⚑ markers as live; the only remaining crypto gate is the Phase-0
> code review of the wrapper against `ctx-poc/`.

**Prerequisite reading (both present and consumed):**
[`crypto-dedup-research.md`](./crypto-dedup-research.md) (the decision that we do
per-owner dedup with a shared/looked-up DEK, and that this *requires* a
key-committing chunk AEAD) and [`01-crypto-envelope.md`](./01-crypto-envelope.md)
(the byte-exact current envelope: AES-256-GCM chunks, `nonce‖ct‖tag`, per-object
DEK, `enc_suite_id` per object-version). This doc slots the committing AEAD into
that envelope and specifies the migration off the non-committing format.

---

## 0. The facts this recommendation is built on

From the two prerequisite docs, anchored to their claims:

1. **Current chunk AEAD (the OLD, non-committing format).** AES-256-GCM, 12-byte
   **random** nonce prepended, 16-byte tag suffix — wire `nonce(12) ‖ ct ‖ tag(16)`
   (01 §C1/C2). Single registered suite `hip-enc/aes256gcm` (01 §1.1). The chunk
   AAD (V2) currently binds **object** identity:
   `LE16(len bucket_id)‖bucket_id‖LE16(len object_id)‖object_id‖LE32(part_number)‖LE32(chunk_index)`
   (01 §C3).
2. **DEK / envelope.** Random 32-byte DEK, AES-256-GCM-wrapped under a per-bucket
   KEK (`nonce‖ct‖tag`, 60 bytes), KEK wrapped by OVH KMS (opaque JWE) or a local
   key (01 §4, §C4–C7). Blobs are **encrypted once**; CopyObject/references reuse
   existing ciphertext (dedup §Q4, §Rec).
3. **The dedup decision (why we are here).** Adopt **Option A**: per-owner dedup,
   random DEK + random nonce, refcounted references, wrapped-DEK copied on
   CopyObject (dedup §Rec). To make one ciphertext blob referenceable by many
   objects, the chunk AAD is **rebound from object identity to blob identity**
   (`blob_id ‖ chunk_index`, plus a suite/version byte) (dedup §Q3, pitfall 2).
4. **The established requirement.** With a blob's DEK **selected from metadata**
   and handed to GCM (many wrapped-DEK envelopes point at one blob), we are in the
   "one ciphertext, many candidate keys" setting — a **partitioning oracle**
   (dedup §Q3, §executive-summary 4). AES-256-GCM is **not** key-committing, so
   the new format **MUST** add key commitment. This doc does not re-litigate that;
   it *picks the construction*. Old object-identity AAD was an accidental partial
   guard that disappears once the AAD is shared across references (dedup §Q3).
5. **The migration lever already exists.** `enc_suite_id` is stored **per
   object-version** specifically to allow a suite bump without a global migration
   (01 §4.5, §8). A new committing suite is a new `enc_suite_id`; old objects keep
   the old suite and the old bytes.

Two consequences fall straight out and shape the choice:

- **Encrypt-once ⇒ no nonce reuse under a fixed key** (dedup §Q4; 01 §2). A random
  96-bit nonce used once per DEK never collides with itself, and the per-blob DEK
  keeps chunk-count-per-key far under the NIST 2³² bound. We therefore do **not**
  need nonce-misuse resistance — which removes AES-GCM-SIV / AES-SIV from
  contention on their headline feature (and they are not committing anyway, §3).
- **12-byte nonce + AES-256-GCM is the incumbent primitive** (01 §C1, §10.1 maps
  it to RustCrypto `aes-gcm`). The committing layer is built *over* it, not as a
  replacement — preserving the existing crate choice and read/write path shape.

---

## 1. Why commitment is non-negotiable here (one paragraph)

Standard AEADs (AES-GCM, ChaCha20-Poly1305, OCB) guarantee confidentiality and
authenticity **only against an adversary who does not know the key**; a ciphertext
was never designed to *bind* to the key that produced it. It is cheap to craft an
AES-GCM ciphertext that decrypts to attacker-chosen *valid* plaintexts under two
or more keys — Albertini et al. build a single blob that is a valid PDF under one
key and a valid executable under another, and break three shipped products with
the gap.[^adg] When key selection is driven by attacker-influenceable
input — here, a shared-DEK lookup that picks which wrapped DEK to unwrap for a
blob — each decryption attempt tests a *set* of candidate keys at once, turning a
2^k brute force into a binary search: a **partitioning oracle**.[^partition] Our
blob-dedup read path is exactly this setting (dedup §Q3). The fix is an AEAD whose
ciphertext is a *commitment* to the key (and, ideally, to the nonce and the
blob-identity AAD): a blob decrypts under **exactly one** DEK.[^chan][^bh]

---

## 2. The committing-AE security ladder (what "committing" means precisely)

The literature settled on a ladder `CMT-ℓ`, where ℓ counts the encryption inputs
the ciphertext commits to (Bellare–Hoang; NIST "Landscape" survey):[^bh][^landscape]

| Notion | Ciphertext commits to | What an attacker cannot do |
|---|---|---|
| **CMT-1** (key commitment) | key `K` | find `(K₁,N₁,A₁,M₁)`, `(K₂,N₂,A₂,M₂)` with `K₁≠K₂` giving the same ciphertext |
| **CMT-3** | `(K, N, A)` | as above, also varying nonce/AD |
| **CMT-4** (full commitment) | `(K, N, A, M)` — *all* inputs | produce **any** two distinct input tuples with the same ciphertext; equivalent to the encryption function being **collision-resistant** |

`CMT-4 ⇒ CMT-1`. CMT-1 is the *minimum* that closes the partitioning oracle (§1).
CMT-4 additionally binds the **blob-identity AAD** (fact 3) and the nonce, so a
stored blob cannot be re-presented under a *different* claimed blob identity —
directly valuable for a content-addressed, refcounted dedup store where the AAD is
now the blob's identity.

Two facts the reviewer should hold onto:[^landscape]

- **Bits of committing security matter.** Commitment is an *offline* target (grind
  collisions at leisure), so 64-bit committing security is too weak for a
  standard; aim for **128-bit**, which needs a **256-bit** commitment output for
  CMT-4 (collision resistance ⇒ birthday bound).
- **Every standardized AEAD we might reach for has zero committing security.** GCM,
  XSalsa20/Poly1305, ChaCha20/Poly1305 and OCB are all CMT-1-*insecure* as
  shipped.[^landscape]

---

## 3. AES-GCM-SIV is misuse-resistant but NOT committing — stated plainly

AES-GCM-SIV (RFC 8452) is nonce-misuse-*resistant*: reusing a nonce degrades
gracefully instead of catastrophically. Useful, and **orthogonal to commitment**.
AES-GCM-SIV is **not key-committing**: because its ciphertext size is independent
of the AD length its authenticator cannot be collision-resistant, and
Menda–Len–Grubbs–Ristenpart's context-discovery attacks explicitly break
commitment for SIV (and CCM, EAX, GCM, OCB3).[^context] The dedup research already
flags this (dedup §Q3, final bullet). The RustCrypto `aes-gcm-siv` crate inherits
it. **Do not reach for AES-GCM-SIV to solve the partitioning oracle** — and per
§0 we do not need its misuse resistance anyway (encrypt-once). It is out.

---

## 4. Candidate constructions compared

Every candidate is a *transform over* a standard nonce-based AEAD (nAE); we supply
the nAE (AES-256-GCM, the incumbent) and a hash.

| # | Construction | CMT level | Ct. expansion (per chunk, vs plaintext) | Extra cost / op | Rust support |
|---|---|---|---|---|---|
| A | **Plain AES-256-GCM** (OLD / status quo) | **none** | 28 B (12 nonce + 16 tag) | 0 | `aes-gcm` ✅ |
| B | **Padding fix / CAU** (Albertini et al.: prepend a zero block, check on open) | **CMT-1** (≤96-bit; 384-bit-expansion variant for 128-bit) | +≥16 B (padding block) | ~1 extra AES block | hand-rolled over `aes-gcm` |
| C | **Explicit commitment** `C = H(DEK ‖ …)` stored beside blob, verified on open (Gueron; UtC/HtE family) | **CMT-1** base; **CMT-4** if `H` binds `(K,N,A,T)` | +32 B (commitment) | 1 hash of a short string | `sha2`/`blake3` + `aes-gcm` |
| D | **CTX** (Chan–Rogaway; Bellare–Hoang) — commitment tag `= H(K,N,A,T)` | **CMT-4** | +16 B over GCM (32-B tag replaces 16-B tag) | 1 hash of a short const-length string | hand-rolled over `aes-gcm` + hash |
| E | **DNDK-GCM** (Gueron, IETF draft) — derive DEK *and* 32-B commitment from a root key + nonce | **CMT-1**, ~128-bit | +32 B commitment (48 B total) | 5 AES calls (derive) | no crate; hand-rolled |

Per-candidate notes:

- **B (padding fix).** Cheap, analyzed, but only **CMT-1**, and the no-expansion
  GCM variant delivers ~64-bit committing security; the 128-bit variant costs
  384-bit expansion.[^landscape] Weaker guarantee than D for a similar footprint.
- **C (explicit commitment).** Operationally the simplest bolt-on — the system
  already stores per-chunk sidecar metadata in `part_chunks` (`cipher_size_bytes`,
  `plain_size_bytes`, `cid`, optional `checksum`; 01 §3.4), so a commitment column
  is a natural add. Base form (`C=H(DEK)`) is CMT-1; fold `(N,A,T)` into the hash
  and it *is* CTX (row D). This is the low-risk fallback if the reviewer wants the
  ciphertext blob stream left byte-identical to plain GCM.
- **D (CTX).** The literature's efficiency sweet spot: **CMT-4** for one hash over
  a *constant-length* string (independent of chunk size), commitment *replacing*
  the AEAD tag.[^chan][^bh] Recommended (§5).
- **E (DNDK-GCM).** The AWS/Gueron production lineage; folds key-derivation and
  commitment together, 128-bit target.[^dndk] Only CMT-1, and it *derives* the DEK
  from a root key — which fights our "random per-blob DEK, wrapped per reference"
  envelope (fact 2). Good prior art to cite; not the fit.

**Rust availability, bottom line.** RustCrypto ships the *primitives* (`aes-gcm`
0.11 — already the chosen crate, 01 §10.1; plus `chacha20poly1305`, `aes-gcm-siv`,
`sha2`, `blake3`) but there is **no committing-AEAD crate** — no off-the-shelf
CTX/HtE type.[^rc][^rcaead] Whichever of {B,C,D,E} we pick, we implement it
ourselves as a thin wrapper over `aes-gcm` + a hash. That wrapper is small but is
*cryptographic glue a human must review* (§7). `aes-gcm` exposes the detached-tag /
AAD API the wrapper needs (`AeadInOut`, `encrypt_in_place_detached` /
`decrypt_in_place_detached`, a `Tag` type).[^rc]

---

## 5. Recommendation — **CTX (CMT-4) over AES-256-GCM with SHA-256**, new suite `hip-enc/aes256gcm-ctx-v1`

Adopt **CTX** (construction D), instantiated over the incumbent AES-256-GCM with
SHA-256 as the commitment hash. It is the strongest notion (CMT-4 = full
commitment to key + nonce + **blob-identity AAD** + message), peer-reviewed
(Chan–Rogaway ESORICS '22; Bellare–Hoang EUROCRYPT '22), and its overhead is a
single hash of a short constant-length string plus a modest number of bytes on the
wire.[^chan][^bh][^landscape]

Why CTX over the CMT-1 options (B, C-base, E): the new AAD **is the blob
identity** (fact 3), so binding it (CMT-4) is not a luxury — it means a stored
blob provably belongs to its claimed content-addressed identity, which is the
whole point of a refcounted dedup store. CTX buys that for one short hash, with a
strictly stronger guarantee than B/C-base/E.

### 5.1 Primitive choices

| Parameter | Choice | Reason |
|---|---|---|
| AEAD `nAE` | **AES-256-GCM** | incumbent (01 §C1); 12-byte nonce; AES-NI on servers; audited `aes-gcm`. Alt: ChaCha20-Poly1305 for no-AES-NI clients (Q6). |
| DEK | random **256-bit** per blob | per the dedup envelope (fact 2) |
| Nonce `N` | random **96-bit** per blob, prepended | unchanged from 01 §C2; encrypt-once ⇒ never reused under a DEK |
| AAD `A` | **blob identity** `blob_id ‖ chunk_index` (+ suite byte) | the dedup rebinding (fact 3; dedup §Q3 pitfall 2) |
| Commitment hash `H` | **SHA-256** (256-bit output) | 128-bit committing security (birthday); SHA-NI. Alt: BLAKE3 — already used elsewhere for `body_blake3` (01 §7) — see Q4 |
| Suite id | **`hip-enc/aes256gcm-ctx-v1`** (new) | discriminates committing from OLD `hip-enc/aes256gcm`; rides the existing `enc_suite_id` mechanism (fact 5) |

### 5.2 Wire / storage layout

The recommended v1 implementation keeps the GCM tag on the wire (so the existing
`aes-gcm` verified-decrypt path is reused verbatim) and appends the 32-byte CTX
commitment. This is CTX with the internal tag retained (see §5.4 for why, and for
the pure-CTX 32-byte variant).

```
Per-chunk blob, NEW committing suite  hip-enc/aes256gcm-ctx-v1:
  ┌───────────┬──────────────────────────────────────────────────────────┐
  │ nonce N   │ 12 bytes random  (prepended, exactly as OLD — 01 §C2)      │
  │ ct        │ |plaintext| bytes  (AES-256-GCM ciphertext body)           │
  │ tag T     │ 16 bytes  (GCM tag, as OLD)                                │
  │ CT        │ 32 bytes  CTX commitment = SHA-256( LP(DEK,N,A,T) )         │  ← NEW
  └───────────┴──────────────────────────────────────────────────────────┘
  Per-chunk overhead: 12 + 16 + 32 = 60 B  (OLD was 28 B; +32 B per chunk).
  Suite is recorded out-of-band in object_versions.enc_suite_id (01 §4.5),
  so a reader dispatches OLD vs NEW on the suite, NOT on an in-blob flag.
```

`A` (blob identity) is not stored inside the blob — it is reconstructed from
metadata on both seal and open (exactly as the OLD chunk AAD is reconstructed,
01 §9 step 5), now from `blob_id ‖ chunk_index` instead of object identity.

`part_chunks.cipher_size_bytes` (01 §3.4) becomes `plaintext + 60` for the new
suite. Storing `CT` inline (rather than in a DB sidecar column) keeps the blob
self-describing and travels the commitment with the ciphertext to Arion, matching
the existing "nonce travels in the ciphertext" philosophy (01 §C2). A sidecar
column in `part_chunks` is a viable alternative (Q7).

### 5.3 Pseudocode

```text
SEAL(DEK, N, A, M):                       # DEK,N random per blob; encrypt-once
    (ct, T) = AES_256_GCM.encrypt_detached(key=DEK, nonce=N, aad=A, msg=M)   # T = 16-byte GCM tag
    CT      = SHA256( LP(DEK) ‖ LP(N) ‖ LP(A) ‖ LP(T) )       # 32 bytes
    return  N ‖ ct ‖ T ‖ CT

OPEN(DEK, blob, A):                        # DEK chosen by shared-DEK lookup; suite says NEW
    parse N ‖ ct ‖ T ‖ CT from blob
    CT' = SHA256( LP(DEK) ‖ LP(N) ‖ LP(A) ‖ LP(T) )
    if not constant_time_eq(CT', CT):      # COMMITMENT check — closes the partitioning oracle
        return REJECT
    M = AES_256_GCM.decrypt_detached(key=DEK, nonce=N, aad=A, ct=ct, tag=T)   # AE integrity, as OLD
    if M == FAIL: return REJECT
    return M
```

`LP(x)` is an unambiguous length-prefixed (prefix-free) encoding of each field so
that different `(DEK,N,A,T)` tuples can never collide in the hash pre-image —
required for the CMT-4 proof to bite (Q2). Reuse the little-endian length-prefix
convention already in the codebase for the chunk AAD (`struct.pack("<H"/"<I")`,
01 §C3) for consistency. The `CT` compare **MUST** be constant-time (Q5).

Why this is CMT-4: `CT = H(DEK,N,A,T)` is a collision-resistant hash whose input
includes the GCM tag `T`, and `T` is deterministic in `(DEK,N,A,ct)` with `ct`
deterministic in the message; a collision on the full blob therefore forces an
`H` collision, i.e. identical `(DEK,N,A,T)` — full commitment.[^chan] Retaining
`T` costs 16 bytes over pure CTX but lets `OPEN` use the crate's already-audited
verified decrypt.

### 5.4 Implementation reality over RustCrypto — one honest caveat

Pure CTX (Chan–Rogaway) *drops* `T` from the wire and makes `CT` the sole
authenticator, giving a 32-byte tag total (**+16 B** over OLD GCM, not +32 B). But
then `OPEN` must **recompute** `T` from `(DEK,N,A,ct)` and run **unauthenticated**
CTR decryption — and the high-level `aes-gcm` crate exposes neither "recompute tag
only" nor "CTR-decrypt without tag check" as one call, so pure CTX needs the
lower-level pieces (`aes` + `ctr` + `ghash`/`polyval`) wired together. Two
implementable paths — the reviewer picks (Q1):

- **D-pragmatic (recommended for v1; §5.2):** retain `T`, append `CT`. 60-byte
  overhead. Uses only `aes-gcm` + `sha2` — the exact incumbent GCM path plus one
  hash and one constant-time compare. Lowest implementation risk.
- **D-pure (later optimization):** drop `T`, 32-byte tag total, +16 B over OLD.
  Exact literature CTX; needs a reviewed low-level GHASH/CTR assembly of the
  primitive. Adopt only if the 32 B/chunk matters and the low-level code gets its
  own review.

---

## 6. Migration note — OLD (plain GCM) → NEW (committing)

- **OLD format** = suite `hip-enc/aes256gcm`, plain AES-256-GCM, `nonce‖ct‖tag`,
  **object-identity** AAD (01 §C1–C3), **not committing**. Every object the Python
  service wrote is in this suite and is exposed to the partitioning oracle (§1) for
  as long as it exists in that form. The Rust product **MUST still read it**
  (01 §C15 fixes v5 as the floor; the suite gate at `object_reader.py:243` already
  dispatches on `enc_suite_id`).
- **NEW format** = suite `hip-enc/aes256gcm-ctx-v1`, CTX (§5.2), **blob-identity**
  AAD, committing.
- **The re-encrypt migration is the only thing that moves data to the new format,
  and it is the *same* re-encrypt that the dedup rebinding already requires.**
  Commitment binds the ciphertext to the DEK, and the AAD is *also* changing
  (object-identity → blob-identity), so there is no in-place upgrade and no
  rewrap-only shortcut: you must decrypt and re-encrypt the bytes. Migration per
  object-version: *read OLD chunks → unwrap OLD DEK → GCM-decrypt+verify under the
  OLD object-identity AAD → re-chunk into content-addressed blobs → SEAL each blob
  under CTX with a **fresh** random DEK + fresh nonce and the NEW blob-identity AAD
  → write suite `hip-enc/aes256gcm-ctx-v1` → wrap the new DEK per reference under
  the owner KEK → swap references / refcount → drop the OLD chunks.*
- **Coexistence is free** because `enc_suite_id` is per object-version (fact 5;
  01 §4.5, §8): readers dispatch OLD vs NEW on the suite id, no in-blob flag
  needed. Until every referenced blob is on the committing suite, the oracle risk
  persists on the residual OLD set, so migration is **security-relevant, not
  cosmetic** — prioritize it and track "object-versions still on
  `hip-enc/aes256gcm`" to zero.
- **Dedup interaction.** Re-encryption changes the ciphertext bytes; blob dedup
  **MUST** key off a **private, owner-scoped `blake3(plaintext)` map** (a *separate*
  identity from the AAD `blob_id`, which is opaque/pre-assigned — doc 25 §T3), not
  the ciphertext, or a re-encrypted blob looks "new." This is
  the dedup doc's model (dedup §Q3, pitfall 2); §5's AAD uses that same `blob_id`.

---

## 7. Open questions — what the crypto reviewer must confirm

- **⚑ Q1 — CMT level & variant.** Ratify **CMT-4 via CTX** (§5) over the CMT-1
  options. If CMT-1 is judged sufficient (oracle-closure only, blob-AAD binding
  not required), construction C-base (`C=H(DEK)`) is cheaper. And within CTX,
  choose **D-pragmatic (60 B, `aes-gcm` only)** vs **D-pure (44 B, low-level)**
  (§5.4).
- **⚑ Q2 — Canonical hash input.** Sign off the prefix-free `LP(...)` encoding of
  `(DEK,N,A,T)` and a domain-separation constant (e.g. `"hip-enc/ctx/v1"`) so no
  two distinct tuples collide in the hash pre-image. Load-bearing for the CMT-4
  proof. Recommend reusing the existing LE length-prefix convention (01 §C3).
- **⚑ Q3 — Committing-security target.** Confirm **128-bit** committing security
  (⇒ 256-bit `CT`). Is 128 right for our offline threat model, or is 256-bit
  committing (512-bit `CT`) wanted?
- **⚑ Q4 — Hash choice.** SHA-256 (SHA-NI) vs **BLAKE3** (already a dependency for
  `body_blake3`, 01 §7). Both are 256-bit CR; pick one and pin it in the suite id.
- **⚑ Q5 — Constant-time compare.** Confirm the `CT` comparison uses constant-time
  equality (`subtle::ConstantTimeEq`); a data-dependent early exit reopens a
  timing oracle.
- **⚑ Q6 — Cipher choice.** AES-256-GCM (server AES-NI, incumbent) vs
  ChaCha20-Poly1305 (no-AES-NI clients). Both take the 12-byte nonce and are
  CTX-able. Confirm one suite or a per-context choice (a second suite id).
- **⚑ Q7 — Commitment placement.** Inline in the blob (§5.2, recommended,
  self-describing, travels to Arion) vs a `part_chunks` sidecar column (DB-side,
  keeps blob bytes = plain-GCM). Either is fine if integrity-checked on open;
  confirm which the storage/streamer layer prefers.
- **⚑ Q8 — DEK scoping under dedup.** §5 is agnostic to whether the sealing DEK is
  per-object-version (01 §4) or per-blob (as the dedup "one blob, one DEK, wrapped
  per reference" text implies, dedup §Q3). CTX commits *per AEAD invocation*
  (per chunk) regardless, but the reviewer should confirm the final DEK→blob
  mapping so "exactly one DEK opens this blob" is enforced end-to-end, and that no
  code path re-seals an existing DEK+nonce (encrypt-once — the whole 12-byte-nonce
  safety argument, 01 §2, dedup §Q4, rests on it).
- **⚑ Q9 — Copy fast-path AAD.** 01 §Q1 already flags that the v5 copy fast-path
  reuses source chunk CIDs while the AAD binds the source object_id. Rebinding the
  AAD to `blob_id` (fact 3) *resolves* that latent bug for the NEW suite (the AAD
  no longer names an object), but confirm the OLD-suite copy path is handled (gated
  off or byte-copied) during coexistence.

---

## Sources

Committing-AEAD literature (retrieved 2026-09-15):

- Albertini, Duong, Gueron, Kölbl, Luykx, Schmieg, **"How to Abuse and Fix
  Authenticated Encryption Without Key Commitment"**, USENIX Security 2022 —
  multi-key ciphertexts, padding fix, generic explicit commitment.
  https://www.usenix.org/conference/usenixsecurity22/presentation/albertini ·
  ePrint https://eprint.iacr.org/2020/1456
- Len, Grubbs, Ristenpart, **"Partitioning Oracle Attacks"**, USENIX Security
  2021. https://www.usenix.org/conference/usenixsecurity21/presentation/len ·
  ePrint https://eprint.iacr.org/2020/1491
- Chan, Rogaway, **"On Committing Authenticated-Encryption"**, ESORICS 2022 — the
  **CTX** construction, CMT-4. https://eprint.iacr.org/2022/1260 (PDF
  https://eprint.iacr.org/2022/1260.pdf)
- Bellare, Hoang, **"Efficient Schemes for Committing Authenticated Encryption"**,
  EUROCRYPT 2022 — CMT-1..CMT-4 framework, CTX, HtE/UtC/RtC transforms.
  https://eprint.iacr.org/2022/268
- Bellare, Hoang, Wu, **"The Landscape of Committing Authenticated Encryption"**,
  NIST 3rd Workshop on Block-Cipher Modes 2023 — CMT-1/CMT-4 definitions, the
  comparison table (GCM / CAU-C1 / padding fix / CTX bits & expansion), the
  128-bit-target argument.
  https://csrc.nist.gov/csrc/media/Events/2023/third-workshop-on-block-cipher-modes-of-operation/documents/accepted-papers/The%20Landscape%20of%20Committing%20Authenticated%20Encryption.pdf
- Menda, Len, Grubbs, Ristenpart, **"Context Discovery and Commitment Attacks"**,
  EUROCRYPT 2023 — breaks commitment for GCM, CCM, EAX, **SIV**, OCB3.
  https://eprint.iacr.org/2023/526
- Gueron, **"Key Committing AEADs"**, ePrint 2020/1153 — generic commitment add-on
  and a key-committing AES-GCM. https://eprint.iacr.org/2020/1153
- Gueron, **"Double Nonce Derive Key AES-GCM (DNDK-GCM)"**, IETF draft
  draft-gueron-cfrg-dndkgcm — derive DEK + 32-byte commitment, 48-byte expansion,
  128-bit target. https://datatracker.ietf.org/doc/draft-gueron-cfrg-dndkgcm/

Rust ecosystem (retrieved 2026-09-15):

- RustCrypto **AEADs** repo — crates `aes-gcm`, `chacha20poly1305`, `aes-gcm-siv`,
  `aes-siv`, …; **no committing-AEAD crate**. https://github.com/RustCrypto/AEADs
- `aes-gcm` crate (v0.11.x; `AeadInOut` detached-tag API, `Aes256Gcm`, 96-bit
  nonce) — already the chosen chunk-cipher crate (01 §10.1).
  https://docs.rs/aes-gcm/latest/aes_gcm/
- `aes-gcm-siv` crate (misuse-resistant; **not committing**).
  https://docs.rs/aes-gcm-siv/latest/aes_gcm_siv/

Repository context (prerequisites, consumed):
[`crypto-dedup-research.md`](./crypto-dedup-research.md),
[`01-crypto-envelope.md`](./01-crypto-envelope.md).

[^adg]: Albertini et al., USENIX Security 2022 — https://www.usenix.org/conference/usenixsecurity22/presentation/albertini
[^partition]: Len, Grubbs, Ristenpart, USENIX Security 2021 — https://www.usenix.org/conference/usenixsecurity21/presentation/len
[^chan]: Chan, Rogaway, ESORICS 2022 (CTX) — https://eprint.iacr.org/2022/1260
[^bh]: Bellare, Hoang, EUROCRYPT 2022 — https://eprint.iacr.org/2022/268
[^landscape]: Bellare, Hoang, Wu, "The Landscape of Committing AE", NIST 2023 — https://csrc.nist.gov/csrc/media/Events/2023/third-workshop-on-block-cipher-modes-of-operation/documents/accepted-papers/The%20Landscape%20of%20Committing%20Authenticated%20Encryption.pdf
[^context]: Menda et al., EUROCRYPT 2023 — https://eprint.iacr.org/2023/526
[^dndk]: Gueron, DNDK-GCM IETF draft — https://datatracker.ietf.org/doc/draft-gueron-cfrg-dndkgcm/
[^rc]: RustCrypto `aes-gcm` — https://docs.rs/aes-gcm/latest/aes_gcm/
[^rcaead]: RustCrypto AEADs repo — https://github.com/RustCrypto/AEADs
