# 25 — Crypto verification & freeze: CTX-over-frames committing AEAD

**Status:** Verification report for the crypto reviewer. This is the artifact the
reviewer signs and the implementer follows. It **verifies** (against the primary
literature, NIST, the RustCrypto source, and a working PoC) the construction
accepted in [`decisions-register.md`](./decisions-register.md) §A (A1/A4/A5) and
specified in [`21-committing-aead-construction.md`](./21-committing-aead-construction.md)
and [`23-chunking-and-range-prior-art.md`](./23-chunking-and-range-prior-art.md).
It does **not** re-litigate the accepted parameters; it confirms they are correct
and implementable, pins every byte, and flags the (small) places that need a tweak.

**Suite id frozen by this doc:** `hip-enc/aes256gcm-ctx-frames-v1`.

**Method.** Primary papers read from PDF (Chan–Rogaway ESORICS'22 eprint 2022/1260;
Bellare–Hoang EUROCRYPT'22 eprint 2022/268), NIST SP 800-38D §5.2.1.1/§8.3 read
from PDF, STREAM nonce layouts read from the Tink and age specs, the RustCrypto
`aes-gcm` 0.11.1 / `aead` 0.6.1 API read from the vendored crate source, and a
throwaway Rust PoC built and tested locally (`cargo test`: 9/9 pass) that emits
golden vectors. Toolchain: rustc/cargo 1.97.1, aarch64-apple-darwin.

---

## 0. Verdict

| Item | Accepted parameter | Verdict |
|---|---|---|
| CTX committing transform at CMT-4 | ✔ | **Confirmed.** CTX is CMT-4 (= CAE-XX = "full commitment"), proved from collision resistance of `H` (Chan–Rogaway Thm 2; Bellare–Hoang). |
| D-pragmatic wire `N‖ct‖T‖CT` | ✔ | **Confirmed CMT-4** — identical committing strength to pure CTX; the retained GCM tag `T` is a deterministic function of the committed inputs, so it costs 16 B of wire and **zero** security. |
| Commitment hash SHA-256, 256-bit `CT` | ✔ | **Confirmed** 128-bit committing security (birthday on a 256-bit CR hash). One **caveat** and a **lean toward BLAKE3** — see §1.4 / §7-T1. |
| 256 KiB frames (tunable 64 KiB) in 4 MiB blob | ✔ | **Confirmed** safe and standard; per-DEK frame count ≤16 (256 KiB) / ≤64 (64 KiB). |
| STREAM nonce = random prefix ‖ frame counter | ✔ | **Confirmed**, byte-pinned as `prefix(7)‖ctr32(4,BE)‖final_flag(1)` — the exact Tink layout. **Tweak: add the 1-byte final-frame flag** (§2, §7-T2). |
| Per-blob random DEK, encrypt-once, wrapped under owner KEK | ✔ | **Confirmed.** No (key,nonce) reuse is possible (§2.3). |
| AAD = `blob_id‖frame_index‖suite_id` | ✔ | **Confirmed** and byte-pinned (§1.5). |
| Per-owner copy-dedup | ✔ | **Confirmed** consistent with the construction (§5). |
| `blob_id = blake3(whole framed ciphertext)` | ✔ | **Confirmed** consistent with dedup/refcount — with **one ordering note** (§5.5, §7-T3). |
| Rust crates | — | **Pinned:** `aes-gcm 0.11.1`, `aead 0.6.1`, `sha2 0.10.9`, `blake3 1.8.7`, `subtle 2.6.1`. **No committing-AEAD crate exists** → thin hand-rolled wrapper. `aead::stream` **does not exist in 0.6.1** (removed) and would not fit anyway (§4). |

**Bottom line: ratify the package as written, with two byte-level tweaks folded in
(final-frame flag byte; injective length-prefixed commitment preimage with a
domain-separation constant) and one documented hash-choice note (BLAKE3 ≥ SHA-256
here).** All are already reflected in the byte-exact spec below and pass the PoC.

---

## 1. Exact construction (verified against Chan–Rogaway and Bellare–Hoang)

### 1.1 The literature CTX, verbatim

Chan–Rogaway define CTX over a tag-based nAE `Π=(E,D)` whose `E` splits into `E1`
(produces the core `C`, `|C|=|M|`) and `E2` (produces the tag `T`). AES-GCM
"satisfy[ies] these structural demands" (the CTR core is `E1`, GHASH-derived tag is
`E2`). CTX (their Fig. 2):

```
CTX.E(K,N,A,M):                         CTX.D(K,N,A,C̄):
  20  C  ← Π.E1(K,N,A,M)                  30  C ‖ T* ← C̄
  21  T  ← Π.E2(K,N,A,M)                  31  M  ← Π.D1(K,N,A,C)
  22  T* ← H(K,N,A,T)                     32  T  ← Π.E2(K,N,A,M)
  23  ret C ‖ T*                          33  if T* ≠ H(K,N,A,T) then ret ⊥
                                          34  ret M
```

> "just replace the tag `T` with an alternative tag `T* = H(K,N,A,T)` … not only
> works to commit to `K`, `N`, and `A`, but also to the underlying message `M`."
> (Chan–Rogaway §4.)

**Theorem 2 (Chan–Rogaway), verbatim bound:** `Adv^{cae-XX}_{CTX}(A) ≤ Adv^{col}_H(B)`.
I.e. **breaking CTX's full-commitment security is exactly as hard as finding an `H`
collision.** The proof: a CAE-XX win is two distinct tuples
`(K_i,N,A,M,C‖T*)`, `(K_j,N',A',M',C‖T*)`; since both produced the same `T*`,
`H(K_i,N,A,T)=H(K_j,N',A',T')`; the tuples can't be equal (bijectivity of `E1`
forces `M=M'` if the rest match, contradiction), so they are an `H` collision.

Bellare–Hoang (eprint 2022/268) give the ladder the register cites: **CMT-1**
commits to `K`; **CMT-4** commits to `(K,N,A,M)` — "all the inputs to SE.Enc";
"CMT-4 → CMT-1 … and the implication is strict." They state plainly that
"GCM … ChaCha20/Poly1305 and OCB are all CMT-1-insecure" as shipped — which is the
whole reason a transform is mandatory here. CTX's CAE-XX ≡ Bellare–Hoang CMT-4.

### 1.2 Our wire — "D-pragmatic" — and why it is still CMT-4

Pure CTX drops `T` and ships `C‖T*` (a 32-byte tag, +16 B over plain GCM). Our v1
**retains** the GCM tag `T` on the wire and **appends** `CT` (= `T*`):

```
frame_i wire  (suite hip-enc/aes256gcm-ctx-frames-v1):
  ┌──────────┬───────────────────────┬──────────┬───────────────────────────┐
  │ N_i (12) │ ct_i (|plaintext_i|)  │ T_i (16) │ CT_i (32)                 │
  │ STREAM   │ AES-256-GCM core      │ GCM tag  │ = H( LP(DEK,N_i,A_i,T_i)) │
  │ nonce    │ (E1)                  │ (E2)     │   the CTX commitment      │
  └──────────┴───────────────────────┴──────────┴───────────────────────────┘
  overhead = 12 + 16 + 32 = 60 B / frame   (PoC: 32-B plaintext → 92-B frame ✓)
```

**Claim (verified by the CTX proof + PoC): D-pragmatic achieves CMT-4.** Treat the
committing target as the whole wire `N‖ct‖T‖CT`. Suppose two distinct input tuples
produce identical wire. Then identical `N`, `ct`, `T`, and `CT`. Identical `CT` with
`H` collision-resistant and an **injective** preimage encoding (§1.3) forces
`(DEK,N,A,T)` equal across the two tuples ⇒ same `K,N,A`; with `K,N,A` fixed and
`ct` identical, GCM's `E1` bijectivity forces `M` equal ⇒ the tuples are equal,
contradiction. So the map is collision-resistant = **CMT-4**. The retained `T` is
`E2(K,N,A,M)` — a deterministic function of already-committed inputs — so it adds
no adversarial freedom; it only lets `OPEN` reuse the crate's audited GCM verified
decrypt instead of hand-rolled CTR+GHASH. **This is the correct v1 trade: full
CMT-4, `aes-gcm`+hash only, +16 B/frame vs pure CTX.** (Doc 21 §5.4 D-pure remains
a later size optimization needing its own low-level review.)

`OPEN` order matters and is fixed: **check `CT` first (constant-time), then GCM
verify, then the final-flag check.** Checking the commitment before trusting the
key is what closes the partitioning oracle; the PoC enforces this order.

### 1.3 Commitment PREIMAGE — the canonical, injective encoding (load-bearing)

The papers write `H(K,N,A,T)` abstractly (modeled as RO in Chan–Rogaway; as a
collision-resistant PRF in Bellare–Hoang). **A real implementation MUST serialize
`(DEK,N,A,T)` injectively**, or two distinct tuples could map to one preimage byte
string and collide `H` trivially — voiding the CMT-4 proof. This is doc 21 Q2, and
it is the single most important thing the implementer must not improvise.

**Frozen encoding** (`LP(x) = LE32(len(x)) ‖ x`, a length-prefix that makes the
concatenation prefix-free/injective even though every field here is fixed-length):

```
preimage = LP(DOMAIN_SEP) ‖ LP(DEK) ‖ LP(N) ‖ LP(A) ‖ LP(T)

  DOMAIN_SEP = "hip-enc/ctx/v1"   (ASCII, 14 bytes; hex 6869702d656e632f6374782f7631)
  DEK        = 32 bytes
  N          = 12 bytes  (the STREAM nonce, §2)
  A          = the AAD (§1.5), variable only via suite_id
  T          = 16 bytes  (the GCM tag)

CT = SHA-256(preimage)          # 32 bytes, the frozen v1 hash
CT_blake3 = BLAKE3(preimage)    # 32 bytes, the alternative (§1.4)
```

- **Why length-prefix at all when fields are fixed-length?** `A` embeds `suite_id`,
  a string that could change length across suites; length-prefixing makes injectivity
  hold **unconditionally and for future suites**, and costs 20 bytes of hashing.
  Reuse of the codebase's LE length-prefix convention (`01` §C3) is deliberate.
- **Domain separation** prevents a `CT` value from ever being valid in another
  hashing context (e.g. `blob_id`'s own BLAKE3, or a future MAC), i.e. cross-protocol
  collisions.

### 1.4 Hash choice — SHA-256 verified correct; BLAKE3 recommended (defense-in-depth)

Both give 256-bit output ⇒ ~2^128 collision work ⇒ **128-bit committing security**,
which the NIST "Landscape" argument sets as the standard target (offline attack ⇒
128-bit, ⇒ 256-bit output). Chan–Rogaway independently "recommend having CTX tag
length be 160-bits over … 128-bits"; 256 clears that comfortably.

**Verified caveat (Bellare–Hoang §3, exact wording):** *"if one considers using
`SHA256(K‖N‖A)[1:k]`, one must beware of the extension attack, to avoid which one
should only use this if `k=128`."* This warns about **length-extension** when SHA-256
is used as a **truncated committing PRF with the key as a prefix**. Our construction
is **not** in that danger zone: (a) CTX needs only **collision resistance**, which
length-extension does not break; (b) we use the **full 32-byte** output (no
truncation); (c) the preimage is **fixed-structure and the reader recomputes over the
exact same bytes** — there is no oracle that returns `H(secret‖known)` and later
accepts `H(secret‖known‖ext)`. So **SHA-256 is cryptographically correct here.**

Nonetheless, **prefer BLAKE3** for v1 (a genuine, low-cost improvement, not a
required fix): it is (1) not Merkle–Damgård, so length-extension is structurally
impossible — the whole class is off the table; (2) **already a dependency** of this
system (`body_blake3`, `01` §7, and `blob_id = blake3(...)`), so no new crate; (3)
fast without SHA-NI (matters for no-AES-NI clients / the ChaCha suite alt, `21` Q6).
SHA-256 wins only on server SHA-NI throughput, which is irrelevant at 20 bytes of
extra hashing per frame. **Recommendation: pin BLAKE3 in the suite id; keep SHA-256
as the documented, equally-sound fallback.** Either way the choice is frozen *in the
suite id* so a reader never guesses. (The PoC emits both digests in every vector.)

### 1.5 AAD — byte-exact

```
A_i = blob_id (32 bytes, raw)  ‖  LE32(frame_index i)  ‖  suite_id (UTF-8 bytes)

  suite_id = "hip-enc/aes256gcm-ctx-frames-v1"  (31 bytes; hex ...6672616d65732d7631)
```

- `blob_id` is the raw 32-byte BLAKE3 digest (not hex) — half the bytes, and it is
  what identdifies the content-addressed blob (`23` §4.4). It is **reconstructed from
  metadata on seal and open**, never stored in the frame (mirrors `01` §9 step 5).
- `frame_index` is **per-blob**, 0-based, LE32 (matches the codebase's `<I`
  convention, `01` §C3). It binds each frame to its slot ⇒ anti-splice / anti-reorder
  across frames and across blobs.
- `suite_id` in the AAD binds the algorithm identity into the tag (belt to the
  out-of-band `enc_suite_id` suspenders), so a downgrade/confusion to another suite
  can't reuse the bytes.

---

## 2. STREAM nonce layout — byte-pinned + no-reuse proof

### 2.1 The frozen layout (exact Tink AES-GCM-HKDF-STREAMING geometry)

```
N_i (12 bytes) = nonce_prefix (7, random per blob) ‖ frame_index (4, BIG-endian) ‖ final_flag (1)
   final_flag = 0x01  if frame i is the LAST frame of the blob
              = 0x00  otherwise
```

Verified reference layouts:
- **Tink AES-GCM-HKDF-STREAMING:** `NoncePrefix(7) ‖ i(4, big-endian) ‖ b(1)`, where
  `b = 0x00 if i<n-1 else 0x01`; total 12. **We adopt this exactly.**
- **age:** `ctr(11, big-endian) ‖ b(1)`, `b = 0x01` final else `0x00`; total 12 (age
  omits the random prefix because it derives a fresh per-file key — analogous to our
  per-blob DEK).
- **AWS Encryption SDK:** per-frame deterministic IV built from the frame sequence
  number; 2^32 frame ceiling.

**Byte budget rationale.** 7+4+1 = 12 exactly. A **32-bit** frame counter permits
2^32 frames/blob; we use ≤16 (256 KiB) or ≤64 (64 KiB) — 26+ bits of headroom. The
7-byte random prefix is, as the task notes, **belt-and-suspenders given a fresh DEK
per blob** (age proves a pure counter is safe under a fresh per-message key); it
costs nothing and hardens against a hypothetical DEK-reuse bug. The frame counter is
**big-endian** to match Tink/age/ESDK convention (the AAD's `frame_index` stays
LE per the codebase's `<I` convention — the two encodings are independent and each
matches its own precedent; the implementer must not cross them).

### 2.2 Tweak: include the final-frame flag (T2)

Doc 23 §4.4's sketch wrote `NoncePrefix(~8) ‖ ctr32 ‖ final-byte` (~8+4+1 = 13,
over budget). **Pinned correction: prefix = 7 bytes, not 8**, so the final-flag byte
fits in 12. The final-flag is worth keeping (not dropping to reclaim a prefix byte):
it gives **in-band truncation resistance** independent of the DB. The flag is inside
the GCM nonce, so it is authenticated by `T` **and** committed by `CT`; an attacker
cannot flip `0x00→0x01` (PoC test `final_flag_is_authenticated_in_nonce` → rejects).
And if the true final frame is dropped, the new last frame still carries `0x00`, so
a reader expecting a final frame detects the truncation (PoC test
`truncation_flag_mismatch_rejects`). The metadata-known frame count is the
suspenders; the flag is the belt. (If the reviewer prefers zero in-band truncation
logic and relies solely on the DB frame count, dropping the flag and using an 8-byte
prefix is also sound — but the flag is free and strictly safer.)

### 2.3 PROOF: no (key, nonce) reuse; frame counts far under all limits

Let `k` be a blob's DEK. **Uniqueness of the key.** `k` is a fresh 32-byte CSPRNG
value generated for exactly one blob and used to seal that blob **once** (encrypt-once;
`crypto-dedup-research` §Q4, `01` §2; CopyObject/references reuse ciphertext and
re-wrap `k`, never re-run GCM — §5.3). No other blob uses `k`.

**Uniqueness of the nonce under `k`.** Within the blob, `N_i` embeds `frame_index i`
in bytes 7–10; distinct frames ⇒ distinct `i` ⇒ distinct `N_i`. The `nonce_prefix`
is constant within the blob (correct — uniqueness comes from the counter) and the
`final_flag` only ever *reinforces* distinctness. Therefore **every (`k`, `N_i`)
pair is globally unique**: distinct across blobs because `k` differs; distinct within
a blob because `i` differs. GCM's catastrophic failure mode (nonce reuse under one
key) is unreachable. The 7-byte random prefix additionally makes the *nonce alone*
collision-improbable even if two DEKs were ever equal by accident.

**Counter/limit headroom.** Frames per blob = ⌈4 MiB / frame⌉ = **16** (256 KiB) or
**64** (64 KiB); a per-bucket knob never makes a 4 MiB blob exceed the 32-bit
counter. Tink's and AWS ESDK's 2^32 segment ceiling and NIST's invocation bound
(§3) are all ≥ 2^32; we sit at ≤2^6. Even a 5 TiB object (≈21 M frames total at
64 KiB) keeps **≤64 frames per DEK** because the DEK is per-blob, so no per-key
ceiling is approached anywhere.

---

## 3. AES-GCM usage limits (NIST SP 800-38D) — satisfied with ~26 orders of margin

Verified from the standard (PDF):

- **§5.2.1.1 input-length bounds:** `len(P) ≤ 2^39 − 256` bits, `len(A) ≤ 2^64 − 1`,
  `1 ≤ len(IV) ≤ 2^64 − 1`.
- **§8.3 invocation bound** (applies to the RBG-based construction and any
  non-96-bit-deterministic IV — the safe reading for our STREAM-with-random-prefix
  nonce): *"The total number of invocations of the authenticated encryption function
  shall not exceed 2^32 … with the given key."*

**Our numbers per DEK:** each DEK seals **one 4 MiB blob = ≤16–64 frames**.
- Invocations/key ≤ 64 ⋘ 2^32 (headroom ≥ 2^26).
- Plaintext/invocation ≤ 256 KiB = 2^21 bits (`len(P)`) ⋘ 2^39−256 (headroom ≥ 2^18).
- AAD/invocation ≈ 67 bytes ⋘ 2^64−1.

All three NIST limits hold with astronomical margin. The per-blob-DEK design (A2) is
what guarantees it structurally; no runtime counter/limit enforcement is required
(consistent with the "no artificial caps" rule).

---

## 4. Rust crate feasibility — pinned, and the honest gaps

### 4.1 Frozen crate set (resolved & built locally on rustc 1.97.1)

| Crate | Version | Role |
|---|---|---|
| `aes-gcm` | **0.11.1** | AES-256-GCM. `Aes256Gcm`, `KeyInit`, 12-byte `Nonce`, detached `Tag`. Matches `01` §10.1. |
| `aead` | **0.6.1** | trait layer (pulled in by `aes-gcm`). Detached API used directly. |
| `sha2` | **0.10.9** | SHA-256 commitment (if SHA-256 chosen). |
| `blake3` | **1.8.7** | BLAKE3 commitment (recommended, §1.4) **and** `blob_id`. |
| `subtle` | **2.6.1** | `ConstantTimeEq` for the `CT` compare (doc 21 Q5). |

### 4.2 The detached API the wrapper uses (verified in vendored source)

`aes-gcm` 0.11.1 re-exports `aead::{AeadCore, AeadInOut, KeyInit, ...}`. `AeadInOut`
(the non-deprecated path) exposes:
- `encrypt_inout_detached(&self, nonce: &Nonce, aad: &[u8], buffer: InOutBuf) -> Result<Tag>`
- `decrypt_inout_detached(&self, nonce: &Nonce, aad: &[u8], buffer: InOutBuf, tag: &Tag) -> Result<()>`

(`&mut [u8]` coerces to `InOutBuf` via `.into()`.) The deprecated
`AeadInPlace::{encrypt,decrypt}_in_place_detached` still work as thin shims. The PoC
uses `encrypt_inout_detached` / `decrypt_inout_detached` cleanly (no deprecation
warnings). GCM appends the tag as a 16-byte suffix — matching the `nonce‖ct‖tag`
philosophy the codebase already relies on.

### 4.3 No committing-AEAD crate — confirmed; and `aead::stream` is gone

- **There is no CTX / committing-AEAD crate in RustCrypto or elsewhere.** Confirmed
  (doc 21's finding still holds). We build a **thin wrapper** = `aes-gcm` detached +
  one hash + one `subtle` compare. That wrapper is ~120 lines (see PoC) and is the
  "cryptographic glue a human must review" doc 21 §4 flags.
- **`aead::stream` is NOT available in `aead 0.6.1`** — the `stream` module/feature
  was removed after the 0.5 line (0.6.1 features are only `alloc`, `rand_core`,
  `getrandom`, `dev`). So the option of leaning on the crate's STREAM is moot.
  **Even if it existed we would not use it:** (a) it implements Rogaway–Bellare
  STREAM, **not** CTX — zero committing security, the exact property we need; (b) it
  is a **sequential** online-AE API, whereas ranged reads need **independent
  random-access** per frame with a **deterministic ciphertext offset** per frame
  index (`23` §4.4). We therefore hand-roll per-frame seal/open (the PoC), borrowing
  only STREAM's *nonce geometry* (§2), which is the sound part to reuse.

---

## 5. Composition edges

### 5.1 Multipart part-boundary vs frame-boundary
S3 parts (client MPU layer, 5 MiB–5 GiB, ≤10 000) are subdivided by us into 4 MiB
blobs, each framed into 256 KiB frames (`23` §5.2). **Frames never straddle a blob**,
and **blobs never straddle a part** (a part's last blob is short; a blob's last frame
is short). `frame_index` is per-blob, so no cross-part coupling enters the AAD. A
part's chunk/frame sizes are stored per-part (`23` §4.4, `01` §C8) so readers never
assume config — preserve that discipline.

### 5.2 CopyObject = metadata-only, DEK re-wrap, NO re-encrypt (same-owner v1)
Per-owner dedup (A3) + per-blob DEK (A2): a copy **references the same ciphertext
blobs** and **re-wraps the blob DEK** under the destination context's owner KEK — no
GCM re-run, so encrypt-once (§2.3) is preserved. Crucially, **the frame AAD is now
`blob_id`, not object identity**, so a copied object decrypts its frames unchanged —
this **resolves the latent v5 copy-fast-path AAD bug** (`01` Q1 / doc 21 Q9) that
existed only because the old AAD named the object. The old suite's copy path must
still be gated/byte-copied during coexistence (doc 21 Q9).

### 5.3 Migration re-encrypt target
The one-time OLD→NEW migration (doc 21 §6) emits **frames**, not whole-chunk CTX
seals (doc 23 §4.3, register A4): read OLD chunk → GCM-verify under old
object-identity AAD → re-chunk into content-addressed blobs → for each blob pick a
fresh random DEK + fresh 7-byte prefix → **seal each 256 KiB frame** with CTX + the
new `blob_id‖frame_index‖suite` AAD → wrap the DEK per reference under the owner KEK.
This re-encrypts the corpus **exactly once** (the reason framing is folded in now).

### 5.4 Ranged read: plaintext-range → covering-frames → single HCFS ranged-GET
Frame ciphertext size is constant (`frame_plain + 60`), so
`ct_offset(i) = i × (frame_plain + 60)` is **deterministic** — the reader maps a
plaintext `[start,end]` to covering frames `[f_lo..f_hi]`, issues **one** absolute
HCFS `bytes=ct_offset(f_lo)-ct_offset(f_hi+1)-1` GET over just those frames'
ciphertext, decrypts them, and trims first/last with `slice_start`/`slice_end_excl`
(`23` §4.4). This is the design's whole point (≈2 600× vs ≈42 000× amplification for
a 100-byte read). CMT-4 holds per frame, so partial reads are fully committed.

### 5.5 `blob_id = blake3(framed ciphertext)` vs dedup/refcount — ordering note
Content-addressing the **framed ciphertext** is consistent with refcounted dedup
(the blob is one HCFS object named by its hash; `23` §4.4). **But note the
dependency order (T3):** the AAD binds `blob_id`, and `blob_id` is the hash of the
ciphertext that is produced *using* that AAD — a cycle if taken literally. It is
resolved by the fact that **dedup keys off a *plaintext-derived* content identity,
not the ciphertext** (per-owner random-DEK blobs never converge, so ciphertext hash
is not a cross-object dedup key anyway — doc 21 §6 "dedup MUST key off content
identity / `blob_id` (plaintext-derived)"). **The reviewer must confirm the precise
definition of the `blob_id` that goes in the AAD:** it must be a **plaintext-derived,
DEK-independent** identifier fixed *before* sealing (e.g. `blake3(plaintext-of-blob)`
or an assigned content id), **not** `blake3(ciphertext)` — otherwise the AAD can't be
known at seal time and same-plaintext blobs under different DEKs would get different
AAD. `blake3(framed ciphertext)` is fine as the **HCFS storage address / integrity
name**, but the **AAD `blob_id` and the storage address should be named distinctly**
in the implementation to avoid the circularity. This is the one place the accepted
wording ("blob_id = blake3(framed ciphertext)") needs disambiguation before freeze.

---

## 6. PoC — built, tested, golden vectors

**Location:** [`ctx-poc/`](./ctx-poc/) in this repo (preserved from the original session scratchpad;
`src/main.rs` is the reference the implementer mirrors, `golden_vectors.json` the acceptance oracle,
`Cargo.lock` the pinned crate set). See [`ctx-poc/README.md`](./ctx-poc/README.md).

**Results:** `cargo test` → **9/9 pass**, no warnings.

| Test | Asserts |
|---|---|
| `roundtrip_ok`, `empty_plaintext_roundtrip` | SEAL→OPEN returns plaintext (incl. 0-byte frame) |
| `wrong_dek_rejects` | wrong DEK → **`Commitment`** rejection (partitioning-oracle closure), never authenticates |
| `wrong_frame_index_rejects`, `wrong_blob_id_rejects` | AAD mismatch → reject |
| `flipped_commitment_rejects` | 1-bit flip in `CT` → `Commitment` reject |
| `flipped_ciphertext_rejects` | 1-bit flip in `ct` → `Aead` (GCM) reject |
| `truncation_flag_mismatch_rejects` | non-final frame presented as final → `Truncated` |
| `final_flag_is_authenticated_in_nonce` | flipping the wire final-flag → `Commitment` reject (flag is bound by nonce+CT) |

**Golden vector 0** (SHA-256 commitment; deterministic inputs; the PoC also emits the
BLAKE3 commitment per vector so the reviewer can pick the hash without re-running):

```
suite_id      : hip-enc/aes256gcm-ctx-frames-v1
domain_sep    : 6869702d656e632f6374782f7631                                    (= "hip-enc/ctx/v1")
DEK           : 000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f
nonce_prefix  : a0a1a2a3a4a5a6
blob_id       : bb…bb  (32 × 0xbb)
frame_index   : 0        is_final: false
plaintext     : 686970706975732d733320637478206672616d6520766563746f722023303030  ("hippius-s3 ctx frame vector #000")
N (nonce)     : a0a1a2a3a4a5a6 00000000 00      = prefix(7) ‖ ctr32BE(0) ‖ flag(0x00)
AAD           : bb…bb ‖ 00000000 ‖ 6869702d656e632f61657332353667636d2d6374782d6672616d65732d7631
ct (body)     : 62b86cd9ff2136101f10e4e3ed78760da0a022205b1198627b3888c217ff7dc5
T  (GCM tag)  : 806daeed77eaa6ed2a5594d970d4c8a8
CT sha256     : 0bee5da4f0cf26b3785f4eede54b3bb154ca3fa61aa6d7ba01447d2d5fec2a3f
CT blake3     : 1238ef92f334604326c3cb8dbacb76e18902785055274a8fd91d0a24df7b4850
full frame    : a0a1a2a3a4a5a6000000000062b86cd9ff2136101f10e4e3ed78760da0a022205b11
                98627b3888c217ff7dc5806daeed77eaa6ed2a5594d970d4c8a80bee5da4f0cf26b378
                5f4eede54b3bb154ca3fa61aa6d7ba01447d2d5fec2a3f
frame length  : 92 bytes   (= 32 plaintext + 60 overhead ✓)
commit preimg : 0e0000006869702d656e632f6374782f7631 20000000<DEK> 0c000000<N> 43000000<AAD> 10000000<T>
                (LE32 length prefix before each of DOMAIN_SEP, DEK, N, AAD, T)
```

The implementer's acceptance test = reproduce `ct`, `T`, and `CT` for these inputs
byte-for-byte. (Vectors 1 and 2 in the JSON cover a final frame and an empty-plaintext
final frame.)

---

## 7. Flags — where the accepted recommendation needs a tweak

- **T1 — Hash: lean BLAKE3, pin it in the suite id.** SHA-256 is *correct* here
  (§1.4), but BLAKE3 removes the length-extension question entirely, is already a
  dependency, and is faster without SHA-NI. Not a defect in the accepted param — a
  free upgrade. Whichever is chosen, **freeze it in the suite id** so readers never
  guess. *(Doc 21 Q4 / register A5.)*
- **T2 — Nonce: prefix is 7 bytes (not ~8), plus a 1-byte final-frame flag.** Doc 23
  §4.4's "~8 B prefix ‖ ctr32 ‖ final-byte" overflows 12 bytes. Frozen: `7‖4BE‖1`
  (exact Tink geometry). Keep the final-flag (free in-band truncation resistance,
  §2.2). *(Register A5 "ratify the STREAM nonce scheme".)*
- **T3 — Disambiguate `blob_id`.** The AAD's `blob_id` must be **plaintext-derived /
  assigned and known at seal time**, *not* `blake3(framed ciphertext)` (which is only
  computable after sealing and would differ per DEK). Use `blake3(ciphertext)` as the
  **HCFS storage address**, and a distinct plaintext-derived id (or pre-assigned blob
  id) as the **AAD `blob_id`**. Name them separately in code. *(§5.5; the one place
  the accepted wording is literally circular.)*
- **T4 — `OPEN` step order is normative:** constant-time `CT` compare **first**, then
  GCM verify, then final-flag check. Enforce in code and in review; the commitment
  must gate before the key is trusted. *(Doc 21 Q5.)*
- **Minor — endianness split is intentional:** frame counter in the **nonce** is
  **big-endian** (Tink/age/ESDK); `frame_index` in the **AAD** is **little-endian**
  (codebase `<I`). Both are pinned; do not unify them.

Everything else in A1/A4/A5 is confirmed as written and implementable.

---

## Sources

Primary literature (PDFs read this session, 2026-09-15):
- Chan, Rogaway, **"On Committing Authenticated-Encryption"**, ESORICS 2022 —
  CTX construction (Fig. 2), Theorem 2 (`Adv^{cae-XX}_{CTX} ≤ Adv^{col}_H`).
  https://eprint.iacr.org/2022/1260 (PDF https://eprint.iacr.org/2022/1260.pdf)
- Bellare, Hoang, **"Efficient Schemes for Committing Authenticated Encryption"**,
  EUROCRYPT 2022 — CMT-1/CMT-4 ladder, "GCM/ChaCha20-Poly1305/OCB … CMT-1-insecure",
  HtE, the SHA-256 truncation/length-extension caveat (§3).
  https://eprint.iacr.org/2022/268 (PDF https://eprint.iacr.org/2022/268.pdf)
- Albertini, Duong, Gueron, Kölbl, Luykx, Schmieg, **"How to Abuse and Fix
  Authenticated Encryption Without Key Commitment"**, USENIX Security 2022 —
  multi-key ciphertexts; the partitioning-oracle motivation.
  https://www.usenix.org/conference/usenixsecurity22/presentation/albertini
- Len, Grubbs, Ristenpart, **"Partitioning Oracle Attacks"**, USENIX Security 2021.
  https://www.usenix.org/system/files/sec21-len.pdf
- Bellare, Hoang, Wu, **"The Landscape of Committing Authenticated Encryption"**,
  NIST BCM Workshop 2023 — 128-bit committing target ⇒ 256-bit output.

Standards / reference layouts (read this session):
- **NIST SP 800-38D** (GCM), §5.2.1.1 (`len(P) ≤ 2^39−256`), §8.3 (invocations
  `≤ 2^32` for RBG/non-96-bit-deterministic IVs).
  https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication800-38d.pdf
- **Google Tink** AES-GCM-HKDF-STREAMING — nonce `NoncePrefix(7)‖i(4,BE)‖b(1)`,
  `b∈{0x00,0x01}`. https://developers.google.com/tink/streaming-aead/aes_gcm_hkdf_streaming
- **age** spec — payload STREAM nonce `ctr(11,BE)‖b(1)`, 64 KiB chunks.
  https://c2sp.org/age
- **AWS Encryption SDK** — per-frame deterministic IV, 2^32 frame ceiling.

Rust ecosystem (vendored crate source read this session):
- `aes-gcm` **0.11.1**, `aead` **0.6.1** (detached `AeadInOut`; **no `stream`
  module** in 0.6.1), `sha2` **0.10.9**, `blake3` **1.8.7**, `subtle` **2.6.1**.
  No committing-AEAD crate exists → hand-rolled wrapper.

Repository context (consumed):
[`21-committing-aead-construction.md`](./21-committing-aead-construction.md),
[`23-chunking-and-range-prior-art.md`](./23-chunking-and-range-prior-art.md),
[`crypto-dedup-research.md`](./crypto-dedup-research.md),
[`decisions-register.md`](./decisions-register.md) §A,
[`01-crypto-envelope.md`](./01-crypto-envelope.md).

PoC (this session, throwaway): `ctx_poc/src/main.rs` + `golden_vectors.json` in the
session scratchpad. `cargo test` 9/9 pass on rustc 1.97.1.
