# ctx-poc — reference PoC for the committing AEAD (`hip-enc/aes256gcm-ctx-frames-v1`)

**Throwaway verification artifact — NOT production code.** This is the proof-of-concept that
[`../25-crypto-verification.md`](../25-crypto-verification.md) built to verify the frozen crypto
construction. It is preserved here (it was originally in a session scratchpad) because it is the
**reference the `s3-crypto` implementer mirrors** and the **golden-vector oracle** the eventual
production wrapper must reproduce byte-for-byte.

## What it is

A ~250-line single-file PoC of the CTX (CMT-4) committing AEAD over AES-256-GCM, applied per
256 KiB frame with the STREAM nonce, D-pragmatic wire `N ‖ ct ‖ T ‖ CT`. See doc 25 for the full
verification against the primary literature.

- `src/main.rs` — the seal/open reference + adversarial tests. `cargo test` → 9/9 pass; `cargo run`
  emits the golden vectors.
- `golden_vectors.json` — 3 emitted vectors (non-final, final, empty-final) with both SHA-256 and
  BLAKE3 commitments. **This is the acceptance oracle** for the production implementation.
- `Cargo.lock` — pins the exact crate versions doc 25 verified against.

## The crypto sign-off (the remaining Phase-0 gate)

The design is verified; what a cryptographer must still review is the **production** `s3-crypto`
committing wrapper (which does not exist yet — this is greenfield). This PoC is the reference for
that review. The load-bearing details to check in the real implementation:

1. **Injective commitment preimage** — `LP(DOMAIN_SEP)‖LP(DEK)‖LP(N)‖LP(A)‖LP(T)` (`commitment_preimage`).
   A non-injective encoding silently voids the CMT-4 proof.
2. **OPEN step order** — constant-time `CT` compare **first**, then GCM verify, then final-flag
   (`frame_open`). Wrong order reopens the partitioning oracle.
3. **Nonce** — `prefix(7)‖frame_index(4 BE)‖final_flag(1)` (`build_nonce`); note the deliberate
   BE-in-nonce / LE-in-AAD split.
4. **AAD** — `blob_id(32)‖LE32(frame_index)‖suite_id` (`build_aad`), with `blob_id` wired as the
   **opaque, pre-assigned** id (doc 25 §T3), not a hash of plaintext or ciphertext.
5. **Hash choice** — the PoC emits both; the suite pins **BLAKE3** (doc 25 §T1). The production
   wrapper should use BLAKE3, not the SHA-256 shown in `frame_seal`/`frame_open`.

## Run

```
cargo test           # 9/9 adversarial + round-trip tests
cargo run            # prints golden_vectors.json to stdout
```
