//! PoC: CTX (CMT-4) committing AEAD over AES-256-GCM, per-frame, with STREAM nonce.
//! Verifies the "D-pragmatic" wire  N ‖ ct ‖ T ‖ CT  for the hippius-s3 rewrite
//! suite `hip-enc/aes256gcm-ctx-frames-v1`.
//!
//! Throwaway verification artifact — NOT production code. Reference for the implementer.

use aes_gcm::aead::{AeadInOut, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce, Tag};
use serde::Serialize;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

// ---------------------------------------------------------------------------
// Suite constants
// ---------------------------------------------------------------------------
const SUITE_ID: &str = "hip-enc/aes256gcm-ctx-frames-v1";
const DOMAIN_SEP: &[u8] = b"hip-enc/ctx/v1"; // commitment domain separation
const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;
const CT_LEN: usize = 32; // 256-bit commitment -> 128-bit committing security
const PREFIX_LEN: usize = 7; // STREAM random prefix (Tink layout: 7 ‖ 4 ‖ 1 = 12)

#[derive(Debug)]
enum OpenError {
    Commitment,   // CTX commitment mismatch (partitioning-oracle closure)
    Aead,         // GCM tag mismatch
    Truncated,    // final-frame flag says more frames should follow
    Malformed,    // wrong length
}

// ---------------------------------------------------------------------------
// Canonical, injective (length-prefixed) commitment preimage.
//   LP(x) = LE32(len(x)) ‖ x    (all fields are actually fixed-length here, but
//   length-prefixing is future-proof and makes injectivity unconditional)
//   preimage = DOMAIN_SEP-LP ‖ LP(DEK) ‖ LP(N) ‖ LP(A) ‖ LP(T)
// ---------------------------------------------------------------------------
fn lp(out: &mut Vec<u8>, field: &[u8]) {
    out.extend_from_slice(&(field.len() as u32).to_le_bytes());
    out.extend_from_slice(field);
}

fn commitment_preimage(dek: &[u8; 32], nonce: &[u8], aad: &[u8], tag: &[u8]) -> Vec<u8> {
    let mut p = Vec::with_capacity(5 * 4 + DOMAIN_SEP.len() + 32 + 12 + aad.len() + 16);
    lp(&mut p, DOMAIN_SEP);
    lp(&mut p, dek);
    lp(&mut p, nonce);
    lp(&mut p, aad);
    lp(&mut p, tag);
    p
}

fn commit_sha256(dek: &[u8; 32], nonce: &[u8], aad: &[u8], tag: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(commitment_preimage(dek, nonce, aad, tag));
    h.finalize().into()
}

fn commit_blake3(dek: &[u8; 32], nonce: &[u8], aad: &[u8], tag: &[u8]) -> [u8; 32] {
    *blake3::hash(&commitment_preimage(dek, nonce, aad, tag)).as_bytes()
}

// ---------------------------------------------------------------------------
// STREAM nonce:  random_prefix(7) ‖ frame_index(4, BE) ‖ final_flag(1)
//   final_flag = 0x01 for the last frame in the blob, else 0x00 (Tink/age).
// ---------------------------------------------------------------------------
fn build_nonce(prefix: &[u8; PREFIX_LEN], frame_index: u32, is_final: bool) -> [u8; NONCE_LEN] {
    let mut n = [0u8; NONCE_LEN];
    n[0..PREFIX_LEN].copy_from_slice(prefix);
    n[PREFIX_LEN..PREFIX_LEN + 4].copy_from_slice(&frame_index.to_be_bytes());
    n[NONCE_LEN - 1] = if is_final { 0x01 } else { 0x00 };
    n
}

// ---------------------------------------------------------------------------
// AAD (blob identity):  blob_id(32) ‖ LE32(frame_index) ‖ suite_id bytes
// ---------------------------------------------------------------------------
fn build_aad(blob_id: &[u8; 32], frame_index: u32) -> Vec<u8> {
    let mut a = Vec::with_capacity(32 + 4 + SUITE_ID.len());
    a.extend_from_slice(blob_id);
    a.extend_from_slice(&frame_index.to_le_bytes());
    a.extend_from_slice(SUITE_ID.as_bytes());
    a
}

// ---------------------------------------------------------------------------
// SEAL one frame -> N ‖ ct ‖ T ‖ CT
// ---------------------------------------------------------------------------
fn frame_seal(
    dek: &[u8; 32],
    prefix: &[u8; PREFIX_LEN],
    blob_id: &[u8; 32],
    frame_index: u32,
    is_final: bool,
    plaintext: &[u8],
) -> Vec<u8> {
    let nonce = build_nonce(prefix, frame_index, is_final);
    let aad = build_aad(blob_id, frame_index);

    let cipher = Aes256Gcm::new(dek.into());
    let mut buf = plaintext.to_vec();
    let tag: Tag = cipher
        .encrypt_inout_detached(&Nonce::try_from(&nonce[..]).unwrap(), &aad, (&mut buf[..]).into())
        .expect("gcm encrypt");

    let ct = commit_sha256(dek, &nonce, &aad, tag.as_slice());

    let mut out = Vec::with_capacity(NONCE_LEN + buf.len() + TAG_LEN + CT_LEN);
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&buf);
    out.extend_from_slice(tag.as_slice());
    out.extend_from_slice(&ct);
    out
}

// ---------------------------------------------------------------------------
// OPEN one frame. Checks commitment (const-time) BEFORE trusting decrypt,
// then GCM-verifies, then checks the final-frame flag against expectation.
// ---------------------------------------------------------------------------
fn frame_open(
    dek: &[u8; 32],
    blob_id: &[u8; 32],
    frame_index: u32,
    expect_final: bool,
    frame: &[u8],
) -> Result<Vec<u8>, OpenError> {
    if frame.len() < NONCE_LEN + TAG_LEN + CT_LEN {
        return Err(OpenError::Malformed);
    }
    let nonce = &frame[..NONCE_LEN];
    let ct_body_end = frame.len() - TAG_LEN - CT_LEN;
    let body = &frame[NONCE_LEN..ct_body_end];
    let tag = &frame[ct_body_end..ct_body_end + TAG_LEN];
    let ct = &frame[ct_body_end + TAG_LEN..];

    let aad = build_aad(blob_id, frame_index);

    // 1. Commitment check FIRST (closes the partitioning oracle), constant-time.
    let ct_expected = commit_sha256(dek, nonce, &aad, tag);
    if ct_expected.ct_eq(ct).unwrap_u8() != 1 {
        return Err(OpenError::Commitment);
    }

    // 2. Standard AES-256-GCM verified decrypt (reuses audited crate path).
    let cipher = Aes256Gcm::new(dek.into());
    let mut buf = body.to_vec();
    cipher
        .decrypt_inout_detached(
            &Nonce::try_from(nonce).unwrap(),
            &aad,
            (&mut buf[..]).into(),
            &Tag::try_from(tag).unwrap(),
        )
        .map_err(|_| OpenError::Aead)?;

    // 3. Truncation / final-frame flag: the authenticated last nonce byte must
    //    match the reader's expectation (frame count is also known from metadata).
    let flag_is_final = nonce[NONCE_LEN - 1] == 0x01;
    if flag_is_final != expect_final {
        return Err(OpenError::Truncated);
    }

    Ok(buf)
}

// ---------------------------------------------------------------------------
// Golden test vectors
// ---------------------------------------------------------------------------
#[derive(Serialize)]
struct Vector {
    description: String,
    suite_id: String,
    domain_sep_hex: String,
    dek_hex: String,
    nonce_prefix_hex: String,
    blob_id_hex: String,
    frame_index: u32,
    is_final: bool,
    plaintext_hex: String,
    nonce_hex: String,
    aad_hex: String,
    commitment_preimage_hex: String,
    ciphertext_body_hex: String,
    gcm_tag_hex: String,
    commitment_sha256_hex: String,
    commitment_blake3_hex: String,
    full_frame_wire_hex: String,
    frame_len_bytes: usize,
}

fn make_vector(
    description: &str,
    dek: &[u8; 32],
    prefix: &[u8; PREFIX_LEN],
    blob_id: &[u8; 32],
    frame_index: u32,
    is_final: bool,
    plaintext: &[u8],
) -> Vector {
    let nonce = build_nonce(prefix, frame_index, is_final);
    let aad = build_aad(blob_id, frame_index);
    let cipher = Aes256Gcm::new(dek.into());
    let mut buf = plaintext.to_vec();
    let tag: Tag = cipher
        .encrypt_inout_detached(&Nonce::try_from(&nonce[..]).unwrap(), &aad, (&mut buf[..]).into())
        .unwrap();
    let ct_sha = commit_sha256(dek, &nonce, &aad, tag.as_slice());
    let ct_b3 = commit_blake3(dek, &nonce, &aad, tag.as_slice());
    let full = frame_seal(dek, prefix, blob_id, frame_index, is_final, plaintext);
    Vector {
        description: description.into(),
        suite_id: SUITE_ID.into(),
        domain_sep_hex: hex::encode(DOMAIN_SEP),
        dek_hex: hex::encode(dek),
        nonce_prefix_hex: hex::encode(prefix),
        blob_id_hex: hex::encode(blob_id),
        frame_index,
        is_final,
        plaintext_hex: hex::encode(plaintext),
        nonce_hex: hex::encode(nonce),
        aad_hex: hex::encode(&aad),
        commitment_preimage_hex: hex::encode(commitment_preimage(dek, &nonce, &aad, tag.as_slice())),
        ciphertext_body_hex: hex::encode(&buf),
        gcm_tag_hex: hex::encode(tag.as_slice()),
        commitment_sha256_hex: hex::encode(ct_sha),
        commitment_blake3_hex: hex::encode(ct_b3),
        full_frame_wire_hex: hex::encode(&full),
        frame_len_bytes: full.len(),
    }
}

fn main() {
    // Deterministic, hard-coded inputs so vectors are reproducible.
    let dek: [u8; 32] = [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
        0x1e, 0x1f,
    ];
    let prefix: [u8; PREFIX_LEN] = [0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6];
    let blob_id: [u8; 32] = [0xbb; 32];

    let vectors = vec![
        make_vector(
            "frame 0, non-final, 32-byte plaintext",
            &dek, &prefix, &blob_id, 0, false, b"hippius-s3 ctx frame vector #000",
        ),
        make_vector(
            "frame 1, final, 16-byte plaintext",
            &dek, &prefix, &blob_id, 1, true, b"final frame bytes",
        ),
        make_vector(
            "frame 0, final, empty plaintext (0-byte object edge case)",
            &dek, &prefix, &blob_id, 0, true, b"",
        ),
    ];

    println!("{}", serde_json::to_string_pretty(&vectors).unwrap());
}

// ---------------------------------------------------------------------------
// Tests: round-trip + adversarial rejection
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> ([u8; 32], [u8; PREFIX_LEN], [u8; 32]) {
        ([0x11; 32], [0x22; PREFIX_LEN], [0x33; 32])
    }

    #[test]
    fn roundtrip_ok() {
        let (dek, prefix, blob) = fixture();
        let pt = b"the quick brown fox jumps over the lazy dog";
        let f = frame_seal(&dek, &prefix, &blob, 3, true, pt);
        let got = frame_open(&dek, &blob, 3, true, &f).expect("open");
        assert_eq!(got, pt);
    }

    #[test]
    fn wrong_dek_rejects() {
        let (dek, prefix, blob) = fixture();
        let f = frame_seal(&dek, &prefix, &blob, 0, true, b"secret");
        let mut wrong = dek;
        wrong[0] ^= 0x01;
        // A different DEK must fail the COMMITMENT check (the partitioning-oracle
        // closure), not merely GCM — and never authenticate.
        match frame_open(&wrong, &blob, 0, true, &f) {
            Err(OpenError::Commitment) => {}
            other => panic!("expected Commitment rejection, got {:?}", other),
        }
    }

    #[test]
    fn wrong_frame_index_rejects() {
        let (dek, prefix, blob) = fixture();
        let f = frame_seal(&dek, &prefix, &blob, 5, false, b"data");
        // Reader reconstructs AAD with the wrong frame_index -> commitment mismatch.
        assert!(frame_open(&dek, &blob, 6, false, &f).is_err());
    }

    #[test]
    fn wrong_blob_id_rejects() {
        let (dek, prefix, blob) = fixture();
        let f = frame_seal(&dek, &prefix, &blob, 0, true, b"data");
        let other_blob = [0x99; 32];
        assert!(frame_open(&dek, &other_blob, 0, true, &f).is_err());
    }

    #[test]
    fn flipped_commitment_rejects() {
        let (dek, prefix, blob) = fixture();
        let mut f = frame_seal(&dek, &prefix, &blob, 0, true, b"data");
        let last = f.len() - 1;
        f[last] ^= 0x01; // flip a commitment byte
        match frame_open(&dek, &blob, 0, true, &f) {
            Err(OpenError::Commitment) => {}
            other => panic!("expected Commitment rejection, got {:?}", other),
        }
    }

    #[test]
    fn flipped_ciphertext_rejects() {
        let (dek, prefix, blob) = fixture();
        let mut f = frame_seal(&dek, &prefix, &blob, 0, true, b"data-body-here");
        f[NONCE_LEN] ^= 0x01; // flip a ciphertext byte -> GCM tag fails
        match frame_open(&dek, &blob, 0, true, &f) {
            Err(OpenError::Aead) => {}
            other => panic!("expected Aead rejection, got {:?}", other),
        }
    }

    #[test]
    fn truncation_flag_mismatch_rejects() {
        let (dek, prefix, blob) = fixture();
        // Seal a NON-final frame, then the reader expects it to be final
        // (i.e. the real final frame was dropped) -> detected.
        let f = frame_seal(&dek, &prefix, &blob, 2, false, b"midstream");
        match frame_open(&dek, &blob, 2, true, &f) {
            Err(OpenError::Truncated) => {}
            other => panic!("expected Truncated rejection, got {:?}", other),
        }
    }

    #[test]
    fn final_flag_is_authenticated_in_nonce() {
        // An attacker who flips the final-flag byte in the stored nonce cannot
        // make it verify: the flag is part of the GCM nonce AND the commitment.
        let (dek, prefix, blob) = fixture();
        let mut f = frame_seal(&dek, &prefix, &blob, 0, false, b"data");
        f[NONCE_LEN - 1] = 0x01; // flip flag 0x00 -> 0x01 in the wire nonce
        // Reader now sees a "final" flag; commitment was computed over the
        // original nonce, so it mismatches.
        match frame_open(&dek, &blob, 0, true, &f) {
            Err(OpenError::Commitment) => {}
            other => panic!("expected Commitment rejection, got {:?}", other),
        }
    }

    #[test]
    fn empty_plaintext_roundtrip() {
        let (dek, prefix, blob) = fixture();
        let f = frame_seal(&dek, &prefix, &blob, 0, true, b"");
        assert_eq!(frame_open(&dek, &blob, 0, true, &f).unwrap(), b"");
    }
}
