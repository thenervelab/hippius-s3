# Rust Rewrite Spec — Subsystem 01: Encryption / Envelope / Key Management

> **⚠️ THIS DOC DESCRIBES THE LEGACY / CURRENT-PYTHON FORMAT — needed only for the migration DECRYPT
> path.** The NEW system's crypto is **frozen in [`25-crypto-verification.md`](./25-crypto-verification.md)**
> (suite `hip-enc/aes256gcm-ctx-frames-v1`: CTX-over-frames committing AEAD, **per-blob** DEK, STREAM
> nonce, **opaque** `blob_id` AAD, framed single-ranged reads). The greenfield Rust service **never
> writes this format** — it only *reads* it, in a `legacy` decrypt module, to re-encrypt during
> migration. So wherever this doc says "the Rust port must encrypt like X" or "keep one DEK per
> object-version," read it as **"the migration decrypt path must understand the old bytes"** — not as
> the new design. The old-format facts (random prepended nonce, per-object-version DEK, 28-byte
> `nonce‖ct‖tag`, object-identity AAD) are accurate *for the legacy corpus* and are what the decrypt
> path must honor byte-for-byte.

**Status:** Implementation-grade, derived by reading the Python source (not the docs) as of commit on
branch `main`, 2026-09-15. Every claim below is anchored to a `file:line` reference. Where the
committed docs (`CLAUDE.md` files) disagree with the code, the **code wins** and the discrepancy is
called out explicitly.

**Scope:** This subsystem documents the **legacy** envelope the migration must decrypt. Getting any
byte layout, AAD construction, or key-derivation constant wrong means production objects written by
the Python service become permanently undecryptable *by the migration*. Read the
"Compatibility-critical" callout first — but as the **decrypt** contract, not the new write format.

---

## ⚠️ Compatibility-critical — formats that MUST match byte-for-byte

A Rust reimplementation that gets any of the following wrong will fail to decrypt existing
production objects, or will write objects the Python service cannot read. Each is expanded and
code-anchored in the body of this document.

| # | Format / constant | Exact value | Source |
|---|---|---|---|
| C1 | **Chunk AEAD cipher** | AES-256-GCM (96-bit nonce, 128-bit tag) | `crypto_service.py:70-153`, `crypto_service.py:191` |
| C2 | **Per-chunk wire layout** | `nonce(12 bytes) ‖ ciphertext ‖ tag(16 bytes)` — nonce is **random**, prepended, and read back on decrypt (NOT derived) | `crypto_service.py:133-135,150-153` |
| C3 | **Per-chunk AEAD AAD** (suite `hip-enc/aes256gcm`) | `LE_u16(len(bucket_id_utf8)) ‖ bucket_id_utf8 ‖ LE_u16(len(object_id_utf8)) ‖ object_id_utf8 ‖ LE_u32(part_number) ‖ LE_u32(chunk_index)` — **no upload_id, no object_version** | `crypto_service.py:162-176` |
| C4 | **DEK envelope cipher** | AES-256-GCM, layout `nonce(12) ‖ ciphertext ‖ tag(16)`, wrapping a 32-byte DEK under the 32-byte KEK | `envelope_service.py:17-33` |
| C5 | **DEK-wrap AAD** | ASCII `hippius-dek:{bucket_id}:{object_id}:{object_version}` (UUID strings; version is a base-10 int) | `object_writer.py:303`, `object_reader.py:226,241`, `copy_service_v5.py:216,221` |
| C6 | **Local KEK-wrap key derivation** (KMS-disabled mode only) | `SHA-256(b"hippius-local-kek-wrap-v1:" ‖ HIPPIUS_AUTH_ENCRYPTION_KEY_utf8)` → 32-byte AES key; wrap layout `nonce(12) ‖ ct ‖ tag(16)`, **AAD = None** | `local_kek_wrapper.py:30-60` |
| C7 | **DEK / KEK size** | 32 bytes (256-bit) each | `envelope_service.py:8`, `kek_service.py:45` |
| C8 | **Default plaintext chunk size** | 4 MiB = 4194304 bytes, but **readers MUST use the per-part `chunk_size_bytes` from the DB**, never the config | `config.py:575`, `planner.py:36-45`, `writer/CLAUDE.md:73` |
| C9 | **chunk_index scope** | **per-part**, 0-based (resets to 0 at the start of every part). It is NOT a global/object-wide counter | `planner.py:69-73`, `object_writer.py:414-424` |
| C10 | **Object BLAKE3** | `blake3(plaintext)` of the whole object body, unsalted, unkeyed, `max_threads=1`, lowercase hex | `blake3_hash.py:16-23` |
| C11 | **Single-object / per-part ETag** | `MD5(plaintext)` lowercase hex | `object_writer.py:326,436,461` |
| C12 | **Multipart composite ETag** | `MD5( concat( bytes.fromhex(part_md5) for each part in ascending part order ) ).hexdigest() + "-" + str(num_parts)` | `multipart.py:1126-1131` |
| C13 | **Keystore is a separate Postgres DB** | `bucket_keks` + `encryption_keys` tables live in `HIPPIUS_KEYSTORE_DATABASE_URL` (falls back to `DATABASE_URL` if unset). `object_versions` (with `kek_id`, `wrapped_dek`) lives in the MAIN DB | `config.py:159,951-952`, `kek_service.py:410-424`, `key_service.py:35-44` |
| C14 | **OVH KMS wrapped-KEK storage form** | The KMS-returned **JWE string** stored as raw UTF-8 bytes in `bucket_keks.wrapped_kek_bytes` (opaque to us — only the KMS can unwrap it) | `kek_service.py:187-190,263-272` |
| C15 | **Minimum storage version** | v5. Anything `< 5` is rejected (`UnsupportedStorageVersionError`). The rewrite only needs the v5 path | `storage_version.py:4,13-21` |

> **The single biggest trap:** the report that seeded this task said the nonce is "a deterministic
> nonce keyed by GLOBAL chunk index." **That is false for the current code.** The nonce is
> `os.urandom(12)` per chunk and travels in the ciphertext (C2). A deterministic-nonce derivation
> *used* to exist and was deliberately removed (see the long comment at `crypto_service.py:118-132`).
> A Rust port must read the prepended nonce, not derive one. See §2.

---

## 1. Cipher & AEAD

### 1.1 Chunk encryption (object body)

- **Algorithm:** AES-256-GCM, via `cryptography.hazmat.primitives.ciphers.aead.AESGCM`
  (`crypto_service.py:26,134`).
- **Key size:** 32 bytes (the DEK). The DEK is passed in as `key=` (`crypto_service.py:238-240`).
- **Nonce size:** 12 bytes (`AESGCMChunkedAdapter.NONCE_SIZE = 12`, `crypto_service.py:82`).
- **Tag size:** 16 bytes (`TAG_SIZE = 16`, `crypto_service.py:83`). The `cryptography` AESGCM API
  **appends** the tag to the ciphertext automatically.
- **Overhead per chunk:** 28 bytes = 12 (nonce) + 16 (tag) (`crypto_service.py:86-87`).
- **Associated data (AAD):** present and non-empty — see §1.3 / C3.

The only registered suite is `hip-enc/aes256gcm`, mapped to `AESGCMChunkedAdapterV2`
(`crypto_service.py:190-195`). `DEFAULT_SUITE_ID = "hip-enc/aes256gcm"` (`crypto_service.py:195`).
`get_adapter()` falls back to the default for any unknown/None suite id
(`crypto_service.py:204-208`).

> **Legacy note (out of scope but do not delete-support silently):** `services/CLAUDE.md:44-45`
> mentions a `hip-enc/legacy` NaCl SecretBox suite for v≤4 objects, and `key_service.py` still
> generates `nacl.secret.SecretBox` keys (`key_service.py:79`). **Neither is reachable in the v5
> path**: `hip-enc/legacy` is *not* in the `_ADAPTERS` registry (`crypto_service.py:190-192`), and
> `require_supported_storage_version` rejects anything `< 5` (C15). The Rust rewrite targets v5 only.
> `key_service.get_or_create_encryption_key_bytes` and the `encryption_keys` table are legacy and can
> be treated as read-only archaeology unless product decides to migrate v≤4 objects.

### 1.2 DEK envelope encryption (wrapping the DEK under the KEK)

- **Algorithm:** AES-256-GCM (`envelope_service.py:5,23`).
- **Key:** the 32-byte plaintext KEK.
- **Nonce:** 12 bytes random (`WRAP_NONCE_SIZE_BYTES = 12`, `envelope_service.py:9,22`).
- **Tag:** 16 bytes (implicit in `AESGCM.encrypt`; the length check at `envelope_service.py:29`
  requires `>= 12 + 16`).
- **AAD:** the DEK-wrap AAD (C5), passed by every caller.

### 1.3 Per-chunk AEAD AAD — exact byte layout (C3)

The active adapter is `AESGCMChunkedAdapterV2._build_aad` (`crypto_service.py:162-176`). It builds:

```python
parts = []
for s in [bucket_id, object_id]:            # NOTE: order is bucket_id THEN object_id
    s_bytes = s.encode("utf-8")
    parts.append(struct.pack("<H", len(s_bytes)))   # little-endian uint16 length prefix
    parts.append(s_bytes)
parts.append(struct.pack("<II", int(part_number), int(chunk_index)))  # two little-endian uint32
return b"".join(parts)
```

| Offset | Field | Type | Notes |
|---|---|---|---|
| 0 | `len(bucket_id)` | `u16` little-endian | byte length of UTF-8 bucket_id |
| 2 | `bucket_id` | UTF-8 bytes | UUID string, e.g. `"550e8400-e29b-41d4-a716-446655440000"` (36 bytes) |
| 2+B | `len(object_id)` | `u16` little-endian | byte length of UTF-8 object_id |
| 4+B | `object_id` | UTF-8 bytes | UUID string |
| 4+B+O | `part_number` | `u32` little-endian | 1-based |
| 8+B+O | `chunk_index` | `u32` little-endian | **per-part**, 0-based (C9) |

**`upload_id` is NOT in the V2 AAD, and neither is `object_version`.** This is the most important
subtlety in the whole subsystem and directly contradicts two of the committed docs:

- `writer/CLAUDE.md:69` claims the suite binds `(bucket_id, object_id, part_number, chunk_index,
  upload_id)`. That describes the **deprecated V1 adapter** (`AESGCMChunkedAdapter._build_aad`,
  `crypto_service.py:89-104`), which is not registered and not used by v5 writes.
- The write call sites pass an `upload_id` argument (simple PUT passes `""` at
  `object_writer.py:380`; MPU passes the real upload_id at `object_writer.py:804`), but **V2
  discards it**. So the fact that simple-PUT and MPU pass different `upload_id` values is harmless —
  it never enters the tag. A Rust port must likewise ignore `upload_id` in the chunk AAD.

The `bucket_id` used at write time is the real bucket UUID string (`object_writer.py:376` simple
PUT, `object_writer.py:800` MPU), and the read path reconstructs with the same real bucket UUID
(`streamer.py:243-247` → `decrypter.py:47-52` → `crypto_service.py:283-291`). They match.

> **Dead code warning:** `writer/chunker.py:29-49` (`stream_encrypt_to_chunks`) calls
> `encrypt_chunk` with `bucket_id=""`. If that path were live it would produce an AAD that the read
> path (which uses the real bucket_id) could not authenticate. It is **not** called anywhere — the
> only live `encrypt_chunk` call sites are `object_writer.py:373` and `object_writer.py:797`
> (confirmed by grep). Do not port `chunker.py`; if you do, fix the `bucket_id`.

### 1.4 DEK-wrap AAD — exact bytes (C5)

Constructed identically at every call site as:

```python
aad = f"hippius-dek:{bucket_id}:{object_id}:{object_version}".encode("utf-8")
```

- Write (simple PUT): `object_writer.py:303`
- Write (MPU initiate): `multipart.py:354`
- Write (MPU ensure-DEK): `object_writer.py:625,649,654`
- Read: `object_reader.py:226,241`
- Copy re-wrap (src + dest): `copy_service_v5.py:216,221`

`bucket_id` and `object_id` are UUID strings; `object_version` is a base-10 integer with no padding
(`int(resolved_version)` → Python `str()` semantics, e.g. `1`, `2`, `47`). Example AAD bytes:
`hippius-dek:550e8400-e29b-41d4-a716-446655440000:6ba7b810-9dad-11d1-80b4-00c04fd430c8:3`.

**Version binding lives in the DEK-wrap AAD, not the chunk AAD.** The chunk tag does not cover the
version; the DEK's unwrap does. This is why the copy fast-path can re-wrap the same DEK under a new
`(dest_object_id, dest_version)` AAD and reuse the already-encrypted chunk CIDs unchanged
(`copy_service_v5.py:186-224`) — the chunk ciphertext/tag is independent of version because the AAD
that binds the chunk (C3) contains no version, and `object_id` is what changes there.

> **Copy caveat (C3 interaction):** `execute_v5_fast_path_copy` reuses source chunk CIDs verbatim
> (`copy_service_v5.py:148-183`) while changing `object_id`. The source chunks' AEAD AAD embeds the
> **source** `object_id` (C3), yet the destination will decrypt using the **destination**
> `object_id`. This only works if the destination read reconstructs the AAD with the source
> object_id — but `object_reader.py:241` uses `info.get('object_id')`, the destination's. **This is
> a latent open question — see Open Questions Q1.** The copy fast path may be gated off for the
> general case; `services/CLAUDE.md:82` notes it is currently a "latent risk if re-enabled for MPU."

---

## 2. Nonce derivation (there is none — it's random)

**The nonce is `os.urandom(12)` generated fresh per chunk and prepended to the ciphertext**
(`crypto_service.py:133-135`):

```python
nonce = os.urandom(self.NONCE_SIZE)          # 12 random bytes
ct = AESGCM(key).encrypt(nonce, plaintext, aad)
return nonce + ct                             # nonce ‖ ciphertext ‖ tag
```

Decryption reads it back from the front (`crypto_service.py:148-153`):

```python
if len(ciphertext) < self.NONCE_SIZE + self.TAG_SIZE:      # < 28 → CryptoError("ciphertext_too_short")
    raise CryptoError("ciphertext_too_short")
nonce = ciphertext[: self.NONCE_SIZE]                       # first 12 bytes
body  = ciphertext[self.NONCE_SIZE :]                        # rest = ct ‖ tag
return bytes(AESGCM(key).decrypt(nonce, body, aad))
```

### Why the "deterministic nonce keyed by global chunk index" claim is wrong

The seed report described a prior design. The current code carries an explicit tombstone comment
(`crypto_service.py:118-132`) explaining the removal. Paraphrased from the source:

> The nonce used to be `HMAC(DEK, bucket|object|part|chunk)`, and every one of those inputs is
> stable across an `UploadPart` retry — as is the DEK, which is per object-version. So a client
> re-uploading a part with different bytes encrypted different plaintext under the SAME key and
> nonce … the keystream repeats … Safe to change with no migration and no suite bump: the nonce
> travels in the ciphertext and `decrypt_chunk` reads it from there rather than re-deriving, so
> objects written either side of this change decrypt through the identical path.

**Implications for the migration DECRYPT path** (the NEW system does not encrypt in this format — it
uses the STREAM nonce of doc 25):

- **Decrypt (this is all the Rust migration path needs):** read the first 12 bytes as the nonce.
  Never derive. Objects written under *both* the old (derived) and new-Python (random) regimes decrypt
  identically — the derivation is gone from the legacy read path entirely.
- **~~Encrypt~~ (N/A to the rewrite):** the legacy corpus was written with a random prepended 12-byte
  nonce; the greenfield service never writes this — it seals with the doc-25 STREAM nonce
  (`prefix(7)‖frame_index(4 BE)‖final_flag(1)`) into CTX frames.
- **Endianness / width questions in the seed report are moot** — the legacy nonce has no counter,
  offset, or hash feeding it.
- The safety argument for the legacy random 96-bit IVs rests on the DEK being **per object-version**:
  chunk count per key is bounded (~2^20 for a 5 TB object at 4 MiB chunks), far under the NIST SP
  800-38D 2^32 limit (`crypto_service.py:126-128`). *(The NEW system does not inherit this — it uses a
  **per-blob** DEK + STREAM nonce, doc 25 §2/§3, with ≤64 frames per key. This bullet is legacy
  rationale, not a rewrite constraint.)*

---

## 3. Chunking

### 3.1 Chunk size

- Config default: `HIPPIUS_CHUNK_SIZE_BYTES = 4194304` (4 MiB) (`config.py:575`,
  `object_writer.py:689` for MPU reads `config.object_chunk_size_bytes`).
- **`chunk_size` is the PLAINTEXT bytes per chunk.** The ciphertext chunk is `chunk_size + 28`
  bytes (except the final short chunk).
- **Readers MUST use the per-part `chunk_size_bytes` stored in the DB, not the config**
  (`planner.py:36-45`, `writer/CLAUDE.md:73`). Legacy/other objects may have different chunk sizes
  per part. The planner falls back to a hardcoded 4 MiB (`_DEFAULT_CHUNK_SIZE_BYTES`,
  `planner.py:18,68`) only when a part has `size_bytes > 0` but a missing/zero `chunk_size_bytes`
  (a DB inconsistency) — this fallback must match the writer's meta exactly or chunk boundaries
  desync.

### 3.2 How a stream is split

Write side (simple PUT), `object_writer.py:404-424`:

```python
pt_buf = bytearray()
next_chunk_index = 0
async for piece in body_iter:
    pt_buf.extend(piece)
    while len(pt_buf) >= chunk_size:
        buf = bytes(pt_buf[:chunk_size]); del pt_buf[:chunk_size]
        await pipeline.push(buf, int(next_chunk_index)); next_chunk_index += 1
if pt_buf:                                   # final short chunk
    await pipeline.push(bytes(pt_buf), int(next_chunk_index)); next_chunk_index += 1
```

So chunks are fixed `chunk_size` plaintext, with a final possibly-short chunk. The
`encrypt_part_to_chunks` batch helper computes the same boundaries: `num_chunks = (total +
chunk_size - 1) // chunk_size` (`crypto_service.py:248-262`).

### 3.3 Chunk boundaries ↔ parts/versions

- **Simple PUT:** always `part_number = 1` (`object_writer.py:225`). `chunk_index` runs
  `0..num_chunks-1` for that single part. Because there is one part, the per-part index doubles as
  the object index, but the *scope* is still per-part (C9).
- **MPU:** each `UploadPart` is its own part with its own `chunk_index` counter starting at 0
  (`mpu_upload_part_stream`, `object_writer.py:719` `next_chunk_index = 0`). Part `P` chunk `C` is
  uniquely `(part_number=P, chunk_index=C)` in the AAD.
- **Version:** all parts of one object-version share **one DEK** (per object-version), wrapped once
  per version (`object_writer.py:228`, MPU `_ensure_and_get_v5_dek` reuses the version's existing
  envelope for parts 2..N, `object_writer.py:603-626`). A new PUT/overwrite reserves a new
  object_version and a new DEK (`object_writer.py:228,236`).

### 3.4 Per-chunk framing / stored sidecar bytes

- The **only** in-band framing is the 12-byte nonce prefix (C2). There is no magic number, no
  length field, no version byte inside the ciphertext blob. The 16-byte GCM tag is a suffix.
- Out-of-band per-chunk metadata lives in the `part_chunks` table (§6):
  `cipher_size_bytes` (= plaintext + 28), `plain_size_bytes`, `cid`, optional `checksum`
  (`sql/migrations/20251003000000_create_part_chunks.sql:6-16`). The reader uses these to size the
  plan and locate each chunk; the ciphertext blob itself is self-describing only for its nonce.
- Each chunk is stored as its own object on the storage backend (Arion), addressed by its own
  **CID** (`part_chunks.cid`), one CID per `(part_id, chunk_index)`.

---

## 4. Envelope layers

Key hierarchy (`services/CLAUDE.md:28-41`, `kek_service.py:1-22`):

```
   OVH KMS master key (HSM, mTLS)         [required mode]
   — or — HIPPIUS_AUTH_ENCRYPTION_KEY     [disabled mode]
        │ wraps
        ▼
   Bucket KEK  (32 bytes, one active per bucket)
        │ stored WRAPPED in keystore DB: bucket_keks.wrapped_kek_bytes
        │ wraps (AES-256-GCM, envelope_service.wrap_dek)
        ▼
   Object DEK  (32 bytes, per object-version)
        │ stored WRAPPED in main DB: object_versions.wrapped_dek
        │ encrypts (AES-256-GCM per chunk, random nonce)
        ▼
   Chunk ciphertext (stored on Arion, one CID per chunk)
```

### 4.1 DEK generation

`envelope_service.generate_dek()` = `os.urandom(32)` (`envelope_service.py:8,12-14`). RNG source is
the OS CSPRNG. In Rust: `let mut dek = [0u8; 32]; OsRng.fill_bytes(&mut dek);`.

### 4.2 DEK wrapped by the bucket KEK

`envelope_service.wrap_dek(kek, dek, aad)` (`envelope_service.py:17-24`):

```python
nonce = os.urandom(12)
ct = AESGCM(kek).encrypt(nonce, dek, aad)   # aad = "hippius-dek:{bucket_id}:{object_id}:{version}"
return nonce + ct                            # 12 + 32 + 16 = 60 bytes total
```

Wrapped DEK layout:

| Offset | Field | Size |
|---|---|---|
| 0 | nonce | 12 |
| 12 | encrypted DEK | 32 |
| 44 | GCM tag | 16 |
| | **total** | **60 bytes** |

`unwrap_dek` (`envelope_service.py:27-33`) requires `len >= 28` then splits `nonce = [:12]`,
`body = [12:]`, `AESGCM(kek).decrypt(nonce, body, aad)`.

Stored in `object_versions.wrapped_dek` (`bytea`,
`sql/migrations/20260108000000_add_v5_envelope_to_object_versions.sql:22`). The `kek_id` used is
persisted alongside in `object_versions.kek_id` (`uuid`, same migration line 19) so reads always
know which KEK to fetch even after a bucket KEK rotation.

### 4.3 KEK generation and wrapping

Two modes, chosen by `HIPPIUS_KMS_MODE` (`kek_service.py:160-212`):

**KMS-required mode** (`_create_wrapped_kek`, `kek_service.py:177-199`):
- The KMS **generates** the KEK and returns `(plaintext_bytes, wrapped_jwe_string)` in one call
  (`ovh_kms_client.generate_data_key`, §5).
- `wrapped_bytes = wrapped_jwe.encode("utf-8")` — the JWE token stored as UTF-8 bytes
  (`kek_service.py:189`). We never see how KMS wraps it; it is opaque (C14).
- `key_id = cfg.ovh_kms_default_key_id` is stored in `bucket_keks.kms_key_id`.

**KMS-disabled mode** (dev/local, `kek_service.py:200-212`):
- `kek_bytes = os.urandom(32)` generated locally.
- `wrapped_bytes = wrap_key_local(kek_bytes, HIPPIUS_AUTH_ENCRYPTION_KEY)` (§5.4 / C6).
- `key_id = "local"` (`LOCAL_WRAP_KEY_ID`, `local_kek_wrapper.py:23`).

The unwrap path (`_unwrap_kek`, `kek_service.py:215-281`) dispatches on the stored `kms_key_id`:
`"local"` → `unwrap_key_local`; anything else → KMS decrypt. **A KMS-wrapped KEK cannot be unwrapped
in disabled mode** — it raises rather than silently failing (`kek_service.py:249-256`).

### 4.4 Where each wrapped key lives

| Key | Wrapped by | Stored in | Column / form |
|---|---|---|---|
| Bucket KEK | OVH KMS master (or local wrap key) | **keystore DB** `bucket_keks` | `wrapped_kek_bytes bytea` (JWE-UTF8 for KMS, `nonce‖ct‖tag` for local); `kms_key_id text` |
| Object DEK | Bucket KEK | **main DB** `object_versions` | `wrapped_dek bytea` (60-byte `nonce‖ct‖tag`); `kek_id uuid` |
| Chunk data | Object DEK | storage backend (Arion) | ciphertext blob, `part_chunks.cid` points to it |

### 4.5 "v5 envelope"

"v5 envelope" = the `(encryption_version=5, enc_suite_id, enc_chunk_size_bytes, kek_id,
wrapped_dek)` tuple on an `object_versions` row (`object_writer.py:305-321`,
`sql/migrations/20260108000000_add_v5_envelope_to_object_versions.sql:10-22`). The envelope is
written **atomically inside the version-reservation transaction**, before any chunk is written and
before the version becomes serveable, to close the race where a concurrent GET sees a bumped
`current_object_version` with NULL envelope columns and 500s with `v5_missing_envelope_metadata`
(`object_writer.py:240-244`, `writer/CLAUDE.md:28`, MPU `_write_v5_envelope` at
`multipart.py:337-368`). `report_broken_v5_rows.py` exists to find rows where this invariant was
violated historically (200k+ broken rows in prod per `writer/CLAUDE.md:28`).

Column types (all `NULL`-able because the columns were added by migration to a pre-existing table):

| Column | Type | Meaning |
|---|---|---|
| `encryption_version` | `int2` | always `5` for new writes (`object_writer.py:308`) |
| `enc_suite_id` | `text` | `"hip-enc/aes256gcm"` |
| `enc_chunk_size_bytes` | `int4` | plaintext chunk size used for this version |
| `kek_id` | `uuid` | which bucket KEK wraps the DEK |
| `wrapped_dek` | `bytea` | 60-byte wrapped DEK |
| `body_blake3` | `text` | BLAKE3 of plaintext (added `20260824120000` / `20260915120000`) |

---

## 5. OVH KMS protocol

Client: `services/ovh_kms_client.py`. Transport: `httpx.AsyncClient` with **mTLS** (client cert +
key + CA) (`ovh_kms_client.py:112-121`).

### 5.1 Auth

Mutual TLS. Cert/key/CA paths from config (`ovh_kms_client.py:91-93`):
`HIPPIUS_OVH_KMS_CERT_PATH`, `HIPPIUS_OVH_KMS_KEY_PATH`, `HIPPIUS_OVH_KMS_CA_PATH`
(`config.py:640-645`). `verify=ca_path` (or `True` for system CAs). No bearer token / API key —
the client certificate is the credential. `wait_for_certs` polls for the cert files at startup to
survive docker-compose races (`ovh_kms_client.py:123-174`).

### 5.2 Generate data key (creates + wraps a KEK)

- **Method/URL:** `POST /api/{okms_id}/v1/servicekey/{key_id}/datakey`
  (`ovh_kms_client.py:197`).
- **Body:** `{"name": "kek", "size": 256}` (`ovh_kms_client.py:198`).
- **Response JSON:** `{"plaintext": <base64 32-byte key>, "key": <JWE string>}`
  (`ovh_kms_client.py:203-206`). Returns `(base64decode(plaintext), jwe)`.
- `okms_id` = `HIPPIUS_OVH_KMS_OKMS_ID`; `key_id` = `HIPPIUS_OVH_KMS_DEFAULT_KEY_ID` for new keys.

### 5.3 Decrypt data key (unwraps a KEK)

- **Method/URL:** `POST /api/{okms_id}/v1/servicekey/{key_id}/datakey/decrypt`
  (`ovh_kms_client.py:225`).
- **Body:** `{"key": <JWE string>}` (`ovh_kms_client.py:226`).
- **Response JSON:** `{"plaintext": <base64 key>}` → `base64decode(plaintext)`
  (`ovh_kms_client.py:230-233`).
- **`key_id` per call:** new KEKs use the default key id; existing KEKs use the id stored in their
  `bucket_keks.kms_key_id` row, so rotation of the KMS master key is seamless (old keys stay
  decryptable) (`ovh_kms_client.py:56-60`, `kek_service.py:272`).

### 5.4 What KMS wraps vs. what local wrapping does

- **KMS mode:** KMS wraps the **KEK** (returns opaque JWE). Local code never wraps or unwraps the
  KEK; it only ever wraps the **DEK** (under the plaintext KEK the KMS handed back). So even in
  required mode, DEK-wrapping is always the local `envelope_service` AES-256-GCM (C4).
- **Local (disabled) mode:** `local_kek_wrapper` wraps the KEK with a key **derived from
  `HIPPIUS_AUTH_ENCRYPTION_KEY`** (C6):

  ```python
  wrapping_key = sha256(b"hippius-local-kek-wrap-v1:" + secret.encode()).digest()   # 32 bytes
  nonce = os.urandom(12)
  return nonce + AESGCM(wrapping_key).encrypt(nonce, plaintext_kek, associated_data=None)  # AAD None!
  ```

  Note **`associated_data=None`** for the local KEK wrap (`local_kek_wrapper.py:57,85`) — unlike the
  DEK wrap which always has an AAD. Layout `nonce(12) ‖ ct ‖ tag(16)`, so a wrapped 32-byte KEK is
  60 bytes.

### 5.5 `HIPPIUS_KMS_MODE` behavior

- Values: `"required"` or `"disabled"` only; validated at startup, else `ValueError`
  (`config.py:968-969`). Default is `"disabled"` (`config.py:633`).
- **`required`:** `init_kms_client` waits for certs and constructs the KMS client, failing fast if
  unavailable (`kek_service.py:118-137`). No silent fallback. Required config keys
  (`endpoint, okms_id, default_key_id, cert_path, key_path`) are enforced (`config.py:971-982`).
- **`disabled`:** KMS client stays `None`; local wrapping used
  (`kek_service.py:114-116,151-152`). `HIPPIUS_AUTH_ENCRYPTION_KEY` is required in this mode
  (`config.py:987-989`).
- **Retry/backoff:** transient 5xx/429 and timeouts retry with exponential backoff + jitter
  (`_compute_backoff_ms`, `ovh_kms_client.py:39-43`; loop `237-306`). 401/403 → immediate
  `OVHKMSAuthenticationError`, no retry (`ovh_kms_client.py:260-261`). Defaults: `max_retries=3`,
  `base=500ms`, `max=5000ms` (`config.py:647-649`).

---

## 6. Keystore DB & read path to plaintext

### 6.1 Keystore is a separate database

`encryption_database_url = HIPPIUS_KEYSTORE_DATABASE_URL` (`config.py:159`), and if unset it
defaults to `DATABASE_URL` (`config.py:951-952`). Two tables are (lazily) created there:

**`bucket_keks`** (`kek_service.py:410-424`):

| Column | Type | Notes |
|---|---|---|
| `bucket_id` | `UUID NOT NULL` | |
| `kek_id` | `UUID PRIMARY KEY` | |
| `wrapped_kek_bytes` | `BYTEA NOT NULL` | JWE-UTF8 (KMS) or `nonce‖ct‖tag` (local) |
| `kms_key_id` | `TEXT NOT NULL, CHECK (<> '')` | `"local"` or KMS key id |
| `status` | `TEXT NOT NULL DEFAULT 'active'` | rotation modeled via status |
| `created_at` | `TIMESTAMPTZ` | |

Unique partial index `uniq_bucket_active_kek ON bucket_keks(bucket_id) WHERE status='active'`
enforces one active KEK per bucket (`kek_service.py:420-422`). The DDL is created lazily under a
`pg_advisory_xact_lock` (key `0x62756B656B`) because the keystore DB is not targeted by the dbmate
migrator (`kek_service.py:397-424`).

**`encryption_keys`** (legacy v≤4, `key_service.py:34-44`) — SERIAL id, `subaccount_id` =
`sha256(f"{main_account_id}:{bucket_name}")` hex, `encryption_key_b64`. Not used by v5. Ignore for
the rewrite except as archival read-support if product asks.

### 6.2 KEK caching (perf, not correctness — but affects load model)

`kek_service` keeps process-local caches with a sliding TTL (`KEK_CACHE_TTL_SECONDS`, default 300s,
`config.py:627`): `_KEK_CACHE` maps `(bucket_id, kek_id) → plaintext KEK`
(`kek_service.py:51,327-355`) and `_ACTIVE_KEK_CACHE` maps `bucket_id → active kek_id`
(`kek_service.py:58,358-387`). Steady-state PUT touches neither the keystore DB nor KMS. Per-key
singleflight locks coalesce cold-miss KMS unwraps (`kek_service.py:284-324`). A rewrite should
replicate a bounded per-(bucket,kek) cache + singleflight to avoid a KMS stampede, but it is not a
byte-compat concern.

### 6.3 Read path: DEK unwrap (main DB → keystore → KMS)

`services/object_reader.build_stream_context` (`object_reader.py:150-255`):

1. Load object-version row (has `bucket_id`, `object_id`, `object_version`, `enc_suite_id`,
   `kek_id`, `wrapped_dek`) — `object_reader.py:172-180`.
2. `suite_id = info["enc_suite_id"] or "hip-enc/aes256gcm"` (`object_reader.py:178`).
3. `kek_bytes = await get_bucket_kek_bytes(bucket_id, kek_id)` — fetches
   `bucket_keks.{wrapped_kek_bytes, kms_key_id}`, unwraps via KMS or local
   (`kek_service.py:557-603`).
4. `aad = f"hippius-dek:{bucket_id}:{object_id}:{object_version}"` (`object_reader.py:241`).
5. `key_bytes = unwrap_dek(kek_bytes, wrapped_dek, aad)` → plaintext DEK
   (`object_reader.py:242`).
6. Reject unsupported suites: `if not CryptoService.is_supported_suite_id(suite_id): raise
   unsupported_enc_suite_id` (`object_reader.py:243-244`) → mapped to HTTP 501
   (`api/s3/errors.py:237`).
7. **Envelope-race fallback:** if `kek_id`/`wrapped_dek` is NULL on the current version (overwrite in
   flight), fall back to the highest previously-completed serveable version and use its envelope +
   data (`object_reader.py:181-239`). Not `version-1` — numbering is sparse.

An AEAD auth failure on decrypt is `InvalidTag`; a too-short blob is
`CryptoError("ciphertext_too_short")`. Both are grouped as `CIPHERTEXT_UNUSABLE`
(`decrypter.py:17`) and drive a one-shot tier-drop-and-retry in the streamer
(`streamer.py:49-107`).

---

## 7. Hashing

| Hash | Input | Purpose | Code |
|---|---|---|---|
| **BLAKE3** | object **plaintext** (whole body), unsalted/unkeyed, `max_threads=1`, lowercase hex | shown in console as "Arion hash"; stored in `object_versions.body_blake3`. Computed in-flight over the same framed buffers as the rolling MD5, before the Arion hop | `blake3_hash.py:16-23`, `object_writer.py:327,437` |
| **MD5 (single object / part)** | object/part **plaintext**, lowercase hex | S3 ETag for a single-part PUT and per-part ETag for MPU; also Content-MD5 verification | `object_writer.py:326,436,461-462` |
| **MD5 (multipart composite)** | `concat(bytes.fromhex(part_md5) for parts in ascending order)`, then MD5, then `+ "-N"` | S3 composite ETag for completed MPU | `multipart.py:1126-1131`, `head_object_endpoint.py:208` |

Notes:

- **BLAKE3 is NOT salted.** Plain `blake3.blake3(data).hexdigest()`. (Contrast with the *other*
  product in this workspace, HCFS, which salts; do not conflate.) `max_threads=1` is a perf choice
  (updates run on a single-worker FIFO hash pool), not a correctness one — the digest is identical
  regardless of thread count.
- **MD5 is over plaintext**, so ETags match what a client computed locally before upload. The
  writer feeds plaintext chunks to both `hashlib.md5()` and the BLAKE3 hasher inside
  `_ChunkEncryptPipeline` (`object_writer.py:393-399`), and the digests are only finalized after
  `pipeline.flush()` guarantees all chunk updates have been applied in order
  (`object_writer.py:434-437`). The MD5/BLAKE3 update ordering is enforced by a **single-worker
  FIFO** thread pool (`crypto_pool.py:65-92`) — order matters for a rolling digest, so a Rust port
  that hashes on multiple threads must re-serialize updates in chunk order.
- **Content-MD5 enforcement:** if the client sent `Content-MD5`, the finalized MD5 is compared and a
  mismatch raises `BadDigest` before the version becomes visible (`object_writer.py:461-462`).
- **CID / IPFS:** each ciphertext chunk is stored on Arion and addressed by a **CID** recorded in
  `part_chunks.cid` (`sql/migrations/20251003000000_create_part_chunks.sql:10`). The CID is the
  storage-identity of the *ciphertext* chunk, produced by the storage backend/uploader — it is
  **not** an application-computed content hash of the plaintext and is not part of the crypto
  envelope. The read path resolves `(part_number, chunk_index) → cid → backend fetch`
  (`reader/backend_fetch.py:1-16`). `object_versions.ipfs_cid` is a *separate* legacy/object-level
  column read by purge/unpin scripts — the BLAKE3 digest is deliberately kept out of it to avoid
  being mistaken for a pin (`blake3_hash.py:36-44`).

---

## 8. Rotation / versioning of keys

- **DEK:** one per object-version. Every overwrite reserves a new `object_version` and generates a
  fresh DEK (`object_writer.py:228,236`). `rotate=True` in `_ensure_and_get_v5_dek` forces a new DEK
  when the version number does not bump (`object_writer.py:595,647,652`). No DEK re-use across
  versions.
- **Bucket KEK:** modeled by `status='active'` in `bucket_keks`; the unique partial index allows
  exactly one active KEK per bucket (`kek_service.py:420-422`, header doc `kek_service.py:19-21`).
  Rotation is **forward-only and lazy**: new writes wrap under the active KEK; each object-version
  stores the `kek_id` actually used, so old objects keep unwrapping with their original KEK
  (`kek_service.py:56-58`). There is no bulk re-encryption. **No code path currently flips a KEK to
  inactive / creates a second KEK** beyond first-create — rotation is a modeled capability, not an
  automated job. If a rotation feature is added it must invalidate `_ACTIVE_KEK_CACHE`
  (`kek_service.py:372-376`).
- **KMS master key:** rotated at OVH; per-call `key_id` (stored in `bucket_keks.kms_key_id`) keeps
  old KEKs decryptable (`ovh_kms_client.py:56-60`).
- **Suite:** `enc_suite_id` per object-version allows a future suite bump without migration; today
  only `hip-enc/aes256gcm` is registered.
- **Copy re-wrap:** `execute_v5_fast_path_copy` unwraps the source DEK and re-wraps it under the
  destination bucket's active KEK and destination AAD, reusing chunk CIDs (no byte copy)
  (`copy_service_v5.py:186-224`). See Open Question Q1.

---

## 9. Concrete read-path walkthrough (range request → plaintext)

Given a stored object, a target `object_version`, and an HTTP `Range: bytes=start-end`:

1. **Resolve the object-version row** from the main DB: `bucket_id`, `object_id`, `object_version`,
   `enc_suite_id`, `kek_id`, `wrapped_dek`, plus its parts list with each part's `size_bytes`
   (plaintext) and `chunk_size_bytes` (`object_reader.py:159-180`, `planner.py:33-45`).
2. **Unwrap the DEK** (§6.3): fetch the KEK for `(bucket_id, kek_id)` from the keystore (KMS or
   local), then `unwrap_dek(kek, wrapped_dek, aad="hippius-dek:{bucket_id}:{object_id}:{version}")`.
   Result: 32-byte plaintext DEK.
3. **Build the chunk plan** (`planner.build_chunk_plan`, `planner.py:21-98`):
   - Sort parts by `part_number`. Compute each part's plaintext offset within the object
     (`offsets`, cumulative `size_bytes`).
   - For each part, `num_chunks = ceil(plain_size / chunk_size)`.
   - Intersect the requested `[start,end]` with each part's `[part_start, part_end]`; for each
     overlapping chunk index `ci`, emit a `ChunkPlanItem(part_number, chunk_index=ci,
     slice_start?, slice_end_excl?)`. `slice_*` are set only for the partially-covered first/last
     chunk (`planner.py:82-97`). `chunk_index` here is **per-part** (C9).
4. **For each plan item, fetch the ciphertext chunk** (`streamer.stream_plan` → `_wait/_fetch`,
   `streamer.py:266-292`): try local SSD → peer → pool; on miss, `fetch_missing` pulls it from the
   backend by `chunk_backend.backend_identifier` (the CID) into memory
   (`reader/backend_fetch.py`). The ciphertext blob is `nonce(12) ‖ ct ‖ tag(16)`.
5. **Decrypt the chunk** (`decrypter.decrypt_chunk_if_needed` → `CryptoService.decrypt_chunk` →
   `AESGCMChunkedAdapterV2.decrypt_chunk`, `decrypter.py:20-52`, `crypto_service.py:264-291`):
   - Reconstruct AAD (C3): `LE16(len bucket_id)‖bucket_id‖LE16(len object_id)‖object_id‖
     LE32(part_number)‖LE32(chunk_index)`.
   - `nonce = blob[:12]; body = blob[12:]; plaintext = AESGCM(dek).decrypt(nonce, body, aad)`.
   - On `InvalidTag`/too-short, drop the local copy and retry from the next tier exactly once
     (`streamer.py:49-107`).
6. **Slice for the range** (`decrypter.maybe_slice`, `decrypter.py:55-58`): if the plan item carries
   `slice_start`/`slice_end_excl`, return `plaintext[slice_start:slice_end_excl]`; else the whole
   chunk.
7. **Emit** the plaintext slice to the client in plan order. Concatenated across all plan items,
   the bytes are exactly the requested range.

Worked micro-example: object with one part (`part_number=1`), 4 MiB chunks, `Range: bytes=5000000-
6000000`. Plan: chunk 1 (covers bytes 4194304..8388607). `slice_start = 5000000 - 4194304 =
805696`, `slice_end_excl = 6000000 - 4194304 + 1 = 1805697`. Fetch chunk-1 blob, strip 12-byte
nonce, GCM-decrypt with AAD `(bucket, object, part=1, chunk=1)`, return `plaintext[805696:1805697]`.

---

## 10. Rust implementation notes (migration DECRYPT path)

> These notes are for the `legacy` decrypt module that the migration uses to read old objects. The
> NEW write/read path follows doc 25, not this section.

### 10.1 Crate mapping

| Python primitive | Rust crate | Notes |
|---|---|---|
| `cryptography` `AESGCM` (chunk + DEK/KEK wrap) | `aes-gcm` (RustCrypto), `Aes256Gcm` with `Nonce<U12>` | GCM tag is appended by `encrypt`/split by `decrypt`; use `aead::Aead` / `AeadInPlace`. Confirm the crate appends the 16-byte tag as a suffix (it does) to match `nonce‖ct‖tag`. |
| `os.urandom` / `nacl.utils.random` | `rand::rngs::OsRng` + `RngCore::fill_bytes`, or `getrandom` | CSPRNG, 12-byte nonce / 32-byte DEK. |
| `struct.pack("<H"...)`, `struct.pack("<II"...)` | `u16::to_le_bytes`, `u32::to_le_bytes` | Little-endian. Build the AAD by concatenation exactly per C3. |
| `hashlib.md5` | `md-5` (RustCrypto `Md5`) | ETag; feed plaintext in chunk order. |
| `blake3.blake3(...).hexdigest()` | `blake3` crate, `Hasher::new()` + `.finalize().to_hex()` | Unkeyed, unsalted. `max_threads=1` is irrelevant to output. |
| `hashlib.sha256` (local KEK derivation, legacy subaccount id) | `sha2` (`Sha256`) | C6 derivation. |
| `httpx` mTLS to OVH KMS | `reqwest` with `Identity` (client cert+key) + `Certificate` (CA), or `hyper` + `rustls`/`native-tls` | POST JSON, base64-decode `plaintext`. |
| `asyncpg` (main + keystore pools) | `sqlx`/`tokio-postgres` + `deadpool`/`bb8` | Two pools: main DB and keystore DB (may be the same DSN). |
| base64 (KMS payloads) | `base64` crate (standard alphabet, with padding) | KMS `plaintext` field is standard base64. |
| JWE storage | none needed | Store/return the JWE as **opaque UTF-8 bytes**; only OVH KMS parses it. |

### 10.2 Top 3 bit-compat traps

1. **Random nonce, prepended — do not derive (C2).** The seed report's "deterministic nonce keyed by
   global chunk index" is a *removed* design. Encrypt = `OsRng` 12 bytes prepended; decrypt = read
   `blob[:12]`. Deriving a nonce on either side breaks everything.
   (`crypto_service.py:118-135,148-153`.)

2. **The chunk AAD omits `upload_id` and `object_version`, uses `bucket_id` THEN `object_id`, and is
   little-endian length-prefixed (C3).** The registered suite is V2 (`crypto_service.py:162-176`),
   not the V1 layout that `writer/CLAUDE.md:69` and the deprecated `AESGCMChunkedAdapter`
   (`crypto_service.py:89-104`) describe. Getting the field order, the `<H`/`<II` widths/endianness,
   or including `upload_id`/`version` wrong makes every tag fail. `chunk_index` is **per-part**
   (C9), so a Rust port that computes a global chunk index will mis-authenticate every multipart
   object.

3. **Two different AADs with two different shapes.** The **DEK-wrap** AAD is the ASCII string
   `hippius-dek:{bucket_id}:{object_id}:{object_version}` (C5) and *does* include the version; the
   **chunk** AAD is the binary struct (C3) and does *not*. The **local KEK-wrap** uses **no AAD at
   all** (C6, `associated_data=None`). Mixing these up — e.g. reusing the DEK AAD for chunks, or
   passing an AAD to the local KEK wrap — silently produces unreadable data. Also mind the wrapped
   layouts: DEK-wrap = `nonce(12)‖ct(32)‖tag(16)` = 60 bytes; KMS-wrapped KEK = opaque JWE UTF-8
   bytes (not a fixed layout).

### 10.3 Other correctness reminders

- **Reader must read `chunk_size_bytes` per part from the DB** (C8), and size the plan from
  `part_chunks`/`parts` metadata, not the config default.
- **Suite gate:** reject any `enc_suite_id` not in `{"hip-enc/aes256gcm"}` with a 501-equivalent,
  matching `object_reader.py:243-244`.
- **Storage version gate:** reject `storage_version < 5` (C15, `storage_version.py:13-21`).
- **Envelope written atomically before serveability** (§4.5) — preserve the invariant or reintroduce
  the `v5_missing_envelope_metadata` race.
- **KMS-disabled cannot read KMS-wrapped KEKs** — preserve the hard error
  (`kek_service.py:249-256`) rather than falling back.

---

## Open questions

**Q1 — Copy fast-path chunk AAD vs. changed object_id.** `execute_v5_fast_path_copy`
(`copy_service_v5.py:148-224`) reuses the source object's chunk CIDs verbatim while assigning a new
destination `object_id`. The per-chunk AEAD AAD (C3) binds the **source** `object_id`, but the
destination read path reconstructs the chunk AAD with the **destination** `object_id`
(`object_reader.py:241` uses `info.get('object_id')`, and `streamer.py:243` passes the destination
context). By my reading these AADs differ, so a copied object's chunks would fail `InvalidTag` on
read — unless (a) the copy path is currently gated off (services/CLAUDE.md:82 calls it a "latent risk
if re-enabled for MPU"), or (b) something re-keys/re-encrypts chunks that I did not find, or (c) the
reader is somehow given the source object_id. I could not find a re-encrypt step, so **this needs a
runtime test against a copied object before the Rust port relies on CID reuse.** If the Python
service does not actually serve copied objects via this path today, the Rust port should either
byte-copy on COPY or carry the original object_id into the chunk-AAD reconstruction.

**Q2 — Exact OVH KMS `plaintext` base64 variant.** The code uses `base64.b64decode` (standard
alphabet, `ovh_kms_client.py:206,233`). I assumed standard (`+/`, padded) base64. If OVH ever
returns URL-safe base64 the Rust port must match; unverified against a live KMS response.

**Q3 — `object_version` string formatting in the DEK-wrap AAD.** It is `int(...)` interpolated into
an f-string, giving canonical base-10 with no leading zeros or sign for the values used (`1`, `2`,
…). I assume versions are always positive and never formatted with padding anywhere. Confirmed for
all call sites read, but worth a property test in Rust (`format!("{}", version)`), not
`format!("{:03}", …)` etc.

**Q4 — `enc_chunk_size_bytes` vs `parts.chunk_size_bytes` authority.** The writer stores the chunk
size in *both* `object_versions.enc_chunk_size_bytes` (`object_writer.py:316`) and per-part
`parts.chunk_size_bytes`. The reader's planner uses the **per-part** value (`planner.py:36-45`). I
treated per-part as authoritative for reads (matching the code), but the two can in principle
diverge for legacy/variable-chunk objects; the Rust reader should mirror the planner and prefer
per-part.

**Q5 — Committed docs are partly stale.** `writer/CLAUDE.md:30,69` describe a "global chunk index"
and a 5-tuple AAD including `upload_id`. Both contradict the current code (§1.3, §2, C3, C9). I
documented the code. If the Python team still believes the docs, reconcile before porting — but the
code is what wrote the production bytes.
