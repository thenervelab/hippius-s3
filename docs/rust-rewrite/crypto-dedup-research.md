# Crypto & Dedup Research — Rebinding the Chunk AEAD AAD to Enable CopyObject / Blob Dedup

> **Note (post-freeze):** this memo's *conclusions* were adopted — per-owner copy-oriented dedup,
> reject cross-tenant convergent/MLE, and add key commitment. But the *mechanisms* it sketches were
> superseded by the frozen construction in [`25-crypto-verification.md`](./25-crypto-verification.md):
> (1) key commitment is **intrinsic to the wire** (CTX-over-frames, a 32-byte BLAKE3 `CT` per frame) —
> there is **no** separate stored `PRF(DEK, blob_id)` value; (2) the nonce is a **deterministic STREAM
> nonce** (`prefix(7)‖frame_index(4 BE)‖final_flag(1)`), which is safe because it's per-blob-DEK +
> encrypt-once — the memo's "never use a deterministic nonce" caveat targets *convergent/shared-key*
> schemes, not this; (3) the DEK is **per-blob**, not per-object-version (loose phrasing below). Read
> the mechanism details as period research; doc 25 is authoritative.

**Status:** Research memo (online sources, cited). Informs the AAD-rebinding decision for the
greenfield Rust S3 product. Written 2026-09-15.

**Scope of the decision.** Today the chunk AEAD binds object identity in its AAD
(`bucket_id ‖ object_id ‖ part_number ‖ chunk_index`, see
[`01-crypto-envelope.md` §1.3 / C3](./01-crypto-envelope.md)). That binding is what forces
CopyObject to re-encrypt and prevents any blob sharing. We want to rebind the AAD to the *blob's*
content/identity so one ciphertext blob can be referenced by many objects. Two dedup scopes are on
the table:

- **Option A — per-owner, copy-oriented.** Random DEK + random nonce; a blob is encrypted once and
  refcounted only across references owned by the **same account**; DEK wrapped under that owner's
  KEK; no cross-tenant sharing.
- **Option B — cross-tenant convergent.** Identical plaintext across *different tenants* collapses
  to one blob. This is only achievable with **convergent / message-locked encryption** (content-
  derived deterministic key + deterministic nonce).

---

## Executive summary

1. **Cross-tenant dedup (Option B) requires convergent / message-locked encryption (MLE), and MLE
   is a known, published downgrade of confidentiality.** MLE cannot protect low-entropy or
   predictable data: it is inherently open to offline brute-force/dictionary attacks and to
   confirmation-of-a-file attacks, by the design of the primitive, not by implementation error. The
   foundational formalization (Bellare–Keelveedhi–Ristenpart, EUROCRYPT 2013) is explicit that MLE
   can only provide semantic-security-style guarantees for *unpredictable* messages. For a product
   whose selling point is confidentiality, this is the wrong trade.
   [eprint 2012/631](https://eprint.iacr.org/2012/631),
   [smarx.com](https://smarx.com/posts/2020/09/convergent-encryption-and-why-no-one-uses-it/).

2. **The industry consensus for confidentiality-first storage is to NOT do cross-user convergent
   dedup.** Tresorit explicitly uses *non-convergent* crypto and says so as a feature; MEGA, Storj,
   and Apple iCloud (Advanced Data Protection) all use per-object random content keys, which
   structurally prevents cross-user dedup; Tahoe-LAFS added a per-client "convergence secret" that
   *turns cross-user convergence off by default*; restic scopes dedup to a single repository. Where
   dedup exists in encrypted systems, it is per-owner/per-repo, not cross-tenant.
   [Tresorit](https://tresorit.com/security),
   [Tahoe-LAFS convergence secret](https://tahoe-lafs.readthedocs.io/en/latest/convergence-secret.html).

3. **Option A is sound and standard.** Encrypt a blob once with a random DEK + random nonce, wrap
   the DEK under the owner's KEK, refcount references. CopyObject becomes an O(1) metadata operation
   that copies the wrapped-DEK/pointer, exactly like AWS S3 SSE-KMS copies do not require
   re-encrypting object bytes. No convergent-encryption weaknesses apply because the ciphertext is
   not a function of plaintext alone.

4. **One pitfall is REAL regardless of A vs B and we must handle it: AES-256-GCM is not
   key-committing.** The moment a single ciphertext blob is looked up and decrypted under a DEK that
   is *selected from metadata* (i.e., a blob referenced by many wrapped-DEK envelopes), we introduce
   exactly the setting the key-commitment / partitioning-oracle literature warns about: a ciphertext
   that could be made to verify under more than one key. Add a key-commitment mechanism
   (committing-AEAD transform, or an explicit commitment tag) so a blob can only ever be opened by
   the one DEK that wrote it.
   [Partitioning Oracle Attacks, USENIX '21](https://www.usenix.org/system/files/sec21-len.pdf),
   [Abuse/Fix key commitment, USENIX '22](https://www.usenix.org/system/files/sec22-albertini.pdf).

5. **Random 12-byte GCM nonce is fine for a write-once blob** because each blob is encrypted exactly
   once; there is no re-encryption on copy, so there is no per-blob nonce-reuse risk. The only nonce
   concern is the *global* birthday bound across all encryptions under one key — which is a KEK/DEK
   scoping question, addressed by the per-object random DEK you already have (each DEK encrypts only
   one blob's chunks), not by dedup.

**Recommendation:** Adopt **Option A** (per-owner, copy-oriented, random DEK + random nonce, refcount
references, wrapped-DEK copied on CopyObject). **Reject Option B** (cross-tenant convergent) for a
confidentiality product. **Independently of that choice, add key commitment to the chunk AEAD**
because shared/looked-up DEKs make non-committing AES-GCM a liability. Rationale and sources below.

---

## Q1. Convergent / Message-Locked Encryption (MLE): model and known attacks

**What MLE is.** In Message-Locked Encryption the key used to encrypt/decrypt a message is *itself
derived from the message* (typically `K = H(M)`, ciphertext `C = E_K(M)`), so two parties encrypting
the same plaintext produce the same key and — with a deterministic scheme — the same ciphertext,
which is what enables server-side dedup without the server seeing plaintext. Bellare, Keelveedhi and
Ristenpart formalized this primitive (algorithms K, E, D, T with a "tag" T that lets the server test
plaintext-equality of two ciphertexts), gave privacy and "tag-consistency" (integrity) definitions,
and gave ROM security analyses of the natural schemes (which include the deployed "convergent
encryption").
[Message-Locked Encryption and Secure Deduplication, eprint 2012/631](https://eprint.iacr.org/2012/631),
[Springer/EUROCRYPT 2013](https://link.springer.com/chapter/10.1007/978-3-642-38348-9_18).

**What MLE does NOT protect — and why it's inherent, not a bug.** Because the key is a
deterministic function of the plaintext, MLE cannot hide anything about *predictable* plaintexts.
The paper's own security notion (PRV-CDA and its variants) only holds for **unpredictable / high min-
entropy** messages. The concrete, named attacks:

- **Offline brute-force / dictionary attack on low-entropy data.** If the plaintext is drawn from a
  small or guessable set (a config file that differs only in a password, a document from a known
  template, a file from a public corpus), an attacker who obtains the ciphertext can enumerate
  candidate plaintexts, compute `H(candidate)` and `E_{H(candidate)}(candidate)`, and compare. MLE
  provides *no* work factor here — unlike a password hash there is no salt and no deliberate
  slowness. smarx.com's worked example is a Redis config where only the password field is unknown:
  convergent encryption lets you brute-force just that field, fast.
  [smarx.com](https://smarx.com/posts/2020/09/convergent-encryption-and-why-no-one-uses-it/).

- **Confirmation-of-a-file attack.** Anyone who already has a candidate file can check whether a
  given user (or the whole system) is storing it, by encrypting it and comparing to stored
  ciphertext/tags — even though only ciphertext is stored. smarx.com's example: matching ciphertext
  reveals that a user holds a specific bootleg movie.
  [smarx.com](https://smarx.com/posts/2020/09/convergent-encryption-and-why-no-one-uses-it/),
  [Tahoe-LAFS](https://tahoe-lafs.readthedocs.io/en/latest/convergence-secret.html).

- **Learn-the-remaining-information attack.** The generalization of the above: when most of a file
  is known and only a small part is secret, the secret part is recoverable by brute force over the
  unknown portion. This is the smarx Redis-password case stated generally.
  [smarx.com](https://smarx.com/posts/2020/09/convergent-encryption-and-why-no-one-uses-it/).

**Server-aided mitigation: DupLESS / oblivious-PRF key servers.** Bellare, Keelveedhi and
Ristenpart's follow-up, **DupLESS** (USENIX Security 2013), mitigates the *offline* brute-force by
deriving the message key from a **key server via an oblivious PRF (an RSA-blind-signature OPRF)**
instead of from `H(M)` directly. The client blinds the file fingerprint, the key server applies a
secret-keyed PRF, and the key server **rate-limits** requests. Effect: an attacker can no longer
brute-force keys *offline* (they don't know the server's PRF secret) and is throttled *online*;
identical files still converge to identical ciphertext for dedup, but the small per-user key-
encapsulation differs.
[DupLESS, USENIX Security '13](https://www.usenix.org/conference/usenixsecurity13/technical-sessions/presentation/bellare),
[eprint 2013/429](https://eprint.iacr.org/2013/429.pdf).

**Consensus for confidential multi-tenant data.** DupLESS raises the bar but (a) introduces a new,
always-online, security-critical, rate-limiting **key-server** as a single point of trust/failure in
the data path, (b) still permits confirmation-of-a-file within the rate limit and against the key
server, and (c) does not remove the fundamental fact that dedup across tenants *is* a signal that two
tenants hold the same bytes. smarx's conclusion — "the weaknesses of convergent encryption are baked
in… achieving deduplication across users inherently creates these attack vectors" — reflects the
practitioner consensus. The honest statement of the trade is: cross-tenant convergent dedup is a
deliberate confidentiality downgrade whose residual leakage you must be willing to publish to users.
[smarx.com](https://smarx.com/posts/2020/09/convergent-encryption-and-why-no-one-uses-it/).

---

## Q2. What real encrypted / E2E storage products do about dedup + copy

| Product | Dedup scope | Key model | Cross-user convergent dedup? | Source |
|---|---|---|---|---|
| **Tresorit** | None across users (by design) | Per-object random keys, zero-knowledge | **No — explicitly avoided as a feature** ("non-convergent cryptography … impossible to determine when content matches others'") | [tresorit.com/security](https://tresorit.com/security) |
| **MEGA** | Per-user | Random per-file "node key", wrapped under user master key | No (random node keys ⇒ no convergence) | [MEGA Security Whitepaper](https://www.voilatranslate.com/wp-content/uploads/SecurityWhitepaper.pdf) |
| **Storj DCS** | None server-side (client-side E2E) | Random per-segment content key; PBKDF2 salted path keys; AES-256 | No (random content keys; server never has keys) | [Storj DCS Security Data Sheet](https://static.storj.io/documents/storj-dcs-security-data-sheet.pdf), [Storj file-encryption spec](https://github.com/storj-archived/core/blob/master/doc/file-encryption.md) |
| **Cryptomator** | None (client-side vault over any backend) | Per-file random content key wrapped by masterkey | No | [cryptomator.org](https://cryptomator.org/comparisons/tresorit-alternative/) |
| **Tahoe-LAFS** | Convergent, but gated by a per-client **convergence secret** | Content-derived key mixed with a per-install random secret | **Off by default across users** — only holders of the same convergence secret converge; the secret exists specifically to blunt confirmation/brute-force | [Tahoe-LAFS convergence-secret docs](https://tahoe-lafs.readthedocs.io/en/latest/convergence-secret.html), [tahoe-dev "convergent encryption reconsidered"](https://tahoe-lafs.org/pipermail/tahoe-dev/2008-March/000449.html) |
| **restic** | **Per-repository only** | Repo key from password via Argon2id; chunk-level | No cross-user/-repo dedup ("dedup only inside one machine's repository") | [restic vs borg comparison](https://www.matthewswong.com/en/blog/restic-vs-borg-encrypted-backups/) |
| **BorgBackup** | Per-repository (across archives) | Repo key, AES-256 client-side | No cross-user; cross-*archive within one repo* | [borg vs restic](https://dev.to/selfhostingsh/restic-vs-borgbackup-which-backup-tool-to-use-4cmn) |
| **Backblaze (personal backup)** | Detects unchanged/moved files per account | Client-side; optional user key | Per-account only | [Backblaze dedup help](https://help.backblaze.com/hc/en-us/articles/217665548-Deduplication) |
| **Apple iCloud (Advanced Data Protection)** | None across users for E2E categories | Per-file keys generated on device, wrapped in iCloud Keychain domain; HSM-held keys deleted when ADP on | No (device-held per-file keys) | [Apple ADP support](https://support.apple.com/guide/security/advanced-data-protection-for-icloud-sec973254c5f/web), [iCloud data security overview](https://support.apple.com/en-us/102651) |
| **AWS S3 SSE-KMS** | N/A (dedup not a goal) | Envelope: per-object random DEK wrapped by KMS CMK; **CopyObject re-wraps the data key, does not require re-encrypting object bytes**; `UpdateObjectEncryption` re-encrypts the *data key* via envelope, not the data | Not applicable | [AWS: specifying SSE-KMS](https://docs.aws.amazon.com/AmazonS3/latest/userguide/specifying-kms-encryption.html), [AWS: UpdateObjectEncryption](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UpdateObjectEncryption.html) |

**Takeaways.**
- Every *confidentiality-first* product on the list either does **no cross-user dedup** or (Tahoe)
  turns cross-user convergence **off by default**. The ones that dedup encrypted data scope it to a
  **single owner / repository**.
- The AWS SSE-KMS envelope model is the template for O(1) copy: the object bytes are encrypted under
  a per-object DEK, and copy/rewrap operations act on the **wrapped data key**, not the ciphertext
  bytes. This is exactly the shape of Option A. [AWS UpdateObjectEncryption](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UpdateObjectEncryption.html).

---

## Q3. AEAD AAD best practice + the key-commitment problem

**Binding context/position in AAD is standard and good.** Putting positional/context data
(bucket, object, part, chunk index) in the AAD binds each ciphertext to its intended place, which is
correct AEAD hygiene: it stops chunk-splicing/reordering/relocation across objects. The existing
Python design does exactly this (C3). The question is only *what* to bind — object identity vs. blob
content/identity.

**Binding to content/identity vs. object identity is cryptographically sound** *as an AAD choice*:
AAD binds whatever context you decide is authentic for that ciphertext. If a blob is a first-class,
content-addressed, refcounted entity, binding the AAD to the **blob's stable identity** (e.g. its
content hash / blob id and chunk index) rather than to a single owning object is a legitimate model —
it is what makes the same ciphertext validly referenceable by many objects. The catch is not the AAD
content; it is the cipher's **key commitment**.

**AES-GCM (and AES-GCM-SIV, and ChaCha20-Poly1305) are NOT key-committing.** An AEAD is
*key-committing* if a ciphertext verifies under at most one key. GCM is not; it was never claimed to
be. This means an attacker who controls key selection can craft a ciphertext that decrypts to *two
different valid plaintexts under two different keys* (a "key multi-collision"). The
**partitioning-oracle attack** (Len, Grubbs, Ristenpart, USENIX Security 2021) weaponizes this to
recover keys/passwords when a system exposes a decrypt-success/-fail oracle, using efficient
multi-collision constructions against GCM, XSalsa20/Poly1305 and ChaCha20/Poly1305. The community
recommendation from that work is to **standardize and use key-committing AEAD**.
[Partitioning Oracle Attacks, USENIX '21 PDF](https://www.usenix.org/system/files/sec21-len.pdf),
[eprint 2020/1491](https://eprint.iacr.org/2020/1491).

**Does sharing/looking-up a blob's DEK across references raise key-commitment concerns? Yes —
this is the crux.** In Option A a blob is written once under DEK `k`. Every reference stores its own
`wrap_KEK(k)`. On read, the server *selects* which DEK to unwrap based on metadata and hands it to
GCM decryption of the shared blob. That is precisely a "one ciphertext, many candidate keys" setting.
If any path lets an attacker cause a blob to be opened under a DEK other than the one that wrote it
(e.g. a corrupted/confused wrapped-DEK pointer, a refcount/metadata bug, a malicious co-tenant who
can influence which wrapped-DEK is attached to a reference), GCM will *not* detect the wrong key on
its own — it can be made to authenticate. With object-identity AAD today, the AAD differs per object
and acts as an accidental partial guard; once you rebind AAD to blob identity so many references
share it, **that accidental guard disappears**, and non-committing GCM becomes the only thing between
"one blob, one key" and cross-reference key confusion.

**Mitigations (add key commitment):**
- **Committing-AEAD transform.** Albertini, Duong, Gueron, Kölbl, Luykx, Schmieg — *How to Abuse
  and Fix Authenticated Encryption Without Key Commitment* (USENIX Security 2022) — documents three
  real exploitable cases of missing key commitment and gives cheap fixes, including the "padding
  fix" (prepend a fixed constant block that must decrypt correctly).
  [USENIX '22 PDF](https://www.usenix.org/system/files/sec22-albertini.pdf),
  [eprint 2020/1456](https://eprint.iacr.org/2020/1456).
- **Explicit key-commitment tag.** Gueron's *Key Committing AEADs* (eprint 2020/1153) analyzes
  generic constructions that add a commitment property to nonce-based AEAD and gives a
  key-committing version of AES-GCM. Practically: store, alongside each blob, a commitment value
  such as `HMAC/PRF(DEK, blob_id)` (or a hash over key+nonce) and verify it before/at decryption, so
  a blob can only ever be opened by the exact DEK that wrote it.
  [eprint 2020/1153](https://eprint.iacr.org/2020/1153.pdf).
- **CMT-4 / context-committing transforms** (e.g. KIVR, HtE-style) if you want to commit the full
  context (key + nonce + AAD), not just the key. [eprint 2025/1127](https://eprint.iacr.org/2025/1127.pdf).
- Note: **AES-GCM-SIV is nonce-misuse-resistant but is *still not key-committing*** — do not reach
  for it expecting commitment. [Gueron, eprint 2020/1153](https://eprint.iacr.org/2020/1153.pdf).

---

## Q4. Nonce management for shared / dedup'd blobs

**A random 12-byte GCM nonce is fine for a write-once, reference-many blob.** The catastrophic GCM
failure is *nonce reuse under the same key*: reusing a (key, nonce) pair leaks the XOR of plaintexts
**and the GCM authentication subkey**, enabling forgeries.
[PentesterLab GCM nonce reuse](https://pentesterlab.com/glossary/gcm-nonce-reuse),
[elttam key-recovery on GCM](https://www.elttam.com/blog/key-recovery-attacks-on-gcm).
In Option A this never happens *because of dedup*: a blob is encrypted exactly once with one random
DEK and one random nonce, and copies/references reuse the **existing ciphertext** — there is no
second encryption under the same DEK, so there is no per-blob reuse risk. Referencing a blob does not
re-run GCM.

**The remaining nonce concern is the global birthday bound *per key*, which dedup does not worsen.**
Random 96-bit nonces collide at the birthday bound: ~50% collision chance near **2^48** encryptions
under one key; NIST's guidance to keep collision probability below 2^-32 caps a single key at ~**2^32**
encryptions.
[Neil Madden, "Galois/Counter Mode and random nonces"](https://neilmadden.blog/2024/05/23/galois-counter-mode-and-random-nonces/),
[kopia issue #5169](https://github.com/kopia/kopia/issues/5169).
Because your DEK is **per object-version random** (each DEK encrypts only the handful of chunks of a
single blob), you are astronomically far from 2^32 encryptions per DEK — the bound is a non-issue for
DEKs. Watch it only for any *long-lived* key that encrypts a very large number of messages (a KEK
wrapping billions of DEKs is nowhere near the bound either, since each wrap is one encryption under
random nonce and 2^32 wraps is a lot but the safe posture is a fresh KEK per bucket, which you have).
If you ever move to a scheme where one key encrypts an unbounded stream, prefer AES-GCM-SIV or a
derive-per-message-key construction (XChaCha20 / libsodium style).
[Neil Madden](https://neilmadden.blog/2024/05/23/galois-counter-mode-and-random-nonces/).

**Pitfall to avoid:** do NOT switch to a *deterministic* nonce as part of enabling dedup (that is a
convergent-encryption idea). Deterministic nonce + shared key across blobs is how you reintroduce
reuse. Keep the random-nonce, write-once model.

---

## Q5. Per-owner vs cross-tenant dedup — the stance for a confidentiality product

The security stance is unambiguous in the sources: **for a product whose value proposition is
confidentiality, do dedup per-owner (or not at all), never cross-tenant convergent.**

- Cross-tenant convergent dedup *is itself* an observable side channel (dedup hit ⇒ two tenants hold
  the same bytes) and unavoidably enables confirmation-of-a-file and low-entropy brute force
  (Q1). smarx and Tresorit state this plainly.
  [smarx.com](https://smarx.com/posts/2020/09/convergent-encryption-and-why-no-one-uses-it/),
  [tresorit.com/security](https://tresorit.com/security).
- Per-owner dedup carries **none** of these cross-tenant leaks: the only party who can observe that
  "these two of *my own* objects share a blob" is the owner, who by definition already holds both
  plaintexts. There is no confirmation-of-a-file across a trust boundary, and no cross-tenant
  brute-force, because blobs are keyed by *random* DEKs under the *owner's* KEK, not by content.
- This matches what every confidentiality-first product ships: per-repo (restic/borg), per-account
  (MEGA, Backblaze), per-device-key (iCloud ADP, Storj, Cryptomator), or convergence-secret-gated
  (Tahoe). None do cross-tenant convergent dedup.

---

## Risks of Option A vs Option B (grounded in the sources)

### Option A — per-owner, random DEK + random nonce, refcounted references
**Confidentiality risk from dedup: essentially none across trust boundaries.** Blobs are keyed with
random DEKs, so ciphertext is not a function of plaintext; no confirmation-of-a-file, no low-entropy
brute-force, no cross-tenant "same bytes" signal. Dedup facts are visible only within a single owner
who already has the plaintext.
**Copy semantics:** O(1) metadata copy — CopyObject copies the reference + the wrapped DEK (rewrap
under target context as needed), exactly the SSE-KMS envelope pattern.
[AWS UpdateObjectEncryption](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UpdateObjectEncryption.html).
**Residual risks you must engineer for:**
- **Key commitment (SHARED — see below).** Once many references share one blob + DEK, non-committing
  GCM permits key-confusion if metadata/refcount logic is buggy or attacker-influenced. Must add
  commitment. [USENIX '22](https://www.usenix.org/system/files/sec22-albertini.pdf).
- **Refcount integrity.** Deleting a reference must not free a blob another reference needs; a
  refcount bug is a data-loss/data-leak bug. (Engineering, not crypto, but security-relevant.)
- **Nonce birthday bound** — non-issue for per-object random DEKs (Q4).

### Option B — cross-tenant convergent (MLE)
**Confidentiality risk: structural and published.**
- Confirmation-of-a-file across all tenants. [smarx.com](https://smarx.com/posts/2020/09/convergent-encryption-and-why-no-one-uses-it/).
- Offline brute-force / learn-remaining-information on any low-entropy or partially-known plaintext,
  with no work factor. [eprint 2012/631](https://eprint.iacr.org/2012/631),
  [smarx.com](https://smarx.com/posts/2020/09/convergent-encryption-and-why-no-one-uses-it/).
- Dedup hit is itself a cross-tenant equality oracle.
- **Mitigation (DupLESS) is heavy and partial:** requires an always-online, rate-limiting OPRF
  **key server** as a new trusted SPOF in the data path; only blunts *offline* brute-force; does not
  eliminate confirmation within rate limits.
  [DupLESS](https://www.usenix.org/conference/usenixsecurity13/technical-sessions/presentation/bellare).
- **Also still needs key commitment**, plus a deterministic nonce (which reintroduces its own
  reuse-adjacent care). You inherit *all* of Option A's crypto pitfalls **plus** the MLE ones.
**Copy semantics:** same O(1) benefit as A, but that benefit is not worth the confidentiality cost —
Option A already delivers the O(1) copy.

**Net:** Option B buys *cross-tenant* storage savings at the cost of confidentiality guarantees the
product is sold on, and requires a new security-critical service. Option A delivers the copy/dedup
performance win we actually asked for (O(1) CopyObject, blob sharing) with no cross-tenant leakage.

---

## Crypto pitfalls we must handle regardless of A vs B

1. **Add key commitment to the chunk AEAD (highest priority).** AES-256-GCM is not key-committing;
   sharing/looking-up a DEK across many references is exactly the non-committing-AEAD risk setting.
   Use a committing transform (padding fix / HtE) or store & verify an explicit per-blob commitment
   value `PRF(DEK, blob_id)` so a blob opens under one and only one DEK. Do this even for Option A.
   [USENIX '22](https://www.usenix.org/system/files/sec22-albertini.pdf),
   [Gueron eprint 2020/1153](https://eprint.iacr.org/2020/1153.pdf),
   [Partitioning Oracle Attacks](https://www.usenix.org/system/files/sec21-len.pdf).
2. **Bind blob identity + chunk position in AAD** (`blob_id ‖ chunk_index`, and keep a suite/version
   byte). This preserves anti-splicing/anti-reorder while allowing multi-object references. AAD is
   not a substitute for key commitment.
3. **Keep the write-once + random-nonce model.** Never derive a deterministic nonce and never
   re-encrypt a shared blob. (Q4.)
4. **DEK stays per-blob-version random; wrap per reference under the owner KEK.** Do not let a DEK
   span unrelated blobs (keeps you far under the GCM birthday bound and limits blast radius).
5. **Refcount correctness is a security property**, not just a GC nicety.

---

## Where sources are thin or conflict (honesty)

- **iCloud / Backblaze internal dedup-with-encryption specifics** are not fully public. Apple's ADP
  docs confirm per-file device-held keys (which preclude cross-user dedup of E2E categories) but do
  not publish a dedup design; Backblaze docs confirm per-account file-level dedup but not the crypto
  interplay. Claims above are limited to what the public docs state.
  [Apple ADP](https://support.apple.com/guide/security/advanced-data-protection-for-icloud-sec973254c5f/web),
  [Backblaze](https://help.backblaze.com/hc/en-us/articles/217665548-Deduplication).
- **DupLESS effectiveness is a genuine trade, not a clean win.** Its authors are candid that it
  mitigates offline brute-force but relies on the key server's rate-limiting and secrecy; it is not
  a general "convergent dedup is now safe" result.
  [eprint 2013/429](https://eprint.iacr.org/2013/429.pdf).
- **"Content-derived AAD" vs "convergent encryption" are different things and shouldn't be
  conflated.** Binding AAD to a blob id (Option A) does *not* make the scheme convergent — the DEK
  is still random. Only deriving the *key* from content (Option B) creates MLE's weaknesses. The
  research is clear on MLE but there is little formal literature specifically on "random-key blob
  with content-id AAD," precisely because it's an unremarkable, safe AEAD usage as long as key
  commitment is handled.

---

## Recommendation

**Choose Option A. Reject Option B. Add key commitment regardless.**

- **Option A (per-owner, random DEK + random nonce, refcounted references, wrapped-DEK copied on
  CopyObject)** gives us the entire performance goal — O(1) metadata-only CopyObject and blob dedup —
  with **no cross-tenant confidentiality loss**, matching what every confidentiality-first product in
  the survey ships and mirroring the AWS SSE-KMS envelope/copy model.
- **Option B (cross-tenant convergent / MLE)** is a published, inherent confidentiality downgrade
  (confirmation-of-a-file, low-entropy brute force, cross-tenant equality oracle). Its only serious
  mitigation, DupLESS, adds an always-online trusted key server and still leaves residual leakage.
  For a product sold on confidentiality this is the wrong trade, and it does not buy any copy-path
  win that Option A doesn't already provide.
- **Mandatory regardless:** make the chunk AEAD **key-committing**, because the shared-DEK-lookup
  pattern is exactly where non-committing AES-GCM bites. Bind `blob_id ‖ chunk_index` (+ suite/version)
  in AAD, keep the random-nonce write-once model, and keep DEKs per-blob-version random and wrapped
  per reference under the owner's KEK.

### Primary sources
- Bellare, Keelveedhi, Ristenpart, *Message-Locked Encryption and Secure Deduplication*, EUROCRYPT
  2013 — [eprint 2012/631](https://eprint.iacr.org/2012/631) ·
  [Springer](https://link.springer.com/chapter/10.1007/978-3-642-38348-9_18)
- Keelveedhi, Bellare, Ristenpart, *DupLESS: Server-Aided Encryption for Deduplicated Storage*,
  USENIX Security 2013 — [USENIX](https://www.usenix.org/conference/usenixsecurity13/technical-sessions/presentation/bellare) ·
  [eprint 2013/429](https://eprint.iacr.org/2013/429.pdf)
- Len, Grubbs, Ristenpart, *Partitioning Oracle Attacks*, USENIX Security 2021 —
  [PDF](https://www.usenix.org/system/files/sec21-len.pdf) · [eprint 2020/1491](https://eprint.iacr.org/2020/1491)
- Albertini, Duong, Gueron, Kölbl, Luykx, Schmieg, *How to Abuse and Fix Authenticated Encryption
  Without Key Commitment*, USENIX Security 2022 —
  [PDF](https://www.usenix.org/system/files/sec22-albertini.pdf) · [eprint 2020/1456](https://eprint.iacr.org/2020/1456)
- Gueron, *Key Committing AEADs* — [eprint 2020/1153](https://eprint.iacr.org/2020/1153.pdf)
- Neil Madden, *Galois/Counter Mode and random nonces* —
  [neilmadden.blog](https://neilmadden.blog/2024/05/23/galois-counter-mode-and-random-nonces/)
- smarx, *Convergent Encryption and Why No One Uses It* —
  [smarx.com](https://smarx.com/posts/2020/09/convergent-encryption-and-why-no-one-uses-it/)
- Product docs: [Tresorit](https://tresorit.com/security),
  [Tahoe-LAFS convergence secret](https://tahoe-lafs.readthedocs.io/en/latest/convergence-secret.html),
  [MEGA whitepaper](https://www.voilatranslate.com/wp-content/uploads/SecurityWhitepaper.pdf),
  [Storj data sheet](https://static.storj.io/documents/storj-dcs-security-data-sheet.pdf),
  [Apple iCloud ADP](https://support.apple.com/guide/security/advanced-data-protection-for-icloud-sec973254c5f/web),
  [AWS S3 SSE-KMS](https://docs.aws.amazon.com/AmazonS3/latest/userguide/specifying-kms-encryption.html) /
  [UpdateObjectEncryption](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UpdateObjectEncryption.html)
