# tests/staging/ — live drain e2e

Acceptance tests for the SSD→CephFS drain (`hippius-drain`) against the **real
staging gateway** (`https://s3-staging.hippius.com`) with real credentials — no
docker-compose, no mocks. They assert the drain *outcome* an S3 client can observe:
a PUT completes and the object becomes durably, cross-node readable.

## ⚠️ Gated until the drain is live

The suite is **skipped** unless `HIPPIUS_DRAIN_LIVE=1`. That is deliberate: today
staging PUTs do **not** complete the drain, so these assertions would fail on the
known state rather than on a regression. They flip to enforced the moment the drain
is deployed *and* actually drains the api's chunks.

### The deploy blocker (why the drain isn't live yet)

The lifted `hippius-drain` crates speak the **cephor** on-disk contract —
`<root>/<file_id>/<chunk_key>` (2 levels, `sha256(bytes) == chunk_key`, no
`meta.json`). The hippius-s3 api writes a **different** layout, confirmed on a live
staging pod:

```
<object_id>/v<version>/part_<n>/{meta.json, chunk_<i>.bin}   # 4 levels, AES-GCM ciphertext
```

The drain's `LocalSsd::scan` walks only two levels, so against the api layout it
treats `v<version>` as a non-file and discovers **zero** chunks — it would deploy
cleanly but drain nothing (safe, but a no-op; the 502 persists). Making the drain
functional requires re-homing its storage contract onto the api's layout + part-key
derivation (agent `localfs.rs` + the verify-hash contract), or adapting the api to
emit the cephor layout + `cephor_replication_status` rows. Until then, leave
`HIPPIUS_DRAIN_LIVE` unset.

## Running (once the drain is live)

```bash
export AWS_ACCESS_KEY_ID=hip_...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=decentralized
export HIPPIUS_DRAIN_LIVE=1

# TLS: staging's gateway cert SAN is us-east-1.hippius.com, not the gateway host.
# Prefer pointing at the staging CA; the insecure flag is the runnable-today fallback.
export HIPPIUS_S3_CA_BUNDLE=/path/to/staging-ca.pem   # preferred
# export HIPPIUS_S3_INSECURE=1                          # fallback (disables verification)

pytest tests/staging -v
```

### Optional Postgres-level checks

`test_drain_state.py` is additionally gated on `CEPHOR_DATABASE_URL` (or
`DATABASE_URL`). With it set, it asserts `cephor_replication_status` rows reach
`replicated` after uploads. Coarse (fleet-wide, not per-object) until the
api↔drain chunk-key contract is fixed.

```bash
export CEPHOR_DATABASE_URL=postgres://.../objectstore
pytest tests/staging/test_drain_state.py -v
```

## Env

| Variable | Default | Notes |
|---|---|---|
| `HIPPIUS_DRAIN_LIVE` | unset | `1` enables the suite. |
| `HIPPIUS_S3_ENDPOINT` | `https://s3-staging.hippius.com` | Gateway URL. |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | — | Required (`hip_` access key). |
| `AWS_DEFAULT_REGION` | `decentralized` | SigV4 signing region. |
| `HIPPIUS_S3_CA_BUNDLE` | — | CA bundle for TLS verification (preferred). |
| `HIPPIUS_S3_INSECURE` | unset | `1` disables TLS verification (staging cert mismatch fallback). |
| `CEPHOR_DATABASE_URL` | — | Enables the optional Postgres-level checks. |
