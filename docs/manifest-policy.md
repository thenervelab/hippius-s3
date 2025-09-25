# IPFS/S3 Object Manifest Policy

Goal:
Define how objects are represented on IPFS via a manifest today, and outline the target directory layout we may adopt later.

## Today (current behavior)

- Single artifact per object: a `manifest.json` describes the object and references parts by CID. There are no file paths for parts on IPFS; only CIDs.
- Chunks/parts are uploaded to IPFS and pinned; the database tracks `{part_number, cid, size_bytes}`.
- The manifest is published and its CID is stored on the object row; HEAD exposes that CID.
- Publish path is resilient: we first try SDK `s3_publish` with bounded retries and jittered backoff; on failure or missing account context we fall back to upload+pin of the manifest. Behavior is the same from the client perspective (a manifest CID), but logs/metrics annotate `path=sdk|fallback`.
- GET reads the manifest and streams bytes using part CIDs from cache/IPFS.

## Current vs Target Behavior

Current (today) behavior:

- Publish a single manifest JSON per object; parts are CIDs only (no paths).
- `objects.ipfs_cid` points to the manifest CID; GET assembles bytes using part CIDs.

## Implementation Notes

- Write (today): upload+pin part CIDs; upsert parts; publish manifest; set `objects.ipfs_cid` to manifest CID.
- Read (today): read manifest; stream from cache/IPFS via part CIDs.
- Filename Sanitization
  - Use the final segment of the S3 key; replace unsafe characters; preserve extension; cap length.
  - Filename visibility on IPFS gateways depends on the publish path:
    - SDK `s3_publish` preserves the provided filename (gateway listings show a friendly name).
    - Fallback upload+pin publishes the manifest as a single file by CID; friendly filename may not be shown unless explicitly wrapped with a directory. Tests accept either Links (directory) or non‑zero Size (file).

## Migration & Compatibility

- Backfill: synthesize `manifest.json` for legacy objects on first update or via batch.
- Flags: no special flag required; manifest is created once all parts have concrete CIDs. The previously referenced `HIPPIUS_ALWAYS_MANIFEST` flag is not used.

## Testing

- Helpers:
  - `wait_for_parts_cids` – ensure parts have CIDs.
  - `wait_for_object_cid_via_head` – poll until the manifest CID is exposed.
  - IPFS helpers – accept either Links (if published with a filename) or non‑zero Size for the CID.

## Benefits

- Stable layout: no renaming when transitioning to multipart.
- Human-friendly: gateways display familiar filenames.
- Simple logic: always read `manifest.json`, always stream from `parts/`.
- Deterministic: one object = one folder, always the same shape.
