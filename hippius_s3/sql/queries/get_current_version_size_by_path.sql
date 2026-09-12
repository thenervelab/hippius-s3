-- The current version's size for (bucket_name, object_key), for the plan quota gate.
--
-- WHY IT EXISTS. A server-side copy (CopyObject / UploadPartCopy) carries NO request body, so
-- `Content-Length` is 0 and the quota gate was checking zero bytes against the limit -- a 5 TB copy
-- passed a gate with 1 GB of headroom. The size is knowable up front, because it is just the source
-- object's current version, so resolve it instead of trusting a header that cannot carry it.
--
-- Deliberately narrow: one value, indexed lookups only, and no join to parts or part_chunks. This
-- runs on the request path in the gateway middleware, so it must not become a second
-- get_object_head_by_path -- the gate needs a byte count, not metadata.
--
-- Returns NULL when the object does not exist, is soft-deleted, is a delete marker, or is a
-- reserved-but-incomplete version (size_bytes = 0 AND md5 empty, the same "not serveable yet"
-- predicate the download query uses). The caller treats NULL as "size unknown" and falls back to
-- the declared header rather than refusing the request: a copy whose source we cannot resolve is
-- about to 404 in the handler anyway, and guessing a huge number here would refuse it with the
-- wrong error.
--
-- Resolved through resolve_object_id so an ALIAS key -- a second name attached to one object_id by
-- a same-bucket CopyObject -- finds the same object the handler will copy from.
--
-- Parameters: $1: bucket_name (text), $2: object_key (text)
SELECT ov.size_bytes
FROM buckets b
JOIN objects o
  ON o.object_id = resolve_object_id(b.bucket_id, $2)
JOIN object_versions ov
  ON ov.object_id = o.object_id
 AND ov.object_version = o.current_object_version
WHERE b.bucket_name = $1
  AND b.deleted_at IS NULL
  AND o.deleted_at IS NULL
  AND ov.deleted_at IS NULL
  AND NOT ov.is_delete_marker
  AND NOT (ov.size_bytes = 0 AND COALESCE(ov.md5_hash, '') = '')
