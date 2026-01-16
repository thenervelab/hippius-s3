# Object Versioning Strategy

## Two Types of Versions

### 1. User Versions (Future S3 Feature)

**Visibility**: Exposed via S3 API
**Purpose**: Allow users to access previous versions of their objects
**Flag**: `is_migration_version = FALSE`

```sql
-- User creates a new version by uploading to same key
PUT /bucket/myfile.txt  -- version_id: abc123
PUT /bucket/myfile.txt  -- version_id: def456 (new latest)

-- User can list all their versions
GET /bucket?versions
-- Returns: [def456 (latest), abc123]

-- User can get specific version
GET /bucket/myfile.txt?versionId=abc123
```

### 2. Migration Versions (Internal Only)

**Visibility**: Hidden from S3 API
**Purpose**: Safe rollback during storage format migrations
**Flag**: `is_migration_version = TRUE`
**Lifecycle**: Temporary, cleaned up after grace period

```sql
-- System migrates v1 → v4 internally
object_v1 (old_id, is_migration_version=TRUE, is_latest=FALSE)
object_v4 (new_id, is_migration_version=FALSE, is_latest=TRUE)

-- User sees only the latest v4
GET /bucket/myfile.txt
-- Returns: object_v4 only

-- After 7 days, cleanup worker deletes object_v1
```

## Critical Distinction

| Aspect                            | User Versions                      | Migration Versions              |
| --------------------------------- | ---------------------------------- | ------------------------------- |
| **Created By**                    | User PUTs                          | System migration                |
| **Visible in API**                | ✅ Yes                             | ❌ No                           |
| **Exposed in ListObjectVersions** | ✅ Yes                             | ❌ No                           |
| **Can be retrieved**              | ✅ Yes (with versionId)            | ❌ No (internal only)           |
| **Cleanup**                       | User controls (lifecycle policies) | Auto-cleanup after grace period |
| **Purpose**                       | Data protection, compliance        | Migration safety                |

## Schema Design

```sql
-- Both types share the same table, differentiated by flag
CREATE TABLE objects (
    object_id UUID PRIMARY KEY,
    bucket_id UUID NOT NULL,
    object_key TEXT NOT NULL,
    version_id UUID DEFAULT gen_random_uuid(),
    is_latest BOOLEAN DEFAULT TRUE,
    is_migration_version BOOLEAN DEFAULT FALSE,  -- FALSE = user version
    storage_version SMALLINT NOT NULL,
    ...
);
```

## Query Patterns

### Standard Object Retrieval (Current)

```sql
-- Get current version
SELECT * FROM objects
WHERE bucket_id = $1 AND object_key = $2
  AND is_latest = TRUE;
```

### User Version Queries (Future S3 Versioning)

```sql
-- List all user-visible versions
SELECT * FROM objects
WHERE bucket_id = $1 AND object_key = $2
  AND is_migration_version = FALSE
ORDER BY version_created_at DESC;

-- Get specific user version
SELECT * FROM objects
WHERE bucket_id = $1 AND object_key = $2
  AND version_id = $3
  AND is_migration_version = FALSE;
```

### Admin/Migration Queries

```sql
-- Get all versions including migrations (for cleanup)
SELECT * FROM objects
WHERE bucket_id = $1 AND object_key = $2
ORDER BY version_created_at DESC;

-- Find old migration versions to clean up
SELECT o.*
FROM objects o
JOIN storage_migrations sm ON o.object_id = sm.old_object_id
WHERE o.is_migration_version = TRUE
  AND sm.status = 'completed'
  AND sm.completed_at < (now() - interval '7 days');
```

## Migration Lifecycle

```
User uploads file.txt (v1 storage)
  → object_id: aaa-111
  → is_migration_version: FALSE
  → is_latest: TRUE
  → storage_version: 1

[Migration starts]
  1. Download object_id aaa-111 (any version)
  2. Write new object_id bbb-222 (v3 storage)
  3. Mark aaa-111 as migration version:
     UPDATE objects SET is_migration_version = TRUE WHERE object_id = 'aaa-111'
  4. Atomic swap:
     UPDATE objects SET is_latest = FALSE WHERE object_id = 'aaa-111'
     UPDATE objects SET is_latest = TRUE WHERE object_id = 'bbb-222'

[User perspective - unchanged]
GET /bucket/file.txt
  → Returns bbb-222 (v3 storage)
  → User never sees aaa-111 (migration version hidden)

[7 days later - cleanup]
  - Verify bbb-222 still is_latest = TRUE
  - Delete aaa-111 and all parts
  - Unpin old CIDs from IPFS
  - Mark migration as 'cleaned'

[Final state]
  → object_id: bbb-222
  → is_migration_version: FALSE
  → is_latest: TRUE
  → storage_version: 4
  → old object aaa-111 deleted
```

## User Versioning (Future Implementation)

When implementing full S3 versioning support:

```python
# When bucket has versioning enabled
async def handle_put_object_versioned(bucket_id, object_key, data):
    # Create NEW version (user-visible)
    new_version = await writer.write_simple_object(
        plaintext=data,
        object_id=str(uuid.uuid4()),
        is_migration_version=FALSE,  # User version
        ...
    )

    # Mark old version as not latest (but keep it)
    await db.execute("""
        UPDATE objects
        SET is_latest = FALSE
        WHERE bucket_id = $1 AND object_key = $2
          AND is_latest = TRUE
    """, bucket_id, object_key)

    # Make new version latest
    await db.execute("""
        UPDATE objects
        SET is_latest = TRUE
        WHERE object_id = $1
    """, new_version)
```

## Benefits of This Approach

1. **Clean Separation**: User features vs internal operations
2. **Future-Ready**: Schema supports S3 versioning when needed
3. **User-Friendly**: Migrations invisible to users
4. **Safe Migration**: All benefits of versioning (rollback, audit trail)
5. **Resource Efficient**: Auto-cleanup of migration versions

## Configuration

```python
# Migration config
MIGRATION_GRACE_PERIOD_DAYS = 7  # How long to keep old versions
MIGRATION_AUTO_CLEANUP = True    # Auto-delete after grace period

# User versioning config (future)
BUCKET_VERSIONING_ENABLED = False  # Per-bucket setting
VERSION_LIFECYCLE_POLICY = None    # Expiration rules
```

## Monitoring

```sql
-- Count migration versions awaiting cleanup
SELECT COUNT(*)
FROM objects
WHERE is_migration_version = TRUE
  AND is_latest = FALSE;

-- Migration version storage usage
SELECT SUM(size_bytes)
FROM objects
WHERE is_migration_version = TRUE;

-- User version count per object
SELECT object_key, COUNT(*) as version_count
FROM objects
WHERE bucket_id = $1 AND is_migration_version = FALSE
GROUP BY object_key
ORDER BY version_count DESC;
```
