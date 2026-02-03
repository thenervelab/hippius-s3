# PostgreSQL Migration: prod Docker → k8s CNPG

Dumps both databases from the prod Docker Postgres, merges them, and restores into a fresh CNPG cluster on k8s.

## Values to replace

| Placeholder | Description | Example |
|---|---|---|
| `<SSH_HOST>` | SSH alias/hostname for the prod server | `s3` |
| `<CONTAINER>` | Postgres Docker container name on prod | `hippius-s3-db-1` |
| `<NAMESPACE>` | Target k8s namespace | `hippius-s3-staging` |
| `<CLUSTER_NAME>` | Name for the new CNPG cluster | `test-prod-restore-psql` |

## 1. Find the postgres container on prod

```bash
ssh <SSH_HOST> "docker ps --filter ancestor=postgres:15 --format '{{.Names}}'"
```

## 2. Dump both databases

Streams directly to your local machine — no files left on prod.

```bash
ssh <SSH_HOST> "docker exec <CONTAINER> pg_dump -U postgres -d hippius --no-owner --no-privileges" > /tmp/hippius.sql

ssh <SSH_HOST> "docker exec <CONTAINER> pg_dump -U postgres -d hippius_keys --no-owner --no-privileges --exclude-table=schema_migrations" > /tmp/hippius_keys.sql
```

- `--no-owner --no-privileges` so restore doesn't depend on matching roles
- `--exclude-table=schema_migrations` on hippius_keys to avoid conflicts (not needed for keys DB)

## 3. Merge the two dumps

No table name conflicts between the two databases.

```bash
cat /tmp/hippius.sql /tmp/hippius_keys.sql > /tmp/hippius_merged.sql
```

## 4. Create a CNPG cluster

```bash
cat <<'EOF' | kubectl apply -n <NAMESPACE> -f -
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: <CLUSTER_NAME>
spec:
  instances: 1
  primaryUpdateStrategy: unsupervised
  storage:
    storageClass: ceph-filesystem
    size: 50Gi
  postgresql:
    parameters:
      max_connections: "200"
      shared_buffers: "512MB"
  bootstrap:
    initdb:
      database: hippius
      owner: postgres
  enableSuperuserAccess: true
  resources:
    requests:
      cpu: 500m
      memory: 2Gi
    limits:
      cpu: 4000m
      memory: 12Gi
EOF
```

Wait for the pod to be ready:

```bash
kubectl get pods -n <NAMESPACE> -l cnpg.io/cluster=<CLUSTER_NAME> -w
```

## 5. Compress and copy the dump into the pod

`kubectl cp` goes through the k8s API server. For large dumps (1GB+), gzip first to avoid connection timeouts.

```bash
gzip -k /tmp/hippius_merged.sql

kubectl cp /tmp/hippius_merged.sql.gz \
  <NAMESPACE>/<CLUSTER_NAME>-1:/run/hippius_merged.sql.gz -c postgres
```

Decompress inside the pod:

```bash
kubectl exec <CLUSTER_NAME>-1 -n <NAMESPACE> -c postgres -- \
  gunzip /run/hippius_merged.sql.gz
```

## 6. Restore locally inside the pod

Running psql locally inside the pod avoids all network timeout issues.

```bash
kubectl exec <CLUSTER_NAME>-1 -n <NAMESPACE> -c postgres -- \
  psql -U postgres -d hippius -f /run/hippius_merged.sql
```

## 7. Verify

```bash
kubectl exec <CLUSTER_NAME>-1 -n <NAMESPACE> -c postgres -- \
  psql -U postgres -d hippius -c "\dt"

kubectl exec <CLUSTER_NAME>-1 -n <NAMESPACE> -c postgres -- \
  psql -U postgres -d hippius -c "SELECT 'objects' as tbl, count(*) FROM objects UNION ALL SELECT 'buckets', count(*) FROM buckets UNION ALL SELECT 'encryption_keys', count(*) FROM encryption_keys"
```

## 8. Clean up

Remove the dump from the pod:

```bash
kubectl exec <CLUSTER_NAME>-1 -n <NAMESPACE> -c postgres -- \
  rm -f /run/hippius_merged.sql
```

Delete the cluster when no longer needed:

```bash
kubectl delete clusters.postgresql.cnpg.io <CLUSTER_NAME> -n <NAMESPACE>
```

Remove local files:

```bash
rm /tmp/hippius.sql /tmp/hippius_keys.sql /tmp/hippius_merged.sql /tmp/hippius_merged.sql.gz
```

## Notes

- `/run` is used as the writable directory inside CNPG pods (`/tmp` is read-only)
- `kubectl port-forward` is unreliable for large restores — always copy the dump into the pod and restore locally
- Foreign key constraint errors at the end of restore are harmless if this is a fresh cluster — they come from pg_dump ordering constraints after data
