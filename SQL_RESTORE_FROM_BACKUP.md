# PostgreSQL Restore from Backup

Restores use CNPG's `bootstrap.recovery` with the barman-cloud plugin. The backup source is an OVH S3 bucket containing hourly base backups + continuous WAL archives.

## Prerequisites

- `kubectl` access to the target namespace
- The `postgres-backup-creds` secret must exist in the namespace (contains `ACCESS_KEY_ID` and `ACCESS_KEY_SECRET`)

## 1. Create the restore ObjectStore

This points to the same S3 bucket as the source backups. Create a separate ObjectStore so it doesn't interfere with the running cluster's backup config.

```bash
cat <<'EOF' | kubectl apply -n <NAMESPACE> -f -
apiVersion: barmancloud.cnpg.io/v1
kind: ObjectStore
metadata:
  name: restore-source
spec:
  configuration:
    destinationPath: s3://hippius-s3-psql-backup/<ENV>/postgres/
    endpointURL: https://s3.eu-west-par.io.cloud.ovh.net
    s3Credentials:
      accessKeyId:
        name: postgres-backup-creds
        key: ACCESS_KEY_ID
      secretAccessKey:
        name: postgres-backup-creds
        key: ACCESS_KEY_SECRET
    data:
      compression: gzip
      jobs: 2
    wal:
      compression: gzip
      maxParallel: 2
EOF
```

Replace `<NAMESPACE>` with `hippius-s3-staging` or `hippius-s3-production` and `<ENV>` with `staging` or `production`.

## 2. Create the restore cluster

### Restore from latest backup

```bash
cat <<'EOF' | kubectl apply -n <NAMESPACE> -f -
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: postgres-restore-test
spec:
  instances: 1
  storage:
    storageClass: ceph-filesystem
    size: 50Gi
  bootstrap:
    recovery:
      source: source
  externalClusters:
    - name: source
      plugin:
        name: barman-cloud.cloudnative-pg.io
        parameters:
          barmanObjectName: restore-source
          serverName: postgres
  resources:
    requests:
      cpu: 250m
      memory: 1Gi
    limits:
      cpu: 1000m
      memory: 4Gi
EOF
```

### Restore to a specific point in time

Add `recoveryTarget` to the `bootstrap.recovery` section:

```yaml
  bootstrap:
    recovery:
      source: source
      recoveryTarget:
        targetTime: "2026-02-03T08:00:00Z"
```

This replays WAL up to the specified timestamp. Any ISO 8601 timestamp within the backup retention window works.

### Restore a specific base backup by ID

To list available backups, use the AWS CLI:

```bash
aws s3 ls s3://hippius-s3-psql-backup/<ENV>/postgres/postgres/base/ \
  --endpoint-url https://s3.eu-west-par.io.cloud.ovh.net
```

Then specify the backup ID:

```yaml
  bootstrap:
    recovery:
      source: source
      recoveryTarget:
        backupID: "20260203T090202"
```

## 3. Monitor the restore

```bash
kubectl get cluster.postgresql.cnpg.io postgres-restore-test -n <NAMESPACE> -w
kubectl logs -n <NAMESPACE> -l cnpg.io/cluster=postgres-restore-test --all-containers -f
```

Wait until `STATUS` shows `Cluster in healthy state` and `READY` equals `INSTANCES`.

## 4. Verify the data

```bash
POD=$(kubectl get pods -n <NAMESPACE> -l cnpg.io/cluster=postgres-restore-test --no-headers | awk '{print $1}')
kubectl exec -n <NAMESPACE> $POD -c postgres -- psql -U postgres -d hippius -c "\dt"
kubectl exec -n <NAMESPACE> $POD -c postgres -- psql -U postgres -d hippius -c "SELECT count(*) FROM objects;"
```

## 5. Clean up

Once verified, delete the test cluster and ObjectStore:

```bash
kubectl delete cluster.postgresql.cnpg.io postgres-restore-test -n <NAMESPACE>
kubectl delete objectstore.barmancloud.cnpg.io restore-source -n <NAMESPACE>
```

## Notes

- The `serverName: postgres` parameter is required. It tells barman which backup catalog to read (matches the original cluster name).
- The restored cluster does **not** have WAL archiving enabled. To use it as a permanent replacement, add a `.spec.plugins` section pointing to a backup ObjectStore.
- `archive command failed` warnings during restore are expected and harmless.
