# Hippius S3 Kubernetes Deployment

This directory contains Kubernetes manifests for deploying hippius-s3 to a Kubernetes cluster.

## Architecture

- **Namespaces**: `hippius-s3-staging` and `hippius-s3-prod`
- **Deployments**: Gateway (2-4 replicas), API (2-4 replicas), 7 worker types
- **StatefulSets**: PostgreSQL, 6 Redis instances, IPFS
- **Storage**: Longhorn distributed storage with ReadWriteMany for shared cache
- **Ingress**: NGINX with TLS via cert-manager (Let's Encrypt)
- **Monitoring**: ServiceMonitor for Prometheus integration

## Prerequisites

1. **Kubernetes Cluster**: RKE2 v1.32+ with Longhorn storage
2. **Registry Access**: GitHub Container Registry (ghcr.io) via GitHub Actions
3. **GitHub Secrets**: Configure required secrets (see below)
4. **cert-manager**: Installed with `letsencrypt-prod` ClusterIssuer
5. **NGINX Ingress**: Installed and configured
6. **Prometheus Operator**: For ServiceMonitor support (optional)

## Directory Structure

```
k8s/
├── base/               # Base Kubernetes manifests
│   ├── namespace.yaml
│   ├── configmap-*.yaml
│   ├── secret.yaml
│   ├── pvc-*.yaml
│   ├── *-statefulset.yaml
│   ├── *-deployment.yaml
│   ├── services.yaml
│   ├── ingress.yaml
│   ├── networkpolicy.yaml
│   └── servicemonitor.yaml
├── staging/            # Staging overlay
│   ├── kustomization.yaml
│   └── resource-limits.yaml
└── production/         # Production overlay
    ├── kustomization.yaml
    ├── namespace-patch.yaml
    ├── resource-limits.yaml
    ├── postgres-tuning.yaml
    ├── redis-tuning.yaml
    └── ingress-patch.yaml
```

## GitHub Secrets

Configure the following secrets in your GitHub repository:

### Kubernetes Access
- `KUBE_CONFIG`: Base64-encoded kubeconfig (used for both staging and production namespaces)

### Staging Secrets
- `STAGING_DATABASE_URL`: PostgreSQL connection string
- `STAGING_DATABASE_PASSWORD`: PostgreSQL password
- `STAGING_HIPPIUS_SERVICE_KEY`: Hippius service seed phrase
- `STAGING_HIPPIUS_AUTH_ENCRYPTION_KEY`: Encryption key

### Production Secrets
- `PRODUCTION_DATABASE_URL`: PostgreSQL connection string
- `PRODUCTION_DATABASE_PASSWORD`: PostgreSQL password
- `PRODUCTION_HIPPIUS_SERVICE_KEY`: Hippius service seed phrase
- `PRODUCTION_HIPPIUS_AUTH_ENCRYPTION_KEY`: Encryption key

## Deployment

### Automatic Deployment (CI/CD)

Push to `staging` or `production` branch to trigger automatic deployment via GitHub Actions.

```bash
git checkout -b staging
git push origin staging
```

### Manual Deployment

#### 1. Build and push images

```bash
docker build -t ghcr.io/thenervelab/hippius-s3/base:latest -f Dockerfile.base .
docker push ghcr.io/thenervelab/hippius-s3/base:latest

docker build -t ghcr.io/thenervelab/hippius-s3/api:latest .
docker push ghcr.io/thenervelab/hippius-s3/api:latest

docker build -t ghcr.io/thenervelab/hippius-s3/gateway:latest -f gateway/Dockerfile .
docker push ghcr.io/thenervelab/hippius-s3/gateway:latest

docker build -t ghcr.io/thenervelab/hippius-s3/workers:latest -f workers/Dockerfile .
docker push ghcr.io/thenervelab/hippius-s3/workers:latest
```

#### 2. Update secrets

```bash
kubectl create secret generic hippius-s3-secrets \
  --from-literal=DATABASE_URL="postgresql://postgres:PASSWORD@postgres:5432/hippius" \
  --from-literal=HIPPIUS_KEYSTORE_DATABASE_URL="postgresql://postgres:PASSWORD@postgres:5432/hippius" \
  --from-literal=REDIS_URL="redis://redis:6379/0" \
  --from-literal=REDIS_ACCOUNTS_URL="redis://redis-accounts:6379/0" \
  --from-literal=REDIS_CHAIN_URL="redis://redis-chain:6379/0" \
  --from-literal=REDIS_QUEUES_URL="redis://redis-queues:6379/0" \
  --from-literal=REDIS_RATE_LIMITING_URL="redis://redis-rate-limiting:6379/0" \
  --from-literal=REDIS_ACL_URL="redis://redis-acl:6379/0" \
  --from-literal=HIPPIUS_SERVICE_KEY="your-seed-phrase" \
  --from-literal=HIPPIUS_AUTH_ENCRYPTION_KEY="your-encryption-key" \
  --from-literal=HIPPIUS_IPFS_API_URLS="http://ipfs:5001" \
  --from-literal=DATABASE_PASSWORD="your-db-password" \
  --namespace=hippius-s3-staging \
  --dry-run=client -o yaml | kubectl apply -f -
```

#### 3. Deploy to staging

```bash
kubectl apply -k k8s/staging
```

#### 4. Run migrations (one-time)

The migration Job runs automatically during deployment. To manually trigger:

```bash
kubectl delete job db-migrations -n hippius-s3-staging
kubectl apply -k k8s/staging
```

#### 5. Deploy to production

```bash
kubectl apply -k k8s/production
```

## Verification

### Check pod status

```bash
kubectl get pods -n hippius-s3-staging
kubectl get pods -n hippius-s3-prod
```

### Check services

```bash
kubectl get services -n hippius-s3-staging
kubectl get services -n hippius-s3-prod
```

### Check ingress

```bash
kubectl get ingress -n hippius-s3-staging
kubectl get ingress -n hippius-s3-prod
```

### Check logs

```bash
kubectl logs -n hippius-s3-staging deployment/gateway -f
kubectl logs -n hippius-s3-staging deployment/api -f
kubectl logs -n hippius-s3-staging deployment/uploader -f
```

### Test S3 API

```bash
aws s3 --endpoint-url=https://s3-staging.hippius.com ls
aws s3 --endpoint-url=https://s3.hippius.com ls
```

## Scaling

### Manual scaling

```bash
kubectl scale deployment/downloader --replicas=10 -n hippius-s3-staging
kubectl scale deployment/gateway --replicas=4 -n hippius-s3-staging
```

### Auto-scaling (HPA)

Downloader deployment can be auto-scaled based on CPU usage:

```bash
kubectl autoscale deployment/downloader --min=1 --max=10 --cpu-percent=80 -n hippius-s3-staging
```

## Storage Management

### Check PVC status

```bash
kubectl get pvc -n hippius-s3-staging
kubectl get pvc -n hippius-s3-prod
```

### Resize PVC (Longhorn supports online expansion)

```bash
kubectl patch pvc object-cache-pvc -n hippius-s3-staging -p '{"spec":{"resources":{"requests":{"storage":"600Gi"}}}}'
```

## Troubleshooting

### Pod not starting

```bash
kubectl describe pod <pod-name> -n hippius-s3-staging
kubectl logs <pod-name> -n hippius-s3-staging --previous
```

### Database migration issues

```bash
kubectl logs -n hippius-s3-staging job/db-migrations
kubectl describe job db-migrations -n hippius-s3-staging
```

### IPFS connectivity issues

```bash
kubectl exec -it -n hippius-s3-staging ipfs-0 -- ipfs id
kubectl exec -it -n hippius-s3-staging ipfs-0 -- ipfs swarm peers
```

### Redis connection issues

```bash
kubectl exec -it -n hippius-s3-staging redis-0 -- redis-cli ping
kubectl exec -it -n hippius-s3-staging redis-accounts-0 -- redis-cli ping
```

### Gateway 502/504 errors

Check if API is ready:
```bash
kubectl get pods -n hippius-s3-staging -l app=api
kubectl logs -n hippius-s3-staging deployment/api --tail=100
```

### Worker not processing queue

```bash
kubectl logs -n hippius-s3-staging deployment/uploader -f
kubectl exec -it -n hippius-s3-staging redis-queues-0 -- redis-cli LLEN upload_requests
```

## Monitoring

Access Grafana dashboards in hippius-monitoring namespace to view:
- Request rates and latency
- Queue depths
- Cache hit rates
- Worker processing rates
- IPFS upload/download rates

## Rollback

### Rollback deployment

```bash
kubectl rollout undo deployment/gateway -n hippius-s3-staging
kubectl rollout undo deployment/api -n hippius-s3-staging
```

### Rollback to specific revision

```bash
kubectl rollout history deployment/gateway -n hippius-s3-staging
kubectl rollout undo deployment/gateway --to-revision=3 -n hippius-s3-staging
```

## Maintenance

### Update ConfigMap

```bash
kubectl edit configmap hippius-s3-defaults -n hippius-s3-staging
kubectl rollout restart deployment/api -n hippius-s3-staging
kubectl rollout restart deployment/gateway -n hippius-s3-staging
```

### Update Secret

```bash
kubectl edit secret hippius-s3-secrets -n hippius-s3-staging
kubectl rollout restart deployment/api -n hippius-s3-staging
kubectl rollout restart deployment/gateway -n hippius-s3-staging
```

### Restart StatefulSet

```bash
kubectl rollout restart statefulset/postgres -n hippius-s3-staging
kubectl rollout restart statefulset/redis -n hippius-s3-staging
```

## Resource Limits

### Staging
- Gateway: 2 replicas, 500m-2000m CPU, 1Gi-4Gi memory
- API: 2 replicas, 1000m-4000m CPU, 2Gi-8Gi memory
- Downloader: 5 replicas, 250m-1000m CPU, 512Mi-2Gi memory
- PostgreSQL: 500m-4000m CPU, 2Gi-12Gi memory
- IPFS: 500m-2000m CPU, 2Gi-8Gi memory

### Production
- Gateway: 4 replicas, 500m-2000m CPU, 1Gi-4Gi memory
- API: 4 replicas, 1000m-4000m CPU, 2Gi-8Gi memory
- Downloader: 10 replicas, 250m-1000m CPU, 512Mi-2Gi memory
- PostgreSQL: Tuned with 1000 connections, 8GB shared_buffers
- Redis (main): 1000m-4000m CPU, 200Gi-210Gi memory

## Security

### NetworkPolicy
API pods only accept traffic from Gateway and workers within the same namespace.

### Secrets
All sensitive data (DB passwords, service keys) stored in Kubernetes secrets.

### TLS
Ingress automatically provisions TLS certificates via cert-manager and Let's Encrypt.

## Support

For issues or questions:
1. Check pod logs: `kubectl logs -n <namespace> <pod-name>`
2. Check events: `kubectl get events -n <namespace> --sort-by='.lastTimestamp'`
3. Inspect resources: `kubectl describe <resource> <name> -n <namespace>`
