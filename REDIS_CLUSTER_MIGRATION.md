# Redis Cluster Migration Guide

This guide covers the migration from standalone Redis to Redis Cluster for Hippius S3.

## Table of Contents
1. [Overview](#overview)
2. [What is Redis Cluster](#what-is-redis-cluster)
3. [Architecture](#architecture)
4. [Local Development Setup](#local-development-setup)
5. [Production Migration](#production-migration)
6. [Kubernetes Deployment](#kubernetes-deployment)
7. [Scaling Redis Cluster on K8s](#scaling-redis-cluster-on-k8s)
8. [Troubleshooting](#troubleshooting)
9. [Important Caveats](#important-caveats)

---

## Overview

### Why Redis Cluster?

- **Horizontal Scalability**: Instead of one large 500GB Redis instance, distribute data across multiple smaller nodes (3x60GB = 180GB)
- **Better Resource Utilization**: Smaller nodes are easier to manage and deploy
- **Future-Proof**: Easy to scale up by adding more nodes as needed

### Migration Summary

- **Scope**: Only the main Redis cache (redis:6379) is being converted to cluster mode
- **Other Redis instances remain standalone**: redis-accounts, redis-chain, redis-rate-limiting stay as-is
- **Production Setup**: 3 master nodes @ 60GB each = 180GB total (no replicas)
- **Migration Type**: Breaking change with maintenance window required

---

## What is Redis Cluster

### Core Concepts

**Hash Slots**
- Redis Cluster uses 16,384 hash slots to partition data
- Each key is assigned to a slot using: `CRC16(key) mod 16384`
- Slots are distributed across master nodes:
  - Node 1: slots 0-5460 (5,461 slots)
  - Node 2: slots 5461-10922 (5,462 slots)
  - Node 3: slots 10923-16383 (5,461 slots)

**Data Sharding**
- Keys are automatically distributed across nodes based on their hash slot
- Clients are redirected to the correct node for each key operation
- redis-py handles redirects automatically

**Client Behavior**
- Clients maintain a slot-to-node mapping cache
- On `MOVED` error, client updates its mapping and retries
- On `ASK` error during resharding, client temporarily redirects

---

## Architecture

### Current State (Standalone)
```
┌─────────────────────┐
│  Redis Standalone   │
│    200GB Memory     │  ← Single point, all data
└─────────────────────┘
```

### Target State (Cluster)
```
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  Redis Node 1│  │  Redis Node 2│  │  Redis Node 3│
│  60GB Memory │  │  60GB Memory │  │  60GB Memory │
│  Slots 0-5460│  │  Slots 5461+ │  │  Slots 10923+│
└──────────────┘  └──────────────┘  └──────────────┘
         ↓                ↓                ↓
    Cluster Bus (gossip protocol on port +10000)
```

---

## Local Development Setup

### 1. Start Redis Cluster

```bash
# Start the cluster with docker compose
docker compose -f docker-compose.yml -f docker-compose.cluster.yml up -d

# Wait for cluster initialization
docker logs redis-cluster-init -f

# Verify cluster is healthy
docker exec redis-cluster-1 redis-cli cluster info
docker exec redis-cluster-1 redis-cli cluster nodes
```

### 2. Configure Application

Add to your `.env`:
```bash
REDIS_CLUSTER_ENABLED=true
REDIS_URL=172.20.0.20:6379,172.20.0.21:6379,172.20.0.22:6379
```

### 3. Verify Application Connectivity

```bash
# Check logs for cluster initialization
docker compose logs api | grep -i "redis"

# Should see: "Redis Cluster initialized with 3 nodes"
```

### 4. Test Operations

```python
# The application should work transparently with cluster mode
# Queue operations, caching, etc. should work as before
```

---

## Production Migration

### Pre-Migration Checklist

- [ ] Schedule maintenance window (estimated 15-30 minutes)
- [ ] Backup any critical data if needed (though main Redis is a cache with no persistence)
- [ ] Notify users of maintenance window
- [ ] Ensure all services can tolerate brief Redis downtime
- [ ] Review and update monitoring alerts

### Migration Steps

#### 1. Stop All Services

```bash
# Stop all services that use Redis
docker compose stop api uploader downloader unpinner substrate
```

#### 2. Backup Current Redis State (Optional)

```bash
# If you want to preserve any data during migration
docker compose exec redis redis-cli SAVE
docker cp redis:/data/dump.rdb ./redis-backup-$(date +%Y%m%d).rdb
```

#### 3. Deploy Redis Cluster

```bash
# Stop standalone Redis
docker compose stop redis

# Start Redis Cluster
docker compose -f docker-compose.yml \
  -f docker-compose.prod.yml \
  -f docker-compose.prod.cluster.yml up -d

# Wait for cluster initialization
docker logs redis-cluster-init -f
```

#### 4. Verify Cluster Health

```bash
# Check cluster status
docker exec redis-cluster-1 redis-cli cluster info
docker exec redis-cluster-1 redis-cli cluster nodes

# Verify slot distribution
docker exec redis-cluster-1 redis-cli cluster slots

# Expected output:
# cluster_state:ok
# cluster_slots_assigned:16384
# cluster_known_nodes:3
# cluster_size:3
```

#### 5. Update Application Configuration

Update `.env` or environment variables:
```bash
REDIS_CLUSTER_ENABLED=true
REDIS_URL=172.20.0.20:6379,172.20.0.21:6379,172.20.0.22:6379
```

#### 6. Restart Services

```bash
# Start services one by one
docker compose -f docker-compose.yml \
  -f docker-compose.prod.yml \
  -f docker-compose.prod.cluster.yml up -d api

# Verify API is healthy
curl http://localhost:8000/health

# Start workers
docker compose -f docker-compose.yml \
  -f docker-compose.prod.yml \
  -f docker-compose.prod.cluster.yml up -d uploader downloader unpinner substrate
```

#### 7. Monitor and Validate

```bash
# Check application logs
docker compose logs -f api | grep -i redis

# Monitor cluster metrics
docker exec redis-cluster-1 redis-cli --cluster check 172.20.0.20:6379

# Verify operations work
docker exec redis-cluster-1 redis-cli cluster keyslot test_key
docker exec redis-cluster-1 redis-cli set test_key "hello"
docker exec redis-cluster-1 redis-cli get test_key
```

### Rollback Plan (If Needed)

```bash
# Stop cluster
docker compose -f docker-compose.prod.cluster.yml down

# Restore standalone Redis
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d redis

# Update environment
REDIS_CLUSTER_ENABLED=false
REDIS_URL=redis://redis:6379/0

# Restart services
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

---

## Kubernetes Deployment

### Prerequisites

- Kubernetes cluster with at least 180GB available RAM across nodes
- Helm 3.x installed
- kubectl configured to access your cluster

### Deployment Steps

#### 1. Install Redis Cluster with Helm

```bash
# Navigate to Helm chart directory
cd k8s/redis-cluster

# Review values
cat values.yaml

# Install the chart
helm install redis-cluster . \
  --namespace hippius \
  --create-namespace

# Monitor deployment
kubectl get pods -n hippius -w
kubectl logs -n hippius job/redis-cluster-init -f
```

#### 2. Verify Cluster Health

```bash
# Check StatefulSet
kubectl get statefulsets -n hippius

# Check pods
kubectl get pods -n hippius -l app.kubernetes.io/name=redis-cluster

# Verify cluster status
kubectl exec -n hippius redis-cluster-0 -- redis-cli cluster info
kubectl exec -n hippius redis-cluster-0 -- redis-cli cluster nodes
```

#### 3. Get Cluster Connection Info

```bash
# Get service DNS names
kubectl get svc -n hippius

# Connection string format for applications:
# redis-cluster-0.redis-cluster-headless.hippius.svc.cluster.local:6379
# redis-cluster-1.redis-cluster-headless.hippius.svc.cluster.local:6379
# redis-cluster-2.redis-cluster-headless.hippius.svc.cluster.local:6379
```

#### 4. Update Application Configuration

```yaml
# In your application deployment YAML
env:
- name: REDIS_CLUSTER_ENABLED
  value: "true"
- name: REDIS_URL
  value: "redis-cluster-0.redis-cluster-headless.hippius.svc.cluster.local:6379,redis-cluster-1.redis-cluster-headless.hippius.svc.cluster.local:6379,redis-cluster-2.redis-cluster-headless.hippius.svc.cluster.local:6379"
```

---

## Scaling Redis Cluster on K8s

### Horizontal Scaling (Adding Nodes)

#### Step 1: Update Replica Count

```bash
# Edit values.yaml
vim k8s/redis-cluster/values.yaml

# Change replicaCount
replicaCount: 5  # was 3

# Upgrade release
helm upgrade redis-cluster k8s/redis-cluster \
  --namespace hippius \
  --reuse-values \
  --set replicaCount=5
```

#### Step 2: Wait for New Pods

```bash
# Monitor new pods coming up
kubectl get pods -n hippius -w

# Verify new pods are running
kubectl exec -n hippius redis-cluster-3 -- redis-cli ping
kubectl exec -n hippius redis-cluster-4 -- redis-cli ping
```

#### Step 3: Add Nodes to Cluster

```bash
# Get existing node addresses
EXISTING_NODE="redis-cluster-0.redis-cluster-headless.hippius.svc.cluster.local:6379"

# Add new nodes
kubectl exec -n hippius redis-cluster-0 -- \
  redis-cli --cluster add-node \
  redis-cluster-3.redis-cluster-headless.hippius.svc.cluster.local:6379 \
  $EXISTING_NODE

kubectl exec -n hippius redis-cluster-0 -- \
  redis-cli --cluster add-node \
  redis-cluster-4.redis-cluster-headless.hippius.svc.cluster.local:6379 \
  $EXISTING_NODE
```

#### Step 4: Rebalance Slot Distribution

```bash
# Rebalance slots across all nodes
kubectl exec -n hippius redis-cluster-0 -- \
  redis-cli --cluster rebalance \
  redis-cluster-0.redis-cluster-headless.hippius.svc.cluster.local:6379 \
  --cluster-use-empty-masters

# Verify distribution
kubectl exec -n hippius redis-cluster-0 -- \
  redis-cli --cluster check \
  redis-cluster-0.redis-cluster-headless.hippius.svc.cluster.local:6379
```

#### Step 5: Update Application Configuration

```bash
# Update REDIS_URL environment variable to include new nodes
# Deploy updated configuration
kubectl rollout restart deployment/api -n hippius
```

### Vertical Scaling (Increasing Memory)

```bash
# Update memory limits in values.yaml
resources:
  limits:
    memory: 80Gi  # was 65Gi
  requests:
    memory: 75Gi  # was 60Gi

redis:
  maxMemory: "75gb"  # was 60gb

# Upgrade release
helm upgrade redis-cluster k8s/redis-cluster \
  --namespace hippius \
  --reuse-values \
  --values k8s/redis-cluster/values.yaml

# Pods will be recreated with new memory limits
```

### Scaling Best Practices

1. **Monitor Before Scaling**: Check current memory/CPU usage
   ```bash
   kubectl top pods -n hippius -l app.kubernetes.io/name=redis-cluster
   ```

2. **Scale Gradually**: Add 1-2 nodes at a time, verify health before adding more

3. **Rebalance After Scaling**: Always rebalance slots after adding nodes

4. **Update Application**: Ensure application knows about new nodes

5. **Monitor During Resharding**: Resharding impacts performance temporarily

---

## Troubleshooting

### Cluster Won't Form

**Symptoms**: Cluster shows as "cluster_state:fail" or nodes can't see each other

**Solutions**:
```bash
# 1. Check network connectivity between nodes
docker exec redis-cluster-1 redis-cli ping
docker exec redis-cluster-2 redis-cli ping

# 2. Verify cluster bus port (16379) is accessible
docker exec redis-cluster-1 nc -zv redis-cluster-2 16379

# 3. Check cluster meet commands
docker exec redis-cluster-1 redis-cli cluster meet 172.20.0.21 6379
docker exec redis-cluster-1 redis-cli cluster meet 172.20.0.22 6379

# 4. Reset cluster if needed (DESTRUCTIVE)
docker exec redis-cluster-1 redis-cli cluster reset hard
docker exec redis-cluster-2 redis-cli cluster reset hard
docker exec redis-cluster-3 redis-cli cluster reset hard
# Then re-run cluster create
```

### Application Can't Connect

**Symptoms**: Application logs show connection errors

**Solutions**:
```bash
# 1. Verify REDIS_CLUSTER_ENABLED is set
echo $REDIS_CLUSTER_ENABLED

# 2. Check REDIS_URL format (comma-separated)
echo $REDIS_URL

# 3. Test connectivity from application container
docker exec api redis-cli -h 172.20.0.20 -p 6379 ping

# 4. Check application logs for specific errors
docker compose logs api | grep -i redis
```

### MOVED/ASK Errors

**Symptoms**: Logs show "MOVED" or "ASK" errors

**Explanation**: This is normal during resharding, redis-py handles automatically

**Solutions**:
- If persistent, check cluster topology: `redis-cli cluster nodes`
- Verify all nodes are reachable by client
- Ensure client library is up-to-date

### Node Fails

**Symptoms**: One node goes down

**Impact**:
- With 3 master nodes (no replicas): **Data loss** for slots on failed node
- Operations for affected slots will fail

**Solutions**:
```bash
# 1. Check node status
kubectl get pods -n hippius
docker ps | grep redis-cluster

# 2. Review logs
kubectl logs redis-cluster-1 -n hippius
docker logs redis-cluster-1

# 3. Restart node
kubectl delete pod redis-cluster-1 -n hippius  # K8s will recreate
docker restart redis-cluster-1

# 4. After restart, node will rejoin automatically
# Verify with: redis-cli cluster nodes
```

### Performance Issues

**Symptoms**: Slow responses, high latency

**Solutions**:
```bash
# 1. Check memory usage
docker exec redis-cluster-1 redis-cli info memory

# 2. Monitor slow queries
docker exec redis-cluster-1 redis-cli slowlog get 10

# 3. Check for slot migrations
docker exec redis-cluster-1 redis-cli cluster info | grep migrating

# 4. Review client-side latency
docker exec api redis-cli --latency -h 172.20.0.20

# 5. Consider adding more nodes to distribute load
```

---

## Important Caveats

### Multi-Key Operations

**Limitation**: Operations on multiple keys only work if keys are in the same slot

**Problem**:
```python
# This might FAIL if key1 and key2 are on different nodes
redis.mget(['key1', 'key2'])
redis.delete('key1', 'key2')
```

**Solution**: Use hash tags to force keys into same slot
```python
# These will always be on the same node
redis.set('{user:123}:profile', data)
redis.set('{user:123}:settings', settings)
redis.mget(['{user:123}:profile', '{user:123}:settings'])  # Works!
```

**How Hash Tags Work**: Only content between `{}` is hashed
- `{user:123}:profile` → hash("user:123") → slot
- `{user:123}:settings` → hash("user:123") → same slot!

### No Multi-Database Support

**Limitation**: Cluster mode only supports database 0

**Impact**: If you're using `SELECT 1` or multiple databases, you'll need to refactor

**Solution**: Use key prefixes instead
```python
# Instead of: SELECT 1; SET foo bar
# Use: SET db1:foo bar
```

### NATted Environments

**Limitation**: Redis Cluster doesn't work well behind NAT

**Impact**: Docker without host networking can be problematic

**Solution**:
- Use static IPs in Docker (we do this in docker-compose files)
- Or use host networking mode
- In K8s, this is not an issue

### No Automatic Failover Without Replicas

**Limitation**: Our setup has 0 replicas (master-only)

**Impact**: If a node fails, data on that node is lost

**Trade-offs**:
- ✅ Less memory usage (no replica overhead)
- ✅ Simpler setup
- ❌ No automatic failover
- ❌ Data loss on node failure

**Mitigation**:
- Monitor node health closely
- Quick restart on node failure
- Consider adding replicas later if HA is critical

### Client Library Requirements

**Requirement**: Client must support cluster mode

**Status**: ✅ redis-py supports cluster mode with `RedisCluster`

**Note**: Our code automatically switches based on `REDIS_CLUSTER_ENABLED` flag

### Connection Pool Behavior

**Different from Standalone**: Cluster maintains connections to all nodes

**Impact**: More TCP connections overall

**Configuration**: redis-py manages this automatically

---

## Monitoring

### Key Metrics to Monitor

```bash
# Cluster health
redis-cli cluster info

# Node health
redis-cli info stats
redis-cli info memory
redis-cli info cpu

# Slot distribution
redis-cli cluster slots

# Key distribution (rough)
redis-cli --cluster check 172.20.0.20:6379

# Network
redis-cli info clients
redis-cli client list
```

### Prometheus Metrics (If Using Redis Exporter)

- `redis_cluster_state`
- `redis_cluster_slots_assigned`
- `redis_cluster_slots_ok`
- `redis_cluster_slots_fail`
- `redis_cluster_known_nodes`
- `redis_used_memory_bytes`
- `redis_connected_clients`

---

## Additional Resources

- **Redis Cluster Specification**: https://redis.io/docs/reference/cluster-spec/
- **Redis Cluster Tutorial**: https://redis.io/docs/management/scaling/
- **redis-py Cluster Docs**: https://github.com/redis/redis-py
- **Troubleshooting Guide**: https://redis.io/docs/management/troubleshooting/

---

## Summary

You now have:
- ✅ Local development cluster setup (3 nodes, 2GB each)
- ✅ Production Docker Compose config (3 nodes, 60GB each)
- ✅ Kubernetes Helm chart for cluster deployment
- ✅ Python code that supports both standalone and cluster modes
- ✅ Comprehensive migration runbook

**Next Steps**:
1. Test locally with `docker-compose.cluster.yml`
2. Verify application works correctly with cluster mode
3. Schedule production maintenance window
4. Follow production migration steps
5. Deploy to K8s when ready
