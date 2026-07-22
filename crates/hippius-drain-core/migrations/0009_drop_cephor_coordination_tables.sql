-- Goal 2: the loss-tolerant coordination state — leader lease, node heartbeats, and
-- per-node allocations — moved off Postgres onto the redis-queues instance (TTL-keyed,
-- epoch-fenced via Lua; see hippius_drain_core::Coordinator). These three tables churned
-- the WAL on every ~2s leader tick (measured 6995x/3659x/2023x autovacuum on staging) and
-- created a circular recovery dependency when Ceph — and thus PG's WAL fsync — degrades.
-- Their state is ephemeral, so dropping them loses nothing: a fresh leader re-elects and
-- the fleet re-heartbeats within a few ticks. Postgres keeps only the DURABLE drain state
-- (replication status / claims, the landed-part log, GC claims).
DROP TABLE IF EXISTS cephor_allocation;
DROP TABLE IF EXISTS cephor_node_state;
DROP TABLE IF EXISTS cephor_leader_lease;
