-- migrate:up

-- Correct a badly wrong planner statistic on objects.bucket_id.
--
-- THE PROBLEM. ANALYZE samples rows, so it estimates n_distinct from a sample -- and it is famously
-- bad at that when the distribution is heavily skewed. Ours is: 81% of all `objects` rows live in a
-- SINGLE bucket (a JuiceFS bucket with 7.83M live objects; pg_stats shows most_common_freqs[1] =
-- 0.81064). ANALYZE lands on n_distinct = 219 while `buckets` actually holds ~46,463 rows.
--
-- THE CONSEQUENCE. Reached through a JOIN, the planner divides: 168.7M rows / 219 distinct =
-- ~763k objects estimated for ANY bucket. For a small account whose buckets hold ~436 objects that
-- is a 1750x overestimate. Two things fall out of the inflated cost:
--   1. It crosses jit_above_cost (100000), so PG JIT-compiles a query that is index-probe bound.
--      Measured 107ms of a 326ms count for a 1,308-object account -- pure waste.
--   2. It picks a parallel scan over all ~46k buckets with a filter (~47,782 buffers) instead of
--      the idx_buckets_main_account lookup (~5 buffers) it uses when the account id arrives as a
--      literal.
-- Note the estimate is ACCURATE for a literal bucket_id (9.65M est vs 7.83M actual). Only the
-- join-derived form is broken, which is why usage_service.py also splits the bucket lookup out.
--
-- THE FIX. A NEGATIVE n_distinct is interpreted as a FRACTION OF THE TABLE rather than an absolute
-- count, so it tracks growth instead of going stale: -0.0003 x 168.7M ~= 50k distinct buckets,
-- against ~46.5k real. Postgres uses this override in place of whatever ANALYZE computes; ANALYZE
-- may still run freely and will not overwrite it.
--
-- BLAST RADIUS, stated plainly: this changes planning for EVERY query joining on objects.bucket_id,
-- not just the storage count -- ListObjects and the console bucket listing included. It should
-- improve them for the same reason (they are per-bucket lookups that were being costed as if every
-- bucket held 763k objects), but it is a global change to the hottest table in the schema and is
-- worth watching after deploy. It is metadata only: no rewrite, no lock beyond a brief
-- ShareUpdateExclusive, and instantly reversible by the down migration.
--
-- ANALYZE is deliberately NOT run here. It is a sampling pass over a 165M-row table and dbmate runs
-- migrations inline on deploy; the override takes effect for plans generated after the next ANALYZE,
-- and autovacuum will get there on its own. Run `ANALYZE objects;` by hand if you want it sooner.
ALTER TABLE objects ALTER COLUMN bucket_id SET (n_distinct = -0.0003);

-- migrate:down

ALTER TABLE objects ALTER COLUMN bucket_id RESET (n_distinct);
