-- migrate:up

-- Correct a badly wrong planner statistic on objects.bucket_id.
--
-- THE PROBLEM. ANALYZE samples rows, so it estimates n_distinct from a sample -- and it is famously
-- bad at that when the distribution is heavily skewed. Ours is: roughly four fifths of all `objects`
-- rows live in a SINGLE bucket, one holding a filesystem-style workload of millions of small
-- objects. Against tens of thousands of real buckets, ANALYZE lands on n_distinct = 219.
--
-- THE CONSEQUENCE. Reached through a JOIN the planner divides total rows by 219, estimating
-- hundreds of thousands of objects for ANY bucket -- three orders of magnitude high for a typical
-- account. Two things fall out of the inflated cost:
--   1. It crosses jit_above_cost (100000), so PG JIT-compiles a query that is index-probe bound.
--      Measured about a third of the runtime of a small account's count -- pure waste.
--   2. It picks a parallel scan over every bucket with a filter (~47,782 buffers) instead of the
--      idx_buckets_main_account lookup (~5 buffers) it uses when the account id arrives as a
--      literal.
-- Note the estimate is ACCURATE for a literal bucket_id. Only the join-derived form is broken,
-- which is why usage_service.py also splits the bucket lookup out.
--
-- THE FIX. A NEGATIVE n_distinct is interpreted as a FRACTION OF THE TABLE rather than an absolute
-- count, so it tracks growth instead of going stale rather than needing revisiting as the table
-- grows. -0.0003 lands slightly ABOVE the real distinct count, which is the deliberate direction:
-- over-estimating distinctness under-estimates rows-per-bucket, favouring the index paths this
-- exists to restore. Postgres uses this override in place of whatever ANALYZE computes; ANALYZE may
-- still run freely and will not overwrite it. Verified in a rolled-back transaction that SET
-- produces attoptions={n_distinct=-0.0003} and that the down migration's RESET clears it.
--
-- Re-derive the fraction against `SELECT reltuples FROM pg_class WHERE relname='objects'` and the
-- live `buckets` count before changing it; the figures are deliberately not pinned here.
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
