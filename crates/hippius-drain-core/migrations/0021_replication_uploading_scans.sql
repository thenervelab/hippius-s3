-- no-transaction
-- The full-table scan 0020 deliberately left out of its transaction (see its header): with
-- the CHECK already in place NOT VALID, validating it here takes only SHARE UPDATE EXCLUSIVE
-- (the drain's UPDATEs proceed) instead of holding 0020's ACCESS EXCLUSIVE across ~11M rows.
-- Idempotent: re-validating an already-valid constraint is a no-op.
--
-- Not SET LOCAL: there is no transaction for it to be local to. A session-level lock_timeout
-- bounds the lock acquisition; the scan itself blocks nothing the drain does.
SET lock_timeout = '5s';

-- Every existing row already satisfies the widened set, so this cannot fail; it only lets the
-- planner trust the constraint.
ALTER TABLE cephor_replication_status VALIDATE CONSTRAINT cephor_replication_status_status_check;
