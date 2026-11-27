-- A workload is created with zero replicas and activated by a conditional
-- patch, so a create that arrives late starts no Pod. Every other mutation
-- carries a UID and resourceVersion precondition that the API server checks.
-- No durable record of a dispatched mutation is needed.
DROP TABLE materialization_effects;

ALTER TABLE materializations DROP CONSTRAINT materializations_failure_kind_check;
UPDATE materializations SET failure_kind = 'transient' WHERE failure_kind = 'uncertain';
-- Cleanup retries until it succeeds; only an ownership conflict parks it.
UPDATE materializations SET failure_kind = 'transient'
    WHERE state = 'deleting' AND failure_kind = 'deadline';
ALTER TABLE materializations ADD CONSTRAINT materializations_failure_kind_check
    CHECK (failure_kind IN ('transient', 'permanent', 'deadline'));

-- Wakes and cleanup claim independently, each in schedule order.
DROP INDEX materializations_reconcile_scan_idx;
CREATE INDEX materializations_claim_idx
    ON materializations (state, next_attempt_at_unix_millis, materialization_id)
    WHERE state IN ('pending', 'deleting');

-- The ready Pod's own address, recorded beside the Service address that
-- routes use.
ALTER TABLE materializations ADD COLUMN pod_backend_uri text;
