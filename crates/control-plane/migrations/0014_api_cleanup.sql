ALTER TABLE workload_class_versions
    DROP COLUMN template_generation,
    DROP COLUMN default_values;

-- Sealing-key rotation finds the certificates each key still seals.
CREATE INDEX certificates_active_seal_key ON certificates (seal_key_id) WHERE state = 'active';

-- A create compares against the stored resource, so no request record is kept.
DROP TRIGGER instance_idempotency_tombstone ON instances;
DROP TRIGGER route_idempotency_tombstone ON route_bindings;
DROP FUNCTION mark_idempotency_resource_deleted();
DROP TABLE idempotency_records;
