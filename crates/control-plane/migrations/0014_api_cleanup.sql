ALTER TABLE workload_class_versions
    DROP COLUMN template_generation,
    DROP COLUMN default_values;

-- Sealing-key rotation finds the certificates each key still seals.
CREATE INDEX certificates_active_seal_key ON certificates (seal_key_id) WHERE state = 'active';
