-- The control-plane schema. Every timestamp is milliseconds since the Unix
-- epoch on the database clock, the one clock each comparison reads.

CREATE FUNCTION lifecycle_operation_timeout_millis() RETURNS bigint LANGUAGE sql STABLE AS $$
    SELECT COALESCE(NULLIF(current_setting('sleepypods.operation_timeout_ms', true), '')::bigint, 600000);
$$;

CREATE TABLE workload_class_versions (
    class_id text NOT NULL,
    version bigint NOT NULL CHECK (version >= 0),
    manifest_template jsonb NOT NULL,
    value_schema jsonb NOT NULL,
    sleep_policy jsonb NOT NULL,
    exclusivity_keys jsonb NOT NULL,
    PRIMARY KEY (class_id, version)
);

CREATE TABLE instances (
    instance_id text PRIMARY KEY,
    workload_class_id text NOT NULL,
    workload_class_version bigint NOT NULL CHECK (workload_class_version >= 0),
    values jsonb NOT NULL,
    state text NOT NULL
        CHECK (state IN ('cold', 'waking', 'running', 'draining', 'failed', 'deleting')),
    generation bigint NOT NULL CHECK (generation >= 0),
    FOREIGN KEY (workload_class_id, workload_class_version)
        REFERENCES workload_class_versions (class_id, version)
);
CREATE INDEX instances_deleting_work_idx ON instances (instance_id) WHERE state = 'deleting';

-- The last generation each instance ID used. It outlives the instance, so a
-- re-created ID starts above every generation a stale caller could hold.
CREATE TABLE instance_generation_watermarks (
    instance_id text PRIMARY KEY,
    last_generation bigint NOT NULL CHECK (last_generation >= 0)
);
CREATE FUNCTION retain_instance_generation() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE previous_generation bigint;
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO instance_generation_watermarks (instance_id, last_generation)
            VALUES (NEW.instance_id, NEW.generation)
            ON CONFLICT (instance_id) DO NOTHING;
        IF FOUND THEN RETURN NEW; END IF;
        SELECT last_generation INTO previous_generation FROM instance_generation_watermarks
            WHERE instance_id = NEW.instance_id FOR UPDATE;
        NEW.generation := GREATEST(NEW.generation, previous_generation + 1);
    END IF;
    INSERT INTO instance_generation_watermarks (instance_id, last_generation)
        VALUES (NEW.instance_id, NEW.generation)
        ON CONFLICT (instance_id) DO UPDATE
        SET last_generation = GREATEST(instance_generation_watermarks.last_generation, EXCLUDED.last_generation);
    RETURN NEW;
END $$;
CREATE TRIGGER retain_instance_generation BEFORE INSERT OR UPDATE OF generation ON instances
    FOR EACH ROW EXECUTE FUNCTION retain_instance_generation();

CREATE TABLE route_bindings (
    route_binding_id text PRIMARY KEY,
    instance_id text NOT NULL REFERENCES instances (instance_id) ON DELETE CASCADE,
    identity_key text NOT NULL UNIQUE,
    identity_kind text NOT NULL CHECK (identity_kind IN ('http', 'sni')),
    host_kind text NOT NULL CHECK (host_kind IN ('exact', 'wildcard_suffix')),
    host text NOT NULL,
    path_prefix text,
    CHECK (identity_kind = 'http' OR path_prefix IS NULL)
);
CREATE INDEX route_bindings_instance_id_idx ON route_bindings (instance_id);
CREATE INDEX route_bindings_identity_lookup_idx ON route_bindings (identity_kind, host_kind, host);

CREATE TABLE materializations (
    materialization_id text PRIMARY KEY,
    instance_id text NOT NULL REFERENCES instances (instance_id) ON DELETE CASCADE,
    instance_generation bigint NOT NULL CHECK (instance_generation >= 0),
    -- The incarnation the projection renders; fixed from Pending to Deleting.
    projection_generation bigint NOT NULL CHECK (projection_generation >= 0),
    cluster_id text NOT NULL,
    namespace text NOT NULL,
    state text NOT NULL CHECK (state IN ('pending', 'ready', 'deleting', 'deleted')),
    backend_uri text,
    pod_backend_uri text,
    backend_generation bigint NOT NULL CHECK (backend_generation >= 0),
    rendered_objects jsonb NOT NULL,
    exclusivity_keys jsonb NOT NULL,
    reconcile_owner text,
    reconcile_lease_expires_at_unix_millis bigint,
    reconcile_attempt bigint NOT NULL DEFAULT 0 CHECK (reconcile_attempt >= 0),
    -- Changes only with the state, and drives backlog age.
    state_entered_at_unix_millis bigint NOT NULL DEFAULT (extract(epoch from clock_timestamp()) * 1000)::bigint,
    -- Orders due work; a failure moves it past a backoff.
    next_attempt_at_unix_millis bigint NOT NULL DEFAULT (extract(epoch from clock_timestamp()) * 1000)::bigint,
    -- The sleep grace deadline, apart from queue position.
    drain_not_before_unix_millis bigint NOT NULL DEFAULT 0,
    operation_deadline_unix_millis bigint NOT NULL
        DEFAULT ((extract(epoch from clock_timestamp()) * 1000)::bigint + lifecycle_operation_timeout_millis()),
    failure_count integer NOT NULL DEFAULT 0 CHECK (failure_count >= 0),
    failure_kind text CHECK (failure_kind IN ('transient', 'permanent', 'deadline')),
    failure_message text NOT NULL DEFAULT '' CHECK (octet_length(failure_message) <= 2048),
    failure_requires_cleanup boolean NOT NULL DEFAULT false,
    wake_failure_message text NOT NULL DEFAULT '' CHECK (octet_length(wake_failure_message) <= 2048),
    UNIQUE (instance_id, cluster_id, namespace),
    CHECK ((reconcile_owner IS NULL) = (reconcile_lease_expires_at_unix_millis IS NULL))
);
CREATE INDEX materializations_claim_idx
    ON materializations (state, next_attempt_at_unix_millis, materialization_id)
    WHERE state IN ('pending', 'deleting');

CREATE FUNCTION reset_materialization_schedule() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (NEW.state IS DISTINCT FROM OLD.state AND NEW.state IN ('pending', 'deleting'))
        OR NEW.instance_generation IS DISTINCT FROM OLD.instance_generation THEN
        IF NEW.state = 'pending' THEN NEW.wake_failure_message = ''; END IF;
        NEW.failure_requires_cleanup = false;
        NEW.failure_count = 0;
        NEW.failure_kind = NULL;
        NEW.failure_message = '';
        NEW.operation_deadline_unix_millis = GREATEST((extract(epoch from clock_timestamp()) * 1000)::bigint, NEW.drain_not_before_unix_millis) + lifecycle_operation_timeout_millis();
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER materialization_schedule BEFORE UPDATE ON materializations
    FOR EACH ROW EXECUTE FUNCTION reset_materialization_schedule();

CREATE FUNCTION track_materialization_state_age() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state IS DISTINCT FROM OLD.state THEN
        NEW.state_entered_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint;
        IF NEW.next_attempt_at_unix_millis = OLD.next_attempt_at_unix_millis THEN
            NEW.next_attempt_at_unix_millis = NEW.state_entered_at_unix_millis;
        END IF;
        IF NEW.state <> 'deleting' THEN NEW.drain_not_before_unix_millis = 0; END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER materialization_state_age BEFORE UPDATE ON materializations
    FOR EACH ROW EXECUTE FUNCTION track_materialization_state_age();

-- A wake accepted while its instance drains, promoted when the drain finishes.
CREATE TABLE deferred_wake_intents (
    instance_id text PRIMARY KEY REFERENCES instances (instance_id) ON DELETE CASCADE,
    instance_generation bigint NOT NULL,
    projection_generation bigint NOT NULL,
    cluster_id text NOT NULL,
    namespace text NOT NULL,
    backend_generation bigint NOT NULL,
    rendered_objects jsonb NOT NULL,
    exclusivity_keys jsonb NOT NULL
);

-- The Kubernetes objects and exclusivity keys each live materialization holds.
-- A trigger keeps them in step with every state, ref or key change, including
-- force operations and cascading deletes.
CREATE TABLE materialization_object_reservations (
    cluster_id text NOT NULL,
    api_group text NOT NULL,
    kind text NOT NULL,
    namespace text NOT NULL,
    name text NOT NULL,
    materialization_id text NOT NULL REFERENCES materializations ON DELETE CASCADE ON UPDATE CASCADE,
    PRIMARY KEY (cluster_id, api_group, kind, namespace, name)
);
CREATE INDEX materialization_object_reservations_owner_idx
    ON materialization_object_reservations (materialization_id);
CREATE TABLE materialization_key_reservations (
    cluster_id text NOT NULL,
    namespace text NOT NULL,
    key_name text NOT NULL,
    key_value text NOT NULL,
    materialization_id text NOT NULL REFERENCES materializations ON DELETE CASCADE ON UPDATE CASCADE,
    PRIMARY KEY (cluster_id, namespace, key_name, key_value)
);
CREATE INDEX materialization_key_reservations_owner_idx
    ON materialization_key_reservations (materialization_id);

CREATE FUNCTION sync_materialization_reservations() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    ref record;
    owner_id text;
    owner_generation bigint;
BEGIN
    -- Lease renewals and scheduling do not rewrite the reservations.
    IF TG_OP = 'UPDATE' AND NEW.state = OLD.state
        AND NEW.rendered_objects = OLD.rendered_objects
        AND NEW.exclusivity_keys = OLD.exclusivity_keys
        AND NEW.cluster_id = OLD.cluster_id AND NEW.namespace = OLD.namespace THEN
        RETURN NEW;
    END IF;
    DELETE FROM materialization_object_reservations WHERE materialization_id = NEW.materialization_id;
    DELETE FROM materialization_key_reservations WHERE materialization_id = NEW.materialization_id;
    IF NEW.state = 'deleted' THEN RETURN NEW; END IF;
    -- Every acquirer takes references in one order, so two projections that
    -- share several references cannot deadlock. API versions of one group,
    -- and every namespace of a PersistentVolume, share a reservation.
    FOR ref IN
        SELECT DISTINCT
            CASE WHEN strpos(o->>'api_version', '/') > 0 THEN split_part(o->>'api_version', '/', 1) ELSE '' END AS api_group,
            o->>'kind' AS kind,
            CASE WHEN o->>'kind' = 'PersistentVolume' THEN '' ELSE o->>'namespace' END AS namespace,
            o->>'name' AS name
        FROM jsonb_array_elements(NEW.rendered_objects) o
        ORDER BY 1, 2, 3, 4
    LOOP
        INSERT INTO materialization_object_reservations
        VALUES (NEW.cluster_id, ref.api_group, ref.kind, ref.namespace, ref.name, NEW.materialization_id)
        ON CONFLICT DO NOTHING;
        IF NOT FOUND THEN
            SELECT m.instance_id INTO owner_id
            FROM materialization_object_reservations r JOIN materializations m USING (materialization_id)
            WHERE r.cluster_id = NEW.cluster_id AND r.api_group = ref.api_group
                AND r.kind = ref.kind AND r.namespace = ref.namespace AND r.name = ref.name;
            RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'rendered_object_ref_collision',
                DETAIL = format('rendered Kubernetes object ref collision: %s %s %s/%s is already owned by active materialization for instance %s', COALESCE((SELECT o->>'api_version' FROM jsonb_array_elements(NEW.rendered_objects) o WHERE o->>'kind' = ref.kind AND o->>'name' = ref.name LIMIT 1), ref.api_group), ref.kind, ref.namespace, ref.name, owner_id);
        END IF;
    END LOOP;
    FOR ref IN
        SELECT DISTINCT k->>'name' AS name, k->>'value' AS value
        FROM jsonb_array_elements(NEW.exclusivity_keys) k ORDER BY 1, 2
    LOOP
        INSERT INTO materialization_key_reservations
        VALUES (NEW.cluster_id, NEW.namespace, ref.name, ref.value, NEW.materialization_id)
        ON CONFLICT DO NOTHING;
        IF NOT FOUND THEN
            SELECT m.instance_id, m.instance_generation INTO owner_id, owner_generation
            FROM materialization_key_reservations r JOIN materializations m USING (materialization_id)
            WHERE r.cluster_id = NEW.cluster_id AND r.namespace = NEW.namespace
                AND r.key_name = ref.name AND r.key_value = ref.value;
            RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'exclusivity_conflict',
                DETAIL = jsonb_build_object('cluster_id', NEW.cluster_id, 'namespace', NEW.namespace,
                    'key_name', ref.name, 'owner_instance_id', owner_id, 'owner_generation', owner_generation)::text;
        END IF;
    END LOOP;
    RETURN NEW;
END $$;
CREATE TRIGGER materialization_reservations
    AFTER INSERT OR UPDATE ON materializations
    FOR EACH ROW EXECUTE FUNCTION sync_materialization_reservations();

CREATE TABLE materialization_operator_audit_events (
    audit_id bigserial PRIMARY KEY,
    operation text NOT NULL,
    materialization_id text,
    cluster_id text,
    namespace text,
    key_name text,
    operator text NOT NULL,
    reason text NOT NULL,
    created_at_unix_millis bigint NOT NULL DEFAULT (extract(epoch from clock_timestamp()) * 1000)::bigint
);

CREATE TABLE http01_challenges (
    host text NOT NULL,
    token text NOT NULL,
    key_authorization text NOT NULL,
    expires_at_unix_millis bigint NOT NULL,
    PRIMARY KEY (host, token)
);
CREATE INDEX http01_challenges_expires_at_idx ON http01_challenges (expires_at_unix_millis);

-- A bounded transactional outbox of changes to route rule sets, read by
-- revision. Every process reads it independently; no reader removes another's
-- records. The revision row's lock orders allocated ranges with commit.
CREATE TABLE route_change_revision (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    revision bigint NOT NULL CHECK (revision >= 0)
);
INSERT INTO route_change_revision VALUES (true, 0);
CREATE TABLE route_change_outbox (
    revision bigint PRIMARY KEY,
    payload jsonb NOT NULL,
    created_at_unix_millis bigint NOT NULL DEFAULT (extract(epoch from clock_timestamp()) * 1000)::bigint
);
CREATE FUNCTION record_route_changes() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    next_revision bigint;
    event_count bigint;
    source_query text;
    payload_query text;
BEGIN
    source_query = CASE WHEN TG_OP = 'DELETE' THEN 'SELECT to_jsonb(o) AS value FROM old_rows o' ELSE 'SELECT to_jsonb(n) AS value FROM new_rows n' END;
    IF TG_OP = 'UPDATE' AND TG_TABLE_NAME = 'instances' THEN
        source_query = 'SELECT to_jsonb(n) AS value FROM new_rows n JOIN old_rows o USING(instance_id) WHERE (n.state, n.generation) IS DISTINCT FROM (o.state, o.generation)';
    ELSIF TG_OP = 'UPDATE' AND TG_TABLE_NAME = 'materializations' THEN
        source_query = 'SELECT to_jsonb(n) AS value FROM new_rows n JOIN old_rows o USING(materialization_id) WHERE (n.state, n.backend_uri, n.backend_generation, n.instance_generation, n.failure_kind) IS DISTINCT FROM (o.state, o.backend_uri, o.backend_generation, o.instance_generation, o.failure_kind)';
    END IF;
    IF TG_TABLE_NAME = 'route_bindings' THEN
        payload_query = format('SELECT jsonb_build_object(''kind'', ''route'', ''removed'', %L::boolean,
            ''route_binding_id'', value->''route_binding_id'', ''instance_id'', value->''instance_id'',
            ''identity_kind'', value->''identity_kind'', ''host_kind'', value->''host_kind'',
            ''host'', value->''host'', ''path_prefix'', value->''path_prefix'') AS payload FROM (%s) changed', TG_OP = 'DELETE', source_query);
    ELSE
        payload_query = format('SELECT DISTINCT jsonb_build_object(''kind'', ''instance'', ''instance_id'', value->''instance_id'') AS payload FROM (%s) changed', source_query);
    END IF;
    EXECUTE 'SELECT count(*) FROM (' || payload_query || ') p' INTO event_count;
    IF event_count = 0 THEN RETURN NULL; END IF;
    -- One update per statement avoids quadratic same-row version chains for
    -- bulk writes. The held row lock orders every allocated range with commit.
    UPDATE route_change_revision SET revision = revision + event_count WHERE singleton RETURNING revision INTO next_revision;
    EXECUTE 'INSERT INTO route_change_outbox(revision, payload) SELECT $1 + row_number() OVER (), payload FROM (' || payload_query || ') p' USING next_revision - event_count;
    DELETE FROM route_change_outbox WHERE revision <= next_revision - 100000;
    -- Delivered at commit; wakes each replica's dispatcher to read the outbox.
    -- The channel is scoped to this schema, so stores sharing a database wake
    -- only their own dispatchers.
    PERFORM pg_notify('sleepypods_routes_' || md5(TG_TABLE_SCHEMA), '');
    RETURN NULL;
END $$;
CREATE TRIGGER route_bindings_route_change_insert AFTER INSERT ON route_bindings
    REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION record_route_changes();
CREATE TRIGGER route_bindings_route_change_update AFTER UPDATE ON route_bindings
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION record_route_changes();
CREATE TRIGGER route_bindings_route_change_delete AFTER DELETE ON route_bindings
    REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION record_route_changes();
CREATE TRIGGER instances_route_change_insert AFTER INSERT ON instances
    REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION record_route_changes();
CREATE TRIGGER instances_route_change_update AFTER UPDATE ON instances
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION record_route_changes();
CREATE TRIGGER instances_route_change_delete AFTER DELETE ON instances
    REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION record_route_changes();
CREATE TRIGGER materializations_route_change_insert AFTER INSERT ON materializations
    REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION record_route_changes();
CREATE TRIGGER materializations_route_change_update AFTER UPDATE ON materializations
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION record_route_changes();
CREATE TRIGGER materializations_route_change_delete AFTER DELETE ON materializations
    REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION record_route_changes();

-- Certificates and exact hostname bindings, apart from routes and lifecycle.
-- The revision row's lock serializes these rare writes through commit.
CREATE TABLE tls_certificate_revision (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    revision bigint NOT NULL CHECK (revision >= 0)
);
INSERT INTO tls_certificate_revision (revision) VALUES (0);
CREATE FUNCTION certificate_chain_octets(bytea[]) RETURNS bigint
LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT sum(octet_length(value)) FROM unnest($1) AS value $$;
CREATE FUNCTION certificate_chain_nonempty(bytea[]) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT bool_and(value IS NOT NULL AND octet_length(value)>0) FROM unnest($1) AS value $$;
CREATE TABLE certificates (
    certificate_id text PRIMARY KEY,
    version bigint NOT NULL CHECK (version > 0),
    state text NOT NULL CHECK (state IN ('active', 'deleted')),
    not_before_unix_millis bigint NOT NULL,
    not_after_unix_millis bigint NOT NULL,
    dns_names text[] NOT NULL,
    leaf_sha256 bytea,
    chain_der bytea[],
    seal_format smallint,
    seal_key_id text,
    seal_nonce bytea,
    sealed_private_key bytea,
    sealing_revision bigint NOT NULL CHECK (sealing_revision > 0),
    CHECK ((state = 'deleted' AND chain_der IS NULL AND leaf_sha256 IS NULL
        AND seal_format IS NULL AND seal_key_id IS NULL AND seal_nonce IS NULL
        AND sealed_private_key IS NULL AND cardinality(dns_names) = 0)
      OR (state = 'active' AND chain_der IS NOT NULL AND leaf_sha256 IS NOT NULL
        AND seal_format IS NOT NULL AND seal_key_id IS NOT NULL AND seal_nonce IS NOT NULL
        AND sealed_private_key IS NOT NULL AND cardinality(chain_der) BETWEEN 1 AND 16
        AND certificate_chain_nonempty(chain_der)
        AND certificate_chain_octets(chain_der)+octet_length(sealed_private_key)-16 <= 131072
        AND octet_length(leaf_sha256) = 32 AND seal_format = 1
        AND length(seal_key_id) BETWEEN 1 AND 64 AND octet_length(seal_nonce) = 12
        AND octet_length(sealed_private_key) BETWEEN 17 AND 131088
        AND cardinality(dns_names) BETWEEN 1 AND 100
        AND not_before_unix_millis < not_after_unix_millis))
);
-- Sealing-key rotation finds the certificates each key still seals.
CREATE INDEX certificates_active_seal_key ON certificates (seal_key_id) WHERE state = 'active';
CREATE TABLE tls_hostname_bindings (
    hostname text PRIMARY KEY,
    certificate_id text REFERENCES certificates (certificate_id),
    revision bigint NOT NULL CHECK (revision > 0),
    last_invalidating_revision bigint NOT NULL CHECK (last_invalidating_revision > 0 AND last_invalidating_revision <= revision)
);
CREATE INDEX tls_hostname_bindings_certificate ON tls_hostname_bindings (certificate_id) WHERE certificate_id IS NOT NULL;
