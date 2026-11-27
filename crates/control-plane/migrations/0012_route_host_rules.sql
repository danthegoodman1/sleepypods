-- The identity kind of a route binding determines its protocol.
ALTER TABLE route_bindings DROP COLUMN protocol;

CREATE OR REPLACE FUNCTION record_route_changes() RETURNS trigger LANGUAGE plpgsql AS $$
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
    RETURN NULL;
END;
$$;
