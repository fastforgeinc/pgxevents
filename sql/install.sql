-- pgxevents v1.0.0
--
-- Canonical install SQL. Include this in a golang-migrate migration, or
-- rely on pgxevents.WithRuntimeInstall(true) to apply it at startup.
--
-- Idempotent: safe to re-run.

CREATE UNLOGGED TABLE IF NOT EXISTS pgxevents_outbox (
    id         uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    table_name text        NOT NULL,
    action     text        NOT NULL,
    data       jsonb       NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS pgxevents_outbox_created_at_idx
    ON pgxevents_outbox (created_at);

CREATE OR REPLACE FUNCTION pgxevents_notify_event()
    RETURNS TRIGGER AS $$
DECLARE
    snapshot_id uuid;
    row_data    jsonb;
BEGIN
    IF (TG_OP = 'DELETE') THEN
        row_data := to_jsonb(OLD);
    ELSE
        row_data := to_jsonb(NEW);
    END IF;

    INSERT INTO pgxevents_outbox (table_name, action, data)
    VALUES (TG_TABLE_NAME, TG_OP, row_data)
    RETURNING id INTO snapshot_id;

    PERFORM pg_notify('pgxevents_event', snapshot_id::text);

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION pgxevents_notify_event() IS 'pgxevents v1.0.0';
