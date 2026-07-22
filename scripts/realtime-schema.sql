CREATE SCHEMA IF NOT EXISTS realtime;

CREATE TABLE IF NOT EXISTS realtime.schema_migrations (
    version integer NOT NULL PRIMARY KEY,
    name varchar(128) NOT NULL,
    applied_at_ms bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS realtime.messages (
    message_id varchar(64) PRIMARY KEY,
    client_message_id varchar(128) NOT NULL,
    sender_user_id bigint NOT NULL,
    sender_session_id varchar(128) NOT NULL,
    receiver_user_id bigint NOT NULL,
    content text NOT NULL,
    received_at_ms bigint NOT NULL,
    delivered_at_ms bigint NULL,
    read_at_ms bigint NULL,
    created_at_ms bigint NOT NULL,
    CONSTRAINT ck_messages_sender_positive CHECK (sender_user_id > 0),
    CONSTRAINT ck_messages_receiver_positive CHECK (receiver_user_id > 0)
);

ALTER TABLE realtime.messages ADD COLUMN IF NOT EXISTS delivered_at_ms bigint NULL;
ALTER TABLE realtime.messages ADD COLUMN IF NOT EXISTS read_at_ms bigint NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_messages_sender_client_message
    ON realtime.messages (sender_user_id, client_message_id);
CREATE INDEX IF NOT EXISTS ix_messages_receiver_history
    ON realtime.messages (receiver_user_id, received_at_ms DESC, message_id DESC);
CREATE INDEX IF NOT EXISTS ix_messages_sender_history
    ON realtime.messages (sender_user_id, received_at_ms DESC, message_id DESC);
DROP INDEX IF EXISTS realtime.ix_messages_receiver_received;
DROP INDEX IF EXISTS realtime.ix_messages_sender_received;

CREATE TABLE IF NOT EXISTS realtime.outbox (
    event_id varchar(64) PRIMARY KEY,
    payload_json text NOT NULL,
    created_at_ms bigint NOT NULL,
    next_attempt_at_ms bigint NOT NULL,
    published_at_ms bigint NULL,
    attempt_count integer NOT NULL DEFAULT 0,
    locked_by varchar(128) NULL,
    locked_until_ms bigint NULL,
    last_error varchar(2048) NULL,
    target_user_id bigint NOT NULL DEFAULT 0,
    event_type smallint NOT NULL DEFAULT 0
);

ALTER TABLE realtime.outbox ADD COLUMN IF NOT EXISTS target_user_id bigint;
ALTER TABLE realtime.outbox ADD COLUMN IF NOT EXISTS event_type smallint;

UPDATE realtime.outbox
SET
    target_user_id = COALESCE(
        target_user_id,
        NULLIF(BTRIM(payload_json::jsonb ->> 'TargetUserId'), '')::bigint),
    event_type = COALESCE(
        event_type,
        NULLIF(BTRIM(payload_json::jsonb ->> 'Type'), '')::smallint)
WHERE target_user_id IS NULL OR event_type IS NULL;

UPDATE realtime.outbox
SET
    target_user_id = COALESCE(target_user_id, 0),
    event_type = COALESCE(event_type, 0)
WHERE target_user_id IS NULL OR event_type IS NULL;

ALTER TABLE realtime.outbox ALTER COLUMN target_user_id SET DEFAULT 0;
ALTER TABLE realtime.outbox ALTER COLUMN event_type SET DEFAULT 0;
ALTER TABLE realtime.outbox ALTER COLUMN target_user_id SET NOT NULL;
ALTER TABLE realtime.outbox ALTER COLUMN event_type SET NOT NULL;

CREATE INDEX IF NOT EXISTS ix_outbox_pending
    ON realtime.outbox (next_attempt_at_ms, created_at_ms)
    WHERE published_at_ms IS NULL;
CREATE INDEX IF NOT EXISTS ix_outbox_target_user_id
    ON realtime.outbox (target_user_id);
CREATE INDEX IF NOT EXISTS ix_outbox_target_user_event_type
    ON realtime.outbox (target_user_id, event_type);

INSERT INTO realtime.schema_migrations (version, name, applied_at_ms)
VALUES
    (1, 'baseline_schema', (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint),
    (2, 'outbox_typed_target_columns', (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint)
ON CONFLICT (version) DO NOTHING;
