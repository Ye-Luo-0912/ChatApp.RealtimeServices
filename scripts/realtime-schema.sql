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
    content_fingerprint varchar(64) NULL,
    received_at_ms bigint NOT NULL,
    delivered_at_ms bigint NULL,
    read_at_ms bigint NULL,
    created_at_ms bigint NOT NULL,
    CONSTRAINT ck_messages_sender_positive CHECK (sender_user_id > 0),
    CONSTRAINT ck_messages_receiver_positive CHECK (receiver_user_id > 0)
);

ALTER TABLE realtime.messages ADD COLUMN IF NOT EXISTS delivered_at_ms bigint NULL;
ALTER TABLE realtime.messages ADD COLUMN IF NOT EXISTS read_at_ms bigint NULL;
ALTER TABLE realtime.messages ADD COLUMN IF NOT EXISTS content_fingerprint varchar(64) NULL;

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
    event_type smallint NOT NULL DEFAULT 0,
    status smallint NOT NULL DEFAULT 0
);

ALTER TABLE realtime.outbox ADD COLUMN IF NOT EXISTS target_user_id bigint;
ALTER TABLE realtime.outbox ADD COLUMN IF NOT EXISTS event_type smallint;
ALTER TABLE realtime.outbox ADD COLUMN IF NOT EXISTS status smallint;

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

UPDATE realtime.outbox
SET status = CASE
    WHEN published_at_ms IS NOT NULL THEN 1
    ELSE 0
END
WHERE status IS NULL;

ALTER TABLE realtime.outbox ALTER COLUMN target_user_id SET DEFAULT 0;
ALTER TABLE realtime.outbox ALTER COLUMN event_type SET DEFAULT 0;
ALTER TABLE realtime.outbox ALTER COLUMN status SET DEFAULT 0;
ALTER TABLE realtime.outbox ALTER COLUMN target_user_id SET NOT NULL;
ALTER TABLE realtime.outbox ALTER COLUMN event_type SET NOT NULL;
ALTER TABLE realtime.outbox ALTER COLUMN status SET NOT NULL;

DROP INDEX IF EXISTS realtime.ix_outbox_pending;
CREATE INDEX IF NOT EXISTS ix_outbox_pending
    ON realtime.outbox (next_attempt_at_ms, created_at_ms)
    WHERE status = 0;
CREATE INDEX IF NOT EXISTS ix_outbox_dead
    ON realtime.outbox (created_at_ms)
    WHERE status = 2;
CREATE INDEX IF NOT EXISTS ix_outbox_published_cleanup
    ON realtime.outbox (published_at_ms)
    WHERE status = 1;
CREATE INDEX IF NOT EXISTS ix_outbox_target_user_id
    ON realtime.outbox (target_user_id);
CREATE INDEX IF NOT EXISTS ix_outbox_target_user_event_type
    ON realtime.outbox (target_user_id, event_type);

CREATE TABLE IF NOT EXISTS realtime.conversations (
    conversation_id varchar(64) NOT NULL PRIMARY KEY,
    type smallint NOT NULL,
    created_at_ms bigint NOT NULL,
    updated_at_ms bigint NOT NULL,
    last_message_id varchar(64) NULL,
    last_message_preview varchar(256) NULL,
    last_message_at_ms bigint NULL,
    last_sender_user_id bigint NULL,
    title varchar(128) NULL,
    created_by_user_id bigint NULL,
    CONSTRAINT ck_conversations_type_known CHECK (type IN (1, 2))
);

CREATE TABLE IF NOT EXISTS realtime.conversation_members (
    conversation_id varchar(64) NOT NULL,
    user_id bigint NOT NULL,
    peer_user_id bigint NULL,
    joined_at_ms bigint NOT NULL,
    last_read_message_id varchar(64) NULL,
    last_read_at_ms bigint NULL,
    unread_count integer NOT NULL DEFAULT 0,
    is_pinned boolean NOT NULL DEFAULT false,
    pinned_at_ms bigint NULL,
    is_muted boolean NOT NULL DEFAULT false,
    muted_until_ms bigint NULL,
    role smallint NOT NULL DEFAULT 3,
    PRIMARY KEY (conversation_id, user_id),
    CONSTRAINT ck_conversation_members_user_positive CHECK (user_id > 0),
    CONSTRAINT ck_conversation_members_unread_nonnegative CHECK (unread_count >= 0),
    CONSTRAINT ck_conversation_members_role_known CHECK (role IN (1, 2, 3))
);

ALTER TABLE realtime.messages ADD COLUMN IF NOT EXISTS conversation_id varchar(64) NULL;

CREATE INDEX IF NOT EXISTS ix_messages_conversation_history
    ON realtime.messages (conversation_id, received_at_ms DESC, message_id DESC)
    WHERE conversation_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_conversation_members_user_list
    ON realtime.conversation_members (user_id, conversation_id);

CREATE INDEX IF NOT EXISTS ix_conversations_last_message_list
    ON realtime.conversations (last_message_at_ms DESC NULLS LAST, conversation_id DESC);

CREATE INDEX IF NOT EXISTS ix_conversation_members_user_pinned_list
    ON realtime.conversation_members (
        user_id,
        is_pinned DESC,
        pinned_at_ms DESC NULLS LAST,
        conversation_id DESC
    );

CREATE TABLE IF NOT EXISTS realtime.attachments (
    attachment_id varchar(64) NOT NULL PRIMARY KEY,
    uploader_user_id bigint NOT NULL,
    object_key varchar(512) NOT NULL,
    public_url varchar(1024) NULL,
    content_type varchar(128) NOT NULL,
    size_bytes bigint NOT NULL,
    original_name varchar(256) NULL,
    status smallint NOT NULL,
    message_id varchar(64) NULL,
    conversation_id varchar(64) NULL,
    client_attachment_id varchar(128) NULL,
    created_at_ms bigint NOT NULL,
    confirmed_at_ms bigint NULL,
    bound_at_ms bigint NULL,
    CONSTRAINT ck_attachments_uploader_positive CHECK (uploader_user_id > 0),
    CONSTRAINT ck_attachments_size_nonnegative CHECK (size_bytes >= 0),
    CONSTRAINT ck_attachments_status_known CHECK (status IN (0, 1, 2, 3))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_attachments_object_key
    ON realtime.attachments (object_key);
CREATE UNIQUE INDEX IF NOT EXISTS ux_attachments_uploader_client
    ON realtime.attachments (uploader_user_id, client_attachment_id)
    WHERE client_attachment_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_attachments_message
    ON realtime.attachments (message_id)
    WHERE message_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_attachments_uploader_status
    ON realtime.attachments (uploader_user_id, status, created_at_ms);
CREATE INDEX IF NOT EXISTS ix_attachments_unbound_age
    ON realtime.attachments (created_at_ms)
    WHERE status IN (0, 1);

-- Migration020: age-based retention GC keyset index
CREATE INDEX IF NOT EXISTS ix_messages_received_at
    ON realtime.messages (received_at_ms, message_id);

INSERT INTO realtime.schema_migrations (version, name, applied_at_ms)
VALUES
    (1, 'baseline_schema', (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint),
    (2, 'outbox_typed_target_columns', (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint),
    (3, 'outbox_lifecycle', (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint),
    (4, 'message_content_fingerprint', (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint),
    (5, 'conversation_foundation', (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint),
    (6, 'conversation_list_index', (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint),
    (7, 'conversation_member_prefs', (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint),
    (12, 'attachments', (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint),
    (20, 'message_retention_age_index', (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint)
ON CONFLICT (version) DO NOTHING;
