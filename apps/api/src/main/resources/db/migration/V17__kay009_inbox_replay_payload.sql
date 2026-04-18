ALTER TABLE inbox_messages
    ADD COLUMN payload_json jsonb;

CREATE INDEX inbox_messages_replay_idx
    ON inbox_messages (workspace_id, consumer_name, dedupe_key, outcome);
