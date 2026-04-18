ALTER TABLE inbox_messages
    ADD COLUMN retry_count integer NOT NULL DEFAULT 0,
    ADD COLUMN available_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN last_error text,
    ADD COLUMN parked_at timestamptz,
    ADD COLUMN updated_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE inbox_messages
    DROP CONSTRAINT inbox_messages_outcome_check;

ALTER TABLE inbox_messages
    ADD CONSTRAINT inbox_messages_outcome_check CHECK (outcome IN ('PROCESSING', 'PROCESSED', 'FAILED', 'PARKED'));

CREATE INDEX inbox_messages_retry_idx
    ON inbox_messages (consumer_name, outcome, available_at, updated_at);

CREATE TRIGGER inbox_messages_touch_updated_at
    BEFORE UPDATE ON inbox_messages
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();
