ALTER TABLE provider_configs
    ADD COLUMN callback_token text;

UPDATE provider_configs
SET callback_token = encode(gen_random_bytes(32), 'hex')
WHERE callback_token IS NULL;

ALTER TABLE provider_configs
    ALTER COLUMN callback_token SET NOT NULL;

CREATE UNIQUE INDEX provider_configs_callback_token_unique
    ON provider_configs (callback_token);

UPDATE provider_callbacks
SET processing_status = 'FAILED',
    processing_error = COALESCE(processing_error, 'Legacy duplicate status retired; inspect the original callback state.')
WHERE processing_status = 'DUPLICATE';

ALTER TABLE provider_callbacks
    DROP CONSTRAINT provider_callbacks_status_check;

ALTER TABLE provider_callbacks
    ADD CONSTRAINT provider_callbacks_status_check CHECK (processing_status IN ('RECEIVED', 'APPLIED', 'IGNORED_OUT_OF_ORDER', 'FAILED'));

ALTER TABLE refunds
    DROP CONSTRAINT refunds_status_check;

ALTER TABLE refunds
    ALTER COLUMN status SET DEFAULT 'REQUESTED';

ALTER TABLE refunds
    ADD CONSTRAINT refunds_status_check CHECK (status IN ('REQUESTED', 'PROCESSING', 'SUCCEEDED', 'FAILED'));

ALTER TABLE refund_attempts
    DROP CONSTRAINT refund_attempts_status_check;

ALTER TABLE refund_attempts
    ADD CONSTRAINT refund_attempts_status_check CHECK (status IN ('PROCESSING', 'SUCCEEDED', 'FAILED'));

ALTER TABLE reconciliation_mismatches
    DROP CONSTRAINT reconciliation_mismatches_drift_check;

ALTER TABLE reconciliation_mismatches
    ADD CONSTRAINT reconciliation_mismatches_drift_check CHECK (drift_category IN (
        'STATE_MISMATCH',
        'MISSING_INTERNAL',
        'MISSING_PROVIDER',
        'AMOUNT_MISMATCH',
        'OUT_OF_ORDER',
        'MISSING_PROJECTION',
        'MISSING_JOURNAL',
        'FAILED_CALLBACK_BACKLOG',
        'MISSING_INTERNAL_REFERENCE'
    ));
