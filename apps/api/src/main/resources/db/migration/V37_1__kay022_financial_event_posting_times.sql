ALTER TABLE payment_intents
    ADD COLUMN settled_effective_at timestamptz DEFAULT now();

ALTER TABLE payout_requests
    ADD COLUMN requested_effective_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN succeeded_effective_at timestamptz DEFAULT now();

ALTER TABLE refunds
    ADD COLUMN requested_effective_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN succeeded_effective_at timestamptz DEFAULT now();

ALTER TABLE disputes
    ADD COLUMN opened_effective_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN resolved_effective_at timestamptz;

UPDATE payment_intents
SET settled_effective_at = updated_at
WHERE status = 'SETTLED'
  AND settled_effective_at IS NULL;

UPDATE payout_requests
SET requested_effective_at = created_at,
    succeeded_effective_at = CASE WHEN status = 'SUCCEEDED' THEN updated_at ELSE succeeded_effective_at END;

UPDATE refunds
SET requested_effective_at = created_at,
    succeeded_effective_at = CASE WHEN status = 'SUCCEEDED' THEN updated_at ELSE succeeded_effective_at END;

UPDATE disputes
SET opened_effective_at = created_at,
    resolved_effective_at = CASE WHEN status <> 'OPEN' THEN updated_at ELSE resolved_effective_at END;

CREATE INDEX payment_intents_settled_effective_idx
    ON payment_intents (workspace_id, provider_profile_id, currency_code, settled_effective_at)
    WHERE status = 'SETTLED';

CREATE INDEX payout_requests_requested_effective_idx
    ON payout_requests (workspace_id, provider_profile_id, currency_code, requested_effective_at)
    WHERE status IN ('REQUESTED', 'PROCESSING');

CREATE INDEX payout_requests_succeeded_effective_idx
    ON payout_requests (workspace_id, provider_profile_id, currency_code, succeeded_effective_at)
    WHERE status = 'SUCCEEDED';

CREATE INDEX refunds_succeeded_effective_idx
    ON refunds (workspace_id, payment_intent_id, succeeded_effective_at)
    WHERE status = 'SUCCEEDED';

CREATE INDEX disputes_opened_effective_idx
    ON disputes (workspace_id, payment_intent_id, opened_effective_at)
    WHERE status = 'OPEN';
