ALTER TABLE merchant_finance_event_deliveries
    DROP CONSTRAINT merchant_finance_event_deliveries_status_check;

ALTER TABLE merchant_finance_event_deliveries
    ADD COLUMN claim_owner text,
    ADD COLUMN claimed_at timestamptz,
    ADD COLUMN claim_expires_at timestamptz;

ALTER TABLE merchant_finance_event_deliveries
    ADD CONSTRAINT merchant_finance_event_deliveries_status_check
        CHECK (delivery_status IN ('PENDING', 'CLAIMED', 'SUCCEEDED', 'FAILED', 'PARKED'));

CREATE INDEX merchant_finance_event_deliveries_claim_idx
    ON merchant_finance_event_deliveries (workspace_id, delivery_status, claim_expires_at, claimed_at);
