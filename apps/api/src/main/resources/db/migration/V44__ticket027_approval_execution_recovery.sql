ALTER TABLE financial_approval_execution_state
    ADD COLUMN execution_lease_expires_at timestamptz,
    ADD COLUMN retryable_after_failure boolean NOT NULL DEFAULT true,
    ADD COLUMN stale_recovered_at timestamptz;

CREATE INDEX financial_approval_execution_state_stale_idx
    ON financial_approval_execution_state (workspace_id, execution_status, execution_lease_expires_at)
    WHERE execution_status = 'IN_PROGRESS';
