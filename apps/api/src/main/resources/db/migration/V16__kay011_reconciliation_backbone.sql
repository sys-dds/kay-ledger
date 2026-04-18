CREATE TABLE reconciliation_runs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    provider_config_id uuid REFERENCES provider_configs(id),
    run_type text NOT NULL,
    status text NOT NULL DEFAULT 'RUNNING',
    started_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT reconciliation_runs_type_check CHECK (run_type IN ('PAYMENT', 'PAYOUT', 'REFUND', 'FULL')),
    CONSTRAINT reconciliation_runs_status_check CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED'))
);

CREATE TRIGGER reconciliation_runs_touch_updated_at
    BEFORE UPDATE ON reconciliation_runs
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE reconciliation_mismatches (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    reconciliation_run_id uuid NOT NULL REFERENCES reconciliation_runs(id),
    provider_callback_id uuid REFERENCES provider_callbacks(id),
    business_reference_type text NOT NULL,
    business_reference_id uuid NOT NULL,
    drift_category text NOT NULL,
    internal_state text,
    provider_state text,
    suggested_action text NOT NULL,
    repair_status text NOT NULL DEFAULT 'OPEN',
    repair_note text,
    repaired_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT reconciliation_mismatches_reference_check CHECK (business_reference_type IN ('PAYMENT_INTENT', 'PAYOUT_REQUEST', 'REFUND')),
    CONSTRAINT reconciliation_mismatches_drift_check CHECK (drift_category IN ('STATE_MISMATCH', 'MISSING_INTERNAL', 'MISSING_PROVIDER', 'AMOUNT_MISMATCH', 'OUT_OF_ORDER')),
    CONSTRAINT reconciliation_mismatches_repair_status_check CHECK (repair_status IN ('OPEN', 'MARKED', 'APPLIED', 'IGNORED'))
);

CREATE INDEX reconciliation_mismatches_workspace_status_idx
    ON reconciliation_mismatches (workspace_id, repair_status, created_at);

CREATE INDEX reconciliation_mismatches_reference_idx
    ON reconciliation_mismatches (workspace_id, business_reference_type, business_reference_id);

CREATE TRIGGER reconciliation_mismatches_touch_updated_at
    BEFORE UPDATE ON reconciliation_mismatches
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();
