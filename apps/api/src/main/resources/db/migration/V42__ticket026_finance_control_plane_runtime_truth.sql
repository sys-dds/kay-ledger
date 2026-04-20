ALTER TABLE financial_approval_execution_state
    DROP CONSTRAINT financial_approval_execution_state_status_check;

ALTER TABLE financial_approval_execution_state
    ADD COLUMN started_at timestamptz,
    ADD COLUMN last_attempt_at timestamptz,
    ADD COLUMN execution_attempt_count integer NOT NULL DEFAULT 0;

ALTER TABLE financial_approval_execution_state
    ADD CONSTRAINT financial_approval_execution_state_status_check
        CHECK (execution_status IN ('PENDING', 'IN_PROGRESS', 'BLOCKED', 'EXECUTED', 'FAILED')),
    ADD CONSTRAINT financial_approval_execution_state_attempt_check
        CHECK (execution_attempt_count >= 0);

ALTER TABLE financial_approval_requests
    ADD CONSTRAINT financial_approval_requests_action_type_check
        CHECK (action_type IN (
            'FINANCIAL_PERIOD_CLOSE',
            'FINANCIAL_PERIOD_REOPEN',
            'PAYOUT_OPERATOR_SUCCESS',
            'PAYOUT_OPERATOR_RETRY',
            'PAYOUT_OPERATOR_FAILURE',
            'LARGE_REFUND_OR_REVERSAL',
            'DISPUTE_RESOLUTION'
        )),
    ADD CONSTRAINT financial_approval_requests_target_type_check
        CHECK (target_type IN (
            'ACCOUNTING_PERIOD',
            'PAYOUT_REQUEST',
            'PAYMENT_INTENT',
            'DISPUTE'
        ));

ALTER TABLE merchant_finance_event_deliveries
    ADD COLUMN first_attempt_at timestamptz,
    ADD COLUMN final_failure_reason text,
    ADD COLUMN parked_reason text;

CREATE INDEX merchant_finance_event_deliveries_due_idx
    ON merchant_finance_event_deliveries (delivery_status, next_attempt_at, created_at)
    WHERE delivery_status IN ('PENDING', 'FAILED');

ALTER TABLE finance_evidence_exports
    DROP CONSTRAINT finance_evidence_exports_unique;

ALTER TABLE finance_evidence_exports
    ADD COLUMN artifact_id uuid,
    ADD COLUMN export_version integer NOT NULL DEFAULT 1,
    ADD CONSTRAINT finance_evidence_exports_version_check CHECK (export_version > 0);

CREATE TABLE finance_evidence_export_artifacts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    evidence_pack_id uuid NOT NULL,
    artifact_format text NOT NULL,
    artifact_body text NOT NULL,
    artifact_size_bytes bigint NOT NULL,
    checksum_algorithm text NOT NULL,
    checksum_value text NOT NULL,
    generated_by_actor_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT finance_evidence_export_artifacts_workspace_id_unique UNIQUE (workspace_id, id),
    CONSTRAINT finance_evidence_export_artifacts_pack_fk FOREIGN KEY (workspace_id, evidence_pack_id)
        REFERENCES finance_evidence_packs(workspace_id, id) ON DELETE CASCADE,
    CONSTRAINT finance_evidence_export_artifacts_actor_fk FOREIGN KEY (workspace_id, generated_by_actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id),
    CONSTRAINT finance_evidence_export_artifacts_format_check CHECK (artifact_format IN ('JSON', 'CSV')),
    CONSTRAINT finance_evidence_export_artifacts_size_check CHECK (artifact_size_bytes >= 0)
);

CREATE INDEX finance_evidence_export_artifacts_pack_idx
    ON finance_evidence_export_artifacts (workspace_id, evidence_pack_id, created_at DESC);

ALTER TABLE finance_evidence_exports
    ADD CONSTRAINT finance_evidence_exports_artifact_fk FOREIGN KEY (workspace_id, artifact_id)
        REFERENCES finance_evidence_export_artifacts(workspace_id, id);

CREATE INDEX finance_evidence_exports_artifact_idx
    ON finance_evidence_exports (workspace_id, artifact_id);
