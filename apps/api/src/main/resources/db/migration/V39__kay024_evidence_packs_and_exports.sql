CREATE TABLE finance_evidence_packs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    evidence_pack_type text NOT NULL,
    status text NOT NULL DEFAULT 'GENERATED',
    accounting_period_id uuid,
    finalized_statement_id uuid,
    reconciliation_run_id uuid,
    provider_truth_import_id uuid,
    approval_request_id uuid,
    source_reference text NOT NULL,
    generated_by_actor_id uuid NOT NULL,
    snapshot_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT finance_evidence_packs_workspace_id_unique UNIQUE (workspace_id, id),
    CONSTRAINT finance_evidence_packs_period_fk FOREIGN KEY (workspace_id, accounting_period_id)
        REFERENCES financial_accounting_periods(workspace_id, id),
    CONSTRAINT finance_evidence_packs_statement_fk FOREIGN KEY (workspace_id, finalized_statement_id)
        REFERENCES finalized_provider_statements(workspace_id, id),
    CONSTRAINT finance_evidence_packs_run_fk FOREIGN KEY (workspace_id, reconciliation_run_id)
        REFERENCES provider_reconciliation_runs(workspace_id, id),
    CONSTRAINT finance_evidence_packs_import_fk FOREIGN KEY (workspace_id, provider_truth_import_id)
        REFERENCES provider_reconciliation_truth_imports(workspace_id, id),
    CONSTRAINT finance_evidence_packs_approval_fk FOREIGN KEY (workspace_id, approval_request_id)
        REFERENCES financial_approval_requests(workspace_id, id),
    CONSTRAINT finance_evidence_packs_actor_fk FOREIGN KEY (workspace_id, generated_by_actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id),
    CONSTRAINT finance_evidence_packs_type_check CHECK (evidence_pack_type IN (
        'FINALIZED_PROVIDER_STATEMENT',
        'ACCOUNTING_PERIOD_CLOSE_HISTORY',
        'PROVIDER_RECONCILIATION',
        'FINANCIAL_APPROVAL_CHAIN'
    )),
    CONSTRAINT finance_evidence_packs_status_check CHECK (status IN ('GENERATED', 'EXPORTED'))
);

CREATE INDEX finance_evidence_packs_workspace_type_idx
    ON finance_evidence_packs (workspace_id, evidence_pack_type, created_at DESC);

CREATE INDEX finance_evidence_packs_source_idx
    ON finance_evidence_packs (workspace_id, source_reference);

CREATE TRIGGER finance_evidence_packs_touch_updated_at
    BEFORE UPDATE ON finance_evidence_packs
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE finance_evidence_pack_items (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    evidence_pack_id uuid NOT NULL,
    source_reference_type text NOT NULL,
    source_reference_id uuid NOT NULL,
    source_reference_label text,
    item_snapshot_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT finance_evidence_pack_items_pack_fk FOREIGN KEY (workspace_id, evidence_pack_id)
        REFERENCES finance_evidence_packs(workspace_id, id) ON DELETE CASCADE,
    CONSTRAINT finance_evidence_pack_items_unique UNIQUE (workspace_id, evidence_pack_id, source_reference_type, source_reference_id)
);

CREATE INDEX finance_evidence_pack_items_source_idx
    ON finance_evidence_pack_items (workspace_id, source_reference_type, source_reference_id);

CREATE TABLE finance_evidence_exports (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    evidence_pack_id uuid NOT NULL,
    export_status text NOT NULL DEFAULT 'GENERATED',
    artifact_format text NOT NULL,
    artifact_reference text NOT NULL,
    artifact_size_bytes bigint NOT NULL,
    checksum_algorithm text NOT NULL,
    checksum_value text NOT NULL,
    generated_by_actor_id uuid NOT NULL,
    generated_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT finance_evidence_exports_pack_fk FOREIGN KEY (workspace_id, evidence_pack_id)
        REFERENCES finance_evidence_packs(workspace_id, id) ON DELETE CASCADE,
    CONSTRAINT finance_evidence_exports_actor_fk FOREIGN KEY (workspace_id, generated_by_actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id),
    CONSTRAINT finance_evidence_exports_status_check CHECK (export_status IN ('GENERATED')),
    CONSTRAINT finance_evidence_exports_format_check CHECK (artifact_format IN ('JSON', 'CSV')),
    CONSTRAINT finance_evidence_exports_size_check CHECK (artifact_size_bytes >= 0),
    CONSTRAINT finance_evidence_exports_unique UNIQUE (workspace_id, evidence_pack_id, artifact_format)
);

CREATE INDEX finance_evidence_exports_pack_idx
    ON finance_evidence_exports (workspace_id, evidence_pack_id, generated_at DESC);
