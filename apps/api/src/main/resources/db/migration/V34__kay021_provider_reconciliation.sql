CREATE TABLE provider_reconciliation_truth_imports (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    provider_profile_id uuid NOT NULL,
    currency_code char(3) NOT NULL,
    statement_period_start date NOT NULL,
    statement_period_end date NOT NULL,
    source_reference text NOT NULL,
    source_type text NOT NULL DEFAULT 'PROVIDER_STATEMENT',
    status text NOT NULL DEFAULT 'RECORDED',
    imported_by_actor_id uuid,
    imported_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT provider_reconciliation_truth_imports_provider_fk FOREIGN KEY (workspace_id, provider_profile_id)
        REFERENCES provider_profiles(workspace_id, id) ON DELETE RESTRICT,
    CONSTRAINT provider_reconciliation_truth_imports_actor_fk FOREIGN KEY (workspace_id, imported_by_actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id),
    CONSTRAINT provider_reconciliation_truth_imports_currency_check CHECK (currency_code ~ '^[A-Z]{3}$'),
    CONSTRAINT provider_reconciliation_truth_imports_period_check CHECK (statement_period_start <= statement_period_end),
    CONSTRAINT provider_reconciliation_truth_imports_status_check CHECK (status IN ('RECORDED', 'RECONCILED', 'SUPERSEDED')),
    CONSTRAINT provider_reconciliation_truth_imports_workspace_id_unique UNIQUE (workspace_id, id),
    CONSTRAINT provider_reconciliation_truth_imports_unique UNIQUE (
        workspace_id, provider_profile_id, currency_code, statement_period_start, statement_period_end, source_reference
    )
);

CREATE INDEX provider_reconciliation_truth_imports_workspace_idx
    ON provider_reconciliation_truth_imports (workspace_id, provider_profile_id, currency_code, statement_period_start, statement_period_end);

CREATE INDEX provider_reconciliation_truth_imports_source_idx
    ON provider_reconciliation_truth_imports (workspace_id, source_reference);

CREATE TRIGGER provider_reconciliation_truth_imports_touch_updated_at
    BEFORE UPDATE ON provider_reconciliation_truth_imports
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE provider_reconciliation_truth_snapshots (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    truth_import_id uuid NOT NULL,
    provider_profile_id uuid NOT NULL,
    currency_code char(3) NOT NULL,
    statement_period_start date NOT NULL,
    statement_period_end date NOT NULL,
    source_reference text NOT NULL,
    settled_gross_amount_minor bigint NOT NULL DEFAULT 0,
    fee_amount_minor bigint NOT NULL DEFAULT 0,
    net_earnings_amount_minor bigint NOT NULL DEFAULT 0,
    payout_succeeded_amount_minor bigint NOT NULL DEFAULT 0,
    refund_amount_minor bigint NOT NULL DEFAULT 0,
    active_dispute_exposure_amount_minor bigint NOT NULL DEFAULT 0,
    settled_subscription_net_revenue_amount_minor bigint NOT NULL DEFAULT 0,
    provider_payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT provider_reconciliation_truth_snapshots_import_fk FOREIGN KEY (workspace_id, truth_import_id)
        REFERENCES provider_reconciliation_truth_imports(workspace_id, id) ON DELETE CASCADE,
    CONSTRAINT provider_reconciliation_truth_snapshots_provider_fk FOREIGN KEY (workspace_id, provider_profile_id)
        REFERENCES provider_profiles(workspace_id, id) ON DELETE RESTRICT,
    CONSTRAINT provider_reconciliation_truth_snapshots_currency_check CHECK (currency_code ~ '^[A-Z]{3}$'),
    CONSTRAINT provider_reconciliation_truth_snapshots_period_check CHECK (statement_period_start <= statement_period_end),
    CONSTRAINT provider_reconciliation_truth_snapshots_amounts_check CHECK (
        settled_gross_amount_minor >= 0
        AND fee_amount_minor >= 0
        AND net_earnings_amount_minor >= 0
        AND payout_succeeded_amount_minor >= 0
        AND refund_amount_minor >= 0
        AND active_dispute_exposure_amount_minor >= 0
        AND settled_subscription_net_revenue_amount_minor >= 0
    ),
    CONSTRAINT provider_reconciliation_truth_snapshots_unique UNIQUE (workspace_id, truth_import_id, provider_profile_id, currency_code)
);

CREATE INDEX provider_reconciliation_truth_snapshots_workspace_idx
    ON provider_reconciliation_truth_snapshots (workspace_id, provider_profile_id, currency_code, statement_period_start, statement_period_end);

CREATE INDEX provider_reconciliation_truth_snapshots_source_idx
    ON provider_reconciliation_truth_snapshots (workspace_id, source_reference);

CREATE TRIGGER provider_reconciliation_truth_snapshots_touch_updated_at
    BEFORE UPDATE ON provider_reconciliation_truth_snapshots
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE provider_reconciliation_runs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    truth_import_id uuid NOT NULL,
    provider_profile_id uuid NOT NULL,
    currency_code char(3) NOT NULL,
    statement_period_start date NOT NULL,
    statement_period_end date NOT NULL,
    source_reference text NOT NULL,
    status text NOT NULL DEFAULT 'RUNNING',
    started_by_actor_id uuid,
    started_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    unresolved_item_count integer NOT NULL DEFAULT 0,
    resolved_item_count integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT provider_reconciliation_runs_import_fk FOREIGN KEY (workspace_id, truth_import_id)
        REFERENCES provider_reconciliation_truth_imports(workspace_id, id) ON DELETE RESTRICT,
    CONSTRAINT provider_reconciliation_runs_provider_fk FOREIGN KEY (workspace_id, provider_profile_id)
        REFERENCES provider_profiles(workspace_id, id) ON DELETE RESTRICT,
    CONSTRAINT provider_reconciliation_runs_actor_fk FOREIGN KEY (workspace_id, started_by_actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id),
    CONSTRAINT provider_reconciliation_runs_currency_check CHECK (currency_code ~ '^[A-Z]{3}$'),
    CONSTRAINT provider_reconciliation_runs_period_check CHECK (statement_period_start <= statement_period_end),
    CONSTRAINT provider_reconciliation_runs_status_check CHECK (status IN ('RUNNING', 'MATCHED', 'MISMATCHED', 'FAILED')),
    CONSTRAINT provider_reconciliation_runs_counts_check CHECK (unresolved_item_count >= 0 AND resolved_item_count >= 0),
    CONSTRAINT provider_reconciliation_runs_workspace_id_unique UNIQUE (workspace_id, id)
);

CREATE INDEX provider_reconciliation_runs_workspace_idx
    ON provider_reconciliation_runs (workspace_id, provider_profile_id, currency_code, started_at DESC);

CREATE INDEX provider_reconciliation_runs_status_idx
    ON provider_reconciliation_runs (workspace_id, status, started_at DESC);

CREATE INDEX provider_reconciliation_runs_source_idx
    ON provider_reconciliation_runs (workspace_id, source_reference);

CREATE TRIGGER provider_reconciliation_runs_touch_updated_at
    BEFORE UPDATE ON provider_reconciliation_runs
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE provider_reconciliation_items (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    reconciliation_run_id uuid NOT NULL,
    truth_import_id uuid NOT NULL,
    provider_profile_id uuid NOT NULL,
    currency_code char(3) NOT NULL,
    source_reference text NOT NULL,
    mismatch_type text NOT NULL,
    internal_settled_gross_amount_minor bigint,
    provider_settled_gross_amount_minor bigint,
    internal_fee_amount_minor bigint,
    provider_fee_amount_minor bigint,
    internal_net_earnings_amount_minor bigint,
    provider_net_earnings_amount_minor bigint,
    internal_payout_succeeded_amount_minor bigint,
    provider_payout_succeeded_amount_minor bigint,
    internal_refund_amount_minor bigint,
    provider_refund_amount_minor bigint,
    internal_active_dispute_exposure_amount_minor bigint,
    provider_active_dispute_exposure_amount_minor bigint,
    internal_settled_subscription_net_revenue_amount_minor bigint,
    provider_settled_subscription_net_revenue_amount_minor bigint,
    detail_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    status text NOT NULL DEFAULT 'OPEN',
    resolution_outcome text,
    resolution_note text,
    resolved_by_actor_id uuid,
    resolved_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT provider_reconciliation_items_run_fk FOREIGN KEY (workspace_id, reconciliation_run_id)
        REFERENCES provider_reconciliation_runs(workspace_id, id) ON DELETE CASCADE,
    CONSTRAINT provider_reconciliation_items_import_fk FOREIGN KEY (workspace_id, truth_import_id)
        REFERENCES provider_reconciliation_truth_imports(workspace_id, id) ON DELETE RESTRICT,
    CONSTRAINT provider_reconciliation_items_provider_fk FOREIGN KEY (workspace_id, provider_profile_id)
        REFERENCES provider_profiles(workspace_id, id) ON DELETE RESTRICT,
    CONSTRAINT provider_reconciliation_items_resolved_actor_fk FOREIGN KEY (workspace_id, resolved_by_actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id),
    CONSTRAINT provider_reconciliation_items_currency_check CHECK (currency_code ~ '^[A-Z]{3}$'),
    CONSTRAINT provider_reconciliation_items_type_check CHECK (mismatch_type IN (
        'MISSING_INTERNAL_SUMMARY',
        'MISSING_PROVIDER_TRUTH',
        'SETTLED_GROSS_MISMATCH',
        'FEE_MISMATCH',
        'NET_EARNINGS_MISMATCH',
        'PAYOUT_MISMATCH',
        'REFUND_MISMATCH',
        'DISPUTE_EXPOSURE_MISMATCH',
        'SUBSCRIPTION_REVENUE_MISMATCH'
    )),
    CONSTRAINT provider_reconciliation_items_status_check CHECK (status IN ('OPEN', 'RESOLVED')),
    CONSTRAINT provider_reconciliation_items_resolution_check CHECK (
        (status = 'OPEN' AND resolved_by_actor_id IS NULL AND resolved_at IS NULL)
        OR
        (status = 'RESOLVED' AND resolved_by_actor_id IS NOT NULL AND resolved_at IS NOT NULL AND resolution_note IS NOT NULL)
    )
);

CREATE INDEX provider_reconciliation_items_workspace_run_idx
    ON provider_reconciliation_items (workspace_id, reconciliation_run_id, status, mismatch_type);

CREATE INDEX provider_reconciliation_items_unresolved_idx
    ON provider_reconciliation_items (workspace_id, status, created_at DESC)
    WHERE status = 'OPEN';

CREATE INDEX provider_reconciliation_items_provider_idx
    ON provider_reconciliation_items (workspace_id, provider_profile_id, currency_code, mismatch_type);

CREATE INDEX provider_reconciliation_items_source_idx
    ON provider_reconciliation_items (workspace_id, source_reference);

CREATE TRIGGER provider_reconciliation_items_touch_updated_at
    BEFORE UPDATE ON provider_reconciliation_items
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();
