CREATE TABLE financial_accounting_periods (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    period_start date NOT NULL,
    period_end date NOT NULL,
    status text NOT NULL DEFAULT 'OPEN',
    opened_by_actor_id uuid,
    closed_by_actor_id uuid,
    reopened_by_actor_id uuid,
    close_reason text,
    reopen_reason text,
    closed_at timestamptz,
    reopened_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT financial_accounting_periods_actor_open_fk FOREIGN KEY (workspace_id, opened_by_actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id),
    CONSTRAINT financial_accounting_periods_actor_close_fk FOREIGN KEY (workspace_id, closed_by_actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id),
    CONSTRAINT financial_accounting_periods_actor_reopen_fk FOREIGN KEY (workspace_id, reopened_by_actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id),
    CONSTRAINT financial_accounting_periods_period_check CHECK (period_start <= period_end),
    CONSTRAINT financial_accounting_periods_status_check CHECK (status IN ('OPEN', 'CLOSED')),
    CONSTRAINT financial_accounting_periods_close_check CHECK (
        (status = 'OPEN')
        OR
        (status = 'CLOSED' AND closed_by_actor_id IS NOT NULL AND closed_at IS NOT NULL AND close_reason IS NOT NULL)
    ),
    CONSTRAINT financial_accounting_periods_workspace_id_unique UNIQUE (workspace_id, id),
    CONSTRAINT financial_accounting_periods_period_unique UNIQUE (workspace_id, period_start, period_end)
);

CREATE INDEX financial_accounting_periods_workspace_status_idx
    ON financial_accounting_periods (workspace_id, status, period_start DESC);

CREATE INDEX financial_accounting_periods_window_idx
    ON financial_accounting_periods (workspace_id, period_start, period_end);

CREATE TRIGGER financial_accounting_periods_touch_updated_at
    BEFORE UPDATE ON financial_accounting_periods
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE financial_period_closes (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    accounting_period_id uuid NOT NULL,
    status text NOT NULL DEFAULT 'CLOSED',
    close_reason text NOT NULL,
    reopened_reason text,
    closed_by_actor_id uuid NOT NULL,
    reopened_by_actor_id uuid,
    closed_at timestamptz NOT NULL DEFAULT now(),
    reopened_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT financial_period_closes_period_fk FOREIGN KEY (workspace_id, accounting_period_id)
        REFERENCES financial_accounting_periods(workspace_id, id) ON DELETE CASCADE,
    CONSTRAINT financial_period_closes_closed_actor_fk FOREIGN KEY (workspace_id, closed_by_actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id),
    CONSTRAINT financial_period_closes_reopened_actor_fk FOREIGN KEY (workspace_id, reopened_by_actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id),
    CONSTRAINT financial_period_closes_status_check CHECK (status IN ('CLOSED', 'REOPENED')),
    CONSTRAINT financial_period_closes_reopen_check CHECK (
        (status = 'CLOSED' AND reopened_by_actor_id IS NULL AND reopened_at IS NULL)
        OR
        (status = 'REOPENED' AND reopened_by_actor_id IS NOT NULL AND reopened_at IS NOT NULL AND reopened_reason IS NOT NULL)
    ),
    CONSTRAINT financial_period_closes_workspace_id_unique UNIQUE (workspace_id, id)
);

CREATE INDEX financial_period_closes_period_idx
    ON financial_period_closes (workspace_id, accounting_period_id, closed_at DESC);

CREATE INDEX financial_period_closes_status_idx
    ON financial_period_closes (workspace_id, status, closed_at DESC);

CREATE TRIGGER financial_period_closes_touch_updated_at
    BEFORE UPDATE ON financial_period_closes
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE finalized_provider_statements (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    accounting_period_id uuid NOT NULL,
    provider_profile_id uuid NOT NULL,
    currency_code char(3) NOT NULL,
    period_start date NOT NULL,
    period_end date NOT NULL,
    status text NOT NULL DEFAULT 'FINALIZED',
    settled_gross_amount_minor bigint NOT NULL DEFAULT 0,
    fee_amount_minor bigint NOT NULL DEFAULT 0,
    net_earnings_amount_minor bigint NOT NULL DEFAULT 0,
    current_payout_requested_amount_minor bigint NOT NULL DEFAULT 0,
    payout_succeeded_amount_minor bigint NOT NULL DEFAULT 0,
    refund_amount_minor bigint NOT NULL DEFAULT 0,
    active_dispute_exposure_amount_minor bigint NOT NULL DEFAULT 0,
    settled_subscription_net_revenue_amount_minor bigint NOT NULL DEFAULT 0,
    snapshot_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    finalized_by_actor_id uuid NOT NULL,
    finalized_at timestamptz NOT NULL DEFAULT now(),
    reopened_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT finalized_provider_statements_period_fk FOREIGN KEY (workspace_id, accounting_period_id)
        REFERENCES financial_accounting_periods(workspace_id, id) ON DELETE CASCADE,
    CONSTRAINT finalized_provider_statements_provider_fk FOREIGN KEY (workspace_id, provider_profile_id)
        REFERENCES provider_profiles(workspace_id, id) ON DELETE RESTRICT,
    CONSTRAINT finalized_provider_statements_actor_fk FOREIGN KEY (workspace_id, finalized_by_actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id),
    CONSTRAINT finalized_provider_statements_currency_check CHECK (currency_code ~ '^[A-Z]{3}$'),
    CONSTRAINT finalized_provider_statements_period_check CHECK (period_start <= period_end),
    CONSTRAINT finalized_provider_statements_status_check CHECK (status IN ('FINALIZED', 'VOIDED_BY_REOPEN')),
    CONSTRAINT finalized_provider_statements_amounts_check CHECK (
        settled_gross_amount_minor >= 0
        AND fee_amount_minor >= 0
        AND net_earnings_amount_minor >= 0
        AND current_payout_requested_amount_minor >= 0
        AND payout_succeeded_amount_minor >= 0
        AND refund_amount_minor >= 0
        AND active_dispute_exposure_amount_minor >= 0
        AND settled_subscription_net_revenue_amount_minor >= 0
    ),
    CONSTRAINT finalized_provider_statements_workspace_id_unique UNIQUE (workspace_id, id)
);

CREATE INDEX finalized_provider_statements_period_idx
    ON finalized_provider_statements (workspace_id, accounting_period_id, provider_profile_id, currency_code);

CREATE INDEX finalized_provider_statements_provider_idx
    ON finalized_provider_statements (workspace_id, provider_profile_id, currency_code, period_start, period_end);

CREATE UNIQUE INDEX finalized_provider_statements_active_unique
    ON finalized_provider_statements (workspace_id, accounting_period_id, provider_profile_id, currency_code)
    WHERE status = 'FINALIZED';

CREATE TRIGGER finalized_provider_statements_touch_updated_at
    BEFORE UPDATE ON finalized_provider_statements
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE financial_close_audit_events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    accounting_period_id uuid,
    finalized_statement_id uuid,
    event_type text NOT NULL,
    actor_id uuid,
    reason text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT financial_close_audit_events_period_fk FOREIGN KEY (workspace_id, accounting_period_id)
        REFERENCES financial_accounting_periods(workspace_id, id) ON DELETE CASCADE,
    CONSTRAINT financial_close_audit_events_statement_fk FOREIGN KEY (workspace_id, finalized_statement_id)
        REFERENCES finalized_provider_statements(workspace_id, id) ON DELETE CASCADE,
    CONSTRAINT financial_close_audit_events_actor_fk FOREIGN KEY (workspace_id, actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id),
    CONSTRAINT financial_close_audit_events_type_check CHECK (event_type IN ('PERIOD_OPENED', 'PERIOD_CLOSED', 'PERIOD_REOPENED', 'STATEMENT_FINALIZED')),
    CONSTRAINT financial_close_audit_events_reference_check CHECK (accounting_period_id IS NOT NULL OR finalized_statement_id IS NOT NULL)
);

CREATE INDEX financial_close_audit_events_period_idx
    ON financial_close_audit_events (workspace_id, accounting_period_id, created_at, id);

CREATE INDEX financial_close_audit_events_statement_idx
    ON financial_close_audit_events (workspace_id, finalized_statement_id, created_at, id);

CREATE INDEX financial_close_audit_events_type_idx
    ON financial_close_audit_events (workspace_id, event_type, created_at DESC);
