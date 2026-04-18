CREATE TABLE financial_provider_summaries (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    provider_profile_id uuid NOT NULL,
    currency_code char(3) NOT NULL,
    settled_gross_amount_minor bigint NOT NULL DEFAULT 0,
    fee_amount_minor bigint NOT NULL DEFAULT 0,
    net_earnings_amount_minor bigint NOT NULL DEFAULT 0,
    payout_requested_amount_minor bigint NOT NULL DEFAULT 0,
    payout_succeeded_amount_minor bigint NOT NULL DEFAULT 0,
    refund_amount_minor bigint NOT NULL DEFAULT 0,
    dispute_amount_minor bigint NOT NULL DEFAULT 0,
    subscription_revenue_amount_minor bigint NOT NULL DEFAULT 0,
    refreshed_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT financial_provider_summaries_provider_fk FOREIGN KEY (workspace_id, provider_profile_id)
        REFERENCES provider_profiles(workspace_id, id),
    CONSTRAINT financial_provider_summaries_currency_check CHECK (currency_code ~ '^[A-Z]{3}$'),
    CONSTRAINT financial_provider_summaries_amounts_check CHECK (
        settled_gross_amount_minor >= 0
        AND fee_amount_minor >= 0
        AND net_earnings_amount_minor >= 0
        AND payout_requested_amount_minor >= 0
        AND payout_succeeded_amount_minor >= 0
        AND refund_amount_minor >= 0
        AND dispute_amount_minor >= 0
        AND subscription_revenue_amount_minor >= 0
    ),
    CONSTRAINT financial_provider_summaries_unique UNIQUE (workspace_id, provider_profile_id, currency_code)
);

CREATE INDEX financial_provider_summaries_workspace_idx
    ON financial_provider_summaries (workspace_id, provider_profile_id, currency_code);

CREATE TRIGGER financial_provider_summaries_touch_updated_at
    BEFORE UPDATE ON financial_provider_summaries
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE export_jobs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    export_type text NOT NULL,
    status text NOT NULL DEFAULT 'REQUESTED',
    requested_by_actor_id uuid NOT NULL,
    parameters_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    row_count integer NOT NULL DEFAULT 0,
    storage_key text,
    content_type text,
    failure_reason text,
    requested_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT export_jobs_actor_fk FOREIGN KEY (workspace_id, requested_by_actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id),
    CONSTRAINT export_jobs_type_check CHECK (export_type IN ('PROVIDER_STATEMENT', 'PAYOUT_EXPORT', 'REFUND_DISPUTE_EXPORT')),
    CONSTRAINT export_jobs_status_check CHECK (status IN ('REQUESTED', 'RUNNING', 'SUCCEEDED', 'FAILED')),
    CONSTRAINT export_jobs_row_count_check CHECK (row_count >= 0),
    CONSTRAINT export_jobs_workspace_id_unique UNIQUE (workspace_id, id),
    CONSTRAINT export_jobs_storage_success_check CHECK (status <> 'SUCCEEDED' OR storage_key IS NOT NULL)
);

CREATE INDEX export_jobs_workspace_status_idx
    ON export_jobs (workspace_id, status, created_at DESC);

CREATE TRIGGER export_jobs_touch_updated_at
    BEFORE UPDATE ON export_jobs
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE export_artifacts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    export_job_id uuid NOT NULL,
    storage_key text NOT NULL,
    content_type text NOT NULL,
    byte_size bigint NOT NULL,
    row_count integer NOT NULL DEFAULT 0,
    checksum_sha256 text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT export_artifacts_job_fk FOREIGN KEY (workspace_id, export_job_id)
        REFERENCES export_jobs(workspace_id, id),
    CONSTRAINT export_artifacts_size_check CHECK (byte_size >= 0),
    CONSTRAINT export_artifacts_row_count_check CHECK (row_count >= 0),
    CONSTRAINT export_artifacts_storage_unique UNIQUE (workspace_id, storage_key)
);

CREATE INDEX export_artifacts_workspace_job_idx
    ON export_artifacts (workspace_id, export_job_id, created_at DESC);

CREATE TRIGGER export_artifacts_touch_updated_at
    BEFORE UPDATE ON export_artifacts
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();
