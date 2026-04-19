CREATE TABLE region_investigation_read_snapshots (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    source_region text NOT NULL,
    target_region text NOT NULL,
    replication_event_id uuid NOT NULL,
    document_id text NOT NULL,
    document_type text NOT NULL,
    reference_type text NOT NULL,
    reference_id text NOT NULL,
    provider_profile_id text,
    payment_intent_id text,
    refund_id text,
    payout_request_id text,
    dispute_id text,
    subscription_id text,
    provider_event_id text,
    external_reference text,
    business_reference_id text,
    status text,
    occurred_at timestamptz,
    payload_json jsonb NOT NULL,
    source_updated_at timestamptz NOT NULL,
    replicated_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT region_investigation_read_snapshots_regions_check CHECK (source_region <> target_region),
    CONSTRAINT region_investigation_read_snapshots_event_unique UNIQUE (replication_event_id),
    CONSTRAINT region_investigation_read_snapshots_document_unique UNIQUE (target_region, workspace_id, document_id)
);

CREATE INDEX region_investigation_read_snapshots_reference_idx
    ON region_investigation_read_snapshots (target_region, workspace_id, reference_id);

CREATE INDEX region_investigation_read_snapshots_provider_event_idx
    ON region_investigation_read_snapshots (target_region, workspace_id, provider_event_id);

CREATE INDEX region_investigation_read_snapshots_external_reference_idx
    ON region_investigation_read_snapshots (target_region, workspace_id, external_reference);

CREATE TRIGGER region_investigation_read_snapshots_touch_updated_at
    BEFORE UPDATE ON region_investigation_read_snapshots
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE region_provider_summary_snapshots (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    source_region text NOT NULL,
    target_region text NOT NULL,
    replication_event_id uuid NOT NULL,
    provider_profile_id uuid NOT NULL,
    currency_code text NOT NULL,
    settled_gross_amount_minor bigint NOT NULL,
    fee_amount_minor bigint NOT NULL,
    net_earnings_amount_minor bigint NOT NULL,
    current_payout_requested_amount_minor bigint NOT NULL,
    payout_succeeded_amount_minor bigint NOT NULL,
    refund_amount_minor bigint NOT NULL,
    active_dispute_exposure_amount_minor bigint NOT NULL,
    settled_subscription_net_revenue_amount_minor bigint NOT NULL,
    source_refreshed_at timestamptz NOT NULL,
    replicated_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT region_provider_summary_snapshots_regions_check CHECK (source_region <> target_region),
    CONSTRAINT region_provider_summary_snapshots_event_unique UNIQUE (replication_event_id),
    CONSTRAINT region_provider_summary_snapshots_summary_unique UNIQUE (target_region, workspace_id, provider_profile_id, currency_code)
);

CREATE INDEX region_provider_summary_snapshots_workspace_idx
    ON region_provider_summary_snapshots (target_region, workspace_id, provider_profile_id, currency_code);

CREATE TRIGGER region_provider_summary_snapshots_touch_updated_at
    BEFORE UPDATE ON region_provider_summary_snapshots
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();
