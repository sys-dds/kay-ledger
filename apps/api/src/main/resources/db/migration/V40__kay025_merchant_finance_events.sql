CREATE TABLE merchant_finance_event_endpoints (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    provider_profile_id uuid,
    endpoint_url text NOT NULL,
    signing_secret_ref text NOT NULL,
    status text NOT NULL DEFAULT 'ACTIVE',
    event_types text[] NOT NULL DEFAULT ARRAY[]::text[],
    created_by_actor_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT merchant_finance_event_endpoints_workspace_id_unique UNIQUE (workspace_id, id),
    CONSTRAINT merchant_finance_event_endpoints_provider_fk FOREIGN KEY (workspace_id, provider_profile_id)
        REFERENCES provider_profiles(workspace_id, id),
    CONSTRAINT merchant_finance_event_endpoints_actor_fk FOREIGN KEY (workspace_id, created_by_actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id),
    CONSTRAINT merchant_finance_event_endpoints_status_check CHECK (status IN ('ACTIVE', 'PAUSED', 'DISABLED')),
    CONSTRAINT merchant_finance_event_endpoints_url_check CHECK (endpoint_url ~ '^https?://')
);

CREATE INDEX merchant_finance_event_endpoints_workspace_status_idx
    ON merchant_finance_event_endpoints (workspace_id, status, created_at DESC);

CREATE INDEX merchant_finance_event_endpoints_provider_idx
    ON merchant_finance_event_endpoints (workspace_id, provider_profile_id, status);

CREATE TRIGGER merchant_finance_event_endpoints_touch_updated_at
    BEFORE UPDATE ON merchant_finance_event_endpoints
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE merchant_finance_events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    provider_profile_id uuid,
    currency_code char(3),
    accounting_period_id uuid,
    event_type text NOT NULL,
    source_reference_type text NOT NULL,
    source_reference_id uuid NOT NULL,
    payload_json jsonb NOT NULL,
    event_key text NOT NULL,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT merchant_finance_events_workspace_id_unique UNIQUE (workspace_id, id),
    CONSTRAINT merchant_finance_events_provider_fk FOREIGN KEY (workspace_id, provider_profile_id)
        REFERENCES provider_profiles(workspace_id, id),
    CONSTRAINT merchant_finance_events_period_fk FOREIGN KEY (workspace_id, accounting_period_id)
        REFERENCES financial_accounting_periods(workspace_id, id),
    CONSTRAINT merchant_finance_events_currency_check CHECK (currency_code IS NULL OR currency_code ~ '^[A-Z]{3}$'),
    CONSTRAINT merchant_finance_events_unique UNIQUE (workspace_id, event_key)
);

CREATE INDEX merchant_finance_events_type_idx
    ON merchant_finance_events (workspace_id, event_type, occurred_at DESC);

CREATE INDEX merchant_finance_events_source_idx
    ON merchant_finance_events (workspace_id, source_reference_type, source_reference_id);

CREATE TABLE merchant_finance_event_deliveries (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    merchant_finance_event_id uuid NOT NULL,
    endpoint_id uuid NOT NULL,
    delivery_status text NOT NULL DEFAULT 'PENDING',
    attempt_count integer NOT NULL DEFAULT 0,
    last_attempt_at timestamptz,
    next_attempt_at timestamptz,
    response_status integer,
    response_body text,
    signature_algorithm text NOT NULL DEFAULT 'HMAC_SHA256',
    signature_value text,
    dedupe_key text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT merchant_finance_event_deliveries_event_fk FOREIGN KEY (workspace_id, merchant_finance_event_id)
        REFERENCES merchant_finance_events(workspace_id, id) ON DELETE CASCADE,
    CONSTRAINT merchant_finance_event_deliveries_endpoint_fk FOREIGN KEY (workspace_id, endpoint_id)
        REFERENCES merchant_finance_event_endpoints(workspace_id, id) ON DELETE CASCADE,
    CONSTRAINT merchant_finance_event_deliveries_status_check CHECK (delivery_status IN ('PENDING', 'SUCCEEDED', 'FAILED', 'PARKED')),
    CONSTRAINT merchant_finance_event_deliveries_attempt_check CHECK (attempt_count >= 0),
    CONSTRAINT merchant_finance_event_deliveries_unique UNIQUE (workspace_id, dedupe_key)
);

CREATE INDEX merchant_finance_event_deliveries_status_idx
    ON merchant_finance_event_deliveries (workspace_id, delivery_status, next_attempt_at, updated_at DESC);

CREATE INDEX merchant_finance_event_deliveries_event_idx
    ON merchant_finance_event_deliveries (workspace_id, merchant_finance_event_id, created_at);

CREATE TRIGGER merchant_finance_event_deliveries_touch_updated_at
    BEFORE UPDATE ON merchant_finance_event_deliveries
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();
