CREATE TABLE provider_configs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    provider_key text NOT NULL,
    display_name text NOT NULL,
    signing_secret text NOT NULL,
    status text NOT NULL DEFAULT 'ACTIVE',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT provider_configs_status_check CHECK (status IN ('ACTIVE', 'DISABLED')),
    CONSTRAINT provider_configs_workspace_key_unique UNIQUE (workspace_id, provider_key)
);

CREATE TRIGGER provider_configs_touch_updated_at
    BEFORE UPDATE ON provider_configs
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE provider_callbacks (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    provider_config_id uuid NOT NULL REFERENCES provider_configs(id),
    provider_key text NOT NULL,
    provider_event_id text NOT NULL,
    provider_sequence bigint,
    callback_type text NOT NULL,
    business_reference_type text NOT NULL,
    business_reference_id uuid NOT NULL,
    payload_json jsonb NOT NULL,
    signature_header text NOT NULL,
    signature_verified boolean NOT NULL DEFAULT false,
    dedupe_key text NOT NULL,
    processing_status text NOT NULL DEFAULT 'RECEIVED',
    processing_error text,
    applied_at timestamptz,
    received_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT provider_callbacks_status_check CHECK (processing_status IN ('RECEIVED', 'DUPLICATE', 'APPLIED', 'IGNORED_OUT_OF_ORDER', 'FAILED')),
    CONSTRAINT provider_callbacks_reference_check CHECK (business_reference_type IN ('PAYMENT_INTENT', 'PAYOUT_REQUEST', 'REFUND')),
    CONSTRAINT provider_callbacks_dedupe_unique UNIQUE (workspace_id, provider_key, dedupe_key)
);

CREATE INDEX provider_callbacks_workspace_status_idx
    ON provider_callbacks (workspace_id, processing_status, received_at);

CREATE INDEX provider_callbacks_reference_idx
    ON provider_callbacks (workspace_id, business_reference_type, business_reference_id, provider_sequence);

CREATE TRIGGER provider_callbacks_touch_updated_at
    BEFORE UPDATE ON provider_callbacks
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();
