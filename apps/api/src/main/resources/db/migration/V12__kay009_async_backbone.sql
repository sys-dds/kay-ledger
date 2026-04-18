CREATE TABLE outbox_events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid,
    aggregate_type text NOT NULL,
    aggregate_id uuid NOT NULL,
    event_type text NOT NULL,
    payload_json jsonb NOT NULL,
    dedupe_key text NOT NULL,
    status text NOT NULL DEFAULT 'PENDING',
    available_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    retry_count integer NOT NULL DEFAULT 0,
    last_error text,
    parked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT outbox_events_workspace_fk FOREIGN KEY (workspace_id)
        REFERENCES workspaces(id),
    CONSTRAINT outbox_events_status_check CHECK (status IN ('PENDING', 'CLAIMED', 'PUBLISHED', 'PARKED')),
    CONSTRAINT outbox_events_retry_check CHECK (retry_count >= 0)
);

CREATE UNIQUE INDEX outbox_events_dedupe_unique
    ON outbox_events (dedupe_key);

CREATE INDEX outbox_events_due_idx
    ON outbox_events (status, available_at, created_at);

CREATE INDEX outbox_events_workspace_status_idx
    ON outbox_events (workspace_id, status, created_at);

CREATE TRIGGER outbox_events_touch_updated_at
    BEFORE UPDATE ON outbox_events
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE inbox_messages (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid,
    topic text NOT NULL,
    partition_id integer NOT NULL,
    message_key text NOT NULL,
    event_id uuid,
    dedupe_key text NOT NULL,
    consumer_name text NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    outcome text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT inbox_messages_workspace_fk FOREIGN KEY (workspace_id)
        REFERENCES workspaces(id),
    CONSTRAINT inbox_messages_outcome_check CHECK (outcome IN ('PROCESSED', 'DUPLICATE', 'FAILED'))
);

CREATE UNIQUE INDEX inbox_messages_consumer_dedupe_unique
    ON inbox_messages (consumer_name, dedupe_key);

CREATE INDEX inbox_messages_workspace_consumer_idx
    ON inbox_messages (workspace_id, consumer_name, processed_at);

CREATE TABLE subscription_projection (
    workspace_id uuid NOT NULL,
    subscription_id uuid NOT NULL,
    latest_status text NOT NULL,
    latest_entitlement_status text,
    current_cycle_number integer,
    current_plan_id uuid,
    next_renewal_boundary timestamptz,
    last_renewal_outcome text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, subscription_id),
    CONSTRAINT subscription_projection_subscription_fk FOREIGN KEY (workspace_id, subscription_id)
        REFERENCES subscriptions(workspace_id, id)
);

CREATE INDEX subscription_projection_workspace_status_idx
    ON subscription_projection (workspace_id, latest_status, updated_at);

CREATE TABLE payment_projection (
    workspace_id uuid NOT NULL,
    payment_intent_id uuid NOT NULL,
    booking_id uuid,
    subscription_id uuid,
    subscription_cycle_id uuid,
    latest_payment_status text NOT NULL,
    gross_amount_minor bigint NOT NULL,
    fee_amount_minor bigint NOT NULL,
    net_amount_minor bigint NOT NULL,
    provider_profile_id uuid NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, payment_intent_id),
    CONSTRAINT payment_projection_payment_fk FOREIGN KEY (workspace_id, payment_intent_id)
        REFERENCES payment_intents(workspace_id, id)
);

CREATE INDEX payment_projection_subscription_idx
    ON payment_projection (workspace_id, subscription_id, subscription_cycle_id);

CREATE INDEX payment_projection_provider_status_idx
    ON payment_projection (workspace_id, provider_profile_id, latest_payment_status);
