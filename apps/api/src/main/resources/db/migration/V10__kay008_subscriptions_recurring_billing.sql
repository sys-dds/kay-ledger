ALTER TABLE journal_entries
    DROP CONSTRAINT journal_entries_reference_type_check;

ALTER TABLE journal_entries
    ADD CONSTRAINT journal_entries_reference_type_check CHECK (reference_type IN (
        'BOOKING',
        'OFFERING',
        'MANUAL',
        'EXTERNAL',
        'PAYMENT',
        'PAYOUT',
        'REFUND',
        'DISPUTE',
        'SUBSCRIPTION',
        'SUBSCRIPTION_CYCLE'
    ));

CREATE TABLE subscription_plans (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL,
    provider_profile_id uuid NOT NULL,
    plan_code text NOT NULL,
    display_name text NOT NULL,
    billing_interval text NOT NULL,
    currency_code char(3) NOT NULL,
    amount_minor bigint NOT NULL,
    status text NOT NULL DEFAULT 'ACTIVE',
    version integer NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT subscription_plans_workspace_fk FOREIGN KEY (workspace_id)
        REFERENCES workspaces(id),
    CONSTRAINT subscription_plans_provider_fk FOREIGN KEY (workspace_id, provider_profile_id)
        REFERENCES provider_profiles(workspace_id, id),
    CONSTRAINT subscription_plans_code_unique UNIQUE (workspace_id, plan_code),
    CONSTRAINT subscription_plans_interval_check CHECK (billing_interval IN ('MONTHLY', 'YEARLY')),
    CONSTRAINT subscription_plans_currency_check CHECK (currency_code ~ '^[A-Z]{3}$'),
    CONSTRAINT subscription_plans_amount_check CHECK (amount_minor > 0),
    CONSTRAINT subscription_plans_status_check CHECK (status IN ('ACTIVE', 'ARCHIVED'))
);

CREATE UNIQUE INDEX subscription_plans_workspace_id_unique
    ON subscription_plans (workspace_id, id);

CREATE TRIGGER subscription_plans_touch_updated_at
    BEFORE UPDATE ON subscription_plans
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE subscriptions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL,
    customer_profile_id uuid NOT NULL,
    current_plan_id uuid NOT NULL,
    provider_profile_id uuid NOT NULL,
    status text NOT NULL DEFAULT 'PENDING_ACTIVATION',
    start_at timestamptz NOT NULL,
    current_period_start_at timestamptz NOT NULL,
    current_period_end_at timestamptz NOT NULL,
    grace_expires_at timestamptz,
    cancelled_at timestamptz,
    cancellation_effective_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT subscriptions_workspace_fk FOREIGN KEY (workspace_id)
        REFERENCES workspaces(id),
    CONSTRAINT subscriptions_customer_fk FOREIGN KEY (workspace_id, customer_profile_id)
        REFERENCES customer_profiles(workspace_id, id),
    CONSTRAINT subscriptions_current_plan_fk FOREIGN KEY (workspace_id, current_plan_id)
        REFERENCES subscription_plans(workspace_id, id),
    CONSTRAINT subscriptions_provider_fk FOREIGN KEY (workspace_id, provider_profile_id)
        REFERENCES provider_profiles(workspace_id, id),
    CONSTRAINT subscriptions_status_check CHECK (status IN ('PENDING_ACTIVATION', 'ACTIVE', 'GRACE', 'SUSPENDED', 'CANCELLED')),
    CONSTRAINT subscriptions_period_check CHECK (current_period_end_at > current_period_start_at)
);

CREATE UNIQUE INDEX subscriptions_workspace_id_unique
    ON subscriptions (workspace_id, id);

CREATE UNIQUE INDEX subscriptions_active_customer_plan_unique
    ON subscriptions (workspace_id, customer_profile_id, current_plan_id)
    WHERE status IN ('PENDING_ACTIVATION', 'ACTIVE', 'GRACE', 'SUSPENDED');

CREATE INDEX subscriptions_workspace_status_idx
    ON subscriptions (workspace_id, status, current_period_end_at);

CREATE TRIGGER subscriptions_touch_updated_at
    BEFORE UPDATE ON subscriptions
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE subscription_cycles (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL,
    subscription_id uuid NOT NULL,
    cycle_number integer NOT NULL,
    plan_id uuid NOT NULL,
    provider_profile_id uuid NOT NULL,
    customer_profile_id uuid NOT NULL,
    cycle_start_at timestamptz NOT NULL,
    cycle_end_at timestamptz NOT NULL,
    status text NOT NULL DEFAULT 'PENDING_PAYMENT',
    payment_intent_id uuid,
    external_reference text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT subscription_cycles_subscription_fk FOREIGN KEY (workspace_id, subscription_id)
        REFERENCES subscriptions(workspace_id, id),
    CONSTRAINT subscription_cycles_plan_fk FOREIGN KEY (workspace_id, plan_id)
        REFERENCES subscription_plans(workspace_id, id),
    CONSTRAINT subscription_cycles_provider_fk FOREIGN KEY (workspace_id, provider_profile_id)
        REFERENCES provider_profiles(workspace_id, id),
    CONSTRAINT subscription_cycles_customer_fk FOREIGN KEY (workspace_id, customer_profile_id)
        REFERENCES customer_profiles(workspace_id, id),
    CONSTRAINT subscription_cycles_payment_fk FOREIGN KEY (workspace_id, payment_intent_id)
        REFERENCES payment_intents(workspace_id, id),
    CONSTRAINT subscription_cycles_number_check CHECK (cycle_number > 0),
    CONSTRAINT subscription_cycles_period_check CHECK (cycle_end_at > cycle_start_at),
    CONSTRAINT subscription_cycles_status_check CHECK (status IN ('PENDING_PAYMENT', 'PAID', 'FAILED', 'GRACE', 'CANCELLED'))
);

CREATE UNIQUE INDEX subscription_cycles_workspace_id_unique
    ON subscription_cycles (workspace_id, id);

CREATE UNIQUE INDEX subscription_cycles_subscription_number_unique
    ON subscription_cycles (workspace_id, subscription_id, cycle_number);

CREATE UNIQUE INDEX subscription_cycles_payment_unique
    ON subscription_cycles (workspace_id, payment_intent_id)
    WHERE payment_intent_id IS NOT NULL;

CREATE INDEX subscription_cycles_due_idx
    ON subscription_cycles (workspace_id, status, cycle_end_at);

CREATE TRIGGER subscription_cycles_touch_updated_at
    BEFORE UPDATE ON subscription_cycles
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE subscription_plan_changes (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL,
    subscription_id uuid NOT NULL,
    target_plan_id uuid NOT NULL,
    effective_cycle_number integer,
    effective_at timestamptz,
    status text NOT NULL DEFAULT 'SCHEDULED',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT subscription_plan_changes_subscription_fk FOREIGN KEY (workspace_id, subscription_id)
        REFERENCES subscriptions(workspace_id, id),
    CONSTRAINT subscription_plan_changes_target_plan_fk FOREIGN KEY (workspace_id, target_plan_id)
        REFERENCES subscription_plans(workspace_id, id),
    CONSTRAINT subscription_plan_changes_effective_check CHECK (effective_cycle_number IS NOT NULL OR effective_at IS NOT NULL),
    CONSTRAINT subscription_plan_changes_status_check CHECK (status IN ('SCHEDULED', 'APPLIED', 'CANCELLED'))
);

CREATE INDEX subscription_plan_changes_subscription_idx
    ON subscription_plan_changes (workspace_id, subscription_id, status, effective_cycle_number, effective_at);

CREATE TRIGGER subscription_plan_changes_touch_updated_at
    BEFORE UPDATE ON subscription_plan_changes
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE entitlement_grants (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL,
    subscription_id uuid NOT NULL,
    customer_profile_id uuid NOT NULL,
    status text NOT NULL,
    entitlement_code text NOT NULL,
    starts_at timestamptz NOT NULL,
    ends_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT entitlement_grants_subscription_fk FOREIGN KEY (workspace_id, subscription_id)
        REFERENCES subscriptions(workspace_id, id),
    CONSTRAINT entitlement_grants_customer_fk FOREIGN KEY (workspace_id, customer_profile_id)
        REFERENCES customer_profiles(workspace_id, id),
    CONSTRAINT entitlement_grants_status_check CHECK (status IN ('ACTIVE', 'GRACE', 'SUSPENDED', 'CANCELLED'))
);

CREATE UNIQUE INDEX entitlement_grants_subscription_code_unique
    ON entitlement_grants (workspace_id, subscription_id, entitlement_code);

CREATE TRIGGER entitlement_grants_touch_updated_at
    BEFORE UPDATE ON entitlement_grants
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

ALTER TABLE payment_intents
    ALTER COLUMN booking_id DROP NOT NULL,
    ADD COLUMN subscription_id uuid,
    ADD COLUMN subscription_cycle_id uuid;

ALTER TABLE payment_intents
    ADD CONSTRAINT payment_intents_subscription_fk FOREIGN KEY (workspace_id, subscription_id)
        REFERENCES subscriptions(workspace_id, id),
    ADD CONSTRAINT payment_intents_subscription_cycle_fk FOREIGN KEY (workspace_id, subscription_cycle_id)
        REFERENCES subscription_cycles(workspace_id, id);

CREATE UNIQUE INDEX payment_intents_subscription_cycle_unique
    ON payment_intents (workspace_id, subscription_cycle_id)
    WHERE subscription_cycle_id IS NOT NULL;

CREATE INDEX payment_intents_subscription_idx
    ON payment_intents (workspace_id, subscription_id, status, created_at);
