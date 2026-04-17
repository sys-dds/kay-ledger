ALTER TABLE workspace_membership_scopes
    DROP CONSTRAINT workspace_membership_scopes_scope_check;

ALTER TABLE workspace_membership_scopes
    ADD CONSTRAINT workspace_membership_scopes_scope_check CHECK (scope IN (
        'WORKSPACE_READ',
        'ACTOR_READ',
        'MEMBERSHIP_MANAGE',
        'PROFILE_READ',
        'PROFILE_MANAGE',
        'ACCESS_CONTEXT_READ',
        'CATALOG_READ',
        'CATALOG_WRITE',
        'CATALOG_PUBLISH',
        'BOOKING_CREATE',
        'BOOKING_READ',
        'BOOKING_MANAGE',
        'FINANCE_READ',
        'FINANCE_WRITE',
        'PAYMENT_READ',
        'PAYMENT_WRITE'
    ));

INSERT INTO workspace_membership_scopes (membership_id, scope)
SELECT wm.id, payment_scope.scope
FROM workspace_memberships wm
CROSS JOIN LATERAL (
    SELECT unnest(
        CASE
            WHEN wm.role IN ('OWNER', 'ADMIN') THEN ARRAY[
                'PAYMENT_READ',
                'PAYMENT_WRITE'
            ]
            ELSE ARRAY[]::text[]
        END
    ) AS scope
) payment_scope
ON CONFLICT (membership_id, scope) DO NOTHING;

ALTER TABLE financial_accounts
    DROP CONSTRAINT financial_accounts_purpose_check;

ALTER TABLE financial_accounts
    ADD CONSTRAINT financial_accounts_purpose_check CHECK (account_purpose IN (
        'PLATFORM_RECEIVABLE',
        'CASH_PLACEHOLDER',
        'CUSTOMER_PAYABLE',
        'SELLER_PAYABLE',
        'FEE_REVENUE',
        'REFUND_RESERVE',
        'CLEARING',
        'SUSPENSE',
        'AUTHORIZED_FUNDS',
        'CAPTURED_FUNDS',
        'PLATFORM_CLEARING'
    ));

ALTER TABLE journal_entries
    DROP CONSTRAINT journal_entries_reference_type_check;

ALTER TABLE journal_entries
    ADD CONSTRAINT journal_entries_reference_type_check CHECK (reference_type IN (
        'BOOKING',
        'OFFERING',
        'MANUAL',
        'EXTERNAL',
        'PAYMENT'
    ));

CREATE UNIQUE INDEX journal_entries_workspace_id_unique
    ON journal_entries (workspace_id, id);

CREATE TABLE payment_intents (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL,
    booking_id uuid NOT NULL,
    provider_profile_id uuid NOT NULL,
    status text NOT NULL DEFAULT 'CREATED',
    currency_code char(3) NOT NULL,
    gross_amount_minor bigint NOT NULL,
    fee_amount_minor bigint NOT NULL,
    net_amount_minor bigint NOT NULL,
    authorized_amount_minor bigint NOT NULL DEFAULT 0,
    captured_amount_minor bigint NOT NULL DEFAULT 0,
    settled_amount_minor bigint NOT NULL DEFAULT 0,
    external_reference text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT payment_intents_workspace_fk FOREIGN KEY (workspace_id)
        REFERENCES workspaces(id),
    CONSTRAINT payment_intents_workspace_booking_fk FOREIGN KEY (workspace_id, booking_id)
        REFERENCES bookings(workspace_id, id),
    CONSTRAINT payment_intents_workspace_provider_fk FOREIGN KEY (workspace_id, provider_profile_id)
        REFERENCES provider_profiles(workspace_id, id),
    CONSTRAINT payment_intents_status_check CHECK (status IN (
        'CREATED',
        'REQUIRES_ACTION',
        'AUTHORIZED',
        'CAPTURED',
        'CANCELLED',
        'FAILED',
        'SETTLED'
    )),
    CONSTRAINT payment_intents_currency_check CHECK (currency_code ~ '^[A-Z]{3}$'),
    CONSTRAINT payment_intents_amounts_check CHECK (
        gross_amount_minor >= 0
        AND fee_amount_minor >= 0
        AND net_amount_minor >= 0
        AND gross_amount_minor = fee_amount_minor + net_amount_minor
        AND authorized_amount_minor >= 0
        AND captured_amount_minor >= 0
        AND settled_amount_minor >= 0
        AND authorized_amount_minor <= gross_amount_minor
        AND captured_amount_minor <= authorized_amount_minor
        AND settled_amount_minor <= captured_amount_minor
    )
);

CREATE UNIQUE INDEX payment_intents_workspace_booking_unique
    ON payment_intents (workspace_id, booking_id);

CREATE UNIQUE INDEX payment_intents_workspace_id_unique
    ON payment_intents (workspace_id, id);

CREATE INDEX payment_intents_workspace_status_idx
    ON payment_intents (workspace_id, status, created_at);

CREATE INDEX payment_intents_workspace_provider_idx
    ON payment_intents (workspace_id, provider_profile_id, status);

CREATE TRIGGER payment_intents_touch_updated_at
    BEFORE UPDATE ON payment_intents
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE payment_attempts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL,
    payment_intent_id uuid NOT NULL,
    attempt_type text NOT NULL,
    status text NOT NULL,
    amount_minor bigint NOT NULL,
    external_reference text,
    journal_entry_id uuid,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT payment_attempts_workspace_fk FOREIGN KEY (workspace_id)
        REFERENCES workspaces(id),
    CONSTRAINT payment_attempts_workspace_intent_fk FOREIGN KEY (workspace_id, payment_intent_id)
        REFERENCES payment_intents(workspace_id, id),
    CONSTRAINT payment_attempts_workspace_journal_fk FOREIGN KEY (workspace_id, journal_entry_id)
        REFERENCES journal_entries(workspace_id, id),
    CONSTRAINT payment_attempts_type_check CHECK (attempt_type IN ('AUTHORIZE', 'CAPTURE', 'CANCEL', 'SETTLE')),
    CONSTRAINT payment_attempts_status_check CHECK (status IN ('SUCCEEDED', 'FAILED')),
    CONSTRAINT payment_attempts_amount_check CHECK (amount_minor >= 0)
);

CREATE INDEX payment_attempts_intent_idx
    ON payment_attempts (payment_intent_id, created_at);

CREATE INDEX payment_attempts_workspace_type_idx
    ON payment_attempts (workspace_id, attempt_type, created_at);

CREATE TABLE provider_payable_balances (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL,
    provider_profile_id uuid NOT NULL,
    currency_code char(3) NOT NULL,
    payable_amount_minor bigint NOT NULL DEFAULT 0,
    source text NOT NULL DEFAULT 'LEDGER_DERIVED',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT provider_payable_balances_workspace_fk FOREIGN KEY (workspace_id)
        REFERENCES workspaces(id),
    CONSTRAINT provider_payable_balances_provider_fk FOREIGN KEY (workspace_id, provider_profile_id)
        REFERENCES provider_profiles(workspace_id, id),
    CONSTRAINT provider_payable_balances_currency_check CHECK (currency_code ~ '^[A-Z]{3}$'),
    CONSTRAINT provider_payable_balances_amount_check CHECK (payable_amount_minor >= 0),
    CONSTRAINT provider_payable_balances_source_check CHECK (source IN ('LEDGER_DERIVED'))
);

CREATE UNIQUE INDEX provider_payable_balances_provider_currency_unique
    ON provider_payable_balances (workspace_id, provider_profile_id, currency_code);

CREATE TRIGGER provider_payable_balances_touch_updated_at
    BEFORE UPDATE ON provider_payable_balances
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();
