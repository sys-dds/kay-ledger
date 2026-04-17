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
        'PLATFORM_CLEARING',
        'DISPUTE_RESERVE',
        'REFUND_LIABILITY',
        'PAYOUT_CLEARING',
        'FROZEN_PAYABLE'
    ));

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
        'DISPUTE'
    ));

CREATE TABLE payout_requests (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL,
    provider_profile_id uuid NOT NULL,
    currency_code char(3) NOT NULL,
    requested_amount_minor bigint NOT NULL,
    status text NOT NULL DEFAULT 'REQUESTED',
    failure_reason text,
    journal_entry_id uuid,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT payout_requests_workspace_fk FOREIGN KEY (workspace_id)
        REFERENCES workspaces(id),
    CONSTRAINT payout_requests_provider_fk FOREIGN KEY (workspace_id, provider_profile_id)
        REFERENCES provider_profiles(workspace_id, id),
    CONSTRAINT payout_requests_journal_fk FOREIGN KEY (workspace_id, journal_entry_id)
        REFERENCES journal_entries(workspace_id, id),
    CONSTRAINT payout_requests_currency_check CHECK (currency_code ~ '^[A-Z]{3}$'),
    CONSTRAINT payout_requests_amount_check CHECK (requested_amount_minor > 0),
    CONSTRAINT payout_requests_status_check CHECK (status IN ('REQUESTED', 'PROCESSING', 'SUCCEEDED', 'FAILED', 'CANCELLED'))
);

CREATE UNIQUE INDEX payout_requests_workspace_id_unique
    ON payout_requests (workspace_id, id);

CREATE INDEX payout_requests_workspace_provider_idx
    ON payout_requests (workspace_id, provider_profile_id, status, created_at);

CREATE TRIGGER payout_requests_touch_updated_at
    BEFORE UPDATE ON payout_requests
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE payout_attempts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL,
    payout_request_id uuid NOT NULL,
    attempt_number integer NOT NULL,
    status text NOT NULL,
    failure_reason text,
    external_reference text,
    journal_entry_id uuid,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT payout_attempts_payout_fk FOREIGN KEY (workspace_id, payout_request_id)
        REFERENCES payout_requests(workspace_id, id),
    CONSTRAINT payout_attempts_journal_fk FOREIGN KEY (workspace_id, journal_entry_id)
        REFERENCES journal_entries(workspace_id, id),
    CONSTRAINT payout_attempts_number_check CHECK (attempt_number > 0),
    CONSTRAINT payout_attempts_status_check CHECK (status IN ('PROCESSING', 'SUCCEEDED', 'FAILED'))
);

CREATE UNIQUE INDEX payout_attempts_request_number_unique
    ON payout_attempts (workspace_id, payout_request_id, attempt_number);

CREATE TABLE refunds (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL,
    payment_intent_id uuid NOT NULL,
    booking_id uuid NOT NULL,
    refund_type text NOT NULL,
    amount_minor bigint NOT NULL,
    payable_reduction_amount_minor bigint NOT NULL,
    status text NOT NULL DEFAULT 'SUCCEEDED',
    journal_entry_id uuid,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT refunds_intent_fk FOREIGN KEY (workspace_id, payment_intent_id)
        REFERENCES payment_intents(workspace_id, id),
    CONSTRAINT refunds_booking_fk FOREIGN KEY (workspace_id, booking_id)
        REFERENCES bookings(workspace_id, id),
    CONSTRAINT refunds_journal_fk FOREIGN KEY (workspace_id, journal_entry_id)
        REFERENCES journal_entries(workspace_id, id),
    CONSTRAINT refunds_type_check CHECK (refund_type IN ('FULL', 'PARTIAL', 'REVERSAL')),
    CONSTRAINT refunds_amount_check CHECK (amount_minor > 0),
    CONSTRAINT refunds_payable_reduction_check CHECK (payable_reduction_amount_minor >= 0),
    CONSTRAINT refunds_status_check CHECK (status IN ('SUCCEEDED', 'FAILED'))
);

CREATE UNIQUE INDEX refunds_workspace_id_unique
    ON refunds (workspace_id, id);

CREATE INDEX refunds_intent_idx
    ON refunds (workspace_id, payment_intent_id, refund_type, created_at);

CREATE TABLE refund_attempts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL,
    refund_id uuid NOT NULL,
    status text NOT NULL,
    failure_reason text,
    external_reference text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT refund_attempts_refund_fk FOREIGN KEY (workspace_id, refund_id)
        REFERENCES refunds(workspace_id, id),
    CONSTRAINT refund_attempts_status_check CHECK (status IN ('SUCCEEDED', 'FAILED'))
);

CREATE TABLE disputes (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL,
    payment_intent_id uuid NOT NULL,
    booking_id uuid NOT NULL,
    disputed_amount_minor bigint NOT NULL,
    frozen_amount_minor bigint NOT NULL,
    status text NOT NULL DEFAULT 'OPEN',
    resolution text,
    open_journal_entry_id uuid,
    resolve_journal_entry_id uuid,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT disputes_intent_fk FOREIGN KEY (workspace_id, payment_intent_id)
        REFERENCES payment_intents(workspace_id, id),
    CONSTRAINT disputes_booking_fk FOREIGN KEY (workspace_id, booking_id)
        REFERENCES bookings(workspace_id, id),
    CONSTRAINT disputes_open_journal_fk FOREIGN KEY (workspace_id, open_journal_entry_id)
        REFERENCES journal_entries(workspace_id, id),
    CONSTRAINT disputes_resolve_journal_fk FOREIGN KEY (workspace_id, resolve_journal_entry_id)
        REFERENCES journal_entries(workspace_id, id),
    CONSTRAINT disputes_status_check CHECK (status IN ('OPEN', 'WON', 'LOST', 'CLOSED')),
    CONSTRAINT disputes_resolution_check CHECK (resolution IS NULL OR resolution IN ('WON', 'LOST', 'CLOSED')),
    CONSTRAINT disputes_amount_check CHECK (disputed_amount_minor > 0 AND frozen_amount_minor >= 0)
);

CREATE UNIQUE INDEX disputes_workspace_id_unique
    ON disputes (workspace_id, id);

CREATE INDEX disputes_intent_idx
    ON disputes (workspace_id, payment_intent_id, status, created_at);

CREATE TABLE frozen_funds (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL,
    provider_profile_id uuid NOT NULL,
    dispute_id uuid NOT NULL,
    currency_code char(3) NOT NULL,
    amount_minor bigint NOT NULL,
    status text NOT NULL DEFAULT 'FROZEN',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT frozen_funds_provider_fk FOREIGN KEY (workspace_id, provider_profile_id)
        REFERENCES provider_profiles(workspace_id, id),
    CONSTRAINT frozen_funds_dispute_fk FOREIGN KEY (workspace_id, dispute_id)
        REFERENCES disputes(workspace_id, id),
    CONSTRAINT frozen_funds_currency_check CHECK (currency_code ~ '^[A-Z]{3}$'),
    CONSTRAINT frozen_funds_amount_check CHECK (amount_minor > 0),
    CONSTRAINT frozen_funds_status_check CHECK (status IN ('FROZEN', 'RELEASED', 'CONSUMED'))
);

CREATE INDEX frozen_funds_provider_idx
    ON frozen_funds (workspace_id, provider_profile_id, currency_code, status);

CREATE TRIGGER frozen_funds_touch_updated_at
    BEFORE UPDATE ON frozen_funds
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();
