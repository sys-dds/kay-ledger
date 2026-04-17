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
        'FINANCE_WRITE'
    ));

INSERT INTO workspace_membership_scopes (membership_id, scope)
SELECT wm.id, finance_scope.scope
FROM workspace_memberships wm
CROSS JOIN LATERAL (
    SELECT unnest(
        CASE
            WHEN wm.role IN ('OWNER', 'ADMIN') THEN ARRAY[
                'FINANCE_READ',
                'FINANCE_WRITE'
            ]
            ELSE ARRAY[]::text[]
        END
    ) AS scope
) finance_scope
ON CONFLICT (membership_id, scope) DO NOTHING;

CREATE TABLE financial_accounts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid,
    account_code text NOT NULL,
    account_name text NOT NULL,
    account_type text NOT NULL,
    account_purpose text NOT NULL,
    currency_code char(3) NOT NULL,
    status text NOT NULL DEFAULT 'ACTIVE',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT financial_accounts_workspace_fk FOREIGN KEY (workspace_id)
        REFERENCES workspaces(id),
    CONSTRAINT financial_accounts_type_check CHECK (account_type IN ('ASSET', 'LIABILITY', 'REVENUE', 'EXPENSE', 'EQUITY')),
    CONSTRAINT financial_accounts_purpose_check CHECK (account_purpose IN (
        'PLATFORM_RECEIVABLE',
        'CASH_PLACEHOLDER',
        'CUSTOMER_PAYABLE',
        'SELLER_PAYABLE',
        'FEE_REVENUE',
        'REFUND_RESERVE',
        'CLEARING',
        'SUSPENSE'
    )),
    CONSTRAINT financial_accounts_currency_check CHECK (currency_code ~ '^[A-Z]{3}$'),
    CONSTRAINT financial_accounts_status_check CHECK (status IN ('ACTIVE', 'DISABLED'))
);

CREATE UNIQUE INDEX financial_accounts_workspace_code_unique
    ON financial_accounts (
        COALESCE(workspace_id, '00000000-0000-0000-0000-000000000000'::uuid),
        account_code
    );

CREATE INDEX financial_accounts_workspace_idx ON financial_accounts (workspace_id, status);

CREATE TRIGGER financial_accounts_touch_updated_at
    BEFORE UPDATE ON financial_accounts
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE fee_rules (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL,
    offering_id uuid,
    rule_type text NOT NULL,
    flat_amount_minor bigint,
    basis_points integer,
    currency_code char(3) NOT NULL,
    status text NOT NULL DEFAULT 'ACTIVE',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fee_rules_workspace_fk FOREIGN KEY (workspace_id)
        REFERENCES workspaces(id),
    CONSTRAINT fee_rules_workspace_offering_fk FOREIGN KEY (workspace_id, offering_id)
        REFERENCES offerings(workspace_id, id),
    CONSTRAINT fee_rules_rule_type_check CHECK (rule_type IN ('FLAT', 'BASIS_POINTS', 'COMBINED')),
    CONSTRAINT fee_rules_flat_amount_check CHECK (flat_amount_minor IS NULL OR flat_amount_minor >= 0),
    CONSTRAINT fee_rules_basis_points_check CHECK (basis_points IS NULL OR (basis_points >= 0 AND basis_points <= 10000)),
    CONSTRAINT fee_rules_currency_check CHECK (currency_code ~ '^[A-Z]{3}$'),
    CONSTRAINT fee_rules_status_check CHECK (status IN ('ACTIVE', 'DISABLED')),
    CONSTRAINT fee_rules_shape_check CHECK (
        (rule_type = 'FLAT' AND flat_amount_minor IS NOT NULL AND basis_points IS NULL)
        OR
        (rule_type = 'BASIS_POINTS' AND flat_amount_minor IS NULL AND basis_points IS NOT NULL)
        OR
        (rule_type = 'COMBINED' AND flat_amount_minor IS NOT NULL AND basis_points IS NOT NULL)
    )
);

CREATE INDEX fee_rules_workspace_offering_idx ON fee_rules (workspace_id, offering_id, status);

CREATE TRIGGER fee_rules_touch_updated_at
    BEFORE UPDATE ON fee_rules
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE journal_entries (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL,
    reference_type text NOT NULL,
    reference_id uuid,
    offering_id uuid,
    booking_id uuid,
    external_reference text,
    description text NOT NULL,
    status text NOT NULL DEFAULT 'POSTED',
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT journal_entries_workspace_fk FOREIGN KEY (workspace_id)
        REFERENCES workspaces(id),
    CONSTRAINT journal_entries_workspace_offering_fk FOREIGN KEY (workspace_id, offering_id)
        REFERENCES offerings(workspace_id, id),
    CONSTRAINT journal_entries_workspace_booking_fk FOREIGN KEY (workspace_id, booking_id)
        REFERENCES bookings(workspace_id, id),
    CONSTRAINT journal_entries_status_check CHECK (status IN ('POSTED')),
    CONSTRAINT journal_entries_reference_type_check CHECK (reference_type IN ('BOOKING', 'OFFERING', 'MANUAL', 'EXTERNAL'))
);

CREATE INDEX journal_entries_workspace_idx ON journal_entries (workspace_id, created_at);
CREATE INDEX journal_entries_reference_idx ON journal_entries (workspace_id, reference_type, reference_id);
CREATE INDEX journal_entries_booking_idx ON journal_entries (workspace_id, booking_id);

CREATE TABLE journal_postings (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    journal_entry_id uuid NOT NULL,
    account_id uuid NOT NULL,
    entry_side text NOT NULL,
    amount_minor bigint NOT NULL,
    currency_code char(3) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT journal_postings_entry_fk FOREIGN KEY (journal_entry_id)
        REFERENCES journal_entries(id) ON DELETE RESTRICT,
    CONSTRAINT journal_postings_account_fk FOREIGN KEY (account_id)
        REFERENCES financial_accounts(id),
    CONSTRAINT journal_postings_side_check CHECK (entry_side IN ('DEBIT', 'CREDIT')),
    CONSTRAINT journal_postings_amount_check CHECK (amount_minor > 0),
    CONSTRAINT journal_postings_currency_check CHECK (currency_code ~ '^[A-Z]{3}$')
);

CREATE INDEX journal_postings_entry_idx ON journal_postings (journal_entry_id);
CREATE INDEX journal_postings_account_idx ON journal_postings (account_id);

ALTER TABLE bookings
    ADD COLUMN currency_code char(3),
    ADD COLUMN gross_amount_minor bigint,
    ADD COLUMN fee_amount_minor bigint,
    ADD COLUMN net_amount_minor bigint,
    ADD COLUMN financial_reference_id uuid,
    ADD CONSTRAINT bookings_financial_currency_check CHECK (currency_code IS NULL OR currency_code ~ '^[A-Z]{3}$'),
    ADD CONSTRAINT bookings_financial_amounts_check CHECK (
        (currency_code IS NULL AND gross_amount_minor IS NULL AND fee_amount_minor IS NULL AND net_amount_minor IS NULL)
        OR
        (
            currency_code IS NOT NULL
            AND gross_amount_minor IS NOT NULL
            AND fee_amount_minor IS NOT NULL
            AND net_amount_minor IS NOT NULL
            AND gross_amount_minor >= 0
            AND fee_amount_minor >= 0
            AND net_amount_minor >= 0
            AND gross_amount_minor = fee_amount_minor + net_amount_minor
        )
    ),
    ADD CONSTRAINT bookings_financial_reference_fk FOREIGN KEY (financial_reference_id)
        REFERENCES journal_entries(id);
