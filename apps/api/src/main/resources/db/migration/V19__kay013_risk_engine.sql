CREATE TABLE risk_flags (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    reference_type text NOT NULL,
    reference_id uuid NOT NULL,
    rule_code text NOT NULL,
    severity text NOT NULL,
    status text NOT NULL DEFAULT 'OPEN',
    reason text NOT NULL,
    signal_count integer NOT NULL DEFAULT 1,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT risk_flags_reference_check CHECK (reference_type IN (
        'PAYMENT_INTENT',
        'REFUND',
        'PAYOUT_REQUEST',
        'DISPUTE',
        'PROVIDER_PROFILE',
        'RECONCILIATION_MISMATCH',
        'WORKSPACE'
    )),
    CONSTRAINT risk_flags_severity_check CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    CONSTRAINT risk_flags_status_check CHECK (status IN ('OPEN', 'IN_REVIEW', 'RESOLVED', 'DISMISSED')),
    CONSTRAINT risk_flags_signal_count_check CHECK (signal_count > 0),
    CONSTRAINT risk_flags_workspace_id_unique UNIQUE (workspace_id, id),
    CONSTRAINT risk_flags_unique UNIQUE (workspace_id, reference_type, reference_id, rule_code)
);

CREATE INDEX risk_flags_workspace_status_idx
    ON risk_flags (workspace_id, status, severity, updated_at);

CREATE INDEX risk_flags_reference_idx
    ON risk_flags (workspace_id, reference_type, reference_id);

CREATE TRIGGER risk_flags_touch_updated_at
    BEFORE UPDATE ON risk_flags
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE risk_reviews (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    risk_flag_id uuid NOT NULL,
    status text NOT NULL DEFAULT 'OPEN',
    assigned_actor_id uuid,
    opened_at timestamptz NOT NULL DEFAULT now(),
    closed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT risk_reviews_flag_fk FOREIGN KEY (workspace_id, risk_flag_id)
        REFERENCES risk_flags(workspace_id, id),
    CONSTRAINT risk_reviews_assigned_actor_fk FOREIGN KEY (workspace_id, assigned_actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id),
    CONSTRAINT risk_reviews_status_check CHECK (status IN ('OPEN', 'IN_REVIEW', 'RESOLVED', 'DISMISSED')),
    CONSTRAINT risk_reviews_workspace_id_unique UNIQUE (workspace_id, id),
    CONSTRAINT risk_reviews_flag_unique UNIQUE (workspace_id, risk_flag_id)
);

CREATE INDEX risk_reviews_workspace_status_idx
    ON risk_reviews (workspace_id, status, updated_at);

CREATE TRIGGER risk_reviews_touch_updated_at
    BEFORE UPDATE ON risk_reviews
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE risk_decisions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    risk_flag_id uuid NOT NULL,
    review_id uuid,
    reference_type text NOT NULL,
    reference_id uuid NOT NULL,
    outcome text NOT NULL,
    reason text NOT NULL,
    decided_by_actor_id uuid NOT NULL,
    decided_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT risk_decisions_flag_fk FOREIGN KEY (workspace_id, risk_flag_id)
        REFERENCES risk_flags(workspace_id, id),
    CONSTRAINT risk_decisions_review_fk FOREIGN KEY (workspace_id, review_id)
        REFERENCES risk_reviews(workspace_id, id),
    CONSTRAINT risk_decisions_actor_fk FOREIGN KEY (workspace_id, decided_by_actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id),
    CONSTRAINT risk_decisions_reference_check CHECK (reference_type IN (
        'PAYMENT_INTENT',
        'REFUND',
        'PAYOUT_REQUEST',
        'DISPUTE',
        'PROVIDER_PROFILE',
        'RECONCILIATION_MISMATCH',
        'WORKSPACE'
    )),
    CONSTRAINT risk_decisions_outcome_check CHECK (outcome IN ('ALLOW', 'REVIEW', 'BLOCK'))
);

CREATE INDEX risk_decisions_workspace_reference_idx
    ON risk_decisions (workspace_id, reference_type, reference_id, decided_at DESC);

CREATE INDEX risk_decisions_flag_idx
    ON risk_decisions (workspace_id, risk_flag_id, decided_at DESC);
