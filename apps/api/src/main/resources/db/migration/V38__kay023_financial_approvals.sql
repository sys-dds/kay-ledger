CREATE TABLE financial_approval_requests (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    action_type text NOT NULL,
    target_type text NOT NULL,
    target_id uuid,
    provider_profile_id uuid,
    currency_code char(3),
    amount_minor bigint,
    status text NOT NULL DEFAULT 'REQUESTED',
    requested_by_actor_id uuid NOT NULL,
    reason text NOT NULL,
    request_payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT financial_approval_requests_workspace_id_unique UNIQUE (workspace_id, id),
    CONSTRAINT financial_approval_requests_requester_fk FOREIGN KEY (workspace_id, requested_by_actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id),
    CONSTRAINT financial_approval_requests_provider_fk FOREIGN KEY (workspace_id, provider_profile_id)
        REFERENCES provider_profiles(workspace_id, id),
    CONSTRAINT financial_approval_requests_currency_check CHECK (currency_code IS NULL OR currency_code ~ '^[A-Z]{3}$'),
    CONSTRAINT financial_approval_requests_amount_check CHECK (amount_minor IS NULL OR amount_minor >= 0),
    CONSTRAINT financial_approval_requests_status_check CHECK (status IN ('REQUESTED', 'APPROVED', 'REJECTED', 'EXECUTED', 'CANCELLED'))
);

CREATE INDEX financial_approval_requests_workspace_status_idx
    ON financial_approval_requests (workspace_id, status, created_at DESC);

CREATE INDEX financial_approval_requests_target_idx
    ON financial_approval_requests (workspace_id, target_type, target_id);

CREATE INDEX financial_approval_requests_provider_idx
    ON financial_approval_requests (workspace_id, provider_profile_id, currency_code, status);

CREATE TRIGGER financial_approval_requests_touch_updated_at
    BEFORE UPDATE ON financial_approval_requests
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE financial_approval_decisions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    approval_request_id uuid NOT NULL,
    decision text NOT NULL,
    decided_by_actor_id uuid NOT NULL,
    note text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT financial_approval_decisions_request_fk FOREIGN KEY (workspace_id, approval_request_id)
        REFERENCES financial_approval_requests(workspace_id, id) ON DELETE CASCADE,
    CONSTRAINT financial_approval_decisions_actor_fk FOREIGN KEY (workspace_id, decided_by_actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id),
    CONSTRAINT financial_approval_decisions_decision_check CHECK (decision IN ('APPROVED', 'REJECTED')),
    CONSTRAINT financial_approval_decisions_one_terminal_unique UNIQUE (workspace_id, approval_request_id)
);

CREATE INDEX financial_approval_decisions_request_idx
    ON financial_approval_decisions (workspace_id, approval_request_id, created_at);

CREATE TABLE financial_approval_execution_state (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    approval_request_id uuid NOT NULL,
    execution_status text NOT NULL DEFAULT 'PENDING',
    executed_by_actor_id uuid,
    executed_at timestamptz,
    failure_reason text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT financial_approval_execution_state_request_fk FOREIGN KEY (workspace_id, approval_request_id)
        REFERENCES financial_approval_requests(workspace_id, id) ON DELETE CASCADE,
    CONSTRAINT financial_approval_execution_state_actor_fk FOREIGN KEY (workspace_id, executed_by_actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id),
    CONSTRAINT financial_approval_execution_state_status_check CHECK (execution_status IN ('PENDING', 'BLOCKED', 'EXECUTED', 'FAILED')),
    CONSTRAINT financial_approval_execution_state_unique UNIQUE (workspace_id, approval_request_id)
);

CREATE INDEX financial_approval_execution_state_status_idx
    ON financial_approval_execution_state (workspace_id, execution_status, updated_at DESC);

CREATE TRIGGER financial_approval_execution_state_touch_updated_at
    BEFORE UPDATE ON financial_approval_execution_state
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();
