CREATE TABLE operator_search_index_state (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    document_type text NOT NULL,
    reference_type text NOT NULL,
    reference_id uuid NOT NULL,
    search_document_id text NOT NULL,
    source_updated_at timestamptz,
    indexed_at timestamptz,
    status text NOT NULL DEFAULT 'PENDING',
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT operator_search_reference_check CHECK (reference_type IN (
        'PAYMENT_INTENT',
        'PAYMENT_ATTEMPT',
        'REFUND',
        'REFUND_ATTEMPT',
        'PAYOUT_REQUEST',
        'PAYOUT_ATTEMPT',
        'DISPUTE',
        'PROVIDER_CALLBACK',
        'RECONCILIATION_MISMATCH',
        'SUBSCRIPTION',
        'SUBSCRIPTION_CYCLE'
    )),
    CONSTRAINT operator_search_status_check CHECK (status IN ('PENDING', 'INDEXED', 'FAILED')),
    CONSTRAINT operator_search_document_unique UNIQUE (workspace_id, document_type, reference_id),
    CONSTRAINT operator_search_document_id_unique UNIQUE (search_document_id)
);

CREATE INDEX operator_search_workspace_status_idx
    ON operator_search_index_state (workspace_id, status, updated_at);

CREATE INDEX operator_search_reference_idx
    ON operator_search_index_state (workspace_id, reference_type, reference_id);

CREATE TRIGGER operator_search_index_state_touch_updated_at
    BEFORE UPDATE ON operator_search_index_state
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();
