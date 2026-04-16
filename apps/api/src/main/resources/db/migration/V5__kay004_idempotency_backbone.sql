CREATE TABLE idempotency_records (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    scope_kind text NOT NULL,
    workspace_id uuid,
    actor_id uuid,
    route_key text NOT NULL,
    idempotency_key text NOT NULL,
    request_hash text NOT NULL,
    status text NOT NULL DEFAULT 'IN_PROGRESS',
    response_status_code integer,
    response_body jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT idempotency_records_scope_kind_check CHECK (scope_kind IN ('GLOBAL', 'WORKSPACE', 'OPERATOR')),
    CONSTRAINT idempotency_records_status_check CHECK (status IN ('IN_PROGRESS', 'COMPLETED', 'FAILED')),
    CONSTRAINT idempotency_records_response_status_check CHECK (response_status_code IS NULL OR response_status_code BETWEEN 100 AND 599),
    CONSTRAINT idempotency_records_completed_response_check CHECK (
        (status = 'COMPLETED' AND response_status_code IS NOT NULL AND response_body IS NOT NULL)
        OR
        (status <> 'COMPLETED')
    ),
    CONSTRAINT idempotency_records_workspace_fk FOREIGN KEY (workspace_id)
        REFERENCES workspaces(id),
    CONSTRAINT idempotency_records_actor_fk FOREIGN KEY (actor_id)
        REFERENCES actors(id)
);

CREATE UNIQUE INDEX idempotency_records_effective_identity_unique
    ON idempotency_records (
        scope_kind,
        COALESCE(workspace_id, '00000000-0000-0000-0000-000000000000'::uuid),
        COALESCE(actor_id, '00000000-0000-0000-0000-000000000000'::uuid),
        route_key,
        idempotency_key
    );

CREATE INDEX idempotency_records_created_idx ON idempotency_records (created_at);

CREATE TRIGGER idempotency_records_touch_updated_at
    BEFORE UPDATE ON idempotency_records
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();
