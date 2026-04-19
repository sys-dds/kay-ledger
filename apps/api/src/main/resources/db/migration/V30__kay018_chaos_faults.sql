CREATE TABLE region_chaos_faults (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid REFERENCES workspaces(id),
    fault_type text NOT NULL,
    scope text NOT NULL,
    status text NOT NULL DEFAULT 'ACTIVE',
    parameters_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    reason text,
    created_by_actor_id uuid,
    created_at timestamptz NOT NULL DEFAULT now(),
    cleared_at timestamptz,
    CONSTRAINT region_chaos_faults_status_check CHECK (status IN ('ACTIVE', 'CLEARED')),
    CONSTRAINT region_chaos_faults_scope_check CHECK (scope IN ('WORKSPACE', 'REGION')),
    CONSTRAINT region_chaos_faults_type_check CHECK (fault_type IN (
        'DELAY_PROVIDER_CALLBACK_APPLY',
        'DROP_PROVIDER_CALLBACK_APPLY',
        'DUPLICATE_PROVIDER_CALLBACK_APPLY',
        'OUT_OF_ORDER_PROVIDER_CALLBACK_APPLY',
        'REGIONAL_REPLICATION_PUBLISH_BLOCK',
        'REGIONAL_REPLICATION_APPLY_BLOCK',
        'REGIONAL_REPLICATION_PUBLISH_DELAY',
        'REGIONAL_REPLICATION_APPLY_DELAY'
    )),
    CONSTRAINT region_chaos_faults_actor_fk FOREIGN KEY (workspace_id, created_by_actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id)
);

CREATE INDEX region_chaos_faults_active_idx
    ON region_chaos_faults (workspace_id, fault_type, status, created_at DESC);

ALTER TABLE provider_callbacks
    DROP CONSTRAINT provider_callbacks_status_check;

ALTER TABLE provider_callbacks
    ADD CONSTRAINT provider_callbacks_status_check CHECK (processing_status IN ('RECEIVED', 'DELAYED_BY_DRILL', 'APPLIED', 'IGNORED_OUT_OF_ORDER', 'FAILED'));
