CREATE TABLE regional_drift_records (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    drift_type text NOT NULL,
    source_region text,
    target_region text,
    reference_type text NOT NULL,
    reference_id text NOT NULL,
    status text NOT NULL DEFAULT 'OPEN',
    detail_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    detected_at timestamptz NOT NULL DEFAULT now(),
    resolved_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT regional_drift_records_status_check CHECK (status IN ('OPEN', 'RESOLVED')),
    CONSTRAINT regional_drift_records_type_check CHECK (drift_type IN (
        'OWNERSHIP_MISSING_ON_PEER',
        'FAILOVER_HISTORY_MISSING_ON_PEER',
        'DELAYED_PROVIDER_CALLBACK_BACKLOG',
        'SNAPSHOT_CHECKPOINT_WITHOUT_ROW',
        'SNAPSHOT_ROW_BEHIND_CHECKPOINT',
        'REGIONAL_READ_SNAPSHOT_MISSING'
    )),
    CONSTRAINT regional_drift_records_unique_open UNIQUE (workspace_id, drift_type, reference_type, reference_id, status)
);

CREATE INDEX regional_drift_records_workspace_status_idx
    ON regional_drift_records (workspace_id, status, detected_at DESC);

CREATE TRIGGER regional_drift_records_touch_updated_at
    BEFORE UPDATE ON regional_drift_records
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE regional_recovery_actions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    drift_record_id uuid REFERENCES regional_drift_records(id),
    action_type text NOT NULL,
    reference_type text NOT NULL,
    reference_id text NOT NULL,
    status text NOT NULL DEFAULT 'REQUESTED',
    requested_by_actor_id uuid,
    result_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    failure_reason text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    CONSTRAINT regional_recovery_actions_actor_fk FOREIGN KEY (workspace_id, requested_by_actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id),
    CONSTRAINT regional_recovery_actions_status_check CHECK (status IN ('REQUESTED', 'SUCCEEDED', 'FAILED')),
    CONSTRAINT regional_recovery_actions_type_check CHECK (action_type IN (
        'REPLAY_OWNERSHIP_TRANSFER',
        'REDRIVE_DELAYED_PROVIDER_CALLBACK',
        'REPLAY_INVESTIGATION_SNAPSHOT',
        'REPLAY_PROVIDER_SUMMARY_SNAPSHOT'
    ))
);

CREATE INDEX regional_recovery_actions_workspace_created_idx
    ON regional_recovery_actions (workspace_id, created_at DESC);

CREATE TRIGGER regional_recovery_actions_touch_updated_at
    BEFORE UPDATE ON regional_recovery_actions
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();
