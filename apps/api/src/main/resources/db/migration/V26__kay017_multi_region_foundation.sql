CREATE TABLE workspace_region_ownership (
    workspace_id uuid PRIMARY KEY REFERENCES workspaces(id),
    home_region text NOT NULL,
    ownership_epoch bigint NOT NULL DEFAULT 1,
    status text NOT NULL DEFAULT 'ACTIVE',
    transfer_state text NOT NULL DEFAULT 'STABLE',
    updated_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT workspace_region_ownership_epoch_check CHECK (ownership_epoch > 0),
    CONSTRAINT workspace_region_ownership_status_check CHECK (status IN ('ACTIVE', 'TRANSFERRING')),
    CONSTRAINT workspace_region_ownership_transfer_state_check CHECK (transfer_state IN ('STABLE', 'TRANSFER_REQUESTED', 'TRANSFERRED'))
);

CREATE INDEX workspace_region_ownership_home_region_idx
    ON workspace_region_ownership (home_region, status);

CREATE TRIGGER workspace_region_ownership_touch_updated_at
    BEFORE UPDATE ON workspace_region_ownership
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE region_replication_checkpoints (
    source_region text NOT NULL,
    target_region text NOT NULL,
    stream_name text NOT NULL,
    last_applied_event_id uuid,
    last_applied_sequence bigint NOT NULL DEFAULT 0,
    last_applied_at timestamptz,
    lag_millis bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_region, target_region, stream_name),
    CONSTRAINT region_replication_checkpoints_sequence_check CHECK (last_applied_sequence >= 0),
    CONSTRAINT region_replication_checkpoints_lag_check CHECK (lag_millis >= 0),
    CONSTRAINT region_replication_checkpoints_distinct_regions_check CHECK (source_region <> target_region)
);

CREATE TRIGGER region_replication_checkpoints_touch_updated_at
    BEFORE UPDATE ON region_replication_checkpoints
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE workspace_region_failover_events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    from_region text NOT NULL,
    to_region text NOT NULL,
    prior_epoch bigint NOT NULL,
    new_epoch bigint NOT NULL,
    trigger_mode text NOT NULL,
    requested_by_actor_id uuid,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT workspace_region_failover_events_actor_fk FOREIGN KEY (workspace_id, requested_by_actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id),
    CONSTRAINT workspace_region_failover_events_epoch_check CHECK (new_epoch > prior_epoch),
    CONSTRAINT workspace_region_failover_events_regions_check CHECK (from_region <> to_region),
    CONSTRAINT workspace_region_failover_events_trigger_check CHECK (trigger_mode IN ('MANUAL_OPERATOR', 'SIMULATED_REGION_FAILOVER', 'MANUAL_FAILBACK'))
);

CREATE INDEX workspace_region_failover_events_workspace_created_idx
    ON workspace_region_failover_events (workspace_id, created_at DESC);
