ALTER TABLE region_chaos_faults
    ADD CONSTRAINT region_chaos_faults_workspace_required_check CHECK (workspace_id IS NOT NULL);

CREATE UNIQUE INDEX workspace_region_failover_events_replay_unique
    ON workspace_region_failover_events (
        workspace_id,
        from_region,
        to_region,
        prior_epoch,
        new_epoch,
        trigger_mode
    );
