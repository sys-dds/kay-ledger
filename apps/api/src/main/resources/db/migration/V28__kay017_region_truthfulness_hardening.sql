ALTER TABLE region_replication_checkpoints
    ADD COLUMN workspace_id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000';

ALTER TABLE region_replication_checkpoints
    DROP CONSTRAINT region_replication_checkpoints_pkey;

ALTER TABLE region_replication_checkpoints
    ADD PRIMARY KEY (source_region, target_region, stream_name, workspace_id);

ALTER TABLE region_investigation_read_snapshots
    ADD COLUMN replication_sequence bigint NOT NULL DEFAULT 0,
    ADD CONSTRAINT region_investigation_read_snapshots_sequence_check CHECK (replication_sequence >= 0);

ALTER TABLE region_provider_summary_snapshots
    ADD COLUMN replication_sequence bigint NOT NULL DEFAULT 0,
    ADD CONSTRAINT region_provider_summary_snapshots_sequence_check CHECK (replication_sequence >= 0);
