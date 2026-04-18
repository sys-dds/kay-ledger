CREATE TABLE operator_workflows (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    workflow_type text NOT NULL,
    business_reference_type text NOT NULL,
    business_reference_id uuid NOT NULL,
    workflow_id text NOT NULL,
    run_id text,
    trigger_mode text NOT NULL DEFAULT 'API',
    status text NOT NULL DEFAULT 'REQUESTED',
    progress_current integer NOT NULL DEFAULT 0,
    progress_total integer NOT NULL DEFAULT 0,
    progress_message text,
    progress_update_count integer NOT NULL DEFAULT 0,
    requested_by_actor_id uuid,
    requested_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    completed_at timestamptz,
    failure_reason text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT operator_workflows_actor_fk FOREIGN KEY (workspace_id, requested_by_actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id),
    CONSTRAINT operator_workflows_type_check CHECK (workflow_type IN ('EXPORT', 'RECONCILIATION', 'INVESTIGATION_REINDEX')),
    CONSTRAINT operator_workflows_reference_check CHECK (business_reference_type IN ('EXPORT_JOB', 'RECONCILIATION_RUN', 'INVESTIGATION_REINDEX_JOB')),
    CONSTRAINT operator_workflows_trigger_check CHECK (trigger_mode IN ('API', 'MANUAL_RECOVERY', 'RETRY')),
    CONSTRAINT operator_workflows_status_check CHECK (status IN ('REQUESTED', 'RUNNING', 'SUCCEEDED', 'FAILED')),
    CONSTRAINT operator_workflows_progress_check CHECK (progress_current >= 0 AND progress_total >= 0),
    CONSTRAINT operator_workflows_progress_update_count_check CHECK (progress_update_count >= 0),
    CONSTRAINT operator_workflows_requested_started_check CHECK (
        (status = 'REQUESTED' AND started_at IS NULL AND completed_at IS NULL)
        OR (status = 'RUNNING' AND started_at IS NOT NULL AND completed_at IS NULL)
        OR (status = 'SUCCEEDED' AND started_at IS NOT NULL AND completed_at IS NOT NULL)
        OR (status = 'FAILED' AND completed_at IS NOT NULL)
    ),
    CONSTRAINT operator_workflows_workflow_id_unique UNIQUE (workflow_id)
);

CREATE INDEX operator_workflows_workspace_status_idx
    ON operator_workflows (workspace_id, workflow_type, status, requested_at DESC);

CREATE INDEX operator_workflows_reference_idx
    ON operator_workflows (workspace_id, business_reference_type, business_reference_id);

CREATE TRIGGER operator_workflows_touch_updated_at
    BEFORE UPDATE ON operator_workflows
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

ALTER TABLE export_jobs
    ADD COLUMN generation_mode text NOT NULL DEFAULT 'SYNCHRONOUS',
    ADD COLUMN temporal_workflow_id text,
    ADD COLUMN temporal_run_id text,
    ADD COLUMN trigger_mode text NOT NULL DEFAULT 'API',
    ADD COLUMN orchestration_started_at timestamptz,
    ADD COLUMN orchestration_completed_at timestamptz,
    ADD CONSTRAINT export_jobs_generation_mode_check CHECK (generation_mode IN ('SYNCHRONOUS', 'TEMPORAL_ASYNC')),
    ADD CONSTRAINT export_jobs_trigger_mode_check CHECK (trigger_mode IN ('API', 'MANUAL_RECOVERY', 'RETRY')),
    ADD CONSTRAINT export_jobs_orchestration_timestamp_check CHECK (
        generation_mode <> 'TEMPORAL_ASYNC'
        OR (
            (status = 'REQUESTED' AND orchestration_started_at IS NULL AND orchestration_completed_at IS NULL)
            OR (status = 'RUNNING' AND orchestration_started_at IS NOT NULL AND orchestration_completed_at IS NULL)
            OR (status = 'SUCCEEDED' AND orchestration_started_at IS NOT NULL AND orchestration_completed_at IS NOT NULL)
            OR (status = 'FAILED' AND orchestration_completed_at IS NOT NULL)
        )
    ),
    ADD CONSTRAINT export_jobs_temporal_workflow_unique UNIQUE (temporal_workflow_id);

ALTER TABLE reconciliation_runs
    DROP CONSTRAINT reconciliation_runs_status_check;

ALTER TABLE reconciliation_runs
    ALTER COLUMN started_at DROP DEFAULT,
    ALTER COLUMN started_at DROP NOT NULL,
    ADD COLUMN temporal_workflow_id text,
    ADD COLUMN temporal_run_id text,
    ADD COLUMN trigger_mode text NOT NULL DEFAULT 'API',
    ADD COLUMN failure_reason text,
    ADD COLUMN mismatch_count integer NOT NULL DEFAULT 0,
    ADD COLUMN requested_at timestamptz NOT NULL DEFAULT now(),
    ADD CONSTRAINT reconciliation_runs_status_check CHECK (status IN ('REQUESTED', 'RUNNING', 'COMPLETED', 'FAILED')),
    ADD CONSTRAINT reconciliation_runs_trigger_mode_check CHECK (trigger_mode IN ('API', 'MANUAL_RECOVERY', 'RETRY')),
    ADD CONSTRAINT reconciliation_runs_mismatch_count_check CHECK (mismatch_count >= 0),
    ADD CONSTRAINT reconciliation_runs_requested_started_check CHECK (
        (status = 'REQUESTED' AND started_at IS NULL AND completed_at IS NULL)
        OR (status = 'RUNNING' AND started_at IS NOT NULL AND completed_at IS NULL)
        OR (status = 'COMPLETED' AND started_at IS NOT NULL AND completed_at IS NOT NULL)
        OR (status = 'FAILED' AND completed_at IS NOT NULL)
    ),
    ADD CONSTRAINT reconciliation_runs_temporal_workflow_unique UNIQUE (temporal_workflow_id);

UPDATE reconciliation_runs
SET requested_at = created_at;

CREATE TABLE investigation_reindex_jobs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    status text NOT NULL DEFAULT 'REQUESTED',
    trigger_mode text NOT NULL DEFAULT 'API',
    temporal_workflow_id text,
    temporal_run_id text,
    requested_by_actor_id uuid,
    indexed_count integer NOT NULL DEFAULT 0,
    failed_count integer NOT NULL DEFAULT 0,
    failure_reason text,
    requested_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT investigation_reindex_jobs_actor_fk FOREIGN KEY (workspace_id, requested_by_actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id),
    CONSTRAINT investigation_reindex_jobs_status_check CHECK (status IN ('REQUESTED', 'RUNNING', 'SUCCEEDED', 'FAILED')),
    CONSTRAINT investigation_reindex_jobs_trigger_mode_check CHECK (trigger_mode IN ('API', 'MANUAL_RECOVERY', 'RETRY')),
    CONSTRAINT investigation_reindex_jobs_counts_check CHECK (indexed_count >= 0 AND failed_count >= 0),
    CONSTRAINT investigation_reindex_jobs_requested_started_check CHECK (
        (status = 'REQUESTED' AND started_at IS NULL AND completed_at IS NULL)
        OR (status = 'RUNNING' AND started_at IS NOT NULL AND completed_at IS NULL)
        OR (status = 'SUCCEEDED' AND started_at IS NOT NULL AND completed_at IS NOT NULL)
        OR (status = 'FAILED' AND completed_at IS NOT NULL)
    ),
    CONSTRAINT investigation_reindex_jobs_temporal_workflow_unique UNIQUE (temporal_workflow_id)
);

CREATE INDEX investigation_reindex_jobs_workspace_status_idx
    ON investigation_reindex_jobs (workspace_id, status, requested_at DESC);

CREATE TRIGGER investigation_reindex_jobs_touch_updated_at
    BEFORE UPDATE ON investigation_reindex_jobs
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();
