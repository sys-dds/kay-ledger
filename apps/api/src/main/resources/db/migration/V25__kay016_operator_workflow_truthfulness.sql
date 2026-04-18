ALTER TABLE operator_workflows
    ADD COLUMN IF NOT EXISTS requested_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS progress_update_count integer NOT NULL DEFAULT 0;

ALTER TABLE operator_workflows
    ALTER COLUMN started_at DROP DEFAULT,
    ALTER COLUMN started_at DROP NOT NULL;

UPDATE operator_workflows
SET requested_at = COALESCE(requested_at, created_at);

UPDATE operator_workflows
SET started_at = NULL
WHERE status = 'REQUESTED';

UPDATE operator_workflows
SET started_at = COALESCE(started_at, requested_at, created_at)
WHERE status IN ('RUNNING', 'SUCCEEDED');

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'operator_workflows_requested_started_check'
    ) THEN
        ALTER TABLE operator_workflows
            ADD CONSTRAINT operator_workflows_requested_started_check CHECK (
                (status = 'REQUESTED' AND started_at IS NULL AND completed_at IS NULL)
                OR (status = 'RUNNING' AND started_at IS NOT NULL AND completed_at IS NULL)
                OR (status = 'SUCCEEDED' AND started_at IS NOT NULL AND completed_at IS NOT NULL)
                OR (status = 'FAILED' AND completed_at IS NOT NULL)
            );
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'operator_workflows_progress_update_count_check'
    ) THEN
        ALTER TABLE operator_workflows
            ADD CONSTRAINT operator_workflows_progress_update_count_check CHECK (progress_update_count >= 0);
    END IF;
END $$;

DROP INDEX IF EXISTS operator_workflows_workspace_status_idx;

CREATE INDEX operator_workflows_workspace_status_idx
    ON operator_workflows (workspace_id, workflow_type, status, requested_at DESC);

UPDATE export_jobs
SET orchestration_started_at = NULL
WHERE status = 'REQUESTED';

UPDATE export_jobs
SET orchestration_started_at = COALESCE(orchestration_started_at, requested_at, created_at)
WHERE status IN ('RUNNING', 'SUCCEEDED');

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'export_jobs_orchestration_timestamp_check'
    ) THEN
        ALTER TABLE export_jobs
            ADD CONSTRAINT export_jobs_orchestration_timestamp_check CHECK (
                generation_mode <> 'TEMPORAL_ASYNC'
                OR (
                    (status = 'REQUESTED' AND orchestration_started_at IS NULL AND orchestration_completed_at IS NULL)
                    OR (status = 'RUNNING' AND orchestration_started_at IS NOT NULL AND orchestration_completed_at IS NULL)
                    OR (status = 'SUCCEEDED' AND orchestration_started_at IS NOT NULL AND orchestration_completed_at IS NOT NULL)
                    OR (status = 'FAILED' AND orchestration_completed_at IS NOT NULL)
                )
            );
    END IF;
END $$;

ALTER TABLE reconciliation_runs
    ADD COLUMN IF NOT EXISTS requested_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE reconciliation_runs
    ALTER COLUMN started_at DROP DEFAULT,
    ALTER COLUMN started_at DROP NOT NULL;

UPDATE reconciliation_runs
SET requested_at = COALESCE(requested_at, created_at);

UPDATE reconciliation_runs
SET started_at = NULL
WHERE status = 'REQUESTED';

UPDATE reconciliation_runs
SET started_at = COALESCE(started_at, requested_at, created_at)
WHERE status IN ('RUNNING', 'COMPLETED');

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'reconciliation_runs_requested_started_check'
    ) THEN
        ALTER TABLE reconciliation_runs
            ADD CONSTRAINT reconciliation_runs_requested_started_check CHECK (
                (status = 'REQUESTED' AND started_at IS NULL AND completed_at IS NULL)
                OR (status = 'RUNNING' AND started_at IS NOT NULL AND completed_at IS NULL)
                OR (status = 'COMPLETED' AND started_at IS NOT NULL AND completed_at IS NOT NULL)
                OR (status = 'FAILED' AND completed_at IS NOT NULL)
            );
    END IF;
END $$;

ALTER TABLE investigation_reindex_jobs
    ADD COLUMN IF NOT EXISTS requested_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE investigation_reindex_jobs
    ALTER COLUMN started_at DROP DEFAULT,
    ALTER COLUMN started_at DROP NOT NULL;

UPDATE investigation_reindex_jobs
SET requested_at = COALESCE(requested_at, created_at);

UPDATE investigation_reindex_jobs
SET started_at = NULL
WHERE status = 'REQUESTED';

UPDATE investigation_reindex_jobs
SET started_at = COALESCE(started_at, requested_at, created_at)
WHERE status IN ('RUNNING', 'SUCCEEDED');

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'investigation_reindex_jobs_requested_started_check'
    ) THEN
        ALTER TABLE investigation_reindex_jobs
            ADD CONSTRAINT investigation_reindex_jobs_requested_started_check CHECK (
                (status = 'REQUESTED' AND started_at IS NULL AND completed_at IS NULL)
                OR (status = 'RUNNING' AND started_at IS NOT NULL AND completed_at IS NULL)
                OR (status = 'SUCCEEDED' AND started_at IS NOT NULL AND completed_at IS NOT NULL)
                OR (status = 'FAILED' AND completed_at IS NOT NULL)
            );
    END IF;
END $$;

DROP INDEX IF EXISTS investigation_reindex_jobs_workspace_status_idx;

CREATE INDEX investigation_reindex_jobs_workspace_status_idx
    ON investigation_reindex_jobs (workspace_id, status, requested_at DESC);
