ALTER TABLE provider_reconciliation_truth_imports
    DROP CONSTRAINT provider_reconciliation_truth_imports_unique;

ALTER TABLE provider_reconciliation_truth_imports
    DROP CONSTRAINT provider_reconciliation_truth_imports_status_check;

UPDATE provider_reconciliation_truth_imports
SET status = 'MATCHED'
WHERE status = 'RECONCILED';

ALTER TABLE provider_reconciliation_truth_imports
    ADD COLUMN import_version integer NOT NULL DEFAULT 1,
    ADD COLUMN superseded_by_import_id uuid,
    ADD COLUMN superseded_at timestamptz;

ALTER TABLE provider_reconciliation_truth_imports
    ADD CONSTRAINT provider_reconciliation_truth_imports_superseded_by_fk FOREIGN KEY (workspace_id, superseded_by_import_id)
        REFERENCES provider_reconciliation_truth_imports(workspace_id, id) ON DELETE RESTRICT,
    ADD CONSTRAINT provider_reconciliation_truth_imports_status_check CHECK (status IN ('RECORDED', 'MATCHED', 'MISMATCHED', 'RESOLVED', 'SUPERSEDED')),
    ADD CONSTRAINT provider_reconciliation_truth_imports_superseded_check CHECK (
        (status = 'SUPERSEDED' AND superseded_by_import_id IS NOT NULL AND superseded_at IS NOT NULL)
        OR
        (status <> 'SUPERSEDED' AND superseded_by_import_id IS NULL AND superseded_at IS NULL)
    ),
    ADD CONSTRAINT provider_reconciliation_truth_imports_version_unique UNIQUE (
        workspace_id, provider_profile_id, currency_code, statement_period_start, statement_period_end, source_reference, import_version
    );

CREATE INDEX provider_reconciliation_truth_imports_active_idx
    ON provider_reconciliation_truth_imports (workspace_id, provider_profile_id, currency_code, statement_period_start, statement_period_end, source_reference)
    WHERE status <> 'SUPERSEDED';

CREATE TABLE provider_reconciliation_truth_import_events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    truth_import_id uuid NOT NULL,
    event_type text NOT NULL,
    actor_id uuid,
    note text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT provider_reconciliation_truth_import_events_import_fk FOREIGN KEY (workspace_id, truth_import_id)
        REFERENCES provider_reconciliation_truth_imports(workspace_id, id) ON DELETE CASCADE,
    CONSTRAINT provider_reconciliation_truth_import_events_actor_fk FOREIGN KEY (workspace_id, actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id),
    CONSTRAINT provider_reconciliation_truth_import_events_type_check CHECK (event_type IN ('RECORDED', 'SUPERSEDED', 'MATCHED', 'MISMATCHED', 'RESOLVED'))
);

CREATE INDEX provider_reconciliation_truth_import_events_import_idx
    ON provider_reconciliation_truth_import_events (workspace_id, truth_import_id, created_at, id);

ALTER TABLE provider_reconciliation_items
    ADD CONSTRAINT provider_reconciliation_items_workspace_id_unique UNIQUE (workspace_id, id);

CREATE TABLE provider_reconciliation_item_events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    reconciliation_item_id uuid NOT NULL,
    reconciliation_run_id uuid NOT NULL,
    event_type text NOT NULL,
    actor_id uuid,
    resolution_outcome text,
    note text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT provider_reconciliation_item_events_item_fk FOREIGN KEY (workspace_id, reconciliation_item_id)
        REFERENCES provider_reconciliation_items(workspace_id, id) ON DELETE CASCADE,
    CONSTRAINT provider_reconciliation_item_events_run_fk FOREIGN KEY (workspace_id, reconciliation_run_id)
        REFERENCES provider_reconciliation_runs(workspace_id, id) ON DELETE CASCADE,
    CONSTRAINT provider_reconciliation_item_events_actor_fk FOREIGN KEY (workspace_id, actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id),
    CONSTRAINT provider_reconciliation_item_events_type_check CHECK (event_type IN ('RESOLVED', 'REOPENED'))
);

CREATE INDEX provider_reconciliation_item_events_item_idx
    ON provider_reconciliation_item_events (workspace_id, reconciliation_item_id, created_at, id);

CREATE INDEX provider_reconciliation_item_events_run_idx
    ON provider_reconciliation_item_events (workspace_id, reconciliation_run_id, created_at, id);
