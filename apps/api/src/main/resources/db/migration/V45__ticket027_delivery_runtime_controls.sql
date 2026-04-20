CREATE TABLE merchant_finance_delivery_runtime_controls (
    workspace_id uuid PRIMARY KEY REFERENCES workspaces(id) ON DELETE CASCADE,
    paused boolean NOT NULL DEFAULT false,
    pause_reason text,
    updated_by_actor_id uuid,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT merchant_finance_delivery_runtime_controls_actor_fk FOREIGN KEY (workspace_id, updated_by_actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id)
);
