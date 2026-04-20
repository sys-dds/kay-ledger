ALTER TABLE regional_recovery_actions
    ADD COLUMN peer_applied_region text,
    ADD COLUMN peer_applied_at timestamptz,
    ADD COLUMN peer_confirmation_event_id uuid;

CREATE TABLE regional_recovery_action_confirmations (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    recovery_action_id uuid NOT NULL,
    action_type text NOT NULL,
    reference_type text NOT NULL,
    reference_id text NOT NULL,
    applied_region text NOT NULL,
    source_region text NOT NULL,
    apply_event_id uuid NOT NULL,
    confirmation_event_id uuid NOT NULL,
    applied_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT regional_recovery_action_confirmations_unique UNIQUE (workspace_id, recovery_action_id, applied_region, apply_event_id)
);

CREATE INDEX regional_recovery_action_confirmations_workspace_action_idx
    ON regional_recovery_action_confirmations (workspace_id, recovery_action_id, created_at DESC);
