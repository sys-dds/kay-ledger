CREATE TABLE actor_platform_roles (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    actor_id uuid NOT NULL REFERENCES actors(id) ON DELETE CASCADE,
    role text NOT NULL,
    status text NOT NULL DEFAULT 'ACTIVE',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT actor_platform_roles_role_check CHECK (role IN ('OPERATOR')),
    CONSTRAINT actor_platform_roles_status_check CHECK (status IN ('ACTIVE', 'DISABLED')),
    CONSTRAINT actor_platform_roles_actor_role_unique UNIQUE (actor_id, role)
);

CREATE INDEX actor_platform_roles_actor_idx ON actor_platform_roles (actor_id);

CREATE TABLE workspace_membership_scopes (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    membership_id uuid NOT NULL REFERENCES workspace_memberships(id) ON DELETE CASCADE,
    scope text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT workspace_membership_scopes_scope_check CHECK (scope IN (
        'WORKSPACE_READ',
        'ACTOR_READ',
        'MEMBERSHIP_MANAGE',
        'PROFILE_READ',
        'PROFILE_MANAGE',
        'ACCESS_CONTEXT_READ'
    )),
    CONSTRAINT workspace_membership_scopes_membership_scope_unique UNIQUE (membership_id, scope)
);

CREATE INDEX workspace_membership_scopes_membership_idx ON workspace_membership_scopes (membership_id);

INSERT INTO workspace_membership_scopes (membership_id, scope)
SELECT wm.id, default_scope.scope
FROM workspace_memberships wm
CROSS JOIN LATERAL (
    SELECT unnest(
        CASE
            WHEN wm.role IN ('OWNER', 'ADMIN') THEN ARRAY[
                'WORKSPACE_READ',
                'ACTOR_READ',
                'MEMBERSHIP_MANAGE',
                'PROFILE_READ',
                'PROFILE_MANAGE',
                'ACCESS_CONTEXT_READ'
            ]
            WHEN wm.role IN ('PROVIDER', 'CUSTOMER') THEN ARRAY[
                'WORKSPACE_READ',
                'PROFILE_READ',
                'PROFILE_MANAGE',
                'ACCESS_CONTEXT_READ'
            ]
            ELSE ARRAY[]::text[]
        END
    ) AS scope
) default_scope
ON CONFLICT (membership_id, scope) DO NOTHING;

CREATE OR REPLACE FUNCTION kay_ledger_touch_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER workspaces_touch_updated_at
    BEFORE UPDATE ON workspaces
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TRIGGER actors_touch_updated_at
    BEFORE UPDATE ON actors
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TRIGGER workspace_memberships_touch_updated_at
    BEFORE UPDATE ON workspace_memberships
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TRIGGER provider_profiles_touch_updated_at
    BEFORE UPDATE ON provider_profiles
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TRIGGER customer_profiles_touch_updated_at
    BEFORE UPDATE ON customer_profiles
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TRIGGER offerings_touch_updated_at
    BEFORE UPDATE ON offerings
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TRIGGER actor_platform_roles_touch_updated_at
    BEFORE UPDATE ON actor_platform_roles
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TRIGGER workspace_membership_scopes_touch_updated_at
    BEFORE UPDATE ON workspace_membership_scopes
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE OR REPLACE FUNCTION kay_ledger_enforce_provider_profile_role()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM workspace_memberships wm
        WHERE wm.workspace_id = NEW.workspace_id
          AND wm.actor_id = NEW.actor_id
          AND wm.status = 'ACTIVE'
          AND wm.role IN ('OWNER', 'ADMIN', 'PROVIDER')
    ) THEN
        RAISE EXCEPTION 'provider profile actor must have provider-capable workspace role'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION kay_ledger_enforce_customer_profile_role()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM workspace_memberships wm
        WHERE wm.workspace_id = NEW.workspace_id
          AND wm.actor_id = NEW.actor_id
          AND wm.status = 'ACTIVE'
          AND wm.role IN ('OWNER', 'ADMIN', 'CUSTOMER')
    ) THEN
        RAISE EXCEPTION 'customer profile actor must have customer-capable workspace role'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER provider_profiles_enforce_role
    BEFORE INSERT OR UPDATE OF workspace_id, actor_id ON provider_profiles
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_enforce_provider_profile_role();

CREATE TRIGGER customer_profiles_enforce_role
    BEFORE INSERT OR UPDATE OF workspace_id, actor_id ON customer_profiles
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_enforce_customer_profile_role();
