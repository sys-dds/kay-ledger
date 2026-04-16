CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE workspaces (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    slug text NOT NULL UNIQUE,
    display_name text NOT NULL,
    status text NOT NULL DEFAULT 'ACTIVE',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT workspaces_slug_format CHECK (slug ~ '^[a-z0-9][a-z0-9-]{2,62}$'),
    CONSTRAINT workspaces_status_check CHECK (status IN ('ACTIVE', 'SUSPENDED'))
);

CREATE TABLE actors (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    actor_key text NOT NULL UNIQUE,
    display_name text NOT NULL,
    status text NOT NULL DEFAULT 'ACTIVE',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT actors_key_format CHECK (actor_key ~ '^[a-zA-Z0-9][a-zA-Z0-9._:-]{2,127}$'),
    CONSTRAINT actors_status_check CHECK (status IN ('ACTIVE', 'DISABLED'))
);

CREATE TABLE workspace_memberships (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    actor_id uuid NOT NULL REFERENCES actors(id) ON DELETE CASCADE,
    role text NOT NULL,
    status text NOT NULL DEFAULT 'ACTIVE',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT workspace_memberships_role_check CHECK (role IN ('OWNER', 'ADMIN', 'PROVIDER', 'CUSTOMER')),
    CONSTRAINT workspace_memberships_status_check CHECK (status IN ('ACTIVE', 'DISABLED')),
    CONSTRAINT workspace_memberships_workspace_actor_unique UNIQUE (workspace_id, actor_id)
);

CREATE INDEX workspace_memberships_actor_idx ON workspace_memberships (actor_id);

CREATE TABLE provider_profiles (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    actor_id uuid NOT NULL REFERENCES actors(id) ON DELETE CASCADE,
    display_name text NOT NULL,
    status text NOT NULL DEFAULT 'ACTIVE',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT provider_profiles_status_check CHECK (status IN ('ACTIVE', 'DISABLED')),
    CONSTRAINT provider_profiles_workspace_id_unique UNIQUE (workspace_id, id),
    CONSTRAINT provider_profiles_workspace_actor_unique UNIQUE (workspace_id, actor_id),
    CONSTRAINT provider_profiles_membership_fk FOREIGN KEY (workspace_id, actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id) ON DELETE CASCADE
);

CREATE INDEX provider_profiles_workspace_idx ON provider_profiles (workspace_id);

CREATE TABLE customer_profiles (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    actor_id uuid NOT NULL REFERENCES actors(id) ON DELETE CASCADE,
    display_name text NOT NULL,
    status text NOT NULL DEFAULT 'ACTIVE',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT customer_profiles_status_check CHECK (status IN ('ACTIVE', 'DISABLED')),
    CONSTRAINT customer_profiles_workspace_actor_unique UNIQUE (workspace_id, actor_id),
    CONSTRAINT customer_profiles_membership_fk FOREIGN KEY (workspace_id, actor_id)
        REFERENCES workspace_memberships(workspace_id, actor_id) ON DELETE CASCADE
);

CREATE INDEX customer_profiles_workspace_idx ON customer_profiles (workspace_id);

CREATE TABLE offerings (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    provider_profile_id uuid NOT NULL REFERENCES provider_profiles(id) ON DELETE RESTRICT,
    title text NOT NULL,
    status text NOT NULL DEFAULT 'DRAFT',
    pricing_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    duration_minutes integer,
    scheduling_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT offerings_status_check CHECK (status IN ('DRAFT', 'ACTIVE', 'ARCHIVED')),
    CONSTRAINT offerings_duration_check CHECK (duration_minutes IS NULL OR duration_minutes > 0),
    CONSTRAINT offerings_workspace_provider_fk FOREIGN KEY (workspace_id, provider_profile_id)
        REFERENCES provider_profiles(workspace_id, id) ON DELETE RESTRICT
);

CREATE UNIQUE INDEX offerings_workspace_provider_title_unique
    ON offerings (workspace_id, provider_profile_id, lower(title));

CREATE INDEX offerings_workspace_idx ON offerings (workspace_id);
