ALTER TABLE workspace_membership_scopes
    DROP CONSTRAINT workspace_membership_scopes_scope_check;

ALTER TABLE workspace_membership_scopes
    ADD CONSTRAINT workspace_membership_scopes_scope_check CHECK (scope IN (
        'WORKSPACE_READ',
        'ACTOR_READ',
        'MEMBERSHIP_MANAGE',
        'PROFILE_READ',
        'PROFILE_MANAGE',
        'ACCESS_CONTEXT_READ',
        'CATALOG_READ',
        'CATALOG_WRITE',
        'CATALOG_PUBLISH'
    ));

INSERT INTO workspace_membership_scopes (membership_id, scope)
SELECT wm.id, catalog_scope.scope
FROM workspace_memberships wm
CROSS JOIN LATERAL (
    SELECT unnest(
        CASE
            WHEN wm.role IN ('OWNER', 'ADMIN') THEN ARRAY[
                'CATALOG_READ',
                'CATALOG_WRITE',
                'CATALOG_PUBLISH'
            ]
            WHEN wm.role = 'PROVIDER' THEN ARRAY[
                'CATALOG_READ',
                'CATALOG_WRITE',
                'CATALOG_PUBLISH'
            ]
            WHEN wm.role = 'CUSTOMER' THEN ARRAY[
                'CATALOG_READ'
            ]
            ELSE ARRAY[]::text[]
        END
    ) AS scope
) catalog_scope
ON CONFLICT (membership_id, scope) DO NOTHING;

DROP INDEX offerings_workspace_provider_title_unique;

ALTER TABLE offerings
    DROP CONSTRAINT offerings_status_check;

UPDATE offerings
SET status = 'PUBLISHED'
WHERE status = 'ACTIVE';

ALTER TABLE offerings
    ADD CONSTRAINT offerings_status_check CHECK (status IN ('DRAFT', 'PUBLISHED', 'ARCHIVED'));

ALTER TABLE offerings
    ADD COLUMN offer_type text NOT NULL DEFAULT 'SCHEDULED_TIME',
    ADD COLUMN min_notice_minutes integer NOT NULL DEFAULT 0,
    ADD COLUMN max_notice_days integer,
    ADD COLUMN slot_interval_minutes integer,
    ADD COLUMN quantity_available integer;

UPDATE offerings
SET duration_minutes = COALESCE(duration_minutes, 60),
    slot_interval_minutes = COALESCE(slot_interval_minutes, 60)
WHERE offer_type = 'SCHEDULED_TIME';

ALTER TABLE offerings
    ADD CONSTRAINT offerings_offer_type_check CHECK (offer_type IN ('SCHEDULED_TIME', 'QUANTITY')),
    ADD CONSTRAINT offerings_min_notice_check CHECK (min_notice_minutes >= 0),
    ADD CONSTRAINT offerings_max_notice_check CHECK (max_notice_days IS NULL OR max_notice_days > 0),
    ADD CONSTRAINT offerings_slot_interval_check CHECK (slot_interval_minutes IS NULL OR slot_interval_minutes > 0),
    ADD CONSTRAINT offerings_quantity_available_check CHECK (quantity_available IS NULL OR quantity_available > 0),
    ADD CONSTRAINT offerings_offer_type_shape_check CHECK (
        (offer_type = 'SCHEDULED_TIME' AND duration_minutes IS NOT NULL AND slot_interval_minutes IS NOT NULL AND quantity_available IS NULL)
        OR
        (offer_type = 'QUANTITY' AND quantity_available IS NOT NULL)
    ),
    ADD CONSTRAINT offerings_workspace_id_unique UNIQUE (workspace_id, id);

CREATE UNIQUE INDEX offerings_workspace_provider_title_unique
    ON offerings (workspace_id, provider_profile_id, lower(title))
    WHERE status <> 'ARCHIVED';

CREATE TABLE offering_pricing_rules (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL,
    offering_id uuid NOT NULL,
    rule_type text NOT NULL,
    currency_code char(3) NOT NULL,
    amount_minor bigint NOT NULL,
    unit_name text,
    sort_order integer NOT NULL DEFAULT 0,
    status text NOT NULL DEFAULT 'ACTIVE',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT offering_pricing_rules_rule_type_check CHECK (rule_type IN ('FIXED_PRICE', 'PER_UNIT')),
    CONSTRAINT offering_pricing_rules_amount_check CHECK (amount_minor >= 0),
    CONSTRAINT offering_pricing_rules_status_check CHECK (status IN ('ACTIVE', 'DISABLED')),
    CONSTRAINT offering_pricing_rules_workspace_offering_fk FOREIGN KEY (workspace_id, offering_id)
        REFERENCES offerings(workspace_id, id) ON DELETE CASCADE,
    CONSTRAINT offering_pricing_rules_unique_order UNIQUE (workspace_id, offering_id, sort_order)
);

CREATE INDEX offering_pricing_rules_offering_idx ON offering_pricing_rules (workspace_id, offering_id);

CREATE TRIGGER offering_pricing_rules_touch_updated_at
    BEFORE UPDATE ON offering_pricing_rules
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE offering_availability_windows (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL,
    offering_id uuid NOT NULL,
    weekday smallint NOT NULL,
    start_local_time time NOT NULL,
    end_local_time time NOT NULL,
    effective_from date,
    effective_to date,
    status text NOT NULL DEFAULT 'ACTIVE',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT offering_availability_windows_weekday_check CHECK (weekday BETWEEN 1 AND 7),
    CONSTRAINT offering_availability_windows_time_check CHECK (start_local_time < end_local_time),
    CONSTRAINT offering_availability_windows_effective_check CHECK (effective_to IS NULL OR effective_from IS NULL OR effective_from <= effective_to),
    CONSTRAINT offering_availability_windows_status_check CHECK (status IN ('ACTIVE', 'DISABLED')),
    CONSTRAINT offering_availability_windows_workspace_offering_fk FOREIGN KEY (workspace_id, offering_id)
        REFERENCES offerings(workspace_id, id) ON DELETE CASCADE,
    CONSTRAINT offering_availability_windows_unique_window UNIQUE (
        workspace_id,
        offering_id,
        weekday,
        start_local_time,
        end_local_time,
        effective_from
    )
);

CREATE INDEX offering_availability_windows_offering_idx ON offering_availability_windows (workspace_id, offering_id);

CREATE TRIGGER offering_availability_windows_touch_updated_at
    BEFORE UPDATE ON offering_availability_windows
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();
