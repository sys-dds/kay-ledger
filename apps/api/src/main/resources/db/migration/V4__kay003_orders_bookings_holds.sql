CREATE EXTENSION IF NOT EXISTS btree_gist;

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
        'CATALOG_PUBLISH',
        'BOOKING_CREATE',
        'BOOKING_READ',
        'BOOKING_MANAGE'
    ));

INSERT INTO workspace_membership_scopes (membership_id, scope)
SELECT wm.id, booking_scope.scope
FROM workspace_memberships wm
CROSS JOIN LATERAL (
    SELECT unnest(
        CASE
            WHEN wm.role IN ('OWNER', 'ADMIN') THEN ARRAY[
                'BOOKING_CREATE',
                'BOOKING_READ',
                'BOOKING_MANAGE'
            ]
            WHEN wm.role = 'PROVIDER' THEN ARRAY[
                'BOOKING_READ'
            ]
            WHEN wm.role = 'CUSTOMER' THEN ARRAY[
                'BOOKING_CREATE',
                'BOOKING_READ'
            ]
            ELSE ARRAY[]::text[]
        END
    ) AS scope
) booking_scope
ON CONFLICT (membership_id, scope) DO NOTHING;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'customer_profiles_workspace_id_unique'
    ) THEN
        ALTER TABLE customer_profiles
            ADD CONSTRAINT customer_profiles_workspace_id_unique UNIQUE (workspace_id, id);
    END IF;
END $$;

CREATE TABLE bookings (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL,
    offering_id uuid NOT NULL,
    provider_profile_id uuid NOT NULL,
    customer_profile_id uuid NOT NULL,
    offer_type text NOT NULL,
    status text NOT NULL DEFAULT 'HELD',
    scheduled_start_at timestamptz,
    scheduled_end_at timestamptz,
    quantity_reserved integer NOT NULL,
    hold_expires_at timestamptz NOT NULL,
    confirmed_at timestamptz,
    cancelled_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT bookings_offer_type_check CHECK (offer_type IN ('SCHEDULED_TIME', 'QUANTITY')),
    CONSTRAINT bookings_status_check CHECK (status IN ('HELD', 'CONFIRMED', 'CANCELLED', 'EXPIRED')),
    CONSTRAINT bookings_quantity_reserved_check CHECK (quantity_reserved > 0),
    CONSTRAINT bookings_confirmation_check CHECK (
        (status = 'CONFIRMED' AND confirmed_at IS NOT NULL)
        OR
        (status <> 'CONFIRMED')
    ),
    CONSTRAINT bookings_cancelled_check CHECK (
        (status = 'CANCELLED' AND cancelled_at IS NOT NULL)
        OR
        (status <> 'CANCELLED')
    ),
    CONSTRAINT bookings_shape_check CHECK (
        (
            offer_type = 'SCHEDULED_TIME'
            AND scheduled_start_at IS NOT NULL
            AND scheduled_end_at IS NOT NULL
            AND scheduled_start_at < scheduled_end_at
            AND quantity_reserved = 1
        )
        OR
        (
            offer_type = 'QUANTITY'
            AND scheduled_start_at IS NULL
            AND scheduled_end_at IS NULL
            AND quantity_reserved > 0
        )
    ),
    CONSTRAINT bookings_workspace_offering_fk FOREIGN KEY (workspace_id, offering_id)
        REFERENCES offerings(workspace_id, id),
    CONSTRAINT bookings_workspace_provider_fk FOREIGN KEY (workspace_id, provider_profile_id)
        REFERENCES provider_profiles(workspace_id, id),
    CONSTRAINT bookings_workspace_customer_fk FOREIGN KEY (workspace_id, customer_profile_id)
        REFERENCES customer_profiles(workspace_id, id)
);

ALTER TABLE bookings
    ADD CONSTRAINT bookings_workspace_id_unique UNIQUE (workspace_id, id);

ALTER TABLE bookings
    ADD CONSTRAINT bookings_no_overlapping_active_scheduled
    EXCLUDE USING gist (
        workspace_id WITH =,
        offering_id WITH =,
        tstzrange(scheduled_start_at, scheduled_end_at, '[)') WITH &&
    )
    WHERE (offer_type = 'SCHEDULED_TIME' AND status IN ('HELD', 'CONFIRMED'));

CREATE INDEX bookings_workspace_status_idx ON bookings (workspace_id, status, created_at);
CREATE INDEX bookings_workspace_provider_idx ON bookings (workspace_id, provider_profile_id, created_at);
CREATE INDEX bookings_workspace_customer_idx ON bookings (workspace_id, customer_profile_id, created_at);
CREATE INDEX bookings_quantity_active_idx
    ON bookings (workspace_id, offering_id, status)
    WHERE offer_type = 'QUANTITY' AND status IN ('HELD', 'CONFIRMED');

CREATE TRIGGER bookings_touch_updated_at
    BEFORE UPDATE ON bookings
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();

CREATE TABLE booking_holds (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL,
    booking_id uuid NOT NULL,
    status text NOT NULL DEFAULT 'HELD',
    expires_at timestamptz NOT NULL,
    release_reason text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT booking_holds_status_check CHECK (status IN ('HELD', 'RELEASED', 'EXPIRED', 'CANCELLED')),
    CONSTRAINT booking_holds_booking_unique UNIQUE (booking_id),
    CONSTRAINT booking_holds_workspace_booking_fk FOREIGN KEY (workspace_id, booking_id)
        REFERENCES bookings(workspace_id, id) ON DELETE CASCADE
);

CREATE INDEX booking_holds_workspace_status_expiry_idx ON booking_holds (workspace_id, status, expires_at);

CREATE TRIGGER booking_holds_touch_updated_at
    BEFORE UPDATE ON booking_holds
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_touch_updated_at();
