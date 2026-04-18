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
        'BOOKING_MANAGE',
        'FINANCE_READ',
        'FINANCE_WRITE',
        'PAYMENT_READ',
        'PAYMENT_WRITE',
        'SUBSCRIPTION_READ',
        'SUBSCRIPTION_WRITE',
        'SUBSCRIPTION_RENEW'
    ));

INSERT INTO workspace_membership_scopes (membership_id, scope)
SELECT wm.id, subscription_scope.scope
FROM workspace_memberships wm
CROSS JOIN LATERAL (
    SELECT unnest(
        CASE
            WHEN wm.role IN ('OWNER', 'ADMIN') THEN ARRAY[
                'SUBSCRIPTION_READ',
                'SUBSCRIPTION_WRITE',
                'SUBSCRIPTION_RENEW'
            ]
            ELSE ARRAY[]::text[]
        END
    ) AS scope
) subscription_scope
ON CONFLICT (membership_id, scope) DO NOTHING;
