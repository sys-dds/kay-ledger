package com.kayledger.api.subscription.model;

import java.time.Instant;
import java.util.UUID;

public record EntitlementGrant(
        UUID id,
        UUID workspaceId,
        UUID subscriptionId,
        UUID customerProfileId,
        String status,
        String entitlementCode,
        Instant startsAt,
        Instant endsAt,
        Instant createdAt,
        Instant updatedAt) {
}
