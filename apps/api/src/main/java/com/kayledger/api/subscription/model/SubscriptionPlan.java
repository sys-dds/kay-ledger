package com.kayledger.api.subscription.model;

import java.time.Instant;
import java.util.UUID;

public record SubscriptionPlan(
        UUID id,
        UUID workspaceId,
        UUID providerProfileId,
        String planCode,
        String displayName,
        String billingInterval,
        String currencyCode,
        long amountMinor,
        String status,
        int version,
        Instant createdAt,
        Instant updatedAt) {
}
