package com.kayledger.api.subscription.model;

import java.time.Instant;
import java.util.UUID;

public record SubscriptionCycle(
        UUID id,
        UUID workspaceId,
        UUID subscriptionId,
        int cycleNumber,
        UUID planId,
        UUID providerProfileId,
        UUID customerProfileId,
        Instant cycleStartAt,
        Instant cycleEndAt,
        String status,
        UUID paymentIntentId,
        String externalReference,
        Instant createdAt,
        Instant updatedAt) {
}
