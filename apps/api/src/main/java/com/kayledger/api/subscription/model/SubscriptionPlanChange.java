package com.kayledger.api.subscription.model;

import java.time.Instant;
import java.util.UUID;

public record SubscriptionPlanChange(
        UUID id,
        UUID workspaceId,
        UUID subscriptionId,
        UUID targetPlanId,
        Integer effectiveCycleNumber,
        Instant effectiveAt,
        String status,
        Instant createdAt,
        Instant updatedAt) {
}
