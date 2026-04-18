package com.kayledger.api.subscription.model;

import java.time.Instant;
import java.util.UUID;

public record SubscriptionRecord(
        UUID id,
        UUID workspaceId,
        UUID customerProfileId,
        UUID currentPlanId,
        UUID providerProfileId,
        String status,
        Instant startAt,
        Instant currentPeriodStartAt,
        Instant currentPeriodEndAt,
        Instant graceExpiresAt,
        Instant cancelledAt,
        Instant cancellationEffectiveAt,
        Instant createdAt,
        Instant updatedAt) {
}
