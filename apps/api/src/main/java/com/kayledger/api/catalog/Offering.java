package com.kayledger.api.catalog;

import java.time.Instant;
import java.util.UUID;

public record Offering(
        UUID id,
        UUID workspaceId,
        UUID providerProfileId,
        String title,
        String status,
        String pricingMetadata,
        Integer durationMinutes,
        String schedulingMetadata,
        Instant createdAt,
        Instant updatedAt) {
}
