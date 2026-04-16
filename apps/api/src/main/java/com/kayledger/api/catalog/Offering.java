package com.kayledger.api.catalog;

import java.time.Instant;
import java.util.UUID;

public record Offering(
        UUID id,
        UUID workspaceId,
        UUID providerProfileId,
        String title,
        String status,
        String offerType,
        String pricingMetadata,
        Integer durationMinutes,
        Integer minNoticeMinutes,
        Integer maxNoticeDays,
        Integer slotIntervalMinutes,
        Integer quantityAvailable,
        String schedulingMetadata,
        Instant createdAt,
        Instant updatedAt) {
}
