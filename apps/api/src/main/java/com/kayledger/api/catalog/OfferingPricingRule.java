package com.kayledger.api.catalog;

import java.time.Instant;
import java.util.UUID;

public record OfferingPricingRule(
        UUID id,
        UUID workspaceId,
        UUID offeringId,
        String ruleType,
        String currencyCode,
        long amountMinor,
        String unitName,
        int sortOrder,
        String status,
        Instant createdAt,
        Instant updatedAt) {
}
