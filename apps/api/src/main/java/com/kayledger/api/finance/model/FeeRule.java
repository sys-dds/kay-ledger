package com.kayledger.api.finance.model;

import java.time.Instant;
import java.util.UUID;

public record FeeRule(
        UUID id,
        UUID workspaceId,
        UUID offeringId,
        String ruleType,
        Long flatAmountMinor,
        Integer basisPoints,
        String currencyCode,
        String status,
        Instant createdAt,
        Instant updatedAt) {
}
