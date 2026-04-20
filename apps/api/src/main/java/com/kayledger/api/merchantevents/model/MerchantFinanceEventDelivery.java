package com.kayledger.api.merchantevents.model;

import java.time.Instant;
import java.util.UUID;

public record MerchantFinanceEventDelivery(
        UUID id,
        UUID workspaceId,
        UUID merchantFinanceEventId,
        UUID endpointId,
        String deliveryStatus,
        int attemptCount,
        Instant firstAttemptAt,
        Instant lastAttemptAt,
        Instant nextAttemptAt,
        Integer responseStatus,
        String responseBody,
        String finalFailureReason,
        String parkedReason,
        String claimOwner,
        Instant claimedAt,
        Instant claimExpiresAt,
        String signatureAlgorithm,
        String signatureValue,
        String dedupeKey,
        Instant createdAt,
        Instant updatedAt) {
}
