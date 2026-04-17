package com.kayledger.api.payment.model;

import java.time.Instant;
import java.util.UUID;

public record PayoutRequest(
        UUID id,
        UUID workspaceId,
        UUID providerProfileId,
        String currencyCode,
        long requestedAmountMinor,
        String status,
        String failureReason,
        UUID journalEntryId,
        Instant createdAt,
        Instant updatedAt) {
}
