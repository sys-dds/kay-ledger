package com.kayledger.api.payment.model;

import java.time.Instant;
import java.util.UUID;

public record PaymentAttempt(
        UUID id,
        UUID workspaceId,
        UUID paymentIntentId,
        String attemptType,
        String status,
        long amountMinor,
        String externalReference,
        UUID journalEntryId,
        Instant createdAt) {
}
