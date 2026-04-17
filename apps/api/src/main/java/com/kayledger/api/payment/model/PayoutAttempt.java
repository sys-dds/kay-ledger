package com.kayledger.api.payment.model;

import java.time.Instant;
import java.util.UUID;

public record PayoutAttempt(
        UUID id,
        UUID workspaceId,
        UUID payoutRequestId,
        int attemptNumber,
        String status,
        String failureReason,
        String externalReference,
        UUID journalEntryId,
        Instant createdAt) {
}
