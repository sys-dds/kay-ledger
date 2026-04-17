package com.kayledger.api.payment.model;

import java.time.Instant;
import java.util.UUID;

public record DisputeRecord(
        UUID id,
        UUID workspaceId,
        UUID paymentIntentId,
        UUID bookingId,
        long disputedAmountMinor,
        long frozenAmountMinor,
        String status,
        String resolution,
        UUID openJournalEntryId,
        UUID resolveJournalEntryId,
        Instant createdAt,
        Instant updatedAt) {
}
