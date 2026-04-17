package com.kayledger.api.payment.model;

import java.time.Instant;
import java.util.UUID;

public record RefundRecord(
        UUID id,
        UUID workspaceId,
        UUID paymentIntentId,
        UUID bookingId,
        String refundType,
        long amountMinor,
        long payableReductionAmountMinor,
        String status,
        UUID journalEntryId,
        Instant createdAt,
        Instant updatedAt) {
}
