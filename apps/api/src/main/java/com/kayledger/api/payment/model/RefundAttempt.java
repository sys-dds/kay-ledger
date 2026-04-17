package com.kayledger.api.payment.model;

import java.time.Instant;
import java.util.UUID;

public record RefundAttempt(
        UUID id,
        UUID workspaceId,
        UUID refundId,
        String status,
        String failureReason,
        String externalReference,
        Instant createdAt) {
}
