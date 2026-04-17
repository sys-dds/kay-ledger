package com.kayledger.api.payment.model;

import java.time.Instant;
import java.util.UUID;

public record FrozenFund(
        UUID id,
        UUID workspaceId,
        UUID providerProfileId,
        UUID disputeId,
        String currencyCode,
        long amountMinor,
        String status,
        Instant createdAt,
        Instant updatedAt) {
}
