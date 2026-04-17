package com.kayledger.api.payment.model;

import java.time.Instant;
import java.util.UUID;

public record ProviderPayableBalance(
        UUID id,
        UUID workspaceId,
        UUID providerProfileId,
        String currencyCode,
        long payableAmountMinor,
        String source,
        Instant createdAt,
        Instant updatedAt) {
}
