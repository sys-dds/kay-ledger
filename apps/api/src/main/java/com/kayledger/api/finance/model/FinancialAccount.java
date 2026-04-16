package com.kayledger.api.finance.model;

import java.time.Instant;
import java.util.UUID;

public record FinancialAccount(
        UUID id,
        UUID workspaceId,
        String accountCode,
        String accountName,
        String accountType,
        String accountPurpose,
        String currencyCode,
        String status,
        Instant createdAt,
        Instant updatedAt) {
}
