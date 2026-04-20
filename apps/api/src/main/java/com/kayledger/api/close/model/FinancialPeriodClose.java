package com.kayledger.api.close.model;

import java.time.Instant;
import java.util.UUID;

public record FinancialPeriodClose(
        UUID id,
        UUID workspaceId,
        UUID accountingPeriodId,
        String status,
        String closeReason,
        String reopenedReason,
        UUID closedByActorId,
        UUID reopenedByActorId,
        Instant closedAt,
        Instant reopenedAt,
        Instant createdAt,
        Instant updatedAt) {
}
