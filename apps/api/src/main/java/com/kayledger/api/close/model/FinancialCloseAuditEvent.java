package com.kayledger.api.close.model;

import java.time.Instant;
import java.util.UUID;

public record FinancialCloseAuditEvent(
        UUID id,
        UUID workspaceId,
        UUID accountingPeriodId,
        UUID finalizedStatementId,
        String eventType,
        UUID actorId,
        String reason,
        Instant createdAt) {
}
