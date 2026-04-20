package com.kayledger.api.reconciliation.model;

import java.time.Instant;
import java.util.UUID;

public record ReconciliationItemAuditEvent(
        UUID id,
        UUID workspaceId,
        UUID reconciliationItemId,
        UUID reconciliationRunId,
        String eventType,
        UUID actorId,
        String resolutionOutcome,
        String note,
        Instant createdAt) {
}
