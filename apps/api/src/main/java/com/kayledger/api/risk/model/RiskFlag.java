package com.kayledger.api.risk.model;

import java.time.Instant;
import java.util.UUID;

public record RiskFlag(
        UUID id,
        UUID workspaceId,
        String referenceType,
        UUID referenceId,
        String ruleCode,
        String severity,
        String status,
        String reason,
        int signalCount,
        Instant firstSeenAt,
        Instant lastSeenAt,
        Instant createdAt,
        Instant updatedAt) {
}
