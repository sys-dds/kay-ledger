package com.kayledger.api.risk.model;

import java.time.Instant;
import java.util.UUID;

public record RiskDecision(
        UUID id,
        UUID workspaceId,
        UUID riskFlagId,
        UUID reviewId,
        String referenceType,
        UUID referenceId,
        String outcome,
        String reason,
        UUID decidedByActorId,
        Instant decidedAt,
        Instant createdAt) {
}
