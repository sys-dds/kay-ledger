package com.kayledger.api.risk.model;

import java.time.Instant;
import java.util.UUID;

public record RiskReview(
        UUID id,
        UUID workspaceId,
        UUID riskFlagId,
        String status,
        UUID assignedActorId,
        Instant openedAt,
        Instant closedAt,
        Instant createdAt,
        Instant updatedAt) {
}
