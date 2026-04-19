package com.kayledger.api.region.recovery.model;

import java.time.Instant;
import java.util.UUID;

public record RegionalRecoveryAction(
        UUID id,
        UUID workspaceId,
        UUID driftRecordId,
        String actionType,
        String referenceType,
        String referenceId,
        String status,
        UUID requestedByActorId,
        String resultJson,
        String failureReason,
        Instant createdAt,
        Instant updatedAt,
        Instant completedAt) {
}
