package com.kayledger.api.reporting.model;

import java.time.Instant;
import java.util.UUID;

public record ExportJob(
        UUID id,
        UUID workspaceId,
        String exportType,
        String status,
        UUID requestedByActorId,
        String parametersJson,
        int rowCount,
        String storageKey,
        String contentType,
        String failureReason,
        Instant requestedAt,
        Instant completedAt,
        Instant createdAt,
        Instant updatedAt) {
}
