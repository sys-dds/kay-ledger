package com.kayledger.api.region.recovery.model;

import java.time.Instant;
import java.util.UUID;

public record RegionalDriftRecord(
        UUID id,
        UUID workspaceId,
        String driftType,
        String sourceRegion,
        String targetRegion,
        String referenceType,
        String referenceId,
        String status,
        String detailJson,
        Instant detectedAt,
        Instant resolvedAt,
        Instant createdAt,
        Instant updatedAt) {
}
