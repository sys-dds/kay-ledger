package com.kayledger.api.reconciliation.model;

import java.time.Instant;
import java.util.UUID;

public record ReconciliationRun(
        UUID id,
        UUID workspaceId,
        UUID providerConfigId,
        String runType,
        String status,
        String temporalWorkflowId,
        String temporalRunId,
        String triggerMode,
        String failureReason,
        int mismatchCount,
        Instant requestedAt,
        Instant startedAt,
        Instant completedAt,
        Instant createdAt,
        Instant updatedAt) {
}
