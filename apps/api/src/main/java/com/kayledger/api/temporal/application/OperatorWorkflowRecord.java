package com.kayledger.api.temporal.application;

import java.time.Instant;
import java.util.UUID;

public record OperatorWorkflowRecord(
        UUID id,
        UUID workspaceId,
        String workflowType,
        String businessReferenceType,
        UUID businessReferenceId,
        String workflowId,
        String runId,
        String triggerMode,
        String status,
        int progressCurrent,
        int progressTotal,
        String progressMessage,
        UUID requestedByActorId,
        Instant startedAt,
        Instant completedAt,
        String failureReason,
        Instant createdAt,
        Instant updatedAt) {
}
