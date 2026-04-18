package com.kayledger.api.temporal.model;

import java.time.Instant;
import java.util.UUID;

public record OperatorWorkflowStatus(
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
        int progressUpdateCount,
        Instant requestedAt,
        Instant startedAt,
        Instant completedAt,
        String failureReason) {
}
