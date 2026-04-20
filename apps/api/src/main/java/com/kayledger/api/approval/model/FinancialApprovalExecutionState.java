package com.kayledger.api.approval.model;

import java.time.Instant;
import java.util.UUID;

public record FinancialApprovalExecutionState(
        UUID id,
        UUID workspaceId,
        UUID approvalRequestId,
        String executionStatus,
        UUID executedByActorId,
        Instant executedAt,
        String failureReason,
        Instant createdAt,
        Instant updatedAt) {
}
