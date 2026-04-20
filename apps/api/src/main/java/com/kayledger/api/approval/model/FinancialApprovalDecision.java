package com.kayledger.api.approval.model;

import java.time.Instant;
import java.util.UUID;

public record FinancialApprovalDecision(
        UUID id,
        UUID workspaceId,
        UUID approvalRequestId,
        String decision,
        UUID decidedByActorId,
        String note,
        Instant createdAt) {
}
