package com.kayledger.api.approval.model;

import java.time.Instant;
import java.util.UUID;

public record FinancialApprovalRequest(
        UUID id,
        UUID workspaceId,
        String actionType,
        String targetType,
        UUID targetId,
        UUID providerProfileId,
        String currencyCode,
        Long amountMinor,
        String status,
        UUID requestedByActorId,
        String reason,
        String requestPayloadJson,
        Instant createdAt,
        Instant updatedAt) {
}
