package com.kayledger.api.reconciliation.model;

import java.time.Instant;
import java.util.UUID;

public record ReconciliationRun(
        UUID id,
        UUID workspaceId,
        UUID providerConfigId,
        String runType,
        String status,
        Instant startedAt,
        Instant completedAt,
        Instant createdAt,
        Instant updatedAt) {
}
