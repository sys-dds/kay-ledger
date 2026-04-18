package com.kayledger.api.reconciliation.model;

import java.time.Instant;
import java.util.UUID;

public record ReconciliationMismatch(
        UUID id,
        UUID workspaceId,
        UUID reconciliationRunId,
        UUID providerCallbackId,
        String businessReferenceType,
        UUID businessReferenceId,
        String driftCategory,
        String internalState,
        String providerState,
        String suggestedAction,
        String repairStatus,
        String repairNote,
        Instant repairedAt,
        Instant createdAt,
        Instant updatedAt) {
}
