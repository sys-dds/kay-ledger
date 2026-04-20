package com.kayledger.api.evidence.model;

import java.time.Instant;
import java.util.UUID;

public record FinanceEvidencePackItem(
        UUID id,
        UUID workspaceId,
        UUID evidencePackId,
        String sourceReferenceType,
        UUID sourceReferenceId,
        String sourceReferenceLabel,
        String itemSnapshotJson,
        Instant createdAt) {
}
