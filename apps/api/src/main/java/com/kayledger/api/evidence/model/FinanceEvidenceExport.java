package com.kayledger.api.evidence.model;

import java.time.Instant;
import java.util.UUID;

public record FinanceEvidenceExport(
        UUID id,
        UUID workspaceId,
        UUID evidencePackId,
        String exportStatus,
        String artifactFormat,
        String artifactReference,
        long artifactSizeBytes,
        String checksumAlgorithm,
        String checksumValue,
        UUID generatedByActorId,
        Instant generatedAt,
        Instant createdAt) {
}
