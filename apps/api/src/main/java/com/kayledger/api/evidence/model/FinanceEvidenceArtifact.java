package com.kayledger.api.evidence.model;

import java.time.Instant;
import java.util.UUID;

public record FinanceEvidenceArtifact(
        UUID id,
        UUID workspaceId,
        UUID evidencePackId,
        String artifactFormat,
        String artifactBody,
        long artifactSizeBytes,
        String checksumAlgorithm,
        String checksumValue,
        UUID generatedByActorId,
        Instant createdAt) {
}
