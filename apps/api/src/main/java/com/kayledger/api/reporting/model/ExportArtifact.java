package com.kayledger.api.reporting.model;

import java.time.Instant;
import java.util.UUID;

public record ExportArtifact(
        UUID id,
        UUID workspaceId,
        UUID exportJobId,
        String storageKey,
        String contentType,
        long byteSize,
        int rowCount,
        String checksumSha256,
        Instant createdAt,
        Instant updatedAt) {
}
