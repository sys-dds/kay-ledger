package com.kayledger.api.region.model;

import java.time.Instant;
import java.util.UUID;

public record RegionInvestigationSnapshot(
        String documentId,
        String documentType,
        String referenceType,
        String referenceId,
        String status,
        String payloadJson,
        String sourceRegion,
        String targetRegion,
        UUID replicationEventId,
        Instant replicatedAt) {
}
