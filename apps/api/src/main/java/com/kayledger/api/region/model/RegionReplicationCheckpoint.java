package com.kayledger.api.region.model;

import java.time.Instant;
import java.util.UUID;

public record RegionReplicationCheckpoint(
        String sourceRegion,
        String targetRegion,
        String streamName,
        java.util.UUID workspaceId,
        UUID lastAppliedEventId,
        long lastAppliedSequence,
        Instant lastAppliedAt,
        long lagMillis,
        Instant updatedAt) {
}
