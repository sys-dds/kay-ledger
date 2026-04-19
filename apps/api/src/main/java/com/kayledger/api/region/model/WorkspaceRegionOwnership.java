package com.kayledger.api.region.model;

import java.time.Instant;
import java.util.UUID;

public record WorkspaceRegionOwnership(
        UUID workspaceId,
        String homeRegion,
        long ownershipEpoch,
        String status,
        String transferState,
        Instant updatedAt,
        Instant createdAt) {
}
