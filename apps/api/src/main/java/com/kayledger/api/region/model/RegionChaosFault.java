package com.kayledger.api.region.model;

import java.time.Instant;
import java.util.UUID;

public record RegionChaosFault(
        UUID id,
        UUID workspaceId,
        String faultType,
        String scope,
        String status,
        String parametersJson,
        String reason,
        UUID createdByActorId,
        Instant createdAt,
        Instant clearedAt) {
}
