package com.kayledger.api.region.model;

import java.time.Instant;
import java.util.UUID;

public record WorkspaceRegionFailoverEvent(
        UUID id,
        UUID workspaceId,
        String fromRegion,
        String toRegion,
        long priorEpoch,
        long newEpoch,
        String triggerMode,
        UUID requestedByActorId,
        Instant createdAt) {
}
